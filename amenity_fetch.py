#!/usr/bin/env python
"""
amenity_fetch.py – Retrieve OSM POIs (amenities + public-transport stops + medical / care)
for a city, store them as tidy JSON, and respect Overpass API rate limits.

Features:
* Uses Nominatim with the full city string (e.g. "Karlsruhe,Germany") to get the
  correct OSM object and Overpass area ID.
* Falls back to a small Nominatim-based bounding box if area resolution fails.
* After fetching, filters all elements to the city Nominatim bbox to drop outliers
  (e.g. remote KIT campus centroid, other Karlsruhes).
* Queries:
    - general amenities (food, civic, education, etc.),
    - medical locations (doctors, dentists, physio, hospitals, healthcare=*),
    - shop=medical_supply etc.,
    - senior / disabled care facilities,
    - public transport stops/platforms/stations.
* Extracts accessibility / wheelchair info into a dedicated field.
* Prints the Overpass query, per-request HTTP status, retries, fallback use, and
  a final request summary.
"""

import sys
import json
import time
from pathlib import Path
from typing import Dict, Any, List, Tuple

import requests
from tqdm import tqdm

# ----------------------------------------------------------------------
# --------------------------- CONFIGURATION -----------------------------
# ----------------------------------------------------------------------
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# ---- amenity values (general + medical + seniors) --------------------
AMENITY_VALUES = [
    # food / leisure
    "restaurant", "cafe", "fast_food", "bar",

    # public / civic
    "government", "townhall", "courthouse", "office", "library",

    # education
    "school", "university","kindergarten", "childcare", "preschool",

    # money / post / fuel / parking / taxi
    "bank", "atm", "post_office", "fuel", "parking", "taxi",

    # medical (general)
    "clinic", "hospital", "pharmacy",

    # medical (practitioners & therapy)
    "doctors",          # amenity=doctors
    "dentist",
    "physiotherapist",

    # seniors / disabled / care related
    "nursing_home",
    "social_facility",
    "retirement_home",
    "assisted_living",
    "group_home",
]

# ---- healthcare=* values (not all mapped as amenity=...) -------------
HEALTHCARE_VALUES = [
    "doctor",
    "dentist",
    "physiotherapist",
    "physiotherapy",
    "rehabilitation",
    "psychotherapist",
    "psychology",
    "speech_therapist",
    "occupational_therapy",
    "hearing_aids",
    "optometrist",
    "orthoptist",
    "podiatrist",
    "counselling",
    "sample_collection",
]

# ---- shops related to medical / mobility aids ------------------------
MEDICAL_SHOP_VALUES = [
    "medical_supply",
    "mobility_scooter",
    "orthopaedics",
]

# ---- social_facility:for=* (seniors / disabled / mental health) ------
SOCIAL_FOR_VALUES = [
    "senior",
    "elderly",
    "retirement",
    "assisted_living",
    "disabled",
    "handicapped",
    "mental_health",
]

# ---- public transport stop/station types -----------------------------
TRANSPORT_TAGS: Dict[str, List[str]] = {
    # Generic public_transport scheme
    "public_transport": [
        "stop_position",
        "platform",
        "station",
        "stop_area",
        "stop_area_group",
        "stop",
    ],
    # Bus
    "highway": [
        "bus_stop",
        "bus_station",
    ],
    "amenity": [
        "bus_station",
        "ferry_terminal",
    ],
    # Rail / tram / metro
    "railway": [
        "station",
        "halt",
        "stop",
        "tram_stop",
        "subway_entrance",
        "platform",
    ],
}

# ---- rate-limit handling ----
INTER_REQUEST_DELAY = 2          # seconds after a successful request
MAX_RETRIES = 5                  # retries on 429/503/504/502
BASE_BACKOFF = 30                # start back-off seconds (exponential)

# ----------------------------------------------------------------------
# --------------------------- STATS / LOGGING --------------------------
# ----------------------------------------------------------------------
class RequestStats:
    total_requests = 0
    total_success = 0
    total_retries = 0
    last_backoff = 0

    @classmethod
    def log_request(cls, label: str, attempt: int, code: int, retry: bool, backoff: int = 0):
        """
        Print a compact per-request line:
          [area] req=1 attempt=1 status=200 (success)
          [area] req=2 attempt=2 status=504 retry=1 backoff=30s
        """
        prefix = f"[{label or 'overpass'}]"
        if not retry:
            print(f"{prefix} req={cls.total_requests} attempt={attempt} status={code} (success)")
        else:
            print(
                f"{prefix} req={cls.total_requests} attempt={attempt} "
                f"status={code} retry={cls.total_retries} backoff={backoff}s"
            )

# ----------------------------------------------------------------------
# --------------------------- Nominatim helpers ------------------------
# ----------------------------------------------------------------------
def _nominatim_search(city_name: str) -> dict:
    """
    Query Nominatim once for the city_name and return the first result JSON object.
    Uses the full string, e.g. "Karlsruhe,Germany" – so the country is respected.
    """
    url = "https://nominatim.openstreetmap.org/search"
    params = {
        "q": city_name,
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }

    r = requests.get(
        url,
        params=params,
        timeout=10,
        headers={"User-Agent": "LumoOSMFetcher/1.0"}
    )
    r.raise_for_status()
    j = r.json()
    if not j:
        raise RuntimeError(f"Nominatim returned no result for: {city_name}")
    return j[0]


def _area_id_from_nominatim(city_name: str) -> int:
    """
    Get the Nominatim result and convert osm_type/osm_id into an Overpass area ID.
    This respects the full city name including country.
    """
    res = _nominatim_search(city_name)
    osm_type = res["osm_type"]
    osm_id = int(res["osm_id"])

    if osm_type == "relation":
        return 3600000000 + osm_id
    elif osm_type == "way":
        return 2400000000 + osm_id
    elif osm_type == "node":
        # Cities should not normally be nodes at this scale; fallback to bbox.
        raise RuntimeError("City resolved to a node; use bbox instead.")
    else:
        raise RuntimeError(f"Unsupported Nominatim osm_type: {osm_type}")


def _nominatim_bbox(city_name: str) -> Tuple[float, float, float, float]:
    """
    Returns the bounding box (south, west, north, east) for the city using Nominatim.
    """
    res = _nominatim_search(city_name)
    # Nominatim boundingbox order: [south, north, west, east]
    south = float(res["boundingbox"][0])
    north = float(res["boundingbox"][1])
    west  = float(res["boundingbox"][2])
    east  = float(res["boundingbox"][3])
    return south, west, north, east

# ----------------------------------------------------------------------
# --------------------------- QUERY BUILDING ---------------------------
# ----------------------------------------------------------------------
def _build_regex(values: List[str]) -> str:
    """Build a ^(..|..)$ regex for a list of tag values."""
    escaped: List[str] = [
        v.replace("\\", "\\\\").replace('"', '\\"') for v in values
    ]
    if len(escaped) == 1:
        return f"^{escaped[0]}$"
    return "^(" + "|".join(escaped) + ")$"


def _amenity_regex() -> str:
    """Regex for amenity values."""
    return _build_regex(AMENITY_VALUES)


def _city_area_clause(city_name: str) -> str:
    """
    Build an Overpass clause that defines `.searchArea` for *city_name*.

    Prefer using a precise Overpass area ID derived via Nominatim, so that
    the country part of the city string ("Karlsruhe,Germany") is respected.
    If area resolution fails, fall back to a small bbox clause.
    """
    try:
        area_id = _area_id_from_nominatim(city_name)
        return f"area({area_id})->.searchArea;"
    except Exception as e:
        print(f"⚠️ Could not resolve area via Nominatim: {e}")
        print("   Falling back to a small bbox around the city.")
        return _fallback_bbox(city_name)


def _fallback_bbox(city_name: str) -> str:
    """
    Obtain a rough centre point from Nominatim and build a tiny bounding box
    (±0.02° ≈ 2 km) around it. Used when area selector is not available.
    """
    res = _nominatim_search(city_name)
    lat = float(res["lat"])
    lon = float(res["lon"])

    delta = 0.02  # ~2 km; increase if you want a larger fallback
    south = lat - delta
    north = lat + delta
    west = lon - delta
    east = lon + delta
    # The resulting line becomes a valid Overpass area definition.
    return f"({south},{west},{north},{east})->.searchArea;"


def build_overpass_query(city_name: str) -> str:
    """
    Assemble the full Overpass QL query (using the city-area selector or bbox).
    """
    area_clause = _city_area_clause(city_name)
    amenity_re = _amenity_regex()
    healthcare_re = _build_regex(HEALTHCARE_VALUES)
    medical_shop_re = _build_regex(MEDICAL_SHOP_VALUES)
    social_for_re = _build_regex(SOCIAL_FOR_VALUES)

    lines: List[str] = [
        "[out:json][timeout:180];",
        area_clause,
        "",
        "// ----- amenities + public transport + medical / care ---------",
        "(",
        # General + existing amenities
        f'  node["amenity"~"{amenity_re}"](area.searchArea);',
        f'  way["amenity"~"{amenity_re}"](area.searchArea);',
        f'  relation["amenity"~"{amenity_re}"](area.searchArea);',
    ]

    # Healthcare=* objects (doctors, dentists, physio, rehab, etc.)
    lines.extend([
        f'  node["healthcare"~"{healthcare_re}"](area.searchArea);',
        f'  way["healthcare"~"{healthcare_re}"](area.searchArea);',
        f'  relation["healthcare"~"{healthcare_re}"](area.searchArea);',
    ])

    # Shops with medical / mobility focus
    lines.extend([
        f'  node["shop"~"{medical_shop_re}"](area.searchArea);',
        f'  way["shop"~"{medical_shop_re}"](area.searchArea);',
        f'  relation["shop"~"{medical_shop_re}"](area.searchArea);',
    ])

    # Facilities explicitly for seniors / disabled via social_facility:for=*
    lines.extend([
        f'  node["amenity"="social_facility"]["social_facility:for"~"{social_for_re}"](area.searchArea);',
        f'  way["amenity"="social_facility"]["social_facility:for"~"{social_for_re}"](area.searchArea);',
        f'  relation["amenity"="social_facility"]["social_facility:for"~"{social_for_re}"](area.searchArea);',
    ])

    # Public transport objects, grouped by key, OR-ed by value
    for key in sorted(TRANSPORT_TAGS.keys()):
        pattern = _build_regex(TRANSPORT_TAGS[key])
        lines.append(f'  node["{key}"~"{pattern}"](area.searchArea);')
        lines.append(f'  way["{key}"~"{pattern}"](area.searchArea);')
        lines.append(f'  relation["{key}"~"{pattern}"](area.searchArea);')

    lines.extend(
        [
            ");",
            "",
            "// ----- output ------------------------------------------------",
            "out center meta;",
        ]
    )

    return "\n".join(lines)

# ----------------------------------------------------------------------
# -------------------------- REQUEST HANDLING -------------------------
# ----------------------------------------------------------------------
def fetch_overpass(query: str, label: str = "") -> dict:
    """
    Send *query* to the public Overpass API.
    Handles 429/503/504/502 with exponential back-off and respects the
    INTER_REQUEST_DELAY after a successful call.

    Prints detailed per-request status:
    - which phase (label) – e.g. 'area' or 'bbox'
    - request number
    - attempt number
    - HTTP status
    - retries and backoff when applicable
    """
    attempt = 0
    backoff = BASE_BACKOFF

    while True:
        attempt += 1
        RequestStats.total_requests += 1

        try:
            resp = requests.post(
                OVERPASS_URL,
                data={"data": query},
                timeout=300,
                headers={"User-Agent": "LumoOSMFetcher/1.0 (+https://lumo.proton.me)"}
            )
        except requests.RequestException as exc:
            raise RuntimeError(f"Network error contacting Overpass: {exc}") from exc

        code = resp.status_code

        # Success
        if code == 200:
            RequestStats.total_success += 1
            RequestStats.log_request(label, attempt, code, retry=False)
            time.sleep(INTER_REQUEST_DELAY)   # polite pause
            return resp.json()

        # Temporary overloads: retry with backoff
        if code in (429, 503, 504, 502):
            RequestStats.total_retries += 1
            RequestStats.last_backoff = backoff

            if attempt > MAX_RETRIES:
                RequestStats.log_request(label, attempt, code, retry=True, backoff=0)
                raise RuntimeError(
                    f"Overpass kept returning {code} after {MAX_RETRIES} retries."
                )

            RequestStats.log_request(label, attempt, code, retry=True, backoff=backoff)
            time.sleep(backoff)
            backoff *= 2
            continue

        # Unexpected error
        raise RuntimeError(
            f"Overpass returned unexpected status {code}: {resp.text[:500]}"
        )

# ----------------------------------------------------------------------
# -------------------------- DATA CLEANUP ----------------------------
# ----------------------------------------------------------------------
def _extract_accessibility(tags: Dict[str, str]) -> Dict[str, Any]:
    """
    Pull known accessibility flags from the tag dict.

    Main one is "wheelchair", but we also surface a few common variants so you
    don't have to inspect raw tags every time.
    """
    acc: Dict[str, Any] = {}

    # core wheelchair field
    if "wheelchair" in tags:
        acc["wheelchair"] = tags["wheelchair"]

    # generic accessibility field (rare but present)
    if "accessibility" in tags:
        acc["accessibility"] = tags["accessibility"]

    # elevators
    if "elevator" in tags:
        acc["elevator"] = tags["elevator"]

    # toilets accessibility
    if "toilets:wheelchair" in tags:
        acc["toilets:wheelchair"] = tags["toilets:wheelchair"]
    if "wheelchair_toilet" in tags:
        acc["wheelchair_toilet"] = tags["wheelchair_toilet"]

    # more detailed wheelchair descriptions / step-free hints
    if "wheelchair:description" in tags:
        acc["wheelchair:description"] = tags["wheelchair:description"]
    if "step_free" in tags:
        acc["step_free"] = tags["step_free"]
    if "ramp" in tags:
        acc["ramp"] = tags["ramp"]
    if "ramp:wheelchair" in tags:
        acc["ramp:wheelchair"] = tags["ramp:wheelchair"]

    return acc


def simplify_element(el: dict) -> Dict[str, Any] | None:
    """Compact a raw Overpass element into a uniform dict."""
    base: Dict[str, Any] = {
        "osm_id": el.get("id"),
        "osm_type": el.get("type"),
        "tags": el.get("tags", {})
    }

    # Geometry handling
    if el.get("type") == "node":
        base["lat"] = el.get("lat")
        base["lon"] = el.get("lon")
    else:
        centre = el.get("center", {})
        base["lat"] = centre.get("lat")
        base["lon"] = centre.get("lon")

    # If we still don't have coordinates, skip
    if base["lat"] is None or base["lon"] is None:
        return None

    base["accessibility"] = _extract_accessibility(base["tags"])
    return base

# ----------------------------------------------------------------------
# ------------------------------- MAIN --------------------------------
# ----------------------------------------------------------------------
def _run_with_fallback(query: str, city: str, label: str) -> dict:
    """
    Run Overpass query. First try city area; if zero elements or persistent
    timeout-like errors, replace the area with a small bbox around the city.
    """
    print(f"▶ {label}: running area query…")
    try:
        raw = fetch_overpass(query, label=f"{label}-area")
        elements = raw.get("elements", [])
        print(f"✔ {label}: area query returned {len(elements)} elements")
    except RuntimeError as exc:
        msg = str(exc)
        if any(code in msg for code in ("504", "503", "429", "502")):
            print(f"⚠️  {label}: area query failed after retries – will try fallback bounding box.")
            elements = []
            raw = {"elements": []}
        else:
            raise

    if not elements:
        print(f"⛑ {label}: no usable results with area selector – switching to bbox fallback.")
        bbox_clause = _fallback_bbox(city)

        # Replace the line that defines .searchArea with the bbox line.
        lines = query.splitlines()
        new_lines = []
        replaced = False
        for line in lines:
            if ".searchArea;" in line and not replaced:
                new_lines.append(bbox_clause)
                replaced = True
            else:
                new_lines.append(line)
        query_with_bbox = "\n".join(new_lines)

        print(f"\n=== Fallback query ({label}) (truncated) ===")
        print(query_with_bbox[:200] + ("…" if len(query_with_bbox) > 200 else ""))
        print("=== end of fallback query ===\n")

        raw = fetch_overpass(query_with_bbox, label=f"{label}-bbox")
        elements = raw.get("elements", [])
        print(f"✔ {label}: bbox query returned {len(elements)} elements")

    return raw


def main(city: str, out_path: str):
    print(f"🔎 Building Overpass query for '{city}' …")
    query = build_overpass_query(city)

    # ---- show the (truncated) query for debugging --------------------
    print("\n=== Overpass query (truncated) ===")
    print(query[:200] + ("…" if len(query) > 200 else ""))
    print("=== end of query ===\n")

    # Single combined query (amenities + public transport + medical / care)
    raw = _run_with_fallback(query, city, label="amenities+public_transport+medical")
    elements = raw.get("elements", [])

    # ---- bbox filter using the city bounding box ---------------------
    print("📐 Fetching city bounding box from Nominatim for outlier filtering…")
    south, west, north, east = _nominatim_bbox(city)
    print(f"   City bbox: S={south}, W={west}, N={north}, E={east}")

    filtered: List[dict] = []
    outliers = 0

    for el in elements:
        lat = el.get("lat")
        lon = el.get("lon")

        # for ways/relations, lat/lon might be in "center"
        if lat is None or lon is None:
            centre = el.get("center", {})
            lat = centre.get("lat")
            lon = centre.get("lon")

        if lat is None or lon is None:
            # can't locate – drop silently
            continue

        if south <= lat <= north and west <= lon <= east:
            filtered.append(el)
        else:
            outliers += 1

    print(f"📉 Filtered outliers outside city bbox: {outliers}")
    print(f"📦 Remaining features inside bbox: {len(filtered)}")

    print(f"📦 Received {len(filtered)} usable raw elements – simplifying…")
    cleaned: List[dict] = []
    for el in tqdm(filtered):
        s = simplify_element(el)
        if s is not None:
            cleaned.append(s)

    out_file = Path(out_path)
    out_file.write_text(
        json.dumps(cleaned, ensure_ascii=False, indent=2)
    )
    print(f"✅ Done – {len(cleaned)} records written to {out_file.resolve()}")

    # Final stats
    print("\n=== Request summary ===")
    print(f"Total requests:   {RequestStats.total_requests}")
    print(f"Successful:       {RequestStats.total_success}")
    print(f"Retries:          {RequestStats.total_retries}")
    print(f"Last backoff:     {RequestStats.last_backoff}s")
    print("=== end ===")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print('Usage: python amenity_fetch.py "<city name,Country>" <output.json>')
        sys.exit(1)

    city_name = sys.argv[1]
    output_file = sys.argv[2]
    main(city_name, output_file)

