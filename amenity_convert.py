#!/usr/bin/env python
"""
Convert amenity_fetch.py output JSON into a compact GeoJSON FeatureCollection.

Keeps only:
- coordinates (lat, lon + geometry)
- name
- amenity / PT type tags
- accessibility features

Usage:
    python json_to_geojson_minimal.py input.json output.geojson
"""

import sys
import json
from pathlib import Path

import geopandas as gpd
from shapely.geometry import Point

# Main descriptive tags we keep
TYPE_TAG_KEYS = [
    "amenity",
    "public_transport",
    "highway",
    "railway",
    "name",
]

# Accessibility-related tag keys
ACCESS_TAG_KEYS = [
    "wheelchair",
    "toilets:wheelchair",
    "wheelchair:description",
    "wheelchair_toilet",
    "step_free",
    "ramp",
    "ramp:wheelchair",
    "accessibility",
]


def json_to_geojson(input_path: str, output_path: str) -> None:
    in_file = Path(input_path)
    out_file = Path(output_path)

    data = json.loads(in_file.read_text())
    print(f"Loaded {len(data)} objects from {in_file}")

    records = []
    skipped_no_geom = 0

    for obj in data:
        lat = obj.get("lat")
        lon = obj.get("lon")

        if lat is None or lon is None:
            skipped_no_geom += 1
            continue

        tags = obj.get("tags", {}) or {}
        accessibility = obj.get("accessibility", {}) or {}

        props = {}

        # Core OSM fields
        props["osm_id"] = obj.get("osm_id")
        props["osm_type"] = obj.get("osm_type")

        # Explicit coordinates (in addition to geometry)
        props["lat"] = lat
        props["lon"] = lon

        # Type / name tags
        for k in TYPE_TAG_KEYS:
            if k in tags:
                props[k] = tags[k]

        # Accessibility tags: from accessibility dict first, then from tags
        for k in ACCESS_TAG_KEYS:
            if k in accessibility:
                props[f"acc_{k}"] = accessibility[k]
            elif k in tags:
                props[f"acc_{k}"] = tags[k]

        # Geometry
        props["geometry"] = Point(lon, lat)

        records.append(props)

    print(f"Records with geometry: {len(data) - skipped_no_geom}")
    print(f"Final records to write: {len(records)}")

    if not records:
        raise RuntimeError("No records to write – something is off.")

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")

    print(f"Writing GeoJSON to {out_file} …")
    gdf.to_file(out_file, driver="GeoJSON")
    print(f"✅ Wrote {len(gdf)} features to {out_file.resolve()}")



def main():
    if len(sys.argv) != 3:
        print("Usage: python json_to_geojson_minimal.py input.json output.geojson")
        sys.exit(1)

    input_json = sys.argv[1]
    output_geojson = sys.argv[2]
    json_to_geojson(input_json, output_geojson)
    
    out_path = Path(output_geojson)
    try:
        gdf = gpd.read_file(out_path)
    except Exception as exc:
        print(f"Could not read written GeoJSON for subsets: {exc}")
        exit(1)

    key = "acc_wheelchair"
    if key not in gdf.columns:
        print(f"No '{key}' property in features; skipping subset creation.")
        exit(1)

    vals = gdf[key].astype(str).str.lower()

    yes_vals = {"yes", "true", "1", "designated", "limited"}
    # Treat explicit null/None/NaN/empty as NO
    no_vals = {"no", "false", "0", "null", "none", "nan", "unknown", ""}

    yes_mask = vals.isin(yes_vals)
    no_mask = vals.isin(no_vals)

    yes_gdf = gdf[yes_mask]
    no_gdf = gdf[no_mask]

    if not yes_gdf.empty:
        yes_file = out_path.with_name(out_path.stem + "_acc_yes" + out_path.suffix)
        yes_gdf.to_file(yes_file, driver="GeoJSON")
        print(f"Wrote {len(yes_gdf)} wheelchair=YES features to {yes_file}")
    else:
        print("No wheelchair=YES features to write.")

    if not no_gdf.empty:
        no_file = out_path.with_name(out_path.stem + "_acc_no" + out_path.suffix)
        no_gdf.to_file(no_file, driver="GeoJSON")
        print(f"Wrote {len(no_gdf)} wheelchair=NO features to {no_file}")
    else:
        print("No wheelchair=NO features to write.")


if __name__ == "__main__":
    main()

