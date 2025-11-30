#!/usr/bin/env python3
"""
Link OSM points (lat/lon) to CityJSON buildings (EPSG:25832)
and write one pair of JSON files per OSM feature:

  3d/<osm_id>.json
      - epsg: 25832
      - geometry: GeoJSON Point in EPSG:25832
      - properties: all OSM properties (including lat/lon, name, amenity, ...)

  3d/<osm_id>_bld.json
      - osm_id
      - cityjson_tile
      - cityjson_building_id
      - distance_to_building_m
      - cityjson:  <-- SELF-CONTAINED CityJSON doc for that building
            {
              "type": "CityJSON",
              "version": "1.0.1",
              "metadata": {...},
              "transform": {...} (if present),
              "vertices": [ ...only used vertices... ],
              "CityObjects": {
                "<bldg_id>": {
                  "type": "Building",
                  "attributes": {...},
                  "geometry": [ ... with remapped indices ... ]
                }
              }
            }

Requirements:
    pip install geopandas shapely pandas
"""

import json
import copy
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import box, Polygon
from shapely.ops import unary_union

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------

OSM_GEOJSON = "ka.geojson"     # your input OSM GeoJSON file
CITYJSON_DIR = "/mnt_ai/data/ka3d/tp/CityJSON"        # directory with gebaeude_lod2_*.json
OUTPUT_DIR = Path("3d")                # output directory
MAX_BUILDING_DISTANCE = 25             # meters (max distance for nearest match)


# ----------------------------------------------------------------------
# TILE INDEX
# ----------------------------------------------------------------------

def build_tile_index(cityjson_dir: Path) -> gpd.GeoDataFrame:
    """
    Scan all CityJSON files in cityjson_dir, read metadata.geographicalExtent,
    and build a GeoDataFrame with one bounding box per file (EPSG:25832).
    """
    rows = []

    for path in cityjson_dir.glob("gebaeude_lod2_*.json"):
        with path.open(encoding="utf-8") as f:
            data = json.load(f)

        meta = data.get("metadata", {})
        extent = meta.get("geographicalExtent")
        if not extent or len(extent) != 6:
            continue

        xmin, ymin, zmin, xmax, ymax, zmax = extent

        rows.append({
            "filename": path.name,
            "path": str(path),
            "xmin": xmin,
            "ymin": ymin,
            "xmax": xmax,
            "ymax": ymax,
            "geometry": box(xmin, ymin, xmax, ymax),
        })

    if not rows:
        raise RuntimeError(f"No CityJSON tiles found in {cityjson_dir}")

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:25832")


# ----------------------------------------------------------------------
# CITYJSON → BUILDING FOOTPRINTS (FOR MATCHING)
# ----------------------------------------------------------------------

def load_buildings_from_cityjson(cj: dict) -> gpd.GeoDataFrame:
    """
    From a CityJSON dict, extract:

    - a 2D footprint polygon for each Building (union of all GroundSurfaces).

    Returns GeoDataFrame with columns:
        bldg_id   - CityJSON building ID
        geometry  - Shapely footprint polygon (EPSG:25832, for matching only)
    """
    vertices = cj["vertices"]      # [x, y, z] in EPSG:25832
    cityobjs = cj["CityObjects"]

    rows = []

    for cid, obj in cityobjs.items():
        if obj.get("type") != "Building":
            continue

        geoms_raw = obj.get("geometry", [])
        ground_polys = []

        for geom in geoms_raw:
            if geom.get("type") != "MultiSurface":
                continue

            boundaries = geom.get("boundaries", [])
            semantics = geom.get("semantics", {})
            surfaces = semantics.get("surfaces", [])
            values = semantics.get("values", [])

            # indices of GroundSurface in semantics
            ground_indices = {
                i for i, s in enumerate(surfaces)
                if s.get("type") == "GroundSurface"
            }
            if not ground_indices:
                continue

            for surf_idx, rings in enumerate(boundaries):
                # which semantic index used by this surface?
                if values:
                    sem_idx = values[surf_idx]
                    if isinstance(sem_idx, list):
                        sem_idx = sem_idx[0]
                else:
                    sem_idx = None

                if sem_idx not in ground_indices:
                    continue

                for ring in rings:
                    coords = [vertices[i] for i in ring]
                    poly = Polygon([(x, y) for x, y, z in coords])
                    ground_polys.append(poly)

        if not ground_polys:
            continue

        footprint = unary_union(ground_polys)

        rows.append({
            "bldg_id": cid,
            "geometry": footprint,
        })

    if not rows:
        return gpd.GeoDataFrame(
            [], columns=["bldg_id", "geometry"],
            geometry="geometry", crs="EPSG:25832"
        )

    return gpd.GeoDataFrame(rows, geometry="geometry", crs="EPSG:25832")


# ----------------------------------------------------------------------
# BUILD SELF-CONTAINED CITYJSON FOR ONE BUILDING
# ----------------------------------------------------------------------

def make_single_building_cityjson(cj: dict, bldg_id: str) -> dict | None:
    """
    From the full CityJSON dict `cj`, extract a self-contained CityJSON document
    for a single building `bldg_id`:

    - Only that building in CityObjects
    - Only vertices used by that building
    - All top-level keys except CityObjects/vertices copied over (metadata,
      type, version, transform, extensions, appearance, etc.)
    """
    cityobjs = cj.get("CityObjects", {})
    if bldg_id not in cityobjs:
        return None

    full_vertices = cj.get("vertices", [])
    bld_obj = copy.deepcopy(cityobjs[bldg_id])

    # 1) collect all vertex indices used in the building's geometry
    used_indices: set[int] = set()

    def collect_indices(x):
        if isinstance(x, int):
            used_indices.add(x)
        elif isinstance(x, list):
            for e in x:
                collect_indices(e)

    for geom in bld_obj.get("geometry", []):
        collect_indices(geom.get("boundaries", []))

    if not used_indices:
        # building without geometry: still build CityJSON but with empty vertices
        new_vertices = []
        index_map = {}
    else:
        sorted_idx = sorted(used_indices)
        index_map = {old: i for i, old in enumerate(sorted_idx)}
        new_vertices = [full_vertices[i] for i in sorted_idx]

    # 2) remap boundaries to the compact vertex index space
    def remap_boundaries(x):
        if isinstance(x, int):
            return index_map[x]
        elif isinstance(x, list):
            return [remap_boundaries(e) for e in x]
        else:
            return x

    for geom in bld_obj.get("geometry", []):
        geom["boundaries"] = remap_boundaries(geom.get("boundaries", []))

    # 3) build new CityJSON dict with all relevant top-level keys
    single_cj = {k: v for k, v in cj.items() if k not in ("CityObjects", "vertices")}
    single_cj["vertices"] = new_vertices
    single_cj["CityObjects"] = {bldg_id: bld_obj}

    return single_cj


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------

def main():
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    # 1) Read OSM GeoJSON (WGS84) and transform to EPSG:25832
    print(f"Reading OSM GeoJSON: {OSM_GEOJSON}")
    gdf_osm = gpd.read_file(OSM_GEOJSON)

    if gdf_osm.crs is None:
        gdf_osm.set_crs("EPSG:4326", inplace=True)
    elif gdf_osm.crs.to_string() != "EPSG:4326":
        print(f"Warning: OSM CRS is {gdf_osm.crs}, expected EPSG:4326")

    # Transform to 25832 for spatial operations; properties (incl. lat/lon) stay as attributes
    gdf_osm = gdf_osm.to_crs("EPSG:25832")

    # 2) Tile index from CityJSON metadata extents
    print(f"Building tile index from: {CITYJSON_DIR}")
    gdf_tiles = build_tile_index(Path(CITYJSON_DIR))

    # 3) Assign OSM points to tiles
    print("Assigning OSM points to CityJSON tiles …")
    osm_with_tiles = gpd.sjoin(
        gdf_osm,
        gdf_tiles[["filename", "path", "geometry"]],
        how="left",
        predicate="within"
    )

    # Remove join helper columns so future joins don't choke
    for col in ("index_left", "index_right"):
        if col in osm_with_tiles.columns:
            osm_with_tiles = osm_with_tiles.drop(columns=[col])

    no_tile = osm_with_tiles[osm_with_tiles["path"].isna()].copy()
    has_tile = osm_with_tiles[osm_with_tiles["path"].notna()].copy()

    matched_chunks = []
    cityjson_cache: dict[str, dict] = {}

    # 4) Per tile: load CityJSON, extract footprints, and match OSM points via nearest building
    for tile_path_str, group in has_tile.groupby("path"):
        tile_path = Path(tile_path_str)
        print(f"Processing tile: {tile_path}")

        # load CityJSON for this tile (cache)
        if tile_path_str not in cityjson_cache:
            with tile_path.open(encoding="utf-8") as f:
                cj = json.load(f)
            cityjson_cache[tile_path_str] = cj
        else:
            cj = cityjson_cache[tile_path_str]

        bldg_gdf = load_buildings_from_cityjson(cj)

        if bldg_gdf.empty:
            group["bldg_id"] = None
            group["dist_m"] = None
            matched_chunks.append(group)
            continue

        matched = gpd.sjoin_nearest(
            group,
            bldg_gdf[["bldg_id", "geometry"]],
            how="left",
            max_distance=MAX_BUILDING_DISTANCE,
            distance_col="dist_m"
        )

        matched_chunks.append(matched)

    if matched_chunks:
        matched_all = pd.concat(matched_chunks, ignore_index=False)
    else:
        matched_all = osm_with_tiles.copy()
        matched_all["bldg_id"] = None
        matched_all["dist_m"] = None

    # Add rows without tile coverage
    if not no_tile.empty:
        no_tile["bldg_id"] = None
        no_tile["dist_m"] = None
        matched_all = pd.concat([matched_all, no_tile], ignore_index=False)

    matched_all = gpd.GeoDataFrame(matched_all, geometry="geometry", crs="EPSG:25832")
    matched_all.sort_index(inplace=True)

    # 5) Write one <osm_id>.json and one <osm_id>_bld.json per feature
    print(f"Writing output JSON files to: {OUTPUT_DIR}")

    for idx, row in matched_all.iterrows():
        osm_id = row.get("osm_id", idx)

        # transformed geometry in EPSG:25832
        pt_25832 = row.geometry
        x_25832, y_25832 = float(pt_25832.x), float(pt_25832.y)

        # Build "properties" by stripping processing columns
        drop_cols = {
            "geometry", "index_right",
            "xmin", "ymin", "xmax", "ymax",
            "filename", "path",
            "bldg_id",
            "dist_m",
        }
        osm_props = {
            k: v for k, v in row.items()
            if k not in drop_cols
        }

        # ---------- OSM POINT FILE: <osm_id>.json ----------
        poi_data = {
            "osm_id": osm_id,
            "epsg": 25832,
            "geometry": {
                "type": "Point",
                "coordinates": [x_25832, y_25832],
            },
            "properties": dict(osm_props),
        }

        with (OUTPUT_DIR / f"{osm_id}.json").open("w", encoding="utf-8") as f:
            json.dump(poi_data, f, ensure_ascii=False, indent=2)

        # ---------- BUILDING FILE: <osm_id>_bld.json ----------
        bldg_id = row.get("bldg_id")
        dist_m = row.get("dist_m")
        tile_path_str = row.get("path")

        cityjson_single = None
        if bldg_id is not None and tile_path_str in cityjson_cache:
            cj_full = cityjson_cache[tile_path_str]
            cityjson_single = make_single_building_cityjson(cj_full, bldg_id)

        bld_data = {
            "osm_id": osm_id,
            "cityjson_tile": row.get("filename"),
            "cityjson_building_id": bldg_id,
            "distance_to_building_m": float(dist_m) if dist_m is not None and not pd.isna(dist_m) else None,
            "cityjson": cityjson_single,
        }

        with (OUTPUT_DIR / f"{osm_id}_bld.json").open("w", encoding="utf-8") as f:
            json.dump(bld_data, f, ensure_ascii=False, indent=2)

    print("Done.")


if __name__ == "__main__":
    main()
