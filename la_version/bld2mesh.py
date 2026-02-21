import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import trimesh

import sys
import pathlib


# -------------------------------------------------
# Config
# -------------------------------------------------
BASE_DIR = pathlib.Path(".")



if len(sys.argv) < 2:
    raise SystemExit("Usage: python bld2mesh.py <input_json>")

INPUT_FILE = pathlib.Path(sys.argv[1])
OUTPUT_GLB = INPUT_FILE.with_suffix(".glb")

FAIL_ON_ERROR = False  # True = raise exceptions, False = print & exit cleanly


def error(msg: str):
    """Handle errors according to FAIL_ON_ERROR."""
    if FAIL_ON_ERROR:
        raise RuntimeError(msg)
    else:
        print(f"ERROR: {msg}")
        raise SystemExit(1)


# -------------------------------------------------
# Load input JSON
# -------------------------------------------------
try:
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
except Exception as e:
    error(f"Cannot read JSON file {INPUT_FILE}: {e!r}")


# -------------------------------------------------
# Find geometry source (embedded cityjson or external)
# -------------------------------------------------
city = data.get("cityjson", None)
building_id = data.get("cityjson_building_id")

vertices_global = None
building_obj = None

if isinstance(city, dict) and "vertices" in city and "CityObjects" in city and building_id:
    # NEW STYLE: everything embedded
    print("Using embedded 'cityjson' block.")
    vertices_global = np.array(city["vertices"], dtype=float)
    if building_id not in city["CityObjects"]:
        error(f"Building id {building_id!r} not found in embedded CityObjects.")
    building_obj = city["CityObjects"][building_id]

else:
    # OLD STYLE FALLBACK: use external CityJSON tile if possible
    print("No usable embedded 'cityjson' found, trying external CityJSON tile...")

    tile_name = data.get("cityjson_tile")
    bld_feature = data.get("building")

    if not tile_name or not bld_feature:
        error(
            "File has no valid embedded 'cityjson' and no usable old-style "
            "'cityjson_tile' + 'building' keys. Cannot build mesh."
        )

    tile_path = BASE_DIR / tile_name
    try:
        with open(tile_path, "r", encoding="utf-8") as f:
            tile = json.load(f)
    except Exception as e:
        error(f"Cannot read external CityJSON tile {tile_path}: {e!r}")

    if "vertices" not in tile or "CityObjects" not in tile:
        error(
            f"External CityJSON tile {tile_path} has no 'vertices' or 'CityObjects'. "
            "Cannot build mesh."
        )

    vertices_global = np.array(tile["vertices"], dtype=float)

    if not building_id:
        error("Missing 'cityjson_building_id' in file for old-style format.")

    if building_id not in tile["CityObjects"]:
        error(f"Building id {building_id!r} not found in external CityObjects.")

    building_obj = tile["CityObjects"][building_id]

# Final sanity checks
if vertices_global is None or building_obj is None:
    error("Internal error: no vertices or building object resolved.")


# -------------------------------------------------
# Collect polygons by semantic surface type
# -------------------------------------------------
polygons_by_type = defaultdict(list)


geoms = building_obj.get("geometry")
if not geoms:
    error("Building has no 'geometry'.")

for geom in geoms:
    gtype = geom.get("type", "")
    boundaries = geom.get("boundaries", [])
    semantics = geom.get("semantics") or {}
    surf_defs = semantics.get("surfaces") or []
    values = semantics.get("values")

    def get_sem_type(idx_path):
        """
        idx_path = index tuple into semantics['values'] that mirrors
        the nesting of 'boundaries'.
        """
        sem_type = "Unknown"
        if not surf_defs or values is None:
            return sem_type

        try:
            v = values
            for i in idx_path:
                v = v[i]
        except (IndexError, TypeError):
            return sem_type

        if isinstance(v, list):
            sem_idx = v[0]
        else:
            sem_idx = v

        if isinstance(sem_idx, int) and 0 <= sem_idx < len(surf_defs):
            return surf_defs[sem_idx].get("type", "Unknown")

        return sem_type

    # -------------------------------------------------
    # MultiSurface / CompositeSurface:
    # boundaries: [ surface ]
    # surface: [ [outer_ring], [hole1], ... ]
    # semantics.values: [ sem_index ]
    # -------------------------------------------------
    if gtype in ("MultiSurface", "CompositeSurface"):
        for surf_index, surface in enumerate(boundaries):
            if not surface:
                continue

            outer_ring = surface[0]
            if len(outer_ring) < 3:
                continue

            sem_type = get_sem_type((surf_index,))
            polygons_by_type[sem_type].append(outer_ring)

    # -------------------------------------------------
    # Solid / MultiSolid:
    # boundaries: [ solid ]
    # solid: [ shell ]
    # shell: [ surface ]
    # surface: [ [outer_ring], [hole1], ... ]
    # semantics.values: [ [ [ sem_index ] ] ]
    # -------------------------------------------------
    elif gtype in ("Solid", "MultiSolid"):
        for solid_index, solid in enumerate(boundaries):
            if not solid:
                continue

            for shell_index, shell in enumerate(solid):
                if not shell:
                    continue

                for surf_index, surface in enumerate(shell):
                    if not surface:
                        continue

                    outer_ring = surface[0]
                    if len(outer_ring) < 3:
                        continue

                    sem_type = get_sem_type(
                        (solid_index, shell_index, surf_index)
                    )
                    polygons_by_type[sem_type].append(outer_ring)

    # -------------------------------------------------
    # Fallback: treat like MultiSurface if type unknown
    # -------------------------------------------------
    else:
        for surf_index, surface in enumerate(boundaries):
            if not surface:
                continue

            outer_ring = surface[0]
            if len(outer_ring) < 3:
                continue

            sem_type = get_sem_type((surf_index,))
            polygons_by_type[sem_type].append(outer_ring)



if not polygons_by_type:
    error("No polygons found in building geometry.")


# -------------------------------------------------
# Build unified local vertex array for all polygons
# -------------------------------------------------
used_global_indices = sorted(
    {idx for polys in polygons_by_type.values() for poly in polys for idx in poly}
)
if not used_global_indices:
    error("No vertex indices referenced by polygons.")

global_to_local = {g: i for i, g in enumerate(used_global_indices)}

try:
    vertices = vertices_global[used_global_indices, :]
except Exception as e:
    error(f"Failed to slice vertices array with used indices: {e!r}")

# Optional: rebase to local origin
origin = vertices.mean(axis=0)
vertices_local = vertices - origin
print(f"Local origin (EPSG:25832) at: {origin}")


# -------------------------------------------------
# Triangulate polygons per surface type (fan triangulation)
# -------------------------------------------------
def triangulate_fan(ring_global_indices):
    local_ring = [global_to_local[g] for g in ring_global_indices]
    if len(local_ring) < 3:
        return []

    tris = []
    for i in range(1, len(local_ring) - 1):
        tris.append([local_ring[0], local_ring[i], local_ring[i + 1]])
    return tris


meshes = {}

for sem_type, polys in polygons_by_type.items():
    faces = []
    for poly in polys:
        faces.extend(triangulate_fan(poly))

    if not faces:
        continue

    faces = np.array(faces, dtype=int)
    mesh = trimesh.Trimesh(vertices=vertices_local, faces=faces, process=False)
    meshes[sem_type] = mesh

if not meshes:
    error("No faces generated for any surface type.")


# -------------------------------------------------
# Create scene and export
# -------------------------------------------------
scene = trimesh.Scene()
for sem_type, mesh in meshes.items():
    scene.add_geometry(mesh, node_name=sem_type)

try:
    scene.export(OUTPUT_GLB)
except Exception as e:
    error(f"Failed to export GLB/GLTF to {OUTPUT_GLB}: {e!r}")

print(f"Exported {len(meshes)} meshes to {OUTPUT_GLB}")
print("Surface types:", ", ".join(meshes.keys()))
