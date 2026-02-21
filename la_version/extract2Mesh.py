from curses import error
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import trimesh

import sys
import pathlib

import argparse

        
def error(msg: str):
    """Handle errors according to FAIL_ON_ERROR."""
    print(f"ERROR: {msg}")
    raise SystemExit(1)


def extract_building(d, building_id: str, output_dir: str):
    bldId = building_id
    print("Extracting building with ID:", bldId)
    obj = d["CityObjects"]

    verts = d["vertices"]
    print(len(verts))

    item = obj[bldId].copy()

    item_verts = []

    for geom in item["geometry"]:
        for b in geom["boundaries"]:
            print("boundary", b)
            for bb in b:
                for i,idx in enumerate(bb):
                    print(i,idx)
                    print(verts[idx])
                    item_verts.append(verts[idx])
                    current = len(item_verts) - 1
                    bb[i] = current


    item["vertices"] = item_verts
    #d["CityObjects"] = {bldId: item}

    result = {
        "cityjson_building_id": bldId,
        "cityjson": {"CityObjects": {bldId: item}, "vertices": item_verts}
    }
    #d["cityjson_tile"] = tile
    #d["cityjson_building_id"] = bldId

    with open(f"{output_dir}/{bldId}.json", "w") as f:
        json.dump(result, f, indent=2)
    return result


def make_mesh(bld, output_dir):
    building_id = bld.get("cityjson_building_id")

    vertices_global = None
    building_obj = None

    # NEW STYLE: everything embedded
    print("Using embedded 'cityjson' block.")
    vertices_global = np.array(bld["cityjson"]["vertices"], dtype=float)
    if building_id not in bld["cityjson"]["CityObjects"]:
        error(f"Building id {building_id!r} not found in embedded CityObjects.")

    building_obj = bld["cityjson"]["CityObjects"][building_id]


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
    OUTPUT_GLB = f"{output_dir}/{building_id}.glb"
    for sem_type, mesh in meshes.items():
        scene.add_geometry(mesh, node_name=sem_type)

    try:
        scene.export(OUTPUT_GLB)
    except Exception as e:
        error(f"Failed to export GLB/GLTF to {OUTPUT_GLB}: {e!r}")

    print(f"Exported {len(meshes)} meshes to {OUTPUT_GLB}")
    print("Surface types:", ", ".join(meshes.keys()))

    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extract a single building from a CityJSON file and save as a new JSON.")
    parser.add_argument("-i", "--input_file", required=True, help="Path to the input CityJSON file.")
    parser.add_argument("-b", "--building_id", required=False, help="ID of the building to extract.")
    parser.add_argument("-o", "--output_dir", default="buildings", help="Path to the output dir .")
    
    args = parser.parse_args()

    import os
    os.makedirs(args.output_dir, exist_ok=True)
    
    #meta = d["metadata"]
    #blds = obj.keys()

    with open(args.input_file) as f:
        d = json.load(f)

    if args.building_id is None:
        print("No building ID provided. Available building IDs:")
        #extracting all 
        blds = d["CityObjects"].keys()
        for bldId in blds:
            print(bldId)
            bld = extract_building(d, bldId, args.output_dir)
            mesh = make_mesh(bld, args.output_dir)

    else:
        # The code above can be wrapped in a function and called here with args.input_file, args.building_id, and args.output_file
        bld = extract_building(d, args.building_id, args.output_dir)
        mesh = make_mesh(bld, args.output_dir)
    