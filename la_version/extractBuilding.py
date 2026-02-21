import json

srcFile = "/mnt_ai/data/ka3d/hauenstein-20260127/CityJSON/gebaeude_lod2_457000_5428000.json"

bldId = "bldg_DEBWL51140001vVs"

with open(srcFile) as f:
    d = json.load(f)

obj = d["CityObjects"]
meta = d["metadata"]

blds = obj.keys()

verts = d["vertices"]
print(len(verts))

fzi = obj[bldId].copy()

fzi_verts = []

for geom in fzi["geometry"]:
    for b in geom["boundaries"]:
        print("boundary", b)
        for bb in b:
            for i,idx in enumerate(bb):
                print(i,idx)
                print(verts[idx])
                fzi_verts.append(verts[idx])
                current = len(fzi_verts) - 1
                bb[i] = current

d["vertices"] = fzi_verts
d["CityObjects"] = {bldId: fzi}

tile = srcFile.split("/")[-1]
result = {
    "cityjson_tile": tile,
    "cityjson_building_id": bldId,
    "cityjson": d
}
#d["cityjson_tile"] = tile
#d["cityjson_building_id"] = bldId

with open("fzi_bld.json", "w") as f:
    json.dump(result, f, indent=2)
    
    