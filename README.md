# osm-city-3d
Merge OSM objects with cityjson buildings

# Cities
## Karlsruhe

### OSM objects 
Get interesting object via ameity_fetch.py

Create a geojson version for easy (visible) inspection via amenity_convert.py

### Cityjson
Use cityjson from Liegenschaftsamt / Transparenzportal. Extract to folder like
/mnt_ai/data/ka3d/tp/CityJSON with file like gebaeude_lod2_448000_5427000.json

Merge OSM geojson and Cityjson data. Create one file-set (.json, _bld.json) per OSM object

### 3D Building Models
Generate GLB models from _bld.json files
![Example](https://github.com/digital-codes/osm-city-3d/blob/main/157757_bld.png)



