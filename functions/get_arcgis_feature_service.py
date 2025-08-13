# -*- coding: utf-8 -*-
"""
Created on Wed Aug  6 10:22:05 2025

@author: maobrien, ChatGPT
"""
import requests
import time
import geopandas as gpd
from shapely.geometry import Point, Polygon, LineString

def get_arcgis_feature_service(
    feature_layer_url: str,
    where_clause: str = "1=1",
    out_fields: str = "*",
    chunk_size: int = 1000,
    sleep_seconds: float = 0.1,
    verbose: bool = True
) -> gpd.GeoDataFrame:
    """
    Download records from an ArcGIS REST Feature Service and return as a GeoDataFrame in WGS84.

    Parameters:
    - feature_layer_url: ArcGIS Feature Layer URL (ends in /FeatureServer/{layer_id})
    - where_clause: SQL WHERE clause (default "1=1" for all records)
    - out_fields: Comma-separated list of fields to return (default "*")
    - chunk_size: Max records per request (default 1000)
    - sleep_seconds: Time to sleep between requests
    - verbose: Print download progress

    Returns:
    - GeoDataFrame (with geometries in EPSG:4326)
    
    Example usage:
        my_url = 'https://services1.arcgis.com/Hp6G80Pky0om7QvQ/ArcGIS/rest/services/Above_Ground_LNG_Storage_Facilities_gdb/FeatureServer/0'
        gdf = get_arcgis_feature_service(feature_layer_url=my_url)
        # Save to GeoJSON in WGS84
        gdf.to_file("LNG_Storage_Facilities.geojson", driver="GeoJSON")

    """

    def get_service_crs(url):
        r = requests.get(f"{url}?f=json")
        r.raise_for_status()
        service_json = r.json()
        wkid = service_json.get("extent", {}).get("spatialReference", {}).get("latestWkid") \
            or service_json.get("extent", {}).get("spatialReference", {}).get("wkid")
        if not wkid:
            raise ValueError("Unable to detect CRS (WKID) from the Feature Service.")
        return f"EPSG:{wkid}"

    def get_object_ids(url, where):
        params = {'where': where, 'returnIdsOnly': 'true', 'f': 'json'}
        r = requests.get(f"{url}/query", params=params)
        r.raise_for_status()
        return r.json().get('objectIds', [])

    def esri_to_shapely_geometry(geom):
        if not geom:
            return None
        if 'x' in geom and 'y' in geom:
            return Point(geom['x'], geom['y'])
        if 'rings' in geom:
            return Polygon(geom['rings'][0])  # assumes single-ring polygon
        if 'paths' in geom:
            return LineString(geom['paths'][0])  # assumes single-part linestring
        return None

    def download_records(url, object_ids):
        records = []
        geometries = []

        for i in range(0, len(object_ids), chunk_size):
            chunk = object_ids[i:i+chunk_size]
            params = {
                'objectIds': ','.join(map(str, chunk)),
                'outFields': out_fields,
                'returnGeometry': 'true',
                'f': 'json'
            }
            r = requests.get(f"{url}/query", params=params)
            r.raise_for_status()
            features = r.json().get('features', [])

            for f in features:
                attributes = f.get('attributes', {})
                geometry = esri_to_shapely_geometry(f.get('geometry'))
                records.append(attributes)
                geometries.append(geometry)

            if verbose:
                print(f"Downloaded {i + len(chunk)} / {len(object_ids)} records...")
            time.sleep(sleep_seconds)

        return records, geometries

    if verbose:
        print("Detecting service CRS...")
    source_crs = get_service_crs(feature_layer_url)

    if verbose:
        print(f"Source CRS detected: {source_crs}")

    if verbose:
        print("Getting OBJECTIDs...")
    object_ids = get_object_ids(feature_layer_url, where_clause)

    if not object_ids:
        raise RuntimeError("No OBJECTIDs found.")

    if verbose:
        print(f"Found {len(object_ids)} records. Downloading...")

    records, geometries = download_records(feature_layer_url, object_ids)

    gdf = gpd.GeoDataFrame(records, geometry=geometries, crs=source_crs)

    # Reproject to WGS84 if necessary
    if gdf.crs != "EPSG:4326":
        if verbose:
            print(f"Reprojecting from {gdf.crs} to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

    return gdf
