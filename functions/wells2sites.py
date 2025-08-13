# -*- coding: utf-8 -*-
"""
Created on Wed May  8 12:36:45 2024

Create a site-level dataset from well-level data.

@author: momara
"""
import numpy as np
import pandas as pd
import geopandas as gpd
from pyproj import Proj, transform
from tqdm import trange


def calculate_epsg(
    longitude: float,
    latitude: float
):
    """Function returns an EPSG code given a latitude and longitude in decimal degrees for a U.S. location

    Inputs:
    ---
        longitude: Longitude, in decimal degrees (WGS 1984)
        latitude: Latitude, in decimal degrees (WGS 1984)

    Returns:
    ---
        The European Survey Petroleum Group's (EPSG) code

    Dependencies:
    ---
        numpy

    Example:
    ---
        lat_lon = [40, -103]
        epsg_code = calculate_epsg(lat_lon[1], lat_lon[0])

    """
    epsg_ = 32700 - (np.sign(latitude) + 1) / 2 * 100 + np.floor((180 + longitude) / 6) + 1

    return epsg_


def wells2sites(
    gdf: "GeoDataFrame",
    aggreg_funcs: dict = {'Join_Count': 'sum'},
    radius_m: float = 25,
    starting_id: int = 0
):
    """Generates a GeoDataFrame with well site attributes based on well-level information.

    Approach
    ---
            We begin by identifying the appropriate CRS based on the UTM coordinate system, generating unique EPSG codes
        for each latitude/longitude record in the UTM zone(s)
            We then create a buffer polygon of radius `radius_m` [meters] around each location, then merge overlapping buffers
        assuming the merged polygon boundary approximates the site boundary if there are multiple wells on the site.
        The choice of the buffer radius is important and the analyst is encouraged to test sensitivities to different buffer
        radii to select the appropriate buffer radius for the dataset of wells. A good starting point is a buffer radius of
        25m for vertically drilled wells and 50m for horizontally drilled wells.
            Following site boundary definition, we aggregate the site-level attributes based on the aggregation functions
        defined by the `ggreg_funcs` dictionary.
            Finally, a unique `TARGET_FID`, representing the facility id is assigned to each site.

    Inputs:
    ---
        gdf: Panadas GeoDataFrame of wells, CRS must be defined.
        agg_funcs: dictionary of Pandas aggregation functions for consolidating well-level attributes to site-level attributes;
               key-value pairs indicating `key`: attribute/field/column name and `value`: statistical operator for
               aggregation, e.g., `sum`, `mean`, `first`, `last`, etc. The dictionary {Join_Count:'sum'} should be included
               in aggreg_funcs to count the total number of wells on each site.
                e.g. aggreg_funcs = {'lon':'mean', 'lat':'mean', 'CurrentOperator':'first', 'Join_Count':'sum'}
        radius_m: radius for the buffer
        starting_id: starting integer for unique site ID

    Returns:
    ---
        Two GeoDataFrames [CRS: EPSG:4326] with a `TARGET_FID` field for:
            (i) the well level data (all_data_wells)
            (ii) the site-level data (all_data_sites) . The `TARGET_FID` can be used to match individual wells on the site to their site IDs.
        A polygon geodataframe (`all_polys`) is also generated showing the merged polygons representing `radius_m` buffer around each wellhead

    Dependencies:
    ---
        pyproj: pip install pyproj
        pandas, geopandas, numpy
        trange from tqdm: pip install tqdm

    Example:
    ---
       all_polys, all_data_wells, all_data_sites = wells2sites(some_wells_geo_dataframe, aggreg_funcs={'Join_Count':'sum', 'GasProd_MCF':'sum', 'OilProd_BBL':'sum'}, radius_m=25)

    """

    # ================================================================
    # Check CRS of reference geodataframe
    crs_gdf = gdf.crs
    if crs_gdf is not None:
        print("=======================")
        print("CRS of original gdf is: ", crs_gdf)

        # Transform to target CRS: use EPSG codes
        gdf_new = gdf.to_crs('epsg:4326')

        # Append latitude and longitude [decimal degrees if EPSG:4326] attributes to gdf_new
        gdf_new['lon_calc'] = gdf_new.geometry.x
        gdf_new['lat_calc'] = gdf_new.geometry.y

        print("=======================")
        print("CRS of new gdf is: ", gdf_new.crs)
        print("=======================")

    else:
        raise ValueError("!! Error: CRS not set on original gdf !!")

    # Add EPSG codes to each row in the dataframe
    gdf_sel = gdf_new.copy()
    gdf_sel['epsg_code'] = calculate_epsg(gdf_sel['lon_calc'], gdf_sel['lat_calc'])

    # Determine unique epsg
    unique_epsg_codes = gdf_sel.epsg_code.unique()

    # Loop through data representing each unique ESPG code to allow for accurate calculation of spatial extent
    # on the ground when generating buffers around each well location

    all_data_sites = []
    all_data_wells = []
    all_polys = []

    # For setting unique site ID
    len_fids_update = starting_id

    for idx in trange(len(unique_epsg_codes), desc='Running EPSG=> '):

        # EPSG codes
        epsg_code_ = unique_epsg_codes[idx]

        # Data selected
        data_sel = gdf_sel.query('epsg_code == @epsg_code_')

        # Define crs for this dataset
        try:
            data_sel = data_sel.to_crs("EPSG:" + str(int(epsg_code_)))

        except:
            print("!! CRSError; check lat/lon values; Returned EPSG = ", epsg_code_, "\n !! Skipping over these data !!")
            continue

        # Generate buffer around each location
        data_sel2 = data_sel.copy()

        data_sel2['geometry'] = data_sel2.buffer(radius_m)

        # Unary union: collapse these circles into a single shapely MultiPolygon geometry
        mp = data_sel2.geometry.unary_union
        df_diss = gpd.GeoDataFrame(geometry=[mp])
        # Break multipolygon into individual polygons
        dissolved_df = df_diss.explode().reset_index(drop=True)

        # Set CRS of new dataframe
        dissolved_df.crs = data_sel2.crs

        # Next, create unique id for each well site
        len_fids = dissolved_df.shape[0]

        dissolved_df['TARGET_FID'] = [*range(len_fids_update, len_fids + len_fids_update)]
        # ===============================================================
        # For counting # of wells per site
        data_sel['Join_Count'] = 1

        # Spatial join of wells within well site (buff) polygons
        try:
            data_sel.drop(columns=['index_right'], inplace=True)
        except:
            pass

        wellsOnSites = gpd.sjoin(data_sel, dissolved_df, how="left", op="within")

        # ================================================================

        # Apply site attribute aggregation functions as defined in `aggreg_funcs`
        site_attrs = wellsOnSites.groupby(by='TARGET_FID').agg(aggreg_funcs)

        # Update len_fids_update
        len_fids_update = len_fids_update + len_fids

        # ================================================================
        # DETERMINE CENTROID LATITUDE AND LONGITUDE of approximate site boundary
        # Polygon centroid
        dissolved_df['centroid'] = dissolved_df['geometry'].centroid
        dissolved_df = dissolved_df.set_geometry('centroid')
        # Centroid in degrees lat and lon
        lon, lat = np.array(dissolved_df.geometry.centroid.x), np.array(dissolved_df.geometry.centroid.y)
        inProj, outProj = Proj(init='EPSG:' + str(int(epsg_code_))), Proj(init='epsg:4326')
        # inProj, outProj = Proj('EPSG:' + str(int(epsg_code_))), Proj('EPSG:4326')  # pyproj deprecated the init syntax; at some point, replace the line above with this one
        lon2, lat2 = transform(inProj, outProj, lon, lat)
        dissolved_df['centroid_lon'] = lon2
        dissolved_df['centroid_lat'] = lat2

        # Change geometry back to polygon
        dissolved_df = dissolved_df.set_geometry('geometry')
        dissolved_df = dissolved_df.to_crs("EPSG:4326")

        # Merge site_attrs with dissolved_df #
        site_attrs2 = pd.merge(site_attrs, dissolved_df, how='left', on='TARGET_FID')

        # ================================================================
        # Drop geometry column before appending, as this geometry column
        # inherits the well site polygon geometry

        site_attrs2 = site_attrs2.drop(columns=['geometry'])

        # Set geometry again using the `centroid` attribute [POINT]
        site_attrs2['geometry'] = site_attrs2.centroid

        # Drop centroid attribute from gdfs that will be outputs
        site_attrs2 = site_attrs2.drop(columns=['centroid'])
        dissolved_df = dissolved_df.drop(columns=['centroid'])

        # Create GeoDataFrame for site attributes
        site_attrs2 = gpd.GeoDataFrame(site_attrs2, geometry='geometry')

        # Change CRS back to EPSG:4326, before saving results
        # Well site attributes
        site_attrs2 = site_attrs2.to_crs("epsg:4326")
        all_data_sites.append(site_attrs2)

        # Well-level attributes
        all_data_wells.append(wellsOnSites.set_geometry("geometry").to_crs("epsg:4326"))

        # Approximate site boundary data
        all_polys.append(dissolved_df)

    # Concatenate all data for approx. site boundary, wells and well sites
    all_polys = pd.concat(all_polys).reset_index(drop=True)
    all_data_wells = pd.concat(all_data_wells).reset_index(drop=True).drop(columns=['index_right'])
    all_data_sites = pd.concat(all_data_sites).reset_index(drop=True)

    # =======================================================================
    # Set CRS on final dataset to EPSG:4326
    all_polys = all_polys.to_crs("epsg:4326")
    all_data_wells = all_data_wells.to_crs("epsg:4326")
    all_data_sites = all_data_sites.to_crs("epsg:4326")
    # =======================================================================

    return all_polys, all_data_wells, all_data_sites
