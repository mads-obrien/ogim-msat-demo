# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 13:48:02 2022

@author: maobrien
"""

import json
import fiona
from shapely.geometry import shape 
import pandas as pd
import geopandas as gpd

import warnings


def read_iffy_file(filepath):
    '''
    Returns a geodataframe containing the records from the shapefile, filtered so only the valid geometries are kept
    Shapes / geometries that are EXCLUDED get printed to the screen
    
    Based on https://gis.stackexchange.com/questions/277231/geopandas-valueerror-a-linearring-must-have-at-least-3-coordinate-tuples
    '''
    
    #Read data as a collection, ignoring warnings about invalid geometries
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore')
        collection = list(fiona.open(filepath,'r'))
    
    # Capture the CRS of the original shapefile to use later
    crs_dict = fiona.open(filepath).crs
    crs_str = crs_dict['init']
    
    # Create a df that contains the following columns: id, properties (contains dictionary of all attributes), and geometry
    df= pd.DataFrame(collection)
    
    # Create column that says whether record has valid geometry or not
    def isvalid(geom):
        try:
            shape(geom)
            return 1
        except:
            return 0
    df['isvalid'] = df['geometry'].apply(lambda x: isvalid(x))
    
    # Sort valid and invalid records into two different data frames
    df_valid = df[df['isvalid'] == 1]
    df_invalid = df[df['isvalid'] != 1]
    
    # If df_invalid is not empty, Print out invalid records to the screen
    if not df_invalid.empty:
        print('The following records were not read due to invalid geometry:')
        print('----------------------------------------------------')
        for i in range(0,len(df_invalid)):
            print(dict(df_invalid.properties.iloc[i]))
        print('----------------------------------------------------')
    
    # Convert valid records into gdf with corresponding attributes and geometries
    collection = json.loads(df_valid.to_json(orient='records'))
    return gpd.GeoDataFrame.from_features(collection, crs=crs_str)
