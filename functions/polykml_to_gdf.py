# -*- coding: utf-8 -*-
"""
Created on Thu Feb 10 15:56:22 2022

@author: maobrien
"""
from pykml import parser
from shapely.geometry import Polygon
import ast
import geopandas as gpd
import pandas as pd


def polykml_to_gdf(mykml, crs=4326):
    #Create empty temporary dictionary
    temp_dict = {}

    with open(mykml) as f:
        doc = parser.parse(f).getroot().Document.Folder
    for i,placemark in enumerate(doc.iterchildren()):  # LOOP OVER EACH PLACEMARK, AKA POLYGON
        # print(i)
        if hasattr(placemark, 'Polygon'):
            # capture list of polygon vertices as a string (stripping tabs and new lines)
            coord = str(placemark.Polygon.outerBoundaryIs.LinearRing.coordinates).strip() 
            # split the single string into one item for each vertex
            coord_list = coord.split(sep= ' ')
            # https://www.geeksforgeeks.org/python-convert-given-list-into-nested-list/
            output = [list(ast.literal_eval(x)) for x in coord_list]
            # print(output[0])  # print first coordinate to prove it worked
            polygon = Polygon(output)
    
            if hasattr(placemark, 'name'):  # ONLY do if the polygon has a name 
                polyname = str(placemark.name)
                # print(polyname)
                # print(type(polyname))
            else:
                polyname = None
            temp_dict[i] = {'name': polyname, 'geometry': polygon}
        # print('===================================================')
    
    df = pd.DataFrame.from_dict(temp_dict,orient='index')
    return gpd.GeoDataFrame(df,geometry='geometry', crs=crs)

#%% Example

# kmlpath = r'C:\Users\maobrien\OneDrive - MethaneSAT, LLC\raw_data\Public_Data\Middle_East+Caspian\Azerbaijan\Oil_Gas_Infra_.com\Fields_Basins\Gas_Fields_Azerbajan.kml'
# azer_gas_fields = polykml_to_gdf(kmlpath)

# azer_gas_fields.plot()