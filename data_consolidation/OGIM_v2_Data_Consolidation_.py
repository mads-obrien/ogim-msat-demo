# -*- coding: utf-8 -*-
"""
Created on Thu Aug 25 15:53:27 2022

OGIM V2 Data Consolidation

This script combines the 3 regional geopackages we have into a single geopackage

@author: maobrien
"""

import os
import sys
import pandas as pd
import geopandas as gpd
import numpy as np
import fiona
from tqdm import tqdm
from sigfig import round
import pprint
from datetime import datetime

# ogimlib
os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
# from ogimlib import check_invalid_geoms
from data_quality_checks import *
from data_quality_scores import *
from hybridization import get_uniques

# set working directory
os.chdir('C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory')

# set filepath where final geopackage will be saved
out_gpkg_path = "Analysis\\Results\\OGIM_v2\\"

# List of layers that should have a COMMODITY field
# Why? Data quality checks fail otherwise
commodity_layers = ['Offshore_Platforms',
                    'Natural_Gas_Compressor_Stations',
                    'Petroleum_Terminals',
                    'Oil_Natural_Gas_Pipelines']

# List of layers on which to apply data quality scoring (midstream)
midstream_quality_score_layers = ['Crude_Oil_Refineries',
                                  'LNG_Facilities',
                                  'Natural_Gas_Compressor_Stations',
                                  'Natural_Gas_Processing_Facilities',
                                  'Petroleum_Terminals']


# =============================================================================
#%% Define custom functions 
# =============================================================================

def add_region_column(gdf, region_string):
    ''' Add column indicating regional database the record originally came from'''
    gdf['REGION'] = region_string.upper()
    # get column index of COUNTRY column
    loc_of_country_col = gdf.columns.get_loc('COUNTRY')
    # move REGION column position right next to COUNTRY position
    gdf.insert(loc_of_country_col, 'REGION', gdf.pop('REGION'))
    return gdf

class HiddenPrints:
    '''Suppress printing of specific functions'''
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w', encoding="utf-8")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def combine_all_layers(afme, euro, nasa, russ, layer_name):
    ''' Returns one dataframe with all records from multiple regional geopackages appended together'''
    all_data = []
    if layer_name in afme:
        print('Records in AF+ME: '+str(len(afme[layer_name])))
        all_data.append(afme[layer_name])
    
    if layer_name in euro:
        print('Records in EURO: '+str(len(euro[layer_name])))
        all_data.append(euro[layer_name])
        
    if layer_name in nasa:
        print('Records in NA+SA: '+str(len(nasa[layer_name])))
        all_data.append(nasa[layer_name])
        
    if layer_name in russ:
        print('Records in Russia/Central: '+str(len(russ[layer_name])))
        all_data.append(russ[layer_name])
    
    # Concatenate
    data = pd.concat(all_data)
    print("Total # of records = ", data.shape[0])
      
    return data


# =============================================================================
#%% Read in dictionaries for mapping FAC_STATUS to OGIM_STATUS
# =============================================================================

import json
midstream_status_dict_fp = r'C:\Users\maobrien\Documents\GitHub\ogim-msat\analysis_workflows\midstream_status_dictionary.txt'
midstream_status_dict = json.load(open(midstream_status_dict_fp))

wells_status_dict_fp = r'C:\Users\maobrien\Documents\GitHub\ogim-msat\analysis_workflows\wells_status_dictionary.txt'
wells_status_dict = json.load(open(wells_status_dict_fp))


# =============================================================================
#%% Read OGIM standalone data catalog 
# =============================================================================

os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\data')
fp2 = "ogim_standalone_source_table.xlsx"
catalog = pd.read_excel(fp2, sheet_name=0) # ignore warning msg about Data Validation
# Calculate REFRESH_SCORES properly
catalog = refresh_score(catalog)

# Based on the data catalog, create dictionaries that match SRC_IDs to Source Scores and Refresh Scores
src_catalog = dict(zip(catalog['SRC_ID '], catalog.SOURCE_SCORE)) # contains one row per source
refresh_catalog = dict(zip(catalog['SRC_ID '], catalog.REFRESH_SCORE)) # contains one row per source


# =============================================================================
#%% Read in ALL data
# =============================================================================
os.chdir('C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory')

# Read in OGIM data layers from NA+SA v1.2.1 geopackage
fp_nasa = r'Analysis\Results\OGIM_NA_SA_v.1.2.1\OGIM_NA_SA_v1.2.1.gpkg'
# Read in AF+ME geopackage layers
fp_afme = r'Analysis\Results\\OGIM_AF_ME_v2\OGIM_AF_ME_v2_draft.gpkg'
# Read in Europe geopackage layers
fp_euro = r'Analysis\Results\OGIM_Europe_v1\OGIM_Europe_v1.1.gpkg'
# Read in Russia / central Asia
fp_russ = r'Analysis\Results\OGIM_Russia_Central_Asia_v1\OGIM_Russia_Central_Asia_v1.1.gpkg'

# Read in all layers of each geopackage.
# ============================================================
nasa = {}
# nasa_lyrs = fiona.listlayers(fp_nasa)
# nasa_lyrs.remove('Oil_Natural_Gas_Wells')
# nasa_lyrs.remove('Oil_Natural_Gas_Pipelines')

for lyr in tqdm(fiona.listlayers(fp_nasa)):
# for lyr in tqdm(nasa_lyrs):
    nasa[lyr] = gpd.read_file(fp_nasa, layer = lyr)
    nasa[lyr] = add_region_column(nasa[lyr], 'AMERICAS')

    # Remove COMMODITY column from columns that don't need one
    # Why? Data quality checks fail otherwise
    if lyr not in commodity_layers and 'COMMODITY' in nasa[lyr].columns:
        nasa[lyr] = nasa[lyr].drop('COMMODITY', axis=1)
        
 # ============================================================   
afme = {}
countries_to_remove = ['AZERBAIJAN','TURKMENISTAN']
for lyr in tqdm(fiona.listlayers(fp_afme)):
    afme[lyr] = gpd.read_file(fp_afme, layer = lyr)
    afme[lyr] = add_region_column(afme[lyr], 'AFRICA + MIDDLE EAST')
    
    # Remove COMMODITY column from columns that don't need one
    if lyr not in commodity_layers and 'COMMODITY' in afme[lyr].columns:
        afme[lyr] = afme[lyr].drop('COMMODITY', axis=1)
        
    # Remove Turkmenistan and Azerbaijan records from the AF+ME database to avoid duplication
    # they are already in the Russia + Central Asia db
    afme[lyr] = afme[lyr][~afme[lyr].COUNTRY.isin(countries_to_remove)]
    
# ============================================================        
euro = {}
for lyr in tqdm(fiona.listlayers(fp_euro)):
    euro[lyr] = gpd.read_file(fp_euro, layer = lyr)
    euro[lyr] = add_region_column(euro[lyr], 'EUROPE')
    
# ============================================================    
russ = {}
for lyr in tqdm(fiona.listlayers(fp_russ)):
    russ[lyr] = gpd.read_file(fp_russ, layer = lyr)
    russ[lyr] = add_region_column(russ[lyr], 'RUSSIA + CENTRAL ASIA')


# =============================================================================
#%% Create list of layer names for iterating over
# Note: The order of `keylist` is also the order of how layers will be added to the geopackage
# =============================================================================
keylist = list(nasa.keys())
keylist.extend(afme.keys())
keylist.extend(euro.keys())
keylist.extend(russ.keys())
keylist = set(keylist)
keylist.remove('Data_Catalog')
keylist = sorted(list(keylist))

print(keylist)

# =============================================================================
#%% Iterate through each infrastructure layer...
# NOTE: Takes ~50 minutes to run this cell for all infra categories and all regions
# =============================================================================

# Reset the OGIM_ID counter
last_ogim_id = 0

print('Starting Data Consolidation process at '+str(datetime.now())+'\n')
starttime = datetime.now()

# Process each infrastructure type, one at a time.
for layername in keylist:
    print('==================================================================')
    starttime_loop = datetime.now()
    print('Combining all regions for infrastructure layer '+layername+'.....\n')
    
    # Concatenate all records from each of the regional geopackages
    gdf_ = combine_all_layers(afme, euro, nasa, russ, layername)
    
    print('Beginning data quality checks for '+layername+'.....\n')
    with HiddenPrints():
        gdf = data_quality_checks(gdf_, starting_ogim_id = last_ogim_id+1)
    # Advance the OGIM_ID value counter
    last_ogim_id = gdf.OGIM_ID.iloc[-1] 
    
    # Ensure there's just one unique value in the CATEGORY attribute
    if len(gdf.CATEGORY.unique()) != 1:
        print(' !!! WARNING: Inconsistencies in the CATEGORY attribute of '+layername+'\n')
        # Re-assign all rows the most common existing value for CATEGORY column
        most_common_val = gdf.CATEGORY.value_counts().index[0]
        gdf['CATEGORY'] = most_common_val
        print('CATEGORY attribute reassigned so all records equal ' + most_common_val)
    else:
        print('Success: Consistent CATEGORY attribute for '+layername+'\n')


    # STANDARDIZE STATUS ATTRIBUTE
    # ============================================================
    print('Standardizing status column for '+layername+'.....\n')
    if 'FAC_STATUS' in gdf.columns:
        if 'wells' in layername.lower():
            gdf['OGIM_STATUS']  = gdf['FAC_STATUS'].map(wells_status_dict).fillna('Error')
        else:
            gdf['OGIM_STATUS']  = gdf['FAC_STATUS'].map(midstream_status_dict).fillna('Error')
    
        # get column index of FAC_STATUS column
        loc_of_fac_status = gdf.columns.get_loc('FAC_STATUS')
        # move OGIM_STATUS column position right next to FAC_STATUS position
        gdf.insert(loc_of_fac_status+1, 'OGIM_STATUS', gdf.pop('OGIM_STATUS'))
    
    
    # CALCULATE QUALITY SCORES
    # ============================================================
    # Add ATTRIBUTE_SCORE column to gdf
    if 'wells' in layername.lower():
        print('Calculating data quality scores for '+layername+'.....\n')
        with HiddenPrints():
            gdf, attribute_score_gped_wells = attribute_score_wells(gdf)
        
    if layername in midstream_quality_score_layers:
        # Quick hack to ensure the Refineries scores are calculated correctly, since this gdf don't have gas capacity columns
        # TODO - edit the quality score function itself to fix this problem
        if layername == 'Crude_Oil_Refineries':
            gdf['GAS_CAPACITY_MMCFD'] = -999
            gdf['GAS_THROUGHPUT_MMCFD'] = -999
            
        print('Calculating data quality scores for '+layername+'.....\n')
        with HiddenPrints():
            gdf, attribute_score_gped_midstream = attribute_score_midstream(gdf)
            
        # Quick hack to ensure the Refineries scores are calculated correctly (delete temp columns created earlier)
        if layername == 'Crude_Oil_Refineries':
            gdf = gdf.drop(['GAS_CAPACITY_MMCFD','GAS_THROUGHPUT_MMCFD'], axis=1)
    
    # Calculate the rest of the quality scores ONLY for infrastructure layers with Attribute Score
    if (layername in midstream_quality_score_layers) or ('wells' in layername.lower()):
        source_scores = [] # will contain one row per feature
        refresh_scores = [] # will contain one row per feature
        
        # Loop through each record in the gdf
        for idx1, row1 in tqdm(gdf.iterrows(), total=gdf.shape[0]):
            src_id = int(row1.SRC_REF_ID.split(",")[0]) # Use the first data source for cases where there are multiple data sources for the same facility
            
            # Save data source score for the current record
            src_score = src_catalog.get(src_id)
            source_scores.append(src_score)
            
            # Save data refresh score for the current record
            refresh_score = refresh_catalog.get(src_id)
            refresh_scores.append(refresh_score)
            
        # Append scores as new column to gdf 
        gdf["DATA_SOURCE_SCORE"] = source_scores
        gdf["UPDATE_FREQUENCY_SCORE"] = refresh_scores
        
        gdf["AGGREGATE_QUALITY_SCORE"] = [1/16*(gdf["ATTRIBUTE_SCORE"].iloc[x] + 
                                                      gdf["DATA_SOURCE_SCORE"].iloc[x] + 
                                                      gdf["UPDATE_FREQUENCY_SCORE"].iloc[x]) for x in range(gdf.shape[0])]
        

    # Reset indices
    gdf = gdf.reset_index(drop=True)
    
    # Write to GeoPackage
    # ============================================================
    print('Writing layer '+layername+' to Geopackage')
    gdf.to_file(out_gpkg_path+"OGIM_v2.gpkg", layer=layername, driver="GPKG", encoding="utf-8")
    
    # Write to GeoJSON
    gdf.to_file(out_gpkg_path+layername+".geojson", driver="GeoJSON", encoding="utf-8")

    endtime_loop = datetime.now() - starttime_loop
    print('Completed data consolidation for '+layername+' at '+str(datetime.now()))
    print('Duration:'+str(endtime_loop)+'\n')

endtime = datetime.now() - starttime
print('Data Consolidation Finished! Final Duration: '+str(endtime))



#%% Add data catalog layer to geopackage at very end
catalog['geometry'] = None
catalog = gpd.GeoDataFrame(catalog)
catalog.to_file(out_gpkg_path+"OGIM_v2.gpkg", layer='Data_Catalog', driver="GPKG", encoding="utf-8")


#%% Read in GPKG I just created to confirm results
fp_v2 = r'Analysis\Results\OGIM_v2_TEST\OGIM_v2.gpkg'

# Read in all layers of geopackage.
v2 = {}
for lyr in tqdm(fiona.listlayers(fp_v2)):
    v2[lyr] = gpd.read_file(fp_v2, layer = lyr)
    


