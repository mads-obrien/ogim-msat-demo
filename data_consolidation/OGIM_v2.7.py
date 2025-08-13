# -*- coding: utf-8 -*-
"""
Created on 29 October 2024

@author: maobrien
"""

import os
import pandas as pd
import numpy as np
import geopandas as gpd
from datetime import datetime
# from tqdm import tqdm
import time
import fiona
import gc

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (NULL_NUMERIC, replace_missing_strings_with_na,
                     check_df_for_allowed_nans, format_data_catalog,
                     repair_invalid_polygon_geometries)
from data_consolidation_utils import (read_files_by_keyword, HiddenPrints,
                                      keep_only_cited_sources,
                                      replace_missing_dates_with_na,
                                      standardize_category_field,
                                      drop_non_oilgas_wells,
                                      create_ogim_status_column,
                                      get_src_date_from_ref_id)
from internal_review_protocol_Excel import create_internal_review_spreadsheet
from data_quality_checks import data_quality_checks
from standardize_countries import standardize_countries, add_region_column
from assign_offshore_attribute import assign_offshore_attribute
from assign_countries_to_feature_2 import assign_countries_to_feature, assign_stateprov_to_feature

# !!! PARAMETERS TO EDIT before running this script !!!
# -----------------------------------------------------------------------------
# Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# Should QA Excel reports treat US states like countries, when aggregating
# different values on a country basis? If so, set to "True".
# Otherwise, the default is to aggregate all report info by COUNTRY.
treat_US_states_as_countries_in_wells_report = True

# Should any status values that are unsuccessfully mapped to a OGIM_STATUS
# value appear in the GeoPackage as "Error", or as "N/A"?
replace_ogim_status_error_with_na = False

# If any exact duplicate records are detected in our integrated data (where all
# attributes except OGIM_ID are the same), delete them here in the data
# consolidations script as opposed to going back into the integration code to
# solve the root problem
drop_identical_records = True

# =============================================================================
# %% Define path to Bottom Up Infra Inventory directory
# =============================================================================
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'
os.chdir(buii_path)

# get string of today's date
timestr = time.strftime("%Y-%m-%d")

# set filepath(s) where final data will be saved
integration_folder = f'OGIM_{version_num}\\integrated_results\\'
excel_report_folder = f'OGIM_{version_num}\\excel_reports_{timestr}\\'
fp_of_output_gpkg = f'OGIM_{version_num}\\OGIM_{version_num}_{timestr}.gpkg'

# check that your desired output directories exist before proceeding
paths2check = [integration_folder, excel_report_folder]
for path in paths2check:
    if not os.path.exists(path):
        os.makedirs(path)
        print(f"Folder '{path}' didn't exist; it's now been created.")
    else:
        print(f"Folder '{path}' exists.")


print(f'integrated_results folder exists -- {os.path.exists(integration_folder)}')
print(f'excel_reports folder exists -- {os.path.exists(excel_report_folder)}')

# set filepaths to supporting CSVs in our GitHub repository
countrycsv_fp = path_to_github + 'docs\\UN_countries_IEA_regions.csv'
wells_status_dict_fp = path_to_github + 'analysis_workflows\\wells_status_dictionary.csv'
midstream_status_dict_fp = path_to_github + 'analysis_workflows\\midstream_status_dictionary.csv'

# =============================================================================
# %% Load dictionaries for mapping FAC_STATUS to OGIM_STATUS
# =============================================================================
wdf = pd.read_csv(wells_status_dict_fp).fillna('N/A')
wells_status_dict = dict(zip(wdf.original_FAC_STATUS,
                             wdf.new_OGIM_STATUS))

mdf = pd.read_csv(midstream_status_dict_fp).fillna('N/A')
midstream_status_dict = dict(zip(mdf.original_FAC_STATUS,
                                 mdf.new_OGIM_STATUS))


# =============================================================================
# %% Load boundary geometries, for on/offshore analysis
# Will take 10-20 seconds to read in
# =============================================================================
boundary_geoms = r"Public_Data\data\International_data_sets\National_Maritime_Boundaries\marine_and_land_boundaries_seamless.shp"
my_boundary_geoms = gpd.read_file(boundary_geoms)  # may take a while to read in

path_to_state_geoms = r'Public_Data\NaturalEarth\ne_10m_admin_1_states_provinces.shp'
my_state_geoms = gpd.read_file(path_to_state_geoms)

# =============================================================================
# %% Load OGIM standalone data catalog; format properly
# =============================================================================
fp2 = r"Public_Data\data\OGIM_Data_Catalog.xlsx"
catalog = pd.read_excel(fp2, sheet_name='source_table')  # ignore warning msg about Data Validation

catalog = format_data_catalog(catalog)

# Drop the one row that I know is only a SRC_ID number and nothing else
catalog.drop(catalog[catalog.SRC_NAME.isna()].index, inplace=True)

# Create a copy of the catalog where SRC_ID is the index, for later
catalog_ix = catalog.set_index('SRC_ID')

# For now, remove Data Quality Score attributes and other un-needed columns
catalog.drop(['URL_AUTO_DOWNLOAD',
              'SRC_DAY',
              'FAC_CATEGORY',
              'SOURCE_SCORE',
              'REFRESH_SCORE',
              # 'RICHNESS_SCORE',
              'DOWNLOAD_INSTRUCTIONS',
              'FILE_NAME',
              'ORIGINAL_CRS'], axis=1, inplace=True)

# Check catalog for missing values in attribute columns that don't allow nans.
check_df_for_allowed_nans(catalog)

# =============================================================================
# %% LOAD ALL INFRA DATA that I want to combine into one geopackage
# These are OGIM data layers that have been integrated but not "consolidated"
# Code block will take ~ 30 minutes to read in all data
# =============================================================================
os.chdir(integration_folder)
print(f'Reading in data from {integration_folder}')

# Define keywords that are present in the GeoJSON filename that unambiguously
# identify what infrastructure layer those records are destined for.
keywords = {'basin': 'Oil_and_Natural_Gas_Basins',
            'batter': 'Tank_Battery',  # battery OR batteries
            'block': 'Oil_and_Natural_Gas_License_Blocks',
            'components': 'Equipment_and_Components',
            'compressor': 'Natural_Gas_Compressor_Stations',
            'field': 'Oil_and_Natural_Gas_Fields',
            'flaring': 'Natural_Gas_Flaring_Detections',
            'gathering': 'Gathering_and_Processing',
            'injection': 'Injection_and_Disposal',
            'lng': 'LNG_Facilities',
            'platform': 'Offshore_Platforms',  # use platform instead of the word "offshore"
            'pipeline': 'Oil_Natural_Gas_Pipelines',
            'refin': 'Crude_Oil_Refineries',  # refinery OR refineries
            'stations_other': 'Stations_Other',
            'terminal': 'Petroleum_Terminals',
            'wells': 'Oil_and_Natural_Gas_Wells'
            }

data4gpkg = {}

for kwd, lyr in zip(keywords.keys(), keywords.values()):
    print(f'==========================\nNow reading in integrated {lyr}...\n')
    df = read_files_by_keyword(os.getcwd(), 'geojson', kwd)
    print(datetime.now())
    if df is not None:
        data4gpkg[lyr] = df
        print('Concatenated gdf copied to our dictionary for consolidation\n')

print(f'All individual integrated files successfully read in at {str(datetime.now())}')

# =============================================================================
# %% Create OGIM_STATUS field; print out any statuses that couldn't be mapped
# =============================================================================
for key, gdf in data4gpkg.items():
    print(f'\nStandardizing status column for {key}.....')
    if 'wells' in key.lower():
        create_ogim_status_column(gdf, wells=True,
                                  wells_status_dict=wells_status_dict)
    else:
        create_ogim_status_column(gdf, wells=False,
                                  midstream_status_dict=midstream_status_dict)

# Optional: Print a list of statuses that are unmapped
unmapped = pd.DataFrame(columns=['CATEGORY', 'FAC_STATUS'])
for key, gdf in data4gpkg.items():
    if 'OGIM_STATUS' in gdf.columns:
        df_temp = gdf.query("OGIM_STATUS == 'Error'")[['CATEGORY', 'FAC_STATUS']]
        unmapped = unmapped.append(df_temp)
unmapped = unmapped.drop_duplicates(subset=['CATEGORY', 'FAC_STATUS'], keep='first').reset_index(drop=True)

# OPTIONAL: Replace any "error" with N/A
if replace_ogim_status_error_with_na:
    for key, gdf in data4gpkg.items():
        if 'OGIM_STATUS' in gdf.columns:
            print(key)
            gdf.OGIM_STATUS = gdf.OGIM_STATUS.replace({'Error': 'N/A'})

# =============================================================================
# %% Remove certain non-O&G or never-drilled wells
# =============================================================================
# Remove non-O&G well types, e.g., stratigraphic test wells, mineral wells, domestic water wells
data4gpkg['Oil_and_Natural_Gas_Wells'] = drop_non_oilgas_wells(data4gpkg['Oil_and_Natural_Gas_Wells'],
                                                               'FAC_TYPE')
# check that this worked
print(*sorted(data4gpkg['Oil_and_Natural_Gas_Wells'].FAC_TYPE.unique()), sep='\n')

# Remove some wells based on STATUS, e.g. Never Drilled
ogimstatus2drop = ['NOT DRILLED - DROP', 'INSUFFICIENT LOCATION - DROP']
data4gpkg['Oil_and_Natural_Gas_Wells'] = data4gpkg['Oil_and_Natural_Gas_Wells'].query('OGIM_STATUS not in @ogimstatus2drop')
data4gpkg['Oil_and_Natural_Gas_Wells'] = data4gpkg['Oil_and_Natural_Gas_Wells'].reset_index(drop=True)

# check that this worked
print(*sorted(data4gpkg['Oil_and_Natural_Gas_Wells'].OGIM_STATUS.unique()), sep='\n')

# =============================================================================
# %% For now, remove Data Quality Score attributes from the database
# =============================================================================
columns2remove = ['AGGREGATE_QUALITY_SCORE',
                  'UPDATE_FREQUENCY_SCORE',
                  'DATA_SOURCE_SCORE',
                  'ATTRIBUTE_SCORE'
                  ]

# Check to see if geopackage layer contains data quality score fields
# (Assuming every GDF with 'AGGREGATE_SCORE' contains the other fields as well)
# If the layer does contain those fields, drop those fields by name
for key in data4gpkg.keys():
    if 'AGGREGATE_QUALITY_SCORE' in data4gpkg[key].columns:
        data4gpkg[key] = data4gpkg[key].drop(columns2remove, axis=1)
        print(f'Data quality columns dropped in {key}')

# Confirm that dropping the columns worked by looking at all attribute lists
for key in data4gpkg.keys():
    print('==================')
    print(key)
    print(*data4gpkg[key].columns, sep='\n')

# =============================================================================
# %% Search for duplicate records (takes 10ish min)
# =============================================================================
# Populate a dictionary with duplicate records (where all column values except
# OGIM_ID are identical)
# Keys = infrastructure categories, values = duplicate records in that category

dupes = {}

for lyr, gdf in data4gpkg.items():
    # TODO -- un-comment this section once we're using geopandas v0.10.0
    # if any(x in lyr.lower() for x in ['pipeline', 'basin', 'field', 'block']):
    #     gdf = gdf.geometry.normalize()
    gdf_cols = list(gdf.columns)  # This list includes the `geometry` column
    gdf_cols.remove('OGIM_ID')
    dupes[lyr] = gdf[gdf.duplicated(subset=gdf_cols, keep=False)]

# Print what countries amy duplicate records are found in.
for lyr, gdf in dupes.items():
    if not gdf.empty:
        print('\n--------------------------')
        print(f'{lyr} -- {len(dupes[lyr])} duplicate records found\n')
        print(gdf.COUNTRY.value_counts())
        print('\n')
        print(gdf.STATE_PROV.value_counts())

if drop_identical_records:
    for lyr, gdf in data4gpkg.items():
        gdf_cols = list(gdf.columns)  # This list includes the `geometry` column
        gdf_cols.remove('OGIM_ID')
        print(lyr)
        print(f'Records before dedupe: {len(data4gpkg[lyr]):,.0f}')
        data4gpkg[lyr] = data4gpkg[lyr].drop_duplicates(subset=gdf_cols,
                                                        keep='first').reset_index(drop=True)
        print(f'Records after dedupe: {len(data4gpkg[lyr]):,.0f}\n')

# =============================================================================
# %% Confirm that all coordinates in the geometry column are within reasonable
# bounds of a WGS84 dataset
# =============================================================================


def test_gdf_geoms_are_within_wgs84_bounds(gdf):
    # Define bounding box of the world in WGS84 (min_lon, min_lat, max_lon, max_lat)
    min_lon, min_lat, max_lon, max_lat = [-180, -90, 180, 90]

    # Get the bounds of each feature in the GeoSeries. For a point geometry, the
    # min y and max y will be the same (the latitude coordinate); and same logic
    # applies to longitude. For line or polygon features, the bounds of the
    # feature's entire geometry will be returned.
    feature_bounds = gdf['geometry'].bounds

    # Create a mask for features with all of their vertices within the
    # specified bounds, and use that mask to return any erroneous features
    is_within_bounds = (feature_bounds['minx'] >= min_lon) & \
                       (feature_bounds['miny'] >= min_lat) & \
                       (feature_bounds['maxx'] <= max_lon) & \
                       (feature_bounds['maxy'] <= max_lat)

    if is_within_bounds.all():
        print('All features in gdf have in-bounds coordinates.')
        return None
    else:
        out_of_bounds_features = gdf[~is_within_bounds]
        print(f'WARNING: {len(out_of_bounds_features)} features whose coordinates exceed WGS84 bounds.')
        print('Inspect the output GDF to learn more.')
        return out_of_bounds_features


out_of_bounds = {}
for lyr, gdf in data4gpkg.items():
    print('---------------------')
    print(lyr)
    df_temp = test_gdf_geoms_are_within_wgs84_bounds(gdf)
    if df_temp is not None:
        out_of_bounds[lyr] = df_temp

# =============================================================================
# %% To free up memory, delete some objects that I no longer reference
# before the heavy computation part of consolidation begins
# =============================================================================
# Because the last `df` used above has 4M well records, delete it and regain
# some memory for the rest of this script
del df
del df_temp
del mdf
del wdf

gc.collect()
print('Garbage collected; memory reallocated')

# =============================================================================
# %% Create alphabetical list of layer names for iterating over. Note: The
# order of `keylist` is also the order in which layers will be added to the gpkg
# =============================================================================
keylist = sorted(list(data4gpkg.keys()))
print(*keylist, sep='\n')

# # For testing
# keylist = [
#     'Crude_Oil_Refineries',
#     'Equipment_and_Components',
#     'Gathering_and_Processing',
#     'Injection_and_Disposal',
#     'LNG_Facilities',
#     'Natural_Gas_Compressor_Stations',
#     'Natural_Gas_Flaring_Detections',
#     'Offshore_Platforms',
#     'Oil_Natural_Gas_Pipelines',
#     'Oil_and_Natural_Gas_Basins',
#     'Oil_and_Natural_Gas_Fields',
#     'Oil_and_Natural_Gas_License_Blocks',
#     'Oil_and_Natural_Gas_Wells',
#     'Petroleum_Terminals',
#     'Stations_Other',
#     'Tank_Battery'
# ]

# =============================================================================
# %% ITERATE THRU EACH LAYER, add to geopackage
# =============================================================================
# Change current directory to Bottom Up Infra Inventory folder
os.chdir(buii_path)
# Reset the OGIM_ID counter
last_ogim_id = 0

print(f'Starting Data Consolidation process at {str(datetime.now())} \n')
starttime = datetime.now()

# Keep a running list of what sources are used in the final GPKG
src_ids_in_gpkg = []

# Keep list of all OGIM_ID values that end up in the final layer
all_ogim_ids = []
all_ogim_ids_bylayer = {}

# Process each infrastructure type, one at a time.
for layername in keylist:

    print('==================================================================')
    starttime_loop = datetime.now()

    gdf = data4gpkg[layername]

    # Fix some numeric values that are appearing as "NoneType",
    # so that `data_quality_checks` doesn't throw an error
    if 'PIPE_DIAMETER_MM' in gdf.columns:
        gdf['PIPE_DIAMETER_MM'] = gdf['PIPE_DIAMETER_MM'].fillna(NULL_NUMERIC)
    if 'PIPE_LENGTH_KM' in gdf.columns:
        gdf['PIPE_LENGTH_KM'] = gdf['PIPE_LENGTH_KM'].fillna(NULL_NUMERIC)

    # Because the check_invalid_geometries step relies on unique IDs in OGIM_ID,
    # temporarily reset it
    gdf['OGIM_ID'] = np.arange(0, len(gdf))

    print(f'Beginning data quality checks for {layername}.....\n')
    with HiddenPrints():
        gdf = data_quality_checks(gdf, starting_ogim_id=last_ogim_id + 1)
    print(f'Data quality checks for {layername} complete! \n')

    # Advance the OGIM_ID value counter. Next infra type that gets looped over
    # will start numbering where the previous infra category left off.
    last_ogim_id = gdf.OGIM_ID.iloc[-1]

    # If the gdf contains polygons, search for invalid geometries and repair
    # any that are encountered.
    if any(x in layername.lower() for x in ['basin', 'field', 'block']):
        gdf = repair_invalid_polygon_geometries(gdf, geom_col_name='geometry')

    # -------------------------------------------------------------------------
    # Ensure there's just one unique value in the CATEGORY attribute of a layer
    # If there's not, re-assign all rows the most common existing value for CATEGORY
    gdf = standardize_category_field(gdf, 'CATEGORY')

    # -------------------------------------------------------------------------
    # Handle any missing values that may have been overlooked
    gdf = replace_missing_strings_with_na(gdf, ['COUNTRY',
                                                'STATE_PROV',
                                                'ON_OFFSHORE',
                                                'FAC_NAME',
                                                'FAC_ID',
                                                'FAC_TYPE',
                                                'FAC_STATUS',
                                                'OGIM_STATUS',
                                                'OPERATOR',
                                                'COMMODITY',
                                                'DRILL_TYPE',  # include in future runs
                                                'PIPE_MATERIAL'
                                                ])

    # gdf = replace_missing_numbers_with_na(gdf, ['LIQ_CAPACITY_BPD',
    #                                             'LIQ_THROUGHPUT_BPD',
    #                                             'GAS_CAPACITY_MMCFD',
    #                                             'GAS_THROUGHPUT_MMCFD',
    #                                             'NUM_STORAGE_TANKS',
    #                                             'NUM_COMPR_UNITS',
    #                                             'SITE_HP'
    #                                             ])

    gdf = replace_missing_dates_with_na(gdf, ['SRC_DATE',
                                              'INSTALL_DATE',
                                              'SPUD_DATE',
                                              'COMP_DATE'
                                              ])

    # -------------------------------------------------------------------------
    # Replace whatever value is in the SRC_DATE column and populate it with the
    # SRC_DATE that is recorded in the Data Catalog table.
    gdf['SRC_DATE'] = [get_src_date_from_ref_id(x, catalog_ix) for x in gdf['SRC_REF_ID']]

    # -------------------------------------------------------------------------
    # FILL ON_OFFSHORE COLUMN
    # For the Offshore_Platforms layer, assign all records as being offshore.
    # For every other layer, use the `assign_offshore_attribute` function

    if 'offshore' in layername.lower():
        gdf['ON_OFFSHORE'] = 'OFFSHORE'

    else:
        gdf = assign_offshore_attribute(
            gdf,
            boundary_geoms=my_boundary_geoms,
            overwrite_onoff_field=True)
        # Some records might fail the on/offshore assignment in the previous step
        # Fill the null values with 'OFFSHORE'  # FIXME later
        gdf['ON_OFFSHORE'] = gdf['ON_OFFSHORE'].fillna('OFFSHORE')

    # -------------------------------------------------------------------------
    # FILL COUNTRY COLUMN for JUST line and polygon layers
    if any(x in layername.lower() for x in ['pipeline', 'basin', 'field', 'block']):
        gdf = assign_countries_to_feature(
            gdf,
            gdf_country_colname='COUNTRY',
            gdf_uniqueid_field='OGIM_ID',
            boundary_geoms=my_boundary_geoms,
            overwrite_country_field=True)

    gdf.COUNTRY = gdf.COUNTRY.str.upper()

    # -------------------------------------------------------------------------
    # FILL STATE_PROV COLUMN
    # For line and polygon features, only assign a STATE_PROV value(s) if the
    # feature is in the US or Canada. For point features, assign every record
    # a STATE_PROV value if it doesn't already have one. (No overwriting)
    if any(x in layername.lower() for x in ['pipeline', 'basin', 'field', 'block']):
        gdf = assign_stateprov_to_feature(
            gdf,
            gdf_stateprov_colname='STATE_PROV',
            gdf_uniqueid_field='OGIM_ID',
            boundary_geoms=my_state_geoms,
            limit_assignment_to_usa_can=True,  # important: this param is True
            overwrite_stateprov_field=True)

    else:
        gdf = assign_stateprov_to_feature(
            gdf,
            gdf_stateprov_colname='STATE_PROV',
            gdf_uniqueid_field='OGIM_ID',
            boundary_geoms=my_state_geoms,
            limit_assignment_to_usa_can=False,
            overwrite_stateprov_field=False)

        # For point-type records where STATE_PROV is N/A, if the ON_OFFSHORE
        # value is 'OFFSHORE', make the STATE_PROV value 'OFFSHORE WATERS'
        no_state_plus_offshore_mask = (gdf.STATE_PROV.isin(['N/A', np.nan, None]) & gdf.ON_OFFSHORE == 'OFFSHORE')
        gdf.loc[no_state_plus_offshore_mask, 'STATE_PROV'] = 'OFFSHORE WATERS'

    gdf.STATE_PROV = gdf.STATE_PROV.str.upper()

    # -------------------------------------------------------------------------
    # STANDARDIZE COUNTRY NAMES
    gdf.rename(columns={"COUNTRY": "COUNTRY_OLD"}, inplace=True)
    gdf = standardize_countries(gdf,
                                'COUNTRY_OLD',
                                'COUNTRY',
                                path_to_country_csv=countrycsv_fp)

    # Move the new standardized COUNTRY column next to STATE_PROV
    loc_of_country_col = gdf.columns.get_loc('COUNTRY_OLD')
    gdf.insert(loc_of_country_col, 'COUNTRY', gdf.pop('COUNTRY'))
    gdf.drop('COUNTRY_OLD', axis=1, inplace=True)

    # -------------------------------------------------------------------------
    # CREATE REGION COLUMN
    gdf = add_region_column(gdf,
                            'COUNTRY',
                            path_to_country_csv_=countrycsv_fp)

    # -------------------------------------------------------------------------
    # REMOVE ANY TEMPORARY FIELDS BEFORE WRITING OUTPUT
    if 'index_right' in gdf.columns:
        gdf = gdf.drop('index_right', axis=1)
    if 'STATENAME' in gdf.columns:
        gdf = gdf.drop('STATENAME', axis=1)

    # Reset dataframe indices (note, this does NOT reset the OGIM IDs)
    gdf_ = gdf.reset_index(drop=True)

    # -------------------------------------------------------------------------
    # Record what SRC_IDs and OGIM_IDs are present in this layer, plus the
    # infrastructure layer each OGIM_ID is found in
    src_ids_in_gpkg.append(gdf_.SRC_REF_ID.unique())
    all_ogim_ids.extend(list(gdf_.OGIM_ID))
    all_ogim_ids_bylayer[layername] = list(gdf_.OGIM_ID)

    # -------------------------------------------------------------------------
    # Write just this layer to a GeoJSON
    # print(f'Writing layer {layername} as a geoJSON...')
    # gdf_.to_file(final_layers + layername + ".geojson",
    #              driver="GeoJSON",
    #              encoding="utf-8")

    # -------------------------------------------------------------------------
    # Export this layer to a GeoPackage
    print(f'Writing layer {layername} to final Geopackage... {fp_of_output_gpkg}')
    gdf_.to_file(fp_of_output_gpkg,
                 layer=layername,
                 driver="GPKG",
                 encoding="utf-8")

    endtime_duration = datetime.now() - starttime
    print(f'Completed data consolidation for {layername} at {str(datetime.now())}')
    print(f'Duration: {str(endtime_duration)}\n')

    # -------------------------------------------------------------------------
    # Create excel output report
    file_name = layername + "_" + timestr + "_.xlsx"
    out_put_path = excel_report_folder + file_name

    # If user has specified that Excel reports should treat US states like
    # countries, then TEMPORARILY change values in the COUNTRY column to
    # include the STATE_PROV value instead (for USA records only)
    if treat_US_states_as_countries_in_wells_report and (layername == 'Oil_and_Natural_Gas_Wells'):
        usa_str = 'UNITED STATES OF AMERICA'
        gdf_.loc[gdf_.COUNTRY == usa_str, 'COUNTRY'] = gdf_['STATE_PROV']

    # Generate report
    print(f'Creating Excel report for {layername}')
    with HiddenPrints():
        create_internal_review_spreadsheet(gdf_, out_put_path)

    # -------------------------------------------------------------------------

endtime_duration = datetime.now() - starttime
print(f'Completed data consolidation for GEOPACKAGE at {str(datetime.now())}')
print(f'Duration: {str(endtime_duration)}\n')

# =============================================================================
# %% CHECK that OGIM_ID is unique and consecutive across the entire geopackage
# =============================================================================

# Check that each OGIM_ID value appears only once in the geopackage
if all(pd.Series(all_ogim_ids).value_counts() == 1):
    print('Yay! No duplicate OGIM_IDs')

else:
    print('WARNING: There are duplicate OGIM_IDs')
    counts = pd.Series(all_ogim_ids).value_counts()
    dupes = list(counts[counts > 1].index)
    print('Printing duplicate OGIM_IDs and the infra layer they appear in...')
    for num in dupes:
        for key in all_ogim_ids_bylayer.keys():
            if num in all_ogim_ids_bylayer[key]:
                print(f'{key} - {num} appears {counts[num]} times')

# Check if OGIM_IDs are consecutive, with no breaks in between
# e.g., "101", "102", "103"....
if all(np.diff(sorted(all_ogim_ids)) == 1):
    print('Yay! OGIM_IDs are all consecutive')
else:
    print('WARNING: There are non-consecutive OGIM_IDs')
    # TODO - find a way to print the offending non-consecutive IDs
    # l = np.array(sorted(lnoconsec))
    # booleanmask = (np.diff(l) != 1)

# =============================================================================
# %% Add Data Catalog
# =============================================================================
# Subset the whole data catalog to only Source IDs that appear in the
# geopackage I just created
catalog_subset = keep_only_cited_sources(catalog, src_ids_in_gpkg)
catalog_subset['geometry'] = None
catalog_subset = gpd.GeoDataFrame(catalog_subset)

# Write catalog layer to GeoPackage
# ==============================
catalog_subset.to_file(fp_of_output_gpkg,
                       layer='Data_Catalog',
                       driver="GPKG",
                       encoding="utf-8")

# Check that all layers were written to GeoPackage as expected by reading
# the layer titles with fiona
print(*fiona.listlayers(fp_of_output_gpkg), sep='\n')

# =============================================================================
# %% OPTIONAL - After CLEARING everything in your Spyder window,
# read in the gdf you just created to confirm that all records cite a SRC_ID
# that's actually in the Data Catalog.
# =============================================================================
import os
import geopandas as gpd
import fiona
from tqdm import tqdm
import matplotlib.pyplot as plt

path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from data_consolidation_utils import confirm_src_ids_match_catalog_entries

os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory')
version_num = 'v2.7'
fp_of_output_gpkg = f'OGIM_{version_num}\\OGIM_{version_num}_{timestr}.gpkg'

ogim = {}

# Read in OGIM
for lyr in tqdm(fiona.listlayers(fp_of_output_gpkg)):
    if lyr == 'Data_Catalog':
        catalog = gpd.read_file(fp_of_output_gpkg, layer=lyr)
    else:
        ogim[lyr] = gpd.read_file(fp_of_output_gpkg, layer=lyr)


confirm_src_ids_match_catalog_entries(ogim, catalog)

# =============================================================================
# %% OPTIONAL - Plot each layer of the geopackage
# =============================================================================
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))

for lyr in ogim.keys():
    try:
        base = world.plot(color='white', edgecolor='grey')
        ogim[lyr].plot(ax=base, color='blue')
        # Add a title to the plot using Matplotlib, and display it
        plt.title(lyr)
        plt.show()
    except ValueError as e:
        print("Error:", e)  # Print the specific ValueError message
