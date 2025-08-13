# -*- coding: utf-8 -*-
"""
Created on 15 August 2024

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

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.6'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'
os.chdir(buii_path)

# get string of today's date
timestr = time.strftime("%Y-%m-%d")

# set filepath(s) where final data will be saved
integration_folder = f'OGIM_{version_num}\\integrated_results\\'
final_layers = f'OGIM_{version_num}\\layercreation_results_{timestr}\\'
excel_report_folder = f'OGIM_{version_num}\\layercreation_results_{timestr}\\excel_reports\\'
fp_of_output_gpkg = f'OGIM_{version_num}\\OGIM_{version_num}.gpkg'

# check that your desired output directories exist before proceeding
print(f'integrated_results folder exists -- {os.path.exists(integration_folder)}')
print(f'layercreation_results_{timestr} folder exists -- {os.path.exists(final_layers)}')
print(f'excel_reports folder exists -- {os.path.exists(excel_report_folder)}')

# set filepaths to supporting CSVs in our GitHub repository
countrycsv_fp = path_to_github + 'docs\\UN_countries_IEA_regions.csv'
wells_status_dict_fp = path_to_github + 'analysis_workflows\\wells_status_dictionary.csv'
midstream_status_dict_fp = path_to_github + 'analysis_workflows\\midstream_status_dictionary.csv'

# Should QA Excel reports be aggregated by COUNTRY (default) or STATE_PROV?
excel_reports_by_state_prov = True


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

# Check catalog for missing values in attributes that don't allow them.
check_df_for_allowed_nans(catalog)

# =============================================================================
# %% LOAD ALL INFRA DATA that I want to combine into one geopackage
# These are OGIM data layers that have been integrated but not "consolidated"
# Code block will take ~ 20-30 minutes to read in all data
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

data4geopackage = {}

for kwd, lyr in zip(keywords.keys(), keywords.values()):
    print(f'==========================\nNow reading in integrated {lyr}...\n')
    df = read_files_by_keyword(os.getcwd(), 'geojson', kwd)
    print(datetime.now())
    if df is not None:
        data4geopackage[lyr] = df
        print('Concatenated gdf copied to our dictionary for consolidation\n')

print(f'All individual integrated files successfully read in at {str(datetime.now())}')

# =============================================================================
# %% Fix some quirks in the US HIFLD datasets
# TODO - ensure these location abbreviations are dealt with in the original
# data integration script, and not here
# =============================================================================
# Some US territories were not properly un-abbreviated
for key in data4geopackage.keys():
    data4geopackage[key]['STATE_PROV'].replace({' WASHINGTON': 'WASHINGTON',
                                                'PR': 'PUERTO RICO',
                                                'GM': 'GUAM',  # despite the fact the proper abbrev is 'GU'...
                                                'MP': 'NORTHERN MARIANA ISLANDS',
                                                'VI': 'US VIRGIN ISLANDS'},
                                               inplace=True)

# =============================================================================
# %% Create OGIM_STATUS field; print out any statuses that couldn't be mapped
# =============================================================================
for key, gdf in data4geopackage.items():
    print(f'\nStandardizing status column for {key}.....')
    if 'wells' in key.lower():
        create_ogim_status_column(gdf, wells=True,
                                  wells_status_dict=wells_status_dict)
    else:
        create_ogim_status_column(gdf, wells=False,
                                  midstream_status_dict=midstream_status_dict)

# =============================================================================
# %% Remove certain non-O&G or never-drilled wells
# =============================================================================
# Remove non-O&G well types, e.g., stratigraphic test wells, mineral wells, domestic water wells
data4geopackage['Oil_and_Natural_Gas_Wells'] = drop_non_oilgas_wells(data4geopackage['Oil_and_Natural_Gas_Wells'],
                                                                     'FAC_TYPE')
# check that this worked
print(*sorted(data4geopackage['Oil_and_Natural_Gas_Wells'].FAC_TYPE.unique()), sep='\n')

# Remove some wells based on STATUS, e.g. Never Drilled
ogimstatus2drop = ['NOT DRILLED - DROP', 'INSUFFICIENT LOCATION - DROP']
data4geopackage['Oil_and_Natural_Gas_Wells'] = data4geopackage['Oil_and_Natural_Gas_Wells'].query('OGIM_STATUS not in @ogimstatus2drop')
data4geopackage['Oil_and_Natural_Gas_Wells'] = data4geopackage['Oil_and_Natural_Gas_Wells'].reset_index(drop=True)

# check that this worked
print(*sorted(data4geopackage['Oil_and_Natural_Gas_Wells'].OGIM_STATUS.unique()), sep='\n')

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
for key in data4geopackage.keys():
    if 'AGGREGATE_QUALITY_SCORE' in data4geopackage[key].columns:
        data4geopackage[key] = data4geopackage[key].drop(columns2remove, axis=1)
        print(f'Data quality columns dropped in {key}')

# Confirm that dropping the columns worked by looking at all attribute lists
for key in data4geopackage.keys():
    print('==================')
    print(key)
    print(*data4geopackage[key].columns, sep='\n')

# =============================================================================
# %% Search for duplicate records (takes 10ish min)
# =============================================================================
dupes = {}

# Search for duplicates
for lyr, gdf in data4geopackage.items():
    # TODO -- un-comment this section once we're using geopandas v0.10.0
    # if any(x in lyr.lower() for x in ['pipeline', 'basin', 'field', 'block']):
    #     gdf = gdf.geometry.normalize()
    gdf_cols = list(gdf.columns)  # This list includes the `geometry` column
    gdf_cols.remove('OGIM_ID')
    dupes[lyr] = gdf[gdf.duplicated(subset=gdf_cols, keep=False)]

# What countries are the records in?
for lyr, gdf in dupes.items():
    if not gdf.empty:
        print('\n--------------------------')
        print(f'{lyr} -- {len(dupes[lyr])} duplicate records found\n')
        print(gdf.COUNTRY.value_counts())
        print('\n')
        print(gdf.STATE_PROV.value_counts())

# =============================================================================
# %% Create alphabetical list of layer names for iterating over
# Note: The order of `keylist` is also the order of how layers will be added to the geopackage
# =============================================================================
keylist = sorted(list(data4geopackage.keys()))
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
os.chdir(buii_path)
# Reset the OGIM_ID counter
last_ogim_id = 0

print(f'Starting Data Consolidation process at {str(datetime.now())} \n')
starttime = datetime.now()

# Keep a running list of what sources are used in the final GPKG
src_ids_in_gpkg = []

# Keep list of all OGIM_ID values that end up in the final layer
all_ogim_ids = pd.Series()
all_ogim_ids_bylayer = {}

# Process each infrastructure type, one at a time.
for layername in keylist:

    print('==================================================================')
    starttime_loop = datetime.now()

    gdf = data4geopackage[layername]

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
    # Advance the OGIM_ID value counter
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
                                                # 'DRILL_TYPE',  # include in future runs
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

    # Check an OGIM dataframe for missing values in attributes that don't allow
    # them, and print out the results.
    # print(f'Checking {gdf} for nulls in columns where nulls are forbidden....')
    # check_df_for_allowed_nans(gdf)

    # -------------------------------------------------------------------------
    # Replace whatever value is in the SRC_DATE column and populate it with the
    # SRC_DATE that is recorded in the Data Catalog table
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
    # For line and polygon features, only assign a STATE_PROV value if the
    # feature is in the US or Canada. For point features, assign every record
    # a STATE_PROV value
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
            limit_assignment_to_usa_can=False,  # important: this param is False
            overwrite_stateprov_field=True)

        # For point-type records where STATE_PROV is N/A, if the ON_OFFSHORE
        # value is 'OFFSHORE', make the STATE_PROV value 'OFFSHORE WATERS'
        missing_state_and_offshore_mask = (gdf.STATE_PROV.isin(['N/A', np.nan, None]) & gdf.ON_OFFSHORE == 'OFFSHORE')
        gdf.loc[missing_state_and_offshore_mask, 'STATE_PROV'] = 'OFFSHORE WATERS'

    gdf.STATE_PROV = gdf.STATE_PROV.str.upper()

    # -------------------------------------------------------------------------
    # STANDARDIZE COUNTRY NAMES
    gdf.rename(columns={"COUNTRY": "COUNTRY_OLD"}, inplace=True)
    gdf = standardize_countries(gdf,
                                'COUNTRY_OLD',
                                'COUNTRY',
                                path_to_country_csv=countrycsv_fp)

    # Move COUNTRY_NEW column position right next to COUNTRY position
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

    # Reset indices (note, this does NOT reset the OGIM IDs)
    gdf_ = gdf.reset_index(drop=True)

    # -------------------------------------------------------------------------
    # Record what SRC_IDs and OGIM_IDs are present in this layer, plus the
    # infrastructure layer each OGIM_ID is found in
    src_ids_in_gpkg.append(gdf_.SRC_REF_ID.unique())
    all_ogim_ids = all_ogim_ids.append(gdf_.OGIM_ID)
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

    # If user has specified that Excel reports should be summarized by the
    # STATE_PROV field as opposed to the country field, then TEMPORARILY change
    # values in the COUNTRY column to include the record's STATE_PROV value instead
    if excel_reports_by_state_prov:
        gdf_.loc[gdf_.COUNTRY == 'MEXICO', 'STATE_PROV'] = 'MEXICO'  # FIXME
        gdf_['COUNTRY'] = gdf_['STATE_PROV']

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
if all(all_ogim_ids.value_counts() == 1):
    print('Yay! No duplicate OGIM_IDs')

else:
    print('WARNING: There are duplicate OGIM_IDs')
    counts = all_ogim_ids.value_counts()
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
