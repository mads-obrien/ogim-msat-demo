# -*- coding: utf-8 -*-
"""
Created on Fri Sep  8 09:40:46 2023

Data consolidation of OGIM_v3

@author: maobrien
"""

import os
import sys
import pandas as pd
import numpy as np
import glob
import geopandas as gpd
from sigfig import round
from datetime import datetime
from tqdm import tqdm
import time

path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'

os.chdir(path_to_github + 'functions')
# from ogimlib import check_invalid_geoms
from ogimlib import *
from internal_review_protocol_Excel import *
from data_quality_scores import *
from data_quality_checks import data_quality_checks
from standardize_countries import standardize_countries, add_region_column
from assign_offshore_attribute import assign_offshore_attribute
from abbreviation_utils import *
# from hybridization import get_uniques

# -----------------------------------------------------------------------------
# set working directory to Bottom_Up_Infra_Inventory
os.chdir('C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory')

# set filepath where final data will be saved
final_layers = "Analysis\\Results\\OGIM_v3_NA\\layercreation_results_can_mex_usa\\"
excel_report_folder = "Analysis\\Results\\OGIM_v3_NA\\layercreation_results_can_mex_usa\\"
fp_of_output_gpkg = "Analysis\\Results\\OGIM_v3_NA\\OGIM_v3_NA.gpkg"

countrycsv_fp = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\docs\\UN_countries_IEA_regions.csv'

# get string of today's date
timestr = time.strftime("%Y-%m-%d")

# =============================================================================
# %% Define custom functions
# =============================================================================


def read_files_by_keyword(folderpath, file_suffix, keyword):
    """Read all files of a certain type, and append them, if their filename contains a keyword"""
    wildcard = '*' + keyword + '*.' + file_suffix
    files = glob.glob(folderpath + '\\' + wildcard)
    all_data = []

    if files:  # if there are files containing the keyword...

        for file in tqdm(files):
            gdf_ = gpd.read_file(file)
            all_data.append(gdf_)
        # Concatenate
        data = pd.concat(all_data)
        print("Total # of records = ", data.shape[0])
        print(data.head())
        return data



def keep_only_cited_sources(catalog, src_ids_in_gpkg):
    ''' Only retain Data Catalog records that are cited by the final geopackage.

    Parameters
    ----------
    catalog : Pandas DataFrame
        `ogim_standalone_source_table.xlsx` read in as a DataFrame

    src_ids_in_gpkg : list
        list of SRC_ID numbers that are actually cited in all the `SRC_REF_ID`
        columns of the OGIM geopackage

    Returns
    -------
    Pandas DataFrame containing selected data catalog rows

    '''
    # flatten the list of SRC_REF_IDs, in case the list is nested
    flat_list = [item for sublist in src_ids_in_gpkg for item in sublist]
    # split multi-source list values into individual SRC_ID numbers, for example "80, 82"
    src_list = []
    for number in flat_list:
        src_list.extend(number.split(","))
    # strip trailing spaces from any SRC_REF_ID strings
    src_list = [s.strip() for s in src_list]
    # Convert strings to integers if SRC_ID column of Data Catalog is integer
    if catalog.SRC_ID.dtype == 'int64':
        src_list = [int(s) for s in src_list]

    # Drop records from the catalog
    catalog_final = catalog.copy()
    return catalog_final[catalog_final.SRC_ID.isin(src_list)]


class HiddenPrints:
    '''Suppress printing of specific functions'''

    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w', encoding="utf-8")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def replace_missing_strings_with_na(gdf,
                                    columns2check,
                                    limit_acceptable_columns=False):
    """Replaces none-like string values in gdf columns with standard missing data marker in-place.

    Parameters
    ----------
    gdf : GeoPandas GeoDataFrame
        DESCRIPTION
    columns2check: list of strings
        list of column names in `gdf` that contain string values
    Returns
    -------
    The input `gdf` with none-like string values replaced.

    """
    possible_missing_values = ['NOT AVAILABLE',
                               'NA',
                               'NAN',
                               'UNKNOWN',
                               'UNAVAILABLE',
                               'UNCLASSIFIED',
                               'UNDESIGNATED',
                               'NO DATA',
                               'NONE',
                               'NONE_SPECIFIED',
                               '?',
                               None]

    acceptable_columns = ['COUNTRY',
                          'STATE_PROV',
                          'ON_OFFSHORE',
                          'FAC_NAME',
                          'FAC_ID',
                          'FAC_TYPE',
                          'FAC_STATUS',
                          'OGIM_STATUS',
                          'OPERATOR',
                          'COMMODITY'
                          ]

    for column in columns2check:

        if column in gdf.columns:

            if limit_acceptable_columns == True and column not in acceptable_columns:
                continue

            else:
                gdf[column] = gdf[column].astype(str)
                gdf[column] = gdf[column].str.upper()
                gdf[column] = gdf[column].replace(possible_missing_values, NULL_STRING)

    return gdf


def replace_missing_dates_with_na(gdf, columns2check):
    """Replaces none-like date values in gdf columns with standard missing data marker in-place.

    Parameters
    ----------
    gdf : GeoPandas GeoDataFrame
        DESCRIPTION.
    columns2check: list of strings
        list of column names in `gdf` that contain date values
    Returns
    -------
    The input `gdf` with none-like date values replaced.

    """
    possible_missing_values = ['NOT AVAILABLE',
                               'NA',
                               'NAN',
                               'UNKNOWN',
                               '1800-01-01',
                               None]

    acceptable_columns = ['SRC_DATE',
                          'INSTALL_DATE',
                          'SPUD_DATE',
                          'COMP_DATE'
                          ]

    for column in columns2check:

        if column in acceptable_columns and column in gdf.columns:

            gdf[column] = gdf[column].replace(possible_missing_values, NULL_DATE)

    return gdf


# =============================================================================
# %% Read in dictionaries for mapping FAC_STATUS to OGIM_STATUS
# =============================================================================
wdf = pd.read_csv(path_to_github + r'analysis_workflows\wells_status_dictionary.csv').fillna('N/A')
wells_status_dict = dict(zip(wdf.original_FAC_STATUS,
                             wdf.new_OGIM_STATUS))

mdf = pd.read_csv(path_to_github + r'analysis_workflows\midstream_status_dictionary.csv').fillna('N/A')
midstream_status_dict = dict(zip(mdf.original_FAC_STATUS,
                                 mdf.new_OGIM_STATUS))


# =============================================================================
# %% Read in boundary geometries, for on/offshore analysis
# Will take 10-20 seconds to read in
# =============================================================================
boundary_geoms = r"Public_Data\data\International_data_sets\National_Maritime_Boundaries\marine_and_land_boundaries_seamless.shp"
my_boundary_geoms = gpd.read_file(boundary_geoms)  # may take a while to read in


# =============================================================================
# %% Read OGIM standalone data catalog
# =============================================================================
fp2 = r"Public_Data\data\ogim_standalone_source_table.xlsx"
catalog = pd.read_excel(fp2, sheet_name='source_table')  # ignore warning msg about Data Validation

# Calculate REFRESH_SCORES properly
catalog = refresh_score(catalog)

# # Based on the data catalog, create dictionaries that match SRC_IDs to Source Scores and Refresh Scores
# src_catalog = dict(zip(catalog['SRC_ID'], catalog.SOURCE_SCORE))  # contains one row per source
# refresh_catalog = dict(zip(catalog['SRC_ID'], catalog.REFRESH_SCORE))  # contains one row per source


# =============================================================================
# %% Read all infrastructure layers that I want to combine into one geopackage
# These are OGIM data layers that have been integrated but not "consolidated"
# =============================================================================
os.chdir(r'Analysis\Results\OGIM_v3_NA\integrated_results')

keywords = {'basin': 'Oil_and_Natural_Gas_Basins',
            'batter': 'Tank_Battery',
            'block': 'Oil_and_Natural_Gas_License_Blocks',
            'components': 'Equipment_and_Components',
            'compressor': 'Natural_Gas_Compressor_Stations',
            'field': 'Oil_and_Natural_Gas_Fields',
            'flaring': 'Natural_Gas_Flaring_Detections',
            'gathering': 'Gathering_and_Processing',
            'injection': 'Injection_and_Disposal',
            'lng': 'LNG_Facilities',
            'offshore': 'Offshore_Platforms',
            'pipeline': 'Oil_Natural_Gas_Pipelines',
            'refin': 'Crude_Oil_Refineries',
            'stations_other': 'Stations_Other',
            'terminal': 'Petroleum_Terminals',
            'wells': 'Oil_and_Natural_Gas_Wells'}

everything = {}

for kwd, lyr in zip(keywords.keys(), keywords.values()):
    df = read_files_by_keyword(os.getcwd(), 'geojson', kwd)
    if df is not None:
        everything[lyr] = df

print(f'All individual integrated files successfully read in at {str(datetime.datetime.now())}')

# =============================================================================
# %% Fix some quirks in the US datasets
# =============================================================================
# Drop some Canada records that were in the HIFLD data but didn't get removed
# (If they are left in, they are effectively double-counting facilities provided
# by canadian sources). All the HIFLD Canadian records have a STATE_PROV of
# the provincial abbreviation
for key, df in everything.items():
    can_provs = list(can_abbrev_to_province.keys())
    everything[key] = everything[key][~everything[key].STATE_PROV.isin(can_provs)]

for key, df in everything.items():
    everything[key]['STATE_PROV'].replace({' WASHINGTON': 'WASHINGTON',
                                           'PR': 'PUERTO RICO',
                                           'GM': 'GUAM',  # despite the fact the proper abbrev is 'GU'...
                                           'MP': 'NORTHERN MARIANA ISLANDS',
                                           'VI': 'US VIRGIN ISLANDS'},
                                          inplace=True)

wellcols2remove = ['PUB_PRIV', 'ORIGINAL_SRC']
everything['Oil_and_Natural_Gas_Wells'] = everything['Oil_and_Natural_Gas_Wells'].drop(wellcols2remove, axis=1)

everything['Oil_Natural_Gas_Pipelines']['COUNTRY'].replace({'UNITED STATES ': 'UNITED STATES'},
                                                           inplace=True)

# =============================================================================
# %% Remove stratigraphic test wells and mineral wells
# =============================================================================
types2drop = ['CORE TEST',
              'HELIUM WELL',
              'LITHIUM WELL',
              'PART 625 TEST WELL',
              'POTASH WELL',
              'POTASH FREEZE HOLE (',
              'POTASH SHAFT HOLE',
              'POTASH SOLUTIONING WELL',
              'POTASH TEST WELL',
              'POTASH WASTE DISPOSAL WELL',
              'STRAT TEST',
              'STRATIGRAPHIC',
              'STRATIGRAPHIC TEST',
              'STRATIGRAPHIC TEST WELL',
              'STRATIGRAPHIC TEST WITH RECORDS RELEASED TO THE PUBLIC',
              'STRATIGRAPHIC TEST, NON OIL/GAS RELATED (HAS DOWN-HOLE LOG)',
              'TEST WELL'
              ]

everything['Oil_and_Natural_Gas_Wells'] = everything['Oil_and_Natural_Gas_Wells'].query('FAC_TYPE not in @types2drop')

# check that this worked
print(*sorted(everything['Oil_and_Natural_Gas_Wells'].FAC_TYPE.unique()), sep='\n')


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
for key in everything.keys():
    if 'AGGREGATE_QUALITY_SCORE' in everything[key].columns:
        everything[key] = everything[key].drop(columns2remove, axis=1)
        print(f'Data quality columns dropped in {key}')

# Confirm that dropping the columns worked by looking at all attribute lists
for key in everything.keys():
    print('==================')
    print(key)
    print(*everything[key].columns, sep='\n')

# =============================================================================
# %% Create list of layer names for iterating over (alphabetical)
# Note: The order of `keylist` is also the order of how layers will be added to the geopackage
# =============================================================================
keylist = list(everything.keys())
keylist = set(keylist)
keylist = sorted(list(keylist))

print(*keylist, sep='\n')


# =============================================================================
# %% Iterate through each layer, add to geopackage
# =============================================================================
os.chdir('C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory')
# Reset the OGIM_ID counter
last_ogim_id = 0

print(f'Starting Data Consolidation process at {str(datetime.datetime.now())} \n')
starttime = datetime.datetime.now()

# Keep a running list of what sources are used in the final GPKG
src_ids_in_gpkg = []

# Process each infrastructure type, one at a time.
for layername in keylist:

    print('==================================================================')
    starttime_loop = datetime.datetime.now()

    gdf = everything[layername]

    # Fix some numeric values that are appearing as "NoneType",
    # so that `data_quality_checks` doesn't throw an error
    if 'PIPE_DIAMETER_MM' in gdf.columns:
        gdf['PIPE_DIAMETER_MM'] = gdf['PIPE_DIAMETER_MM'].fillna(-999)
    if 'PIPE_LENGTH_KM' in gdf.columns:
        gdf['PIPE_LENGTH_KM'] = gdf['PIPE_LENGTH_KM'].fillna(-999)

    # Because the check_invalid_geometries step relies on unique IDs in OGIM_ID,
    # temporarily reset it
    gdf['OGIM_ID'] = np.arange(0, len(gdf))

    print(f'Beginning data quality checks for {layername}.....\n')
    # with HiddenPrints():
    gdf = data_quality_checks(gdf, starting_ogim_id=last_ogim_id + 1)

    # Advance the OGIM_ID value counter
    last_ogim_id = gdf.OGIM_ID.iloc[-1]

    # -------------------------------------------------------------------------
    # Ensure there's just one unique value in the CATEGORY attribute
    if len(gdf.CATEGORY.unique()) != 1:
        # print(' !!! WARNING: Inconsistencies in the CATEGORY attribute of ' + layername + '\n')
        # Re-assign all rows the most common existing value for CATEGORY column
        most_common_val = gdf.CATEGORY.value_counts().index[0]
        gdf['CATEGORY'] = most_common_val
        # print('CATEGORY attribute reassigned so all records equal ' + most_common_val)
    else:
        print(f'Success: Consistent CATEGORY attribute for {layername}\n')

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
                                                'COMMODITY'
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

    # -----------------------------------------------------------------------------
    # STANDARDIZE STATUS ATTRIBUTE
    print(f'Standardizing status column for {layername}.....\n')
    if 'FAC_STATUS' in gdf.columns:
        if 'wells' in layername.lower():
            gdf['OGIM_STATUS'] = gdf['FAC_STATUS'].map(wells_status_dict).fillna('Error')
        else:
            gdf['OGIM_STATUS'] = gdf['FAC_STATUS'].map(midstream_status_dict).fillna('Error')

        # get column index of FAC_STATUS column
        loc_of_fac_status = gdf.columns.get_loc('FAC_STATUS')
        # move OGIM_STATUS column position right next to FAC_STATUS position
        gdf.insert(loc_of_fac_status + 1, 'OGIM_STATUS', gdf.pop('OGIM_STATUS'))

    # !!! DROP wells with certain status values from OGIM
    if 'wells' in layername.lower():
        ogimstatus2drop = ['NOT DRILLED - DROP', 'INSUFFICIENT LOCATION - DROP']
        gdf = gdf.query('OGIM_STATUS not in @ogimstatus2drop')

    # -----------------------------------------------------------------------------
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

    # -----------------------------------------------------------------------------
    # CREATE REGION COLUMN
    gdf = add_region_column(gdf,
                            'COUNTRY',
                            path_to_country_csv_=countrycsv_fp)

    # -----------------------------------------------------------------------------
    # FILL ON_OFFSHORE COLUMN
    # gdf['ON_OFFSHORE_OLD'] = gdf['ON_OFFSHORE']

    gdf = assign_offshore_attribute(
        gdf,
        my_boundary_geoms,
        overwrite_onoff_field=True)

    # Reset indices
    gdf_ = gdf.reset_index(drop=True)

    # -----------------------------------------------------------------------------
    # Write just this layer to a GeoJSON
    print(f'Writing layer {layername} as a geoJSON...')
    gdf_.to_file(final_layers + layername + ".geojson",
                 driver="GeoJSON",
                 encoding="utf-8")

    # -----------------------------------------------------------------------------
    # Add this layer to a GeoPackage
    print('Writing layer ' + layername + ' to final Geopackage... ' + fp_of_output_gpkg)
    gdf_.to_file(fp_of_output_gpkg,
                 layer=layername,
                 driver="GPKG",
                 encoding="utf-8")

    # record what SRC_IDs are actually present
    src_ids_in_gpkg.append(gdf_.SRC_REF_ID.unique())

    endtime_duration = datetime.datetime.now() - starttime
    print(f'Completed data consolidation for {layername} at {str(datetime.datetime.now())}')
    print(f'Duration: {str(endtime_duration)}\n')

    # -----------------------------------------------------------------------------
    # Create excel output report
    # TEMPORARILY change the COUNTRY value for USA and CAN records to read the name
    # of the state/province instead, so that records get summarized by province in the
    # Excel report
    gdf_.loc[gdf_.COUNTRY != 'MEXICO', 'COUNTRY'] = gdf_['STATE_PROV']

    file_name = layername + "_" + timestr + "_.xlsx"
    out_put_path = excel_report_folder + file_name

    # Generate report
    print(f'Creating Excel report for {layername}')
    with HiddenPrints():
        create_internal_review_spreadsheet(gdf_, out_put_path)

    # -----------------------------------------------------------------------------

endtime_duration = datetime.datetime.now() - starttime
print(f'Completed data consolidation for GEOPACKAGE at {str(datetime.datetime.now())}')
print(f'Duration: {str(endtime_duration)}\n')


# =============================================================================
# %% Add Data Catalog
# =============================================================================
# Capitalize everything
cols_ = list(catalog.columns[2:])
for attr in cols_:
    catalog[attr] = [str(catalog[attr].iloc[x]).upper() for x in range(catalog.shape[0])]
catalog['geometry'] = None
catalog_ = gpd.GeoDataFrame(catalog)

catalog_ = catalog_.replace('NAN', np.nan)

# For now, remove Data Quality Score attributes from the data catalog
# along with other un-needed columns
catalog_.drop(['SOURCE_SCORE',
               'REFRESH_SCORE',
               'RICHNESS_SCORE',
               'DOWNLOAD_INSTRUCTIONS',
               'FILE_NAME',
               'ORIGINAL_FILENAME',
               'ORIGINAL_CRS'], axis=1, inplace=True)

# Subset the whole data catalog to only Source IDs that appear in the
# geopackage I just created
catalog_subset = keep_only_cited_sources(catalog_, src_ids_in_gpkg)

# Write catalog layer to GeoPackage
# ==============================
catalog_subset.to_file(fp_of_output_gpkg,
                       layer='Data_Catalog',
                       driver="GPKG",
                       encoding="utf-8")