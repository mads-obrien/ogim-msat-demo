# -*- coding: utf-8 -*-
"""
Created on Thu Feb 22 10:04:44 2024

Functions used during data consolidation / geopackage creation

@author: maobrien
"""
import os
import sys
import pandas as pd
import geopandas as gpd
import glob
import numpy as np
import datetime


def read_files_by_keyword(folderpath, file_suffix, keyword):
    """Read all files of a certain type, and append them, if their filename contains a keyword"""
    wildcard = '*' + keyword + '*.' + file_suffix
    files = glob.glob(folderpath + '\\' + wildcard)
    all_data = []

    if files:  # if there are files containing the keyword...

        for file in files:
            print(f'------> {os.path.basename(file)}')
            gdf_ = gpd.read_file(file)
            all_data.append(gdf_)
        # Concatenate
        data = pd.concat(all_data).reset_index(drop=True)
        print(f'\nTotal # of records for {keyword} = {data.shape[0]}')
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
    # Remove any string "N/A values from src_list, if they exist
    if 'N/A' in src_list:
        src_list.remove('N/A')
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

            gdf[column] = gdf[column].replace(possible_missing_values, '1900-01-01')

    return gdf


def standardize_category_field(gdf, category_field):
    # Map any inconsistent values we might *expect* to appear (the keys)
    # to one correct, standardized value
    wrong2rightcategories = {
        'REFINERIES': 'CRUDE OIL REFINERIES',
        'REFINERY': 'CRUDE OIL REFINERIES',
        'CRUDE OIL REFINERY': 'CRUDE OIL REFINERIES',
        'EQUIPMENT': 'EQUIPMENT AND COMPONENTS',
        'NATURAL GAS PROCESSING PLANT': 'GATHERING AND PROCESSING',
        'PROCESSING PLANT': 'GATHERING AND PROCESSING',
        'NATURAL GAS PROCESSING FACILITIES': 'GATHERING AND PROCESSING',
        'INJECTION': 'INJECTION AND DISPOSAL',
        'LNG': 'LNG FACILITIES',
        'LNG FACILITY': 'LNG FACILITIES',
        'COMPRESSOR STATIONS': 'NATURAL GAS COMPRESSOR STATIONS',
        'NATURAL GAS COMPRESSOR STATION': 'NATURAL GAS COMPRESSOR STATIONS',
        'NATURAL GAS FLARES': 'NATURAL GAS FLARING DETECTIONS',
        'FLARES': 'NATURAL GAS FLARING DETECTIONS',
        'FLARING DETECTIONS': 'NATURAL GAS FLARING DETECTIONS',
        'OFFSHORE PLATFORMS': 'OFFSHORE PLATFORMS',
        'BASINS': 'OIL AND NATURAL GAS BASINS',
        'FIELDS': 'OIL AND NATURAL GAS FIELDS',
        'OIL AND GAS FIELDS': 'OIL AND NATURAL GAS FIELDS',
        'OIL AND NATURAL GAS BLOCKS': 'OIL AND NATURAL GAS LICENSE BLOCKS',
        'LICENSE BLOCKS': 'OIL AND NATURAL GAS LICENSE BLOCKS',
        'PIPELINES': 'OIL AND NATURAL GAS PIPELINES',
        'WELLS': 'OIL AND NATURAL GAS WELLS',
        'TERMINAL': 'PETROLEUM TERMINALS',
        'TERMINALS': 'PETROLEUM TERMINALS',
        'STATIONS-OTHER': 'STATIONS - OTHER',
        'STATIONS: OTHER': 'STATIONS - OTHER',
        'TANK BATTERY': 'TANK BATTERIES',
        'BATTERIES': 'TANK BATTERIES',
        'BATTERY': 'TANK BATTERIES',
        'TANKS': 'TANK BATTERIES',
        'TANK': 'TANK BATTERIES'
    }
    # Map any "incorrect" / inconsistent CATEGORY values to the proper ones
    gdf[category_field] = gdf[category_field].map(wrong2rightcategories).fillna(gdf[category_field])

    # Confirm that there's just 1 unique value in the CATEGORY column of the gdf
    # If there's not, raise an exception
    if len(gdf[category_field].unique()) != 1:
        print(*gdf[category_field].unique(), sep=', ')
        print('WARNING: There are still multiple CATEGORY values in this gdf')
    return gdf


def drop_non_oilgas_wells(df, fac_type_col):
    types2drop = ['ABANDONED LOCATION, NEVER DRILLED',
                  'CAVEMAN BORE',
                  'CAVEMAN STORAGE BORE',
                  'COAL DRILLING',
                  'CORE HOLE',
                  'CORE TEST',
                  'DOMESTIC WATER',
                  'DOMESTIC WATER WELL',
                  'GROUT WELL',  # well that is used to eliminate water influx at potash operations.
                  'HARD COAL BORE',
                  'HELIUM WELL',
                  'LANDFILL DRILLING',
                  'LANDOWNER WATER WELL',
                  'LITHIUM WELL',
                  'LOST HOLE',
                  'MINERAL OR THERMAL WATER',
                  'OTHER MINERALS',
                  'PART 625 TEST WELL',
                  'POTASH WELL',
                  'POTASH FREEZE HOLE (',
                  'POTASH SHAFT HOLE',
                  'POTASH SOLUTIONING WELL',
                  'POTASH TEST WELL',
                  'POTASH WASTE DISPOSAL WELL',
                  'SALT DRILLING (ROCK SALT AND POTASH)'
                  'SEISMIC EXPLORATORY WELL',
                  'SHAFT (NO DRILLING!)',
                  'STRAT TEST',
                  'STRATIGRAPHIC',
                  'STRATIGRAPHIC TEST',
                  'STRATIGRAPHIC TEST WELL',
                  'STRATIGRAPHIC TEST WITH RECORDS RELEASED TO THE PUBLIC',
                  'STRATIGRAPHIC TEST, NON OIL/GAS RELATED (HAS DOWN-HOLE LOG)',
                  'TEST WELL',
                  'WATER WELL (DOMESTIC)',
                  'WATER WELL, NON OIL/GAS RELATED (HAS DOWN-HOLE LOG)'
                  ]

    df = df[~df[fac_type_col].isin(types2drop)]
    return df.reset_index(drop=True)


def create_ogim_status_column(gdf, wells=False,
                              wells_status_dict=None,
                              midstream_status_dict=None):

    # Use our existing status dictionaries to map FAC_STATUS values
    if 'FAC_STATUS' in gdf.columns:
        if wells is True:
            gdf['OGIM_STATUS'] = gdf['FAC_STATUS'].map(wells_status_dict).fillna('Error')
        else:
            gdf['OGIM_STATUS'] = gdf['FAC_STATUS'].map(midstream_status_dict).fillna('Error')

        # Move OGIM_STATUS column position right next to FAC_STATUS position
        # by getting column index of FAC_STATUS, and setting index of
        # OGIM_STATUS one to the right
        loc_of_fac_status = gdf.columns.get_loc('FAC_STATUS')
        gdf.insert(loc_of_fac_status + 1, 'OGIM_STATUS', gdf.pop('OGIM_STATUS'))

        if 'Error' in gdf.OGIM_STATUS.unique():
            print('\nWARNING: Some FAC_STATUS values were not mapped to an OGIM_STATUS')
            print(gdf.FAC_STATUS[gdf.OGIM_STATUS == 'Error'].value_counts())
            print('\nCountries containing these FAC_STATUS values:')
            print(gdf.COUNTRY[gdf.OGIM_STATUS == 'Error'].value_counts())


def _get_src_date_from_single_ref_id(src_ref_id, catalog_):
    '''Use a (singular) SRC_REF_ID value to look up and get a SRC_DATE from the Data Catalog

    Parameters
    ----------
    src_ref_id : str
        A valid SRC_REF_ID value that is present in the Data Catalog. Must be
        in the format of a single number/source, e.g., '82' or '114'.

    catalog_ix : Pandas DataFrame
        The up-to-date OGIM Data Catalog table, with the 'SRC_ID' column
        specified as the index of the dataframe.

    Returns
    -------
    out_src_date : str
        The SRC_DATE (in 'YYYY-MM-DD' format) that corresponds with the
        SRC_REF_ID provided by the user, from the Data Catalog table.

    '''
    # Check that user-provided SRC_REF_ID is a string; if not, cast it.
    if type(src_ref_id) != str:
        src_ref_id = src_ref_id.astype(str)

    # Confirm that the 'SRC_ID' column is being used as the Data Catalog's
    # index. If not, set it.
    if catalog_.index.name != 'SRC_ID':
        # catalog_ix = catalog_ix.copy().set_index('SRC_ID')
        print('ERROR: Set the index of the Data Catalog equal to SRC_ID first')
        return

    # Error handling if the SRC_ID input isn't present in data catalog
    if src_ref_id not in catalog_.index:
        print(f'ERROR: user-provided SRC_REF_ID "{src_ref_id}" is not present in this Data Catalog')
        return

    year = catalog_.loc[src_ref_id].SRC_YEAR
    month = catalog_.loc[src_ref_id].SRC_MNTH
    day = catalog_.loc[src_ref_id].SRC_DAY

    # If Month or Day is missing, fill with our default value of "1"
    if pd.isna(month):
        month = 1.0  # January
    if pd.isna(day):
        day = 1.0  # first of the month
    # Convert floats to integers before combining them into a date object
    year_, month_, day_ = [np.int64(x) for x in [year, month, day]]
    out_src_date = datetime.date(year_, month_, day_).strftime("%Y-%m-%d")

    return out_src_date


def get_src_date_from_ref_id(src_ref_id, catalog_):
    '''Wrapper for `_get_src_date_from_single_ref_id` that handles multi-source SRC_REF_IDs.

    Parameters
    ----------
    src_ref_id : str
        A valid SRC_REF_ID value that is present in the Data Catalog.
        SRC_REF_ID may be in the format of a single number/source ('82'), or
        multiple numbers/sources separated by commas ('40,41').

    catalog_ : Pandas DataFrame
        The up-to-date OGIM Data Catalog table.

    Returns
    -------
    out_src_date : str
        The SRC_DATE (in 'YYYY-MM-DD' format) that corresponds with the
        SRC_REF_ID provided by the user, from the Data Catalog table.
        If a SRC_REF_ID refers to multiple sources / entries in the Data
        Catalog table with different dates, only the *most recent date* is returned.

    Example usage
    -------
    get_src_date_from_ref_id('3', data_catalog_df)
    >> '2024-04-18'
    get_src_date_from_ref_id('26, 27', data_catalog_df)
    >> '2024-04-19'

    '''
    # Check that user-provided SRC_REF_ID is a string; if not, cast it.
    if type(src_ref_id) != str:
        src_ref_id = src_ref_id.astype(str)

    # Check if the 'SRC_ID' column is being used as the Data Catalog's
    # index. If not, set it.
    if catalog_.index.name != 'SRC_ID':
        # catalog_ix = catalog_ix.copy().set_index('SRC_ID')
        print('ERROR: Set the index of the Data Catalog equal to SRC_ID first')
        return

    # Handle SRC_REF_IDs with comma-separated multiple SRC_IDs
    if ',' in src_ref_id:

        date_list = []

        ids = src_ref_id.split(',')
        ids = [s.strip(' ') for s in ids]

        for src in ids:
            src_date = _get_src_date_from_single_ref_id(src, catalog_)
            date_list.append(src_date)

        # Once date_list is populated, convert string dates to datetime.date() objects
        date_list_dt = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in date_list]
        # Get most recent date, then convert the result back to a string
        most_recent_date = max(date_list_dt)
        out_src_date = most_recent_date.strftime("%Y-%m-%d")

        return out_src_date

    #  Handle SRC_REF_IDs with a single SRC_ID
    else:
        out_src_date = _get_src_date_from_single_ref_id(src_ref_id, catalog_)
        return out_src_date


def confirm_src_ids_match_catalog_entries(ogim_dict, catalog_df):
    '''Check that all SRC_REF_ID values correspond to an existing SRC_ID in the Data Catalog.

    Given a specific version of the OGIM GeoPackage, this function checks
    whether all of the SRC_REF_ID values across all infrastructure layers
    correspond to an existing record (SRC_ID) in the Data Catalog *specific to
    that GeoPackage version.*

    Parameters
    ----------
    ogim_dict : Dictionary
        A dictionary object, where the keys are OGIM layer names and the values
        are GeoDataFrames of the infrastructure data. Make sure that ALL
        existing layers of OGIM are included in this dictionary.
    catalog_df : DataFrame
        The Data Catalog layer included in this version of OGIM / GeoPackage.

    Returns
    -------
    None.

    '''
    if 'Data_Catalog' in ogim_dict.keys():
        del ogim_dict['Data_Catalog']

    # Create then flatten a list of all SRC_IDs present in the infrastructure layers
    src_ids_in_gpkg = []
    for i, gdf in ogim_dict.items():
        src_ids_in_gpkg.append(gdf.SRC_REF_ID.unique())

    # flatten the list of SRC_REF_IDs, in case the list is nested
    flat_list = [item for sublist in src_ids_in_gpkg for item in sublist]
    # split multi-source list values into individual SRC_ID numbers, for example "80, 82"
    src_list = []
    for number in flat_list:
        src_list.extend(number.split(","))
    # strip trailing spaces from any SRC_REF_ID strings
    src_list = [s.strip() for s in src_list]

    # Make sure the SRC_ID numbers you're comparing are the same format as the catalog's
    if catalog_df.SRC_ID.dtype == 'int64':
        src_list = [int(s) for s in src_list]

    # Does the SRC_REF_ID that appears in OGIM records correspond with
    # an entry in the data catalog?
    for num in src_list:
        if num not in catalog_df.SRC_ID.unique():
            print(f'Warning: SRC_REF_ID {i} is not in the Data Catalog of this GeoPackage')
    print("Completed: all SRC_REF_IDs have been checked.")
