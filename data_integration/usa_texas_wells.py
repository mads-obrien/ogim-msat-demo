# -*- coding: utf-8 -*-
"""
Created on Tuesday November 14 2023

Data integration of well data from Texas RRC

@author: maobrien
"""
# Libraries
import pandas as pd
import geopandas as gpd
import numpy as np
# from tqdm import tqdm
# import sys
# import math
import os
import datetime
from tqdm import tqdm
# import math

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_missing_strings_with_na,
                     create_concatenated_well_name, get_duplicate_api_records,
                     transform_CRS, integrate_facs)


def quickmap(gdf, _name):
    print(*gdf.columns, sep='\n')
    base = states[states.name == _name].boundary.plot(color='black')
    gdf.plot(ax=base, markersize=2)


# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Set current working directory to 'OGIM_vX.X\\data\\united_states\\texas' specific
# to my OGIM version within the Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'
my_cwd = os.path.join(buii_path,
                      f'OGIM_{version_num}',
                      'data',
                      'united_states',
                      'texas')
os.chdir(my_cwd)

# Folder in which all integrated data will be saved
outfolder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# Leave this as True while testing integration code;
# Change to False when letting code run (to speed things up)
print_maps = False

# =============================================================================
# %% Read in US state boundaries for quality assurance plotting
# =============================================================================
states = gpd.read_file(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\NaturalEarth\ne_10m_admin_1_states_provinces.shp')
states = states.filter(['name', 'admin', 'geometry'])
states = states[states.admin == 'United States of America'].reset_index(drop=True)

# =============================================================================
# %% Read in SURFACE LOCATIONS + BOTTOM LOCATIONS
# =============================================================================
print(datetime.datetime.now())
print('Texas')
# =============================================================================
# List all files in my directory, then read well surface locations
# Will take 5-ish minutes to read and concatenate them all
fp = 'wells\\'
files = os.listdir(fp)

# Empty dictionary to hold results
s_wells = {}
b_wells = {}

for file in tqdm(files):
    fips = file[4:7]
    if file.endswith('s.shp'):  # surface hole shapefiles
        s_wells[fips] = gpd.read_file(fp + file)
        # print(fips)
    if file.endswith('b.shp'):  # bottom hole shapefiles
        b_wells[fips] = gpd.read_file(fp + file)
        # print(fips)

# Concatenate all county-specific locations into one state-wide gdf
tx_wells_surf = pd.concat(s_wells.values())
tx_wells_surf = tx_wells_surf.reset_index(drop=True)
tx_wells_bot = pd.concat(b_wells.values())
tx_wells_bot = tx_wells_bot.reset_index(drop=True)

# convert surface and bottom holes from NAD27 to WGS84
tx_wells_surf = transform_CRS(tx_wells_surf,
                              target_epsg_code="epsg:4326",
                              appendLatLon=True)

tx_wells_bot = transform_CRS(tx_wells_bot,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(tx_wells_surf, 'Texas')

# =============================================================================
# %% Read in API TABLES (CONTAIN SUPPLEMENTARY ATTRIBUTES FOR WELLS)
# =============================================================================
fp2 = 'wells\\API_tables\\'
files = os.listdir(fp2)

apis = {}

# Use geopandas read_file to import DBF format
for file in tqdm(files):
    fips = file[3:6]
    apis[fips] = gpd.read_file(fp2 + file)
    # print(fips)

# Concatenate all county-specific API tables into one gdf
tx_wells_api = pd.concat(apis.values())
tx_wells_api = tx_wells_api.reset_index(drop=True)

# CHECK: Confirm that every record has a non-null APINUM
tx_wells_api.APINUM.isna().value_counts()  # should be all False
(tx_wells_api.APINUM.str.len() == 8).value_counts()  # should be all True


# =============================================================================
# %% Fix degenerate/incomplete APIs in surface and bottom hole locations
# =============================================================================
# If there are any alphabetical characters in WELLID column, replace the
# WELLID value with nan
tx_wells_surf.loc[tx_wells_surf.WELLID.str.contains('[A-Za-z]', na=False), 'WELLID'] = np.nan
tx_wells_bot.loc[tx_wells_bot.WELLID.str.contains('[A-Za-z]', na=False), 'WELLID'] = np.nan

# Wells with only a 3-digit FIPS code in the API field don't have a WELLID
# and therefore can't be joined to a full record in the API table (API = FIPS + WELLID)
# If there are any cases where a record DOES have a WELLID, but that hasn't
# been properly appended to its FIPS number in the API column, then do so
tx_wells_surf.loc[(tx_wells_surf.API.str.len() == 3) & (tx_wells_surf.WELLID.notna()), 'API'] = tx_wells_surf.API + tx_wells_surf.WELLID
tx_wells_bot.loc[(tx_wells_bot.API.str.len() == 3) & (tx_wells_bot.WELLID.notna()), 'API'] = tx_wells_bot.API + tx_wells_bot.WELLID

# After manually reconstructing the APIs of a few records above,
# Change any remaining records with only a 3-digit FIPS code in their API field to "N/A"
tx_wells_surf.loc[tx_wells_surf.API.str.len() == 3, 'API'] = 'N/A'
tx_wells_bot.loc[tx_wells_bot.API.str.len() == 3, 'API'] = 'N/A'


# =============================================================================
# %% Dedupe surface locations
# =============================================================================
dupes = get_duplicate_api_records(tx_wells_surf, 'API')
# Number of duplicate records: 98
# Number of unique values: 49

# Create df to contain all surface records with API of N/A, AND a df of all unique non-null APIs.
# Exclude N/A API records from these dedupe operations, and append them back in at the end.
tx_wells_surf_yesAPI = tx_wells_surf.loc[tx_wells_surf['API'] != "N/A"]
tx_wells_surf_noAPI = tx_wells_surf.loc[tx_wells_surf['API'] == "N/A"]

# -----------------------------------------------------------------------------
# DEDUPE: Surface holes with the same API value.
# -----------------------------------------------------------------------------
# 1: De-duplicate records with same API that have identical values in all other
# pertinent fields (SYMNUM, WELLID, LAT and LONG)
tx_wells_surf_yesAPI = tx_wells_surf_yesAPI.drop_duplicates(subset=['API',
                                                                    'SYMNUM',
                                                                    'WELLID',
                                                                    'LONG27',
                                                                    'LAT27'],
                                                            keep="last")
dupes = get_duplicate_api_records(tx_wells_surf_yesAPI, 'API')
# Number of duplicate records: 92
# Number of unique values: 46


# 2: In cases where SYMNUM and RELIAB are the same, but the coords are
# ever so slightly different, just keep the first one randomly.
# In RRC's online GIS viewer these kinds of points seem to be genuine accidental duplicates.
tx_wells_surf_yesAPI = tx_wells_surf_yesAPI.drop_duplicates(subset=['API',
                                                                    'SYMNUM',
                                                                    'WELLID',
                                                                    "RELIAB"],
                                                            keep='last')
dupes = get_duplicate_api_records(tx_wells_surf_yesAPI, 'API')
# Number of duplicate records: 50
# Number of unique values: 25


# 3: In cases where the symnum (well type) is the same, but RELIAB is diff,
# keep the last one randomly
tx_wells_surf_yesAPI = tx_wells_surf_yesAPI.drop_duplicates(subset=['API',
                                                                    'SYMNUM',
                                                                    'WELLID'],
                                                            keep='last')
dupes = get_duplicate_api_records(tx_wells_surf_yesAPI, 'API')
# Number of duplicate records: 36
# Number of unique values: 18


# FIXME - this is NOT an ideal solution, but is a quick fix for now!
# 4: For remaining duplicates, keep the record that has the highest number
# in the SYMNUM column -- lower numbers are more likely to be 'permitted locations'
# or 'dry holes' and higher numbers are more likely to be producing, injecting, etc.
tx_wells_surf_yesAPI_sorted = tx_wells_surf_yesAPI.sort_values(by='SYMNUM',
                                                               ascending=False,
                                                               na_position='last')
tx_wells_surf_yesAPI = tx_wells_surf_yesAPI_sorted.drop_duplicates(subset='API',
                                                                   keep='first')
dupes = get_duplicate_api_records(tx_wells_surf_yesAPI, 'API')
# Number of duplicate records: 0
# Number of unique values: 0


# -----------------------------------------------------------------------------
# DEDUPE: Surface holes with no API info
# -----------------------------------------------------------------------------
dupes = get_duplicate_api_records(tx_wells_surf_noAPI, 'latitude_calc')
# Number of duplicate records: 195
# Number of unique values: 83

# 1: Deduplicate records with no API info, but exact same coords and SYMNUM
tx_wells_surf_noAPI = tx_wells_surf_noAPI.drop_duplicates(subset=['SYMNUM',
                                                                  'longitude_calc',
                                                                  'latitude_calc'],
                                                          keep="last")
dupes = get_duplicate_api_records(tx_wells_surf_noAPI, 'latitude_calc')
# Number of duplicate records: 18
# Number of unique values: 9


# FIXME - this is NOT an ideal solution, but is a quick fix for now!
# 2: for remaining duplicates, keep the record that has the highest number
# in the SYMNUM column -- lower numbers are more likely to be 'permitted locations'
# or 'dry holes' and higher numbers are more likely to be producing, injecting, etc.
tx_wells_surf_noAPI_sorted = tx_wells_surf_noAPI.sort_values(by='SYMNUM',
                                                             ascending=False,
                                                             na_position='last')
tx_wells_surf_noAPI = tx_wells_surf_noAPI_sorted.drop_duplicates(subset=['longitude_calc',
                                                                         'latitude_calc'],
                                                                 keep='first')
dupes = get_duplicate_api_records(tx_wells_surf_noAPI, 'latitude_calc')
# Number of duplicate records: 0
# Number of unique values: 0

# -----------------------------------------------------------------------------
# FINALLY, concatenate deduplicated "yes API" records with "no API" records
# -----------------------------------------------------------------------------
tx_wells_surf_deduped = pd.concat([tx_wells_surf_yesAPI,
                                   tx_wells_surf_noAPI])
dupes = get_duplicate_api_records(tx_wells_surf_deduped, 'API')
# Number of duplicate records: 0
# Number of unique values: 0


# =============================================================================
# %% Deduplicate bottom hole locations
# =============================================================================
dupes = get_duplicate_api_records(tx_wells_bot, 'API')
# Number of duplicate records: 27584
# Number of unique values: 11336

# Create df to contain all records with API of N/A, AND a df of all records with API.
tx_wells_bot_yesAPI = tx_wells_bot[tx_wells_bot['API'] != 'N/A']
tx_wells_bot_noAPI = tx_wells_bot[tx_wells_bot['API'] == 'N/A']


# -----------------------------------------------------------------------------
# DEDUPE: Bottom holes with the same API value.
# -----------------------------------------------------------------------------
# 1: De-duplicate records with same API that have identical values in all other
# pertinent fields (SYMNUM, WELLID, LAT and LONG)
tx_wells_bot_yesAPI = tx_wells_bot_yesAPI.drop_duplicates(subset=['API',
                                                                  'SYMNUM',
                                                                  'WELLID',
                                                                  'LONG27',
                                                                  'LAT27'],
                                                          keep="last")
dupes = get_duplicate_api_records(tx_wells_bot_yesAPI, 'API')
# Number of duplicate records: 27295
# Number of unique values: 11294


# 2: In cases where SYMNUM and RELIAB are the same, but the coords are
# ever so slightly different, just keep the first one randomly.
tx_wells_bot_yesAPI = tx_wells_bot_yesAPI.drop_duplicates(subset=['API',
                                                                  'SYMNUM',
                                                                  'WELLID',
                                                                  'RELIAB'],
                                                          keep='first')
dupes = get_duplicate_api_records(tx_wells_bot_yesAPI, 'API')
# Number of duplicate records: 12267
# Number of unique values: 5951


# 3: In cases where the symnum (well type) is the same, but RELIAB is diff,
# keep the last one randomly
tx_wells_bot_yesAPI = tx_wells_bot_yesAPI.drop_duplicates(subset=['API',
                                                                  'SYMNUM',
                                                                  'WELLID'],
                                                          keep='last')
dupes = get_duplicate_api_records(tx_wells_bot_yesAPI, 'API')
# Number of duplicate records: 10035
# Number of unique values: 4922
# Remaining duplicates represent true mulitple bottom-holes affiliated with a
# single surface location!

# EXCLUDE the bottom holes with no API for now, since they can't be joined with surface holes anyway
tx_wells_bot_deduped = tx_wells_bot_yesAPI

# =============================================================================
# %% Join surface + bottom hole attributes based on API,
# retaining records from the surface locations.
# =============================================================================

# Merge surface and bottom holes based on API number
# EXCLUDE the bottom holes with no API for now, since they can't be joined with surface holes anyway
# You'll end up with duplicate records from this operation
s_b_joined = tx_wells_surf_deduped.merge(tx_wells_bot_deduped,
                                         how='left',
                                         on='API',
                                         suffixes=('_surf', '_bot'))


# In cases where the surface hole SYMNUM is 86 or 87 (horiz/directional well),
# use the bottom hole SYMNUM as the well type instead (because it provides more detail)
s_b_joined['symnum_surf_new'] = s_b_joined['SYMNUM_surf']
s_b_joined.loc[s_b_joined.symnum_surf_new.isin([86, 87]), 'symnum_surf_new'] = s_b_joined['SYMNUM_bot']

dupes = get_duplicate_api_records(s_b_joined, 'API')
# Number of duplicate records: 10032
# Number of unique values: 4921


# FIXME - this is NOT an ideal solution, but is a quick fix for now!
# DEDUPE: For remaining duplicate records (which are surface holes that got
# joined to 2+ bottom holes), keep the record that has the highest number
# in the new SYMNUM column -- lower numbers are more likely to be 'permitted locations'
# or 'dry holes' and higher numbers are more likely to be producing, injecting, etc.
s_b_joined_sort = s_b_joined.sort_values(by=['API',
                                             'symnum_surf_new'],
                                         ascending=[True, False],
                                         na_position='last')
s_b_joined = s_b_joined_sort.drop_duplicates(subset='API',
                                             keep='first')
dupes = get_duplicate_api_records(s_b_joined, 'API')
# Number of duplicate records: 0
# Number of unique values: 0


# =============================================================================
# %% DEDUPLICATE API RECORDS
# =============================================================================
dupes = get_duplicate_api_records(tx_wells_api, 'APINUM')
# Number of duplicate records: 560018
# Number of unique values: 210920

# 1: Get rid of records that are identical in every field we care about
tx_wells_api = tx_wells_api.drop_duplicates(subset=['ABSTRACT',
                                                    'APINUM',
                                                    'COMPLETION',
                                                    'PLUG_DATE',
                                                    'FIELD_NAME',
                                                    'LEASE_NAME',
                                                    'GAS_RRCID',
                                                    'OPERATOR'],
                                            keep='first')
dupes = get_duplicate_api_records(tx_wells_api, 'APINUM')
# Number of duplicate records: 473553
# Number of unique values: 193274


# Sort rows so that records with same API are clustered together, and the newest
# completion date (largest integer) appears first in the "cluster", and within
# a completion date, sort so that the largest (newest) plug date appears first.
tx_wells_api['compnum'] = tx_wells_api.COMPLETION.astype(int)
tx_wells_api['plugnum'] = tx_wells_api.PLUG_DATE.astype(int)
tx_wells_api_sorted = tx_wells_api.sort_values(by=['APINUM',
                                                   'compnum',
                                                   'plugnum'],
                                               ascending=[True, False, False],
                                               na_position='last')
# Deduplicate the API table by keeping only the most recent (aka the first)
# record for each API, according to completion date and to some degree plug date
tx_wells_api = tx_wells_api_sorted.drop_duplicates(subset=['APINUM'],
                                                   keep='first')
dupes = get_duplicate_api_records(tx_wells_api, 'APINUM')
# Number of duplicate records: 0
# Number of unique values: 0


# =============================================================================
# %% Use API number to join API table with the well surface location table.
# There should be NO duplicate APIs in the resulting `tx_wells` gdf!
# =============================================================================
tx_wells = s_b_joined.merge(tx_wells_api,
                            how='left',
                            left_on='API',
                            right_on='APINUM')
dupes = get_duplicate_api_records(tx_wells, 'API')
# Number of duplicate records: 0
# Number of unique values: 0

# print(*tx_wells.columns, sep='\n')

# Alter data types as-needed in attributes I will use in the final output.
# Also fill in N/A values.
tx_wells.symnum_surf_new = tx_wells.symnum_surf_new.fillna(0)
tx_wells.symnum_surf_new = tx_wells.symnum_surf_new.astype(int).astype(str)

tx_wells = replace_missing_strings_with_na(tx_wells, ['OPERATOR', 'LEASE_NAME'])

# =============================================================================
# %% Pre-processing: Replace SYMNUM numeric codes with well types
# =============================================================================
symnums = pd.read_csv(r'SYMNUM_codes.csv', header=None)

symnums['code'] = symnums[0].str.split(" ", 1).str[0]
symnums['descrip'] = symnums[0].str.split(" ", 1).str[1]
symdict = dict(zip(symnums.code, symnums.descrip))

tx_wells['WellType'] = tx_wells['symnum_surf_new'].map(symdict).fillna('N/A')

tx_wells.WellType.value_counts()

# =============================================================================
# %% Create and populate a 'facility status' column, based on available FAC_TYPE values
# =============================================================================
tx_wells['status'] = 'N/A'

plugged = ['Plugged Oil Well',
           'Plugged Gas Well',
           'Plugged Oil/Gas Well',
           'Plugged Storage',  # ???????
           'Plugged Brine Mining',   # ???????
           'Plugged Storage/Brine Mining',  # ???????
           'Plugged Storage/Gas']

permitted = ['Permitted Location']

cancelled = ['Canceled Location']

storage = ['Storage Well',
           'Storage from Gas',
           'Storage from Oil',
           'Storage from Oil/Gas',
           'Storage/Brine Mining']

injection = ['Injection/Disposal from Oil',
             'Injection/Disposal Well',
             'Injection/Disposal from Gas',
             'Injection/Disposal from Oil/Gas',
             'Injection/Disposal from Storage',
             'Injection/Disposal from Brine Mining',
             'Injection/Disposal from Storage/Oil',
             'Injection/Disposal from Storage/Gas',
             'Inj./Disposal from Storage/Brine Mining',
             'Injection/Disposal from Brine Mining/Gas']

shutin = ['Shut-In Well (Oil)',
          'Shut-In Well (Gas)']

observ = ['Observation Well',
          'Observation from Oil',
          'Observation from Oil/Gas',
          'Observation from Gas',
          'Observation from Storage/Gas',
          'Observation from Storage',
          'Observation from Storage/Brine Mining']

service = ['Service Well',
           'Service from Storage/Gas',
           'Service from Oil',
           'Service from Brine Mining']

tx_wells.loc[tx_wells.WellType.isin(plugged), 'status'] = 'Plugged'
tx_wells.loc[tx_wells.WellType.isin(permitted), 'status'] = 'Permitted'
tx_wells.loc[tx_wells.WellType.isin(cancelled), 'status'] = 'Cancelled'
tx_wells.loc[tx_wells.WellType.isin(storage), 'status'] = 'Storage'
tx_wells.loc[tx_wells.WellType.isin(injection), 'status'] = 'Injection/Disposal'
tx_wells.loc[tx_wells.WellType.isin(shutin), 'status'] = 'Shut-In'
tx_wells.loc[tx_wells.WellType.isin(observ), 'status'] = 'Observation'
tx_wells.loc[tx_wells.WellType.isin(service), 'status'] = 'Service'


# If a record has a plug date that comes later than its completion date,
# mark its status as plugged
# tx_wells['plugnum'] = tx_wells.PLUG_DATE.astype(int)
tx_wells.loc[tx_wells.plugnum > tx_wells.compnum, 'status'] = 'Plugged'

tx_wells.status.value_counts()
# Plugged               458929
# N/A                   382329
# Permitted              70880
# Injection/Disposal     39887
# Cancelled              34020
# Shut-In                 7653
# Storage                  807
# Observation              542
# Service                   31

# =============================================================================
# %% Add drilling trajectory, where it's known
# =============================================================================
tx_wells['drilltype'] = 'N/A'

# If the surface hole and bottom hole lat AND long coordinates are identical,
# (at least to the 6th decimal place), make an assumption that the well is vertical
condition1 = (tx_wells.LONG27_surf.round(6) == tx_wells.LONG27_bot.round(6))
condition2 = (tx_wells.LAT27_surf.round(6) == tx_wells.LAT27_bot.round(6))
tx_wells.loc[condition1 & condition2, 'drilltype'] = 'Vertical'

# Selectively correct the assumption we made above
# If we know a well is Horizontal or Directional by its SYMNUM, record that
tx_wells.loc[tx_wells.SYMNUM_surf == 86, 'drilltype'] = 'Horizontal'
tx_wells.loc[tx_wells.SYMNUM_surf == 87, 'drilltype'] = 'Directional'

tx_wells.drilltype.value_counts()
# Vertical       804748
# Horizontal     127219
# N/A             37587
# Directional     25524


# =============================================================================
# %% Change completion date (number) to OGIM standard date format (string)
# =============================================================================
# Replace missing date value "0" with OGIM's missing value date
tx_wells.COMPLETION = tx_wells.COMPLETION.astype(str)
tx_wells.COMPLETION.replace({'0': '19000101',
                             'nan': '19000101'},
                            inplace=True)

# Split up the date string into 3 substrings representing YYYY, MM, and DD
# Join these substrings together, separated by hyphens, in a new column
tx_wells['compnew'] = tx_wells['COMPLETION'].apply(lambda x: '-'.join([x[0:4],
                                                                       x[4:6],
                                                                       x[6:8]]))

# If there are any records that have YYYY and MM but no DD, change DD value to "-01"
tx_wells.loc[tx_wells.compnew.str.endswith('-00'), 'compnew'] = tx_wells.compnew.str[0:7] + '-01'

# Some dates contain a MM of "00", which indicate missing month values.
# Detect them all by attempting to convert the date string with datetime;
# for any records with "NaT" datetimes, change the completion date to only
# include the year and revise all these MM-DD values to "01-01"
tx_wells['compdatetime'] = pd.to_datetime(tx_wells['compnew'],
                                          format='%Y-%m-%d',
                                          errors='coerce')
tx_wells.loc[tx_wells.compdatetime.isnull(), 'compnew'] = tx_wells.compnew.str[0:4] + '-01-01'


# =============================================================================
# %% Create well name
# =============================================================================
create_concatenated_well_name(tx_wells,
                              'LEASE_NAME',
                              'WELLID',
                              'wellnamenew')

# tx_wells.wellnamenew.head()

# =============================================================================
# %% INTEGRATION
# =============================================================================
tx_wells['latitude_calc'] = tx_wells['latitude_calc_surf']
tx_wells['longitude_calc'] = tx_wells['longitude_calc_surf']
tx_wells = tx_wells.set_geometry("geometry_surf")

tx_wells_integrated, _errors = integrate_facs(
    tx_wells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Texas",
    src_ref_id="217",
    src_date="2024-04-19",  # Weekly
    on_offshore=None,
    fac_name='wellnamenew',
    fac_id="API",
    fac_type='WellType',
    spud_date=None,
    comp_date='compnew',
    drill_type='drilltype',
    fac_status='status',
    op_name='OPERATOR',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'tx_wells_integrated.geojson'
tx_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')
