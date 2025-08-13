# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 09:35:19 2024

In Texas, lease numbers are re-used across districts, so a lease number ALONE
does not uniquely identify a lease in the state. This script uses a
combination of Lease Number and District Number as a unique identifier for each
production-reporting lease. This identifier is stored in the field
`LEASE_NO_DISTRICT_NO_str` and is used for joining disparate tables together.

@author: maobrien
"""

import os
import geopandas as gpd
import pandas as pd
import datetime
from tqdm import tqdm
import numpy as np

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (get_duplicate_api_records, integrate_production,
                     save_spatial_data, schema_OIL_GAS_PROD)

# set cwd to texas data folder
os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Production_v0\data\texas')

# Set destination folder for exported SHP and JSON outputs
# make sure to end the string with double backslash!
results_folder = "C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory\\Public_Production_v0\\integrated_results\\"

# =============================================================================
# %% Custom function definition
# =============================================================================


def populate_before_after_table_post_integration(i, df, gdf_integrated):

    # populate the columns related to OIL and GAS
    for h, col in zip(['oil', 'gas', 'cond'], ['OIL_BBL', 'GAS_MCF', 'CONDENSATE_BBL']):

        # First, cast any missing values as nan instead of -999, so they don't
        # throw off the sum
        gdf_integrated[col] = gdf_integrated[col].replace({-999: np.nan,
                                                           '-999': np.nan})
        # Record the total hydrocarbon produced in 2022 AFTER integration
        df.at[i, f'{h}_geojson'] = gdf_integrated[col].sum()
        # Calculate the percentage
        x = (df.loc[i, f'{h}_geojson'] / df.loc[i, f'{h}_original'])
        x_as_pct = "{:.4%}".format(x)
        df.at[i, f'{h}_pct_in_geojson'] = x_as_pct


# =============================================================================
# %% Create empty "before and after integration comparison" table
# =============================================================================
before_after_table = pd.DataFrame(index=['TEXAS'],
                                  columns=['oil_original',  # Sum of all 2022 production values from raw data
                                           'oil_agg',  # Sum of all 2022 prod. values after aggregating months together
                                           'oil_geojson',  # Sum of all 2022 prod. values in the integrated geojson result that gets exported
                                           'oil_pct_in_geojson',  # What percent of the original production volume is still reported in the final geojson
                                           'gas_original',
                                           'gas_agg',
                                           'gas_geojson',
                                           'gas_pct_in_geojson',
                                           'cond_original',
                                           'cond_agg',
                                           'cond_geojson',
                                           'cond_pct_in_geojson',
                                           'units_reporting_production_original',  # Count of how many unique APIs, Leases, etc. report production in the original dataset before any cleaning
                                           'units_reporting_production_geojson'  # Count of how many unique APIs, Leases report production in the integrated dataset
                                           ])

# =============================================================================
# %% Read table that relates counties, county FIPS codes, and district codes
# =============================================================================
county_info = pd.read_csv('GP_COUNTY_DATA_TABLE.dsv',
                          sep="}",
                          header=0)

# Convert county number and district number to string, with leading zeroes
county_info['COUNTY_CODE'] = county_info.COUNTY_NO.astype(str).str.zfill(3)
county_info['DISTRICT_NO_str'] = county_info.DISTRICT_NO.astype(str).str.zfill(2)

# Create a dictionary that maps County to District Number
countycode2district = dict(zip(county_info.COUNTY_CODE,
                               county_info.DISTRICT_NO_str))

# =============================================================================
# %% Read table that relates lease numbers, lease names, field names, and district numbers
# =============================================================================
lease_info = pd.read_csv('OG_REGULATORY_LEASE_DW_DATA_TABLE.dsv',
                         sep="}",
                         header=0)

# Standardize nan values so they properly join with well/API records
lease_info = lease_info.replace([np.nan, None], np.nan)

# Reformat lease number to match the GAS_RRCID format
# LEASE_NO = RRC-assigned number representing the lease; unique within a district.
# Add padding zeroes to the LEASE_NO values; how many zeroes to add depends on
# whether the lease is OIL (5 digits total) or GAS (6 digits total)
lease_info['LEASE_NO_str'] = lease_info.LEASE_NO.astype(str)
lease_info.loc[lease_info.OIL_GAS_CODE == 'O', 'LEASE_NO_str'] = lease_info.LEASE_NO_str.str.zfill(5)
lease_info.loc[lease_info.OIL_GAS_CODE == 'G', 'LEASE_NO_str'] = lease_info.LEASE_NO_str.str.zfill(6)

# Reduce to just the columns we need
lease_info = lease_info[['FIELD_NAME',
                         'LEASE_NAME',
                         'LEASE_NO_str',
                         'DISTRICT_NO',
                         'DISTRICT_NAME'
                         ]]

# =============================================================================
# %% Read production records - WARNING, THIS TABLE IS 11 GB

# TODO - FIELD DEFINITIONS
# LEASE_NO =
# FIELD_NO =
# FIELD_TYPE
# LEASE_OIL_PROD_VOL = amount of oil in BBL produced by lease as reported by the operator on a production report.
# LEASE_GAS_PROD_VOL = amount of gas in MCF produced by lease as reported by the operator on a production report.
# LEASE_COND_PROD_VOL = amount of condensate oil in BBL produced by lease as reported by the operator on a production report.
# LEASE_CSGD_PROD_VOL = amount of casinghead gas in MCF produced by lease as reported by the operator on a production report.
# =============================================================================
print(datetime.datetime.now())
tx_prod = pd.read_csv('OG_LEASE_CYCLE_DATA_TABLE.dsv',
                      sep="}",
                      header=0,
                      dtype={'GAS_WELL_NO': str, 'DISTRICT_NAME': str})
print(datetime.datetime.now())
# Quick look at the data
# df2 = tx_prod.sample(2000)
print(tx_prod.columns)

# Filter production records to just 2022
tx_prod_2022 = tx_prod.query("CYCLE_YEAR == 2022").reset_index(drop=True)
del tx_prod  # save memory by deleting the huge table with all years
print(tx_prod_2022.columns)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['TEXAS', 'oil_original'] = tx_prod_2022.LEASE_OIL_PROD_VOL.sum()
before_after_table.at['TEXAS', 'gas_original'] = tx_prod_2022.LEASE_GAS_PROD_VOL.sum() + tx_prod_2022.LEASE_CSGD_PROD_VOL.sum()
before_after_table.at['TEXAS', 'cond_original'] = tx_prod_2022.LEASE_COND_PROD_VOL.sum()

# LEASE_NO = RRC-assigned number representing the lease; unique within a district.
# Add padding zeroes to the LEASE_NO values; how many zeroes to add depends on
# whether the lease is OIL (5 digits total) or GAS (6 digits total)
tx_prod_2022['LEASE_NO_str'] = tx_prod_2022.LEASE_NO.astype(str)
tx_prod_2022.loc[tx_prod_2022.OIL_GAS_CODE == 'O', 'LEASE_NO_str'] = tx_prod_2022.LEASE_NO_str.str.zfill(5)
tx_prod_2022.loc[tx_prod_2022.OIL_GAS_CODE == 'G', 'LEASE_NO_str'] = tx_prod_2022.LEASE_NO_str.str.zfill(6)

# Create a string version of DISTRICT_NO with padded zeroes
tx_prod_2022['DISTRICT_NO_str'] = tx_prod_2022['DISTRICT_NO'].astype(str).str.zfill(2)

# Create the new field 'LEASE_NO_DISTRICT_NO', a string that combines the
# LEASE NO. (string) and DISTRICT NO. into an easily readable ID
tx_prod_2022['LEASE_NO_DISTRICT_NO_str'] = tx_prod_2022['LEASE_NO_str'] + '-' + tx_prod_2022['DISTRICT_NO_str']

# Aggregate my table of monthly production volumes into one annual volume
# per row / per lease
agg_fxns = {'OIL_GAS_CODE': 'first',
            # 'DISTRICT_NO': 'first',
            'DISTRICT_NAME': 'first',
            'LEASE_NO': 'first',
            'LEASE_NAME': 'first',
            # 'LEASE_NO_DISTRICT_NO': 'first',
            'OPERATOR_NAME': 'first',
            'FIELD_NAME': 'first',
            'FIELD_TYPE': 'first',
            'GAS_WELL_NO': 'first',
            'LEASE_OIL_PROD_VOL': 'sum',
            'LEASE_GAS_PROD_VOL': 'sum',
            'LEASE_COND_PROD_VOL': 'sum',
            'LEASE_CSGD_PROD_VOL': 'sum'}

tx_prod_2022_agg = tx_prod_2022.groupby(by=['LEASE_NO_DISTRICT_NO_str'],
                                        as_index=False).agg(agg_fxns)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['TEXAS', 'oil_agg'] = tx_prod_2022_agg.LEASE_OIL_PROD_VOL.sum()
before_after_table.at['TEXAS', 'gas_agg'] = tx_prod_2022_agg.LEASE_GAS_PROD_VOL.sum() + tx_prod_2022_agg.LEASE_CSGD_PROD_VOL.sum()
before_after_table.at['TEXAS', 'cond_agg'] = tx_prod_2022_agg.LEASE_COND_PROD_VOL.sum()

# =============================================================================
# %% Read in WELL SURFACE LOCATIONS + BOTTOM LOCATIONS
# TODO - are bottom locations necessary for this?
# =============================================================================
# Set current working directory to 'State_Raw_Data' folder
os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\OGIM_v2.5\data\united_states\texas')

# List all files in my directory, then read well surface locations
# Will take 5-ish minutes to read and concatenate them all
fp = 'wells\\'
files = os.listdir(fp)

# Empty dictionary to hold results
s_wells = {}
# b_wells = {}

for file in tqdm(files):
    fips = file[4:7]
    if file.endswith('s.shp'):  # surface hole shapefiles
        s_wells[fips] = gpd.read_file(fp + file)
        # print(fips)
    # if file.endswith('b.shp'):  # bottom hole shapefiles
    #     b_wells[fips] = gpd.read_file(fp + file)
    #     # print(fips)

# Concatenate all county-specific locations into one state-wide gdf
tx_wells_surf = pd.concat(s_wells.values())
tx_wells_surf = tx_wells_surf.reset_index(drop=True)
# tx_wells_bot = pd.concat(b_wells.values())
# tx_wells_bot = tx_wells_bot.reset_index(drop=True)

# convert surface and bottom holes from NAD27 to WGS84
tx_wells_surf = tx_wells_surf.to_crs(4326)
# tx_wells_bot = tx_wells_bot.to_crs(4326)

# =============================================================================
# %% Fix degenerate/incomplete APIs in surface and bottom hole locations
# =============================================================================
# If there are any alphabetical characters in WELLID column, replace the
# WELLID value with nan
tx_wells_surf.loc[tx_wells_surf.WELLID.str.contains('[A-Za-z]', na=False), 'WELLID'] = np.nan
# tx_wells_bot.loc[tx_wells_bot.WELLID.str.contains('[A-Za-z]', na=False), 'WELLID'] = np.nan

# Wells with only a 3-digit FIPS code in the API field don't have a WELLID
# and therefore can't be joined to a full record in the API table (API = FIPS + WELLID)
# If there are any cases where a record DOES have a WELLID, but that hasn't
# been properly appended to its FIPS number in the API column, then do so
tx_wells_surf.loc[(tx_wells_surf.API.str.len() == 3) & (tx_wells_surf.WELLID.notna()), 'API'] = tx_wells_surf.API + tx_wells_surf.WELLID
# tx_wells_bot.loc[(tx_wells_bot.API.str.len() == 3) & (tx_wells_bot.WELLID.notna()), 'API'] = tx_wells_bot.API + tx_wells_bot.WELLID

# After manually reconstructing the APIs of a few records above,
# Change any remaining records with only a 3-digit FIPS code in their API field to "N/A"
tx_wells_surf.loc[tx_wells_surf.API.str.len() == 3, 'API'] = 'N/A'
# tx_wells_bot.loc[tx_wells_bot.API.str.len() == 3, 'API'] = 'N/A'

# =============================================================================
# %% Deduplicate APIs in the surface hole records
# =============================================================================
# First, since API is essential for joining location to lease & production,
# drop any surface hole records that DON'T have an API
tx_wells_surf_yesAPI = tx_wells_surf.loc[tx_wells_surf['API'] != "N/A"]

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

tx_wells_surf = tx_wells_surf_yesAPI.copy().reset_index(drop=True)

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
# Delete the geometry column which is empty for all records in this df
tx_wells_api = tx_wells_api.drop('geometry', axis=1)
# Delete other columns I don't need, just to tidy things up
tx_wells_api.drop(['ABSTRACT',
                   'BLOCK',
                   'REFER_TO_A',
                   'SURVEY',
                   'QUADNUM',
                   'OBJECTID_1'],
                  axis=1,
                  inplace=True)

# CHECK: Confirm that every record has a non-null APINUM
tx_wells_api.APINUM.isna().value_counts()  # should be all False
(tx_wells_api.APINUM.str.len() == 8).value_counts()  # should be all True

# If the lease ID field is '000000', cast the value as null
tx_wells_api.GAS_RRCID = tx_wells_api.GAS_RRCID.replace({'000000': np.nan})

# TODO - there are still TONS of duplicate API values in this table,
# DEAL WITH THEM

# =============================================================================
# %% Create a 'LEASE_NO_DISTRICT_NO_str' column in the API table.
# =============================================================================
# Since Lease number is essential for joining location to production,
# drop any record in the API table that does NOT have a Lease number
tx_wells_api = tx_wells_api[tx_wells_api.GAS_RRCID.notnull()]
tx_wells_api = tx_wells_api.reset_index(drop=True)

# Standardize nan values (in the Field Name and Lease Name column) so they
# properly join with well/API records
tx_wells_api = tx_wells_api.replace([np.nan, None], np.nan)

tx_wells_api = tx_wells_api.merge(lease_info,
                                  how='left',
                                  left_on=['FIELD_NAME',
                                           'LEASE_NAME',
                                           'GAS_RRCID'],
                                  right_on=['FIELD_NAME',
                                            'LEASE_NAME',
                                            'LEASE_NO_str'],
                                  suffixes=('_x', '_y'))

# There are still some APIs that have a lease number but failed to join to a
# district via a lease table. For these wells, assume what district the lease
# lies in based on the county code in the API, then populate DISTRICT_NO
tx_wells_api['COUNTY_CODE'] = tx_wells_api.APINUM.str[0:3]
tx_wells_api.loc[tx_wells_api.DISTRICT_NO.isna(), 'DISTRICT_NO'] = tx_wells_api.COUNTY_CODE.map(countycode2district).fillna('999')

# Construct a "lease number + district code" string for each record
tx_wells_api.DISTRICT_NO = tx_wells_api.DISTRICT_NO.astype(int)
tx_wells_api['DISTRICT_NO_str'] = tx_wells_api.DISTRICT_NO.astype(str).str.zfill(2)
tx_wells_api['LEASE_NO_DISTRICT_NO_str'] = tx_wells_api['GAS_RRCID'] + '-' + tx_wells_api['DISTRICT_NO_str']


# =============================================================================
# %% Merge API df and surface hole gdf, so that each surface hole now has
# a lease-district ID associated with it.
# =============================================================================
# NOTE! It is possible for an API to have TWO leases associated with it.
# For example, the API 001-31520 or 039-31768. This might happen if the well
# has produced both oil and gas at some point in the past (from what I can
# tell). BUT, 03931768 is an example where it's an oil well operated by Dow
# chemical on both leases....
# TODO - figure out what is going on here

# Use an INNER join, so that if a surface location is associated with 2+ lease
# numbers, the resulting surface location appears in the resulting join table
# twice, with one lat-long pair associated with each lease.
wells_with_leases = tx_wells_surf.merge(tx_wells_api,
                                        how='inner',
                                        left_on='API',
                                        right_on='APINUM',
                                        suffixes=('_surf', '_api'))

# =============================================================================
# %% Group wells by lease, calculate lease centroids
# =============================================================================
# Dissolve the well location gdf based on Lease-District number. In instances
# where there are multiple wells / APIs associated with a single Lease, a
# multi-part point geometry associated with that lease will result from the
# dissolve. Calculate centroids for all the multi-point features, and
# assign this single-point-only array of geometries to the gdf you will
# ultimately merge with production volumes via Lease-District.
# NOTE: Dissolve step will take a while with this large number of geometries.
# TODO add a datetime print out here

# When multiple wells on a single lease are dissolved together, choose how some
# of the well's fields will be aggregated.
# FIXME - Many well attributes get dropped at this point, because it doesn't
# make sense to just retain a single well's operator, status, etc. and ascribe
# that to an entire Lease. Ask Mark whether any fields should be kept.
lease_agg = {
    'API': 'count',
    'FIELD_NAME': 'first',
    'LEASE_NAME': 'first',
    # 'GAS_RRCID': 'first',  # ?
    'GAS_RRCID': pd.Series.mode,
    'OIL_GAS_CO': 'first',
    'OPERATOR': 'first'}

# Dissolve well records based on lease-district number (TAKES A WHILE)
lease_geoms = wells_with_leases.dissolve(by='LEASE_NO_DISTRICT_NO_str',
                                         aggfunc=lease_agg)
lease_geoms = lease_geoms.reset_index(drop=False)
# CHECK - should be mix of Point and Multipoint, with no null values.
print(lease_geoms.geometry.type.value_counts())
print(lease_geoms.geometry.isna().value_counts())

single_point_geoms_only = lease_geoms.geometry.centroid
lease_geoms['geometry'] = single_point_geoms_only

# Confirmed: no LEASE_NO_DISTRICT_NO_str are lost at this stage. 486,620 retained.

# =============================================================================
# %% Merge Lease-level production volumes with Lease-level geometries (points)
# =============================================================================
tx_prod_merge = pd.merge(tx_prod_2022_agg,
                         lease_geoms,
                         on="LEASE_NO_DISTRICT_NO_str",
                         how="left")
# The CRS of this table gets dropped during pd.merge(), so recast it as a gdf
tx_prod_merge = gpd.GeoDataFrame(tx_prod_merge,
                                 geometry='geometry',
                                 crs='epsg:4326')

tx_prod_merge['Lat'] = tx_prod_merge.geometry.y
tx_prod_merge['Long'] = tx_prod_merge.geometry.x

# DROP any production records that DON'T HAVE A GEOMETRY
tx_prod_merge_final = tx_prod_merge[tx_prod_merge.geometry.notna()]

# Create a gas column that represents gas PLUS any casinghead gas
tx_prod_merge_final['gas_plus_casinghead'] = tx_prod_merge_final.LEASE_GAS_PROD_VOL + tx_prod_merge_final.LEASE_CSGD_PROD_VOL

# =============================================================================
# %% Integrate Texas production data
# =============================================================================
tx_prod_integrated, tx_err = integrate_production(
    tx_prod_merge_final,
    src_date="2024-06-26",  # date on production data dump files
    src_ref_id="252",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Texas",
    # fac_name="",
    fac_id="GAS_RRCID",  # lease number, with NO district # FIXME ?
    # fac_type="",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # fac_status=None,
    op_name="OPERATOR_NAME",
    oil_bbl="LEASE_OIL_PROD_VOL",
    gas_mcf="gas_plus_casinghead",
    # water_bbl=None,
    condensate_bbl="LEASE_COND_PROD_VOL",
    # prod_days=None,
    prod_year="2022",
    entity_type="LEASE",
    fac_latitude='Lat',
    fac_longitude='Long'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('TEXAS',
                                             before_after_table,
                                             tx_prod_integrated)

save_spatial_data(
    tx_prod_integrated,
    "texas_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)
