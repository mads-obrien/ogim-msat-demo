# -*- coding: utf-8 -*-
"""
Created on Tuesday November 14 2023

Data integration of United States well data, from both state governments and HIFLD.

# TODO items:
    [] standardize spacing between sections

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
import numpy as np
import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (clean_a_date_field, replace_missing_strings_with_na,
                     create_concatenated_well_name, get_duplicate_api_records,
                     transform_CRS, integrate_facs, read_msAccess)
# from abbreviation_utils import us_abbrev_to_state


def quickmap(gdf, _name):
    print(*gdf.columns, sep='\n')
    base = states[states.name == _name].boundary.plot(color='black')
    gdf.plot(ax=base, markersize=2)


# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Set current working directory to 'OGIM_vX.X\\data\\united_states' specific
# to my OGIM version within the Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'
my_cwd = os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'united_states')
os.chdir(my_cwd)

# Folder in which all integrated data will be saved
outfolder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# Leave this as True while testing integration code;
# Change to False when letting code run (to speed things up)
print_maps = False

# =============================================================================
# %% Read in US state boundaries for quality assurance plotting and clipping
# =============================================================================
path2states = os.path.join(buii_path, r'Public_Data\data\North_America\United_States_v1.2\states\cb_2023_us_state_500k.shp')
states = gpd.read_file(path2states)
# Filter out some territories
states = states[~states.STUSPS.isin(['AS', 'MP', 'GU', 'PR', 'VI'])].reset_index(drop=True)
# Filter columns to just what's needed
states = states.filter(['NAME', 'geometry'])
states = states.rename(columns={'NAME': 'name'})

# =============================================================================
# %% Alabama
print(datetime.datetime.now())
print('Alabama')
# =============================================================================
al_wells_fp = r"alabama\wells\WebWell_prj.shp"
al_wells = gpd.read_file(al_wells_fp)

al_wells_crs = transform_CRS(al_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(al_wells_crs, 'Alabama')


# In[ OTHER PROCESSING  ]:

# There are LOTS of records with no lat/long information (657 records?) -- just drop them
al_wells_crs = al_wells_crs[~al_wells_crs.latitude_calc.isna()]

clean_a_date_field(al_wells_crs, 'SpudDate')
clean_a_date_field(al_wells_crs, 'StatusDate')
clean_a_date_field(al_wells_crs, 'PermitDate')

# In[  DE-DUPING  ]:

# Check API format
al_wells_crs.API.head()
# Create new column with API-10
al_wells_crs['API10'] = al_wells_crs.API.str[:-6]
al_wells_crs.API10.head()

# Check for duplicate API-14 and API-10
dupes = get_duplicate_api_records(al_wells_crs, 'API')
# Number of duplicate records: 0
# Number of unique values: 0
dupes = get_duplicate_api_records(al_wells_crs, 'API10')
# Number of duplicate records: 2773
# Number of unique values: 1243

# Sort records by API10 and StatusDate date, and keep the record with
# the newer StatusDate. If two StatusDates are the same, then pick the record with
# the newer SpudDate
al_wells_crs_newestfirst = al_wells_crs.sort_values(by=['API10',
                                                        'StatusDate',
                                                        'SpudDate'],
                                                    ascending=[True, False, False],
                                                    na_position='last')
al_wells_crs = al_wells_crs_newestfirst.drop_duplicates(subset=['API10',
                                                                'latitude_calc',
                                                                'longitude_calc'],
                                                        keep='first')

dupes = get_duplicate_api_records(al_wells_crs, 'API10')
# Number of duplicate records: 0
# Number of unique values: 0


# In[  INTEGRATION  ]:

al_wells_integrated, _errors = integrate_facs(
    al_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Alabama",
    src_ref_id="185",
    src_date="2024-04-18",  # Daily
    on_offshore=None,
    fac_name='WellName',
    fac_id="API10",
    fac_type="TypeDesc",
    spud_date="SpudDate",
    comp_date=None,
    drill_type=None,
    fac_status='StatusDesc',
    op_name='Operator',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'al_wells_integrated.geojson'
al_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Alaska
print(datetime.datetime.now())
print('Alaska')
# =============================================================================
ak_wells_fp = r'alaska\wells\wells.xlsx'
ak_wells_csv = pd.read_excel(ak_wells_fp, sheet_name=0)

# Use Wh_CalculatedLongitude and Wh_CalculatedLatitude for geometry in most cases
# This coordinate column has the least number of missing values
ak_wells_csv['latitude_new'] = ak_wells_csv.Wh_CalculatedLatitude
ak_wells_csv['longitude_new'] = ak_wells_csv.Wh_CalculatedLongitude

# In cases where the Wh_Calculated coordinate is NULL, and a
# "Wh_ReportedLongitude" or "Wh_ReportedLatidude" exists, use that instead
ak_wells_csv['Wh_ReportedLatitude'] = ak_wells_csv['Wh_ReportedLatitude'].replace(0, np.nan)
ak_wells_csv['Wh_ReportedLongitude'] = ak_wells_csv['Wh_ReportedLongitude'].replace(0, np.nan)
ak_wells_csv.loc[ak_wells_csv.latitude_new.isna() & ak_wells_csv.Wh_ReportedLatitude.notna(), 'latitude_new'] = ak_wells_csv['Wh_ReportedLatitude']
ak_wells_csv.loc[ak_wells_csv.longitude_new.isna() & ak_wells_csv.Wh_ReportedLongitude.notna(), 'longitude_new'] = ak_wells_csv['Wh_ReportedLongitude']

# Drop records with no lat-long information
ak_wells_csv = ak_wells_csv[ak_wells_csv.latitude_new.notna()]

ak_wells = gpd.GeoDataFrame(ak_wells_csv,
                            geometry=gpd.points_from_xy(ak_wells_csv.longitude_new,
                                                        ak_wells_csv.latitude_new),
                            crs=4267)  # NAD27

ak_wells_crs = transform_CRS(ak_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    print(*ak_wells_crs.columns, sep='\n')
    base = states[states.name == 'Alaska'].boundary.plot(color='black')
    base.set_xlim([-179.5, -130])
    ak_wells_crs.plot(ax=base, markersize=2)

# In[  DE-DUPING  ]:

# Ensure date objects are formatted correctly
clean_a_date_field(ak_wells_crs, 'SpudDate')
clean_a_date_field(ak_wells_crs, 'CompletionDate')
clean_a_date_field(ak_wells_crs, 'LastStatusChange')

# Check API format
ak_wells_crs.Api.head()
# Create new column with API-10
ak_wells_crs.Api = ak_wells_crs.Api.astype(str)
ak_wells_crs['API10'] = ak_wells_crs.Api.str[:-4]
ak_wells_crs.API10.head()

# Check for duplicate APIs; if zero, no further action needed
dupes = get_duplicate_api_records(ak_wells_crs, 'Api')
# Number of duplicate records: 0
# Number of unique values: 0
dupes = get_duplicate_api_records(ak_wells_crs, 'API10')
# Number of duplicate records: 5487
# Number of unique values: 2007

# Sort records by API10 and LastStatusChange date, and keep the record with
# the newer LastStatusChange date (If two LastStatusChange are the same,
# then pick the record with the newer Completion Date)
ak_wells_crs_newestfirst = ak_wells_crs.sort_values(by=['API10',
                                                        'LastStatusChange',
                                                        'CompletionDate'],
                                                    ascending=[True, False, False],
                                                    na_position='last')

ak_wells_crs = ak_wells_crs_newestfirst.drop_duplicates(subset=['API10'],
                                                        keep='first')

dupes = get_duplicate_api_records(ak_wells_crs, 'API10')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:

# Revise onshore and offshore flags
ak_wells_crs['Offshore'].replace({'No': 'Onshore',
                                  'Yes': 'Offshore'},
                                 inplace=True)

# Create a FAC_TYPE field that, at minimum, contains the value in the PermittedClass field
ak_wells_crs['factypenew'] = ak_wells_crs.CurrentClass
# Create a copy of the CurrentStatus field that we will revise in the next step
ak_wells_crs['facstatusnew'] = ak_wells_crs.CurrentStatus

# If the CurrentStatus field contains information about the well's TYPE,
# infer what the well's status actually is
# These status mappings are based on Mark's / my previous mappings of well statuses
types_in_status_field = {'Oil well, single completion': 'PRODUCING',
                         'Oil well, dual completion': 'PRODUCING',
                         'Gas well, single completion': 'PRODUCING',
                         'Gas well, dual completion': 'PRODUCING',
                         'Gas well, triple completion': 'PRODUCING',
                         'Gas injection, single completion': 'INJECTING',
                         'Gas storage well; inject & produce': 'PRODUCING',
                         'Gas well (dual) & Storage well; produce only': 'PRODUCING',
                         'Gas well & Storage well; produce only': 'PRODUCING',
                         'Gas well & Disposal well, dual comp': 'PRODUCING',
                         'Water alt gas injection': 'INJECTING',
                         'Water injection, single pool, two tbg strings': 'INJECTING',
                         'Water injection, single completion': 'INJECTING',
                         'Water injection, dual completion': 'INJECTING',
                         'Water supply well': 'Water supply well',
                         'Disposal injection well, Class 1': 'INJECTING',
                         'Disposal injection well, Class 2': 'INJECTING',
                         'Observation well': 'Observation well',
                         'Information well': 'Information well',
                         'Commingled well (dual), oil': 'PRODUCING',
                         'Commingled well (triple), oil': 'PRODUCING',
                         'Geothermal': 'Geothermal'
                         }

# Replace the well TYPE information in the status field with a well STATUS
ak_wells_crs.facstatusnew.replace(types_in_status_field,
                                  inplace=True)

# Append information about the well's TYPE that's present in the CurrentStatus field
# into my new 'factypenew' field.
ak_wells_crs.loc[ak_wells_crs.CurrentStatus.isin(types_in_status_field.keys()), 'factypenew'] = ak_wells_crs.factypenew + ' - ' + ak_wells_crs.CurrentStatus


# In[  integration  ]:

ak_wells_integrated, _errors = integrate_facs(
    ak_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Alaska",
    src_ref_id="186",
    src_date="2024-04-18",  # Daily
    on_offshore="Offshore",
    fac_name="WellName",
    fac_id="API10",
    fac_type="factypenew",
    spud_date='SpudDate',
    comp_date='CompletionDate',
    drill_type=None,
    fac_status="facstatusnew",
    op_name="OperatorName",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ak_wells_integrated.geojson'
ak_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')

# =============================================================================
# %% Arkansas
print(datetime.datetime.now())
print('Arkansas')
# =============================================================================
ar_wells_fp = r'arkansas\wells\OIL_AND_GAS_WELLS_AOGC.shp'
ar_wells = gpd.read_file(ar_wells_fp)

# Some longitude values are erroneously positive -- fix those
ar_wells.loc[ar_wells.longitude > 0, 'longitude'] = ar_wells.longitude * -1
# drop points that are south of Arkansas
ar_wells = ar_wells[ar_wells.latitude >= 33]

# Create a new GDF based on the lat-long column values rather than
# the original geometries in the shapefile
ar_wells_gdf = gpd.GeoDataFrame(ar_wells,
                                geometry=gpd.points_from_xy(ar_wells['longitude'],
                                                            ar_wells['latitude']),
                                crs=4269)

# Check or modify CRS
ar_wells_crs = transform_CRS(ar_wells_gdf,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ar_wells_crs, 'Arkansas')


# In[  DE-DUPING  ]:

# Check API format
ar_wells_crs.api_wellno.head()
# Create new column with API-10
ar_wells_crs.api_wellno = ar_wells_crs.api_wellno.astype(str)
ar_wells_crs['API10'] = ar_wells_crs.api_wellno.str[:-4]
ar_wells_crs.API10.head()


dupes = get_duplicate_api_records(ar_wells_crs, 'api_wellno')
# Number of duplicate records: 0
# Number of unique values: 0
dupes = get_duplicate_api_records(ar_wells_crs, 'API10')
# Number of duplicate records: 16
# Number of unique values: 8

# TODO  - dedupe API10 records
# As of right now, too many attributes differ and I don't feel confident that
# these 16 records are indeed duplicates (different lat-longs, operators, etc.)


# In[  PRE-PROCESSING  ]:
# Remove unnecessary multiple spaces in well name,
# and replace with single spaces
ar_wells_crs['well_nm_new'] = ar_wells_crs['well_nm'].apply(lambda x: ' '.join(x.split()))
# Fix mis-spelling in well type field
ar_wells_crs.replace({'Expired Premit': 'Expired Permit'}, inplace=True)

# Un-abbreviate STATUS values and TYPE values.
# Abbreviations are defined on the "Metadata" tab of this page
# https://gis.arkansas.gov/product/arkansas-oil-and-gas-wells/
ar_status_dict = {
    'A': 'ACTIVE',
    'AOW': 'ABANDONED ORPHANED WELLS',
    'C': 'COMPLETE',
    'DA': 'DRY AND ABANDONED',
    'DW': 'DOMESTIC WELL',
    'EX': 'EXPIRED PERMIT',
    'IN': 'INACTIVE WELL',
    'PA': 'PLUGGED AND ABANDONED',
    'PR': 'PRODUCING WELL',
    'PW': 'PERMITTED WELL',
    'RW': 'RELEASED WATER WELL',
    'SI': 'SHUT IN',
    'SP': 'SPUD WELL',
    'TA': 'TEMPORARILY ABANDONED',
    'UN': 'N/A',
    'ex': 'EXPIRED PERMIT'
}
ar_wells_crs.wl_status = ar_wells_crs.wl_status.replace(ar_status_dict)


ar_type_dict = {
    'BIW': 'BRINE INJECTION WELL',
    'BSW': 'BRINE SUPPLY WELL',
    'CBM': 'COAL BED METHANE',
    'CBMS': 'COAL BED METHANE SERVICE',
    'EOR': 'ENHANCED OIL RECOVERY',
    'EXP': 'EXPIRED PERMIT',
    'GAS': 'NATURAL GAS WELL',
    'GI': 'GAS INJECTION',
    'GS': 'GAS STORAGE',
    'ISC': 'N/A',  # "In Search Of Code (Unknown)"
    'OIL': 'OIL WELL',
    'SEX': 'SEISMIC EXPLORATORY WELL',
    'SW': 'SALT WATER',
    'SWD': 'SALT WATER DISPOSAL WELL',
    'SWI': 'SALT WATER INJECTION',
    'WDW': 'WASTE DISPOSAL WELL',
    'WS': 'WATER SUPPLY'
}
ar_wells_crs.well_typ = ar_wells_crs.well_typ.replace(ar_type_dict)


# In[integration]:

ar_wells_integrated, _errors = integrate_facs(
    ar_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Arkansas",
    src_ref_id="187",
    src_date="2023-12-13",  # Irregularly
    on_offshore=None,
    fac_name="well_nm_new",
    fac_id="api_wellno",
    fac_type="well_typ",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    fac_status="wl_status",
    op_name="coname",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save as geojson
out_fp = outfolder + 'ar_wells_integrated.geojson'
ar_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% California
print(datetime.datetime.now())
print('California')
# =============================================================================
ca_wells_fp = r'california\wells\Wells_All.shp'
ca_wells = gpd.read_file(ca_wells_fp)  # original epsg=3310

ca_wells_crs = transform_CRS(ca_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ca_wells_crs, 'California')

# In[  DE-DUPING  ]:

# Check API format
ca_wells_crs.API.head()  # API-10 already
# Check for duplicate APIs; if zero, no further action needed
dupes = get_duplicate_api_records(ca_wells_crs, 'API')
# Number of duplicate records: 0
# Number of unique values: 0


# In[  PRE-PROCESSING  ]:

# Since I believe 1801-01-01 dates are California's "N/A" value, alter these
ca_wells_crs.loc[ca_wells_crs.SpudDate == '01/01/1801', 'SpudDate'] = '01/01/1900'
# Ensure date objects are formatted correctly
clean_a_date_field(ca_wells_crs, 'SpudDate')

# Change directional attribute to long form
ca_wells_crs['isDirectio'].replace({'N': 'Vertical', 'Y': 'Horizontal'},
                                   inplace=True)


ca_wells_crs = replace_missing_strings_with_na(ca_wells_crs, ['LeaseName',
                                                              'WellNumber'])

create_concatenated_well_name(ca_wells_crs,
                              'LeaseName',
                              'WellNumber',
                              'WellNameNew')

# In[  INTEGRATION  ]:

ca_wells_integrated, _errors = integrate_facs(
    ca_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="California",
    src_ref_id="188",
    src_date="2024-04-18",  # Daily
    on_offshore=None,
    fac_name='WellNameNew',
    fac_id="API",
    fac_type="WellTypeLa",
    spud_date="SpudDate",
    comp_date=None,
    drill_type="isDirectio",
    fac_status="WellStatus",
    op_name="OperatorNa",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save as geojson
out_fp = outfolder + 'ca_wells_integrated.geojson'
ca_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')

# =============================================================================
# %% Colorado
print(datetime.datetime.now())
print('Colorado')
# =============================================================================
co_wells_fp = r'colorado\wells\Wells.shp'
co_wells = gpd.read_file(co_wells_fp)

co_wells_crs = transform_CRS(co_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(co_wells_crs, 'Colorado')


# In[  DE DUPLICATING  ]:

# Check API format
co_wells_crs.API_Label.head()  # API-10

# Check for duplicate APIs; if zero, no further action needed
# There aren't any missing N/A or null APIs here
dupes = get_duplicate_api_records(co_wells_crs, 'API_Label')
# Number of duplicate records: 906
# Number of unique values: 415

# Drop rows where we know there are "true duplicates", with dupe values in all
# the attributes we care about.
co_wells_crs = co_wells_crs.drop_duplicates(subset=['API_Label',
                                                    'Well_Num',
                                                    'Operator',
                                                    'Spud_Date',
                                                    'Facil_Stat',
                                                    'Latitude',
                                                    'Longitude'],
                                            keep="last")

# Now, all that remains are duplicate API records with different spud date values
dupes = get_duplicate_api_records(co_wells_crs, 'API_Label')
# Number of duplicate records: 112
# Number of unique values: 56

# Ensure date objects are formatted correctly
clean_a_date_field(co_wells_crs, 'Spud_Date')

# Sort records by API and Spud_Date, and keep the record with the OLDER spud date
co_wells_crs_oldestfirst = co_wells_crs.sort_values(by=['API_Label',
                                                        'Spud_Date'],
                                                    ascending=[True, True],
                                                    na_position='last')
co_wells_crs = co_wells_crs_oldestfirst.drop_duplicates(subset=['API_Label'],
                                                        keep='first')

dupes = get_duplicate_api_records(co_wells_crs, 'API_Label')
# Number of duplicate records: 0
# Number of unique values: 0

# Though there are duplicate lat-long pairs remaining, they have different APIs
# and different attributes, so DON'T DEDUPE THESE PAIRS
# dupes = get_duplicate_api_records(co_wells_crs, 'longitude_calc')
# # Number of duplicate records: 1024
# # Number of unique values: 501


# In[  PRE-PROCESSING  ]:

# Change status attribute to long form
# https://ecmc.state.co.us/documents/about/COGIS_Help/Status_Codes.pdf
# co_wells_crs['Facil_Stat'].unique()
co_wells_crs['Facil_Stat'].replace({'AC': 'Active',
                                    'AB': 'Abandoned Drilled Wellbore',
                                    'AL': 'Abandoned Location; Well has not been spud',
                                    'DA': 'Dry and Abandoned',
                                    'DG': 'Drilling',
                                    'DM': 'Domestic Gas',
                                    'IJ': 'Injection Well',
                                    'PA': 'Plugged and Abandoned',
                                    'pa': 'Plugged and Abandoned',
                                    'PR': 'Producing',
                                    'SI': 'Shut In',
                                    'SU': 'Suspended Permit',
                                    'TA': 'Temporarily Abandoned',
                                    'WO': 'Waiting on Completion',
                                    'XX': 'Approved Permit; Not yet reported as spud',
                                    'UN': 'Unknown',
                                    'EP': 'Expired Permit',
                                    'AP': 'Active Permit; Not Yet Spudded',
                                    'SO': 'Suspended Operations',
                                    'CL': 'Closed Location'},
                                   inplace=True)


# In[  integration  ]:

co_wells_integrated, _errors = integrate_facs(
    co_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Colorado",
    src_ref_id="189",
    src_date="2024-04-18",  # Daily
    on_offshore=None,
    fac_name="Well_Title",
    fac_id="API_Label",
    fac_type=None,
    spud_date="Spud_Date",
    comp_date=None,
    drill_type=None,
    fac_status="Facil_Stat",
    op_name="Operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'co_wells_integrated.geojson'
co_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')

# =============================================================================
# %% Illinois  # TODO  Try test export geojson
# TODO - revise input filename so that it auto-fills with the "latest date"
# =============================================================================
# il_wells_fp = r'illinois\ILOIL_Wells.geojson'

# il_wells_fp = r'illinois\wells\ILOIL_Wells_2024-04-19.geojson'
# il_wells = gpd.read_file(il_wells_fp)

# il_wells_fp_2 = r'illinois\wells\IL_WELLS_Borings_Location_Pt.shp'
# il_wells_2 = gpd.read_file(il_wells_fp_2)

# # Coordinates in geometry column are actually in Web Mercator;
# # set the CRS of this gdf directly.
# wv_wells = wv_wells.set_crs('epsg:3857', allow_override=True)


# il_wells_crs = transform_CRS(il_wells,
#                              target_epsg_code="epsg:4326",
#                              appendLatLon=True)

# if print_maps:
#     quickmap(il_wells_crs, 'Illinois')


# # In[  DE DUPLICATING  ]:

# # Check API format
# il_wells_crs.API_NUMBER.head()  # API-12

# =============================================================================
# %% Indiana  # WIP
# =============================================================================
# in_wells_fp = r'Indiana\2023-11-14\IN_DNR_Oil_and_Gas_Wells_%3A_All_Records.geojson'
# in_wells = gpd.read_file(in_wells_fp)

# in_wells_fp_2 = r'indiana\IN_OilAndGasWells_2024-02-01.geojson'
# in_wells_2 = gpd.read_file(in_wells_fp_2)


# in_wells_fp_3 = r'indiana\OilAndGasWells.geojson'
# in_wells_3 = gpd.read_file(in_wells_fp_3)

# in_wells_crs = transform_CRS(in_wells,
#                              target_epsg_code="epsg:4326",
#                              appendLatLon=True)

# if print_maps:
#     quickmap(in_wells_crs, 'Indiana')


# # In[  DE DUPLICATING  ]:

# # Check permit number format
# in_wells_crs.permit_number.head()  # 5-digit permit number, not an API

# dupes = get_duplicate_api_records(in_wells_crs, 'permit_number')
# # Number of duplicate records: 4602
# # Number of unique values: 2301

# # For many/all of the duplicate permit records, one record has a geometry and
# # the other doesn't. Sort the records so that non-null values are listed first

# # Sort records by API and Spud_Date, and keep the record with the OLDER spud date
# testtesttesttesttesttest = in_wells_crs.sort_values(by=['permit_number',
#                                                         'latitude_calc'],
#                                                     ascending=[True, False],
#                                                     na_position='last')
# co_wells_crs = co_wells_crs_oldestfirst.drop_duplicates(subset=['API_Label'],
#                                                         keep='first')


# =============================================================================
# %% Kansas
print(datetime.datetime.now())
print('Kansas')
# =============================================================================
ks_wells = pd.read_csv(r'kansas\wells\ks_wells.txt', sep=',')
# Drop records with no lat-long info
ks_wells = ks_wells[ks_wells.LONGITUDE.notna()]

ks_wells_gdf = gpd.GeoDataFrame(ks_wells,
                                geometry=gpd.points_from_xy(ks_wells.LONGITUDE,
                                                            ks_wells.LATITUDE),
                                crs=4267)  # Original data in NAD27


ks_wells_crs = transform_CRS(ks_wells_gdf,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ks_wells_crs, 'Kansas')


# In[  cleaning date fields  ]:

replace_missing_strings_with_na(ks_wells_crs,
                                ['LEASE',
                                 'WELL',
                                 'CURR_OPERATOR'],
                                limit_acceptable_columns=False)

# Fill in "na date" in the date format used by the original data source
ks_wells_crs.SPUD = ks_wells_crs.SPUD.fillna('01-JAN-1900')
ks_wells_crs.COMPLETION = ks_wells_crs.COMPLETION.fillna('01-JAN-1900')
ks_wells_crs.MODIFIED = ks_wells_crs.MODIFIED.fillna('01-JAN-1900')

# FOR NOW, Manually alter all the year values I know to be typos
# Ideally, fix this with a custom function in the future

# ks_wells_crs['spudyears'] = pd.Series(ks_wells_crs.SPUD.str[-4:]).astype(int)
# ks_wells_crs['compyears'] = pd.Series(ks_wells_crs.COMPLETION.str[-4:]).astype(int)
# ks_wells_crs['modyears'] = pd.Series(ks_wells_crs.MODIFIED.str[-4:]).astype(int)
# x = ks_wells_crs[ks_wells_crs.spudyears < 1900]
# x = ks_wells_crs[ks_wells_crs.compyears < 1900]
# ks_wells_crs['modyears'].value_counts()

ks_wells_crs.SPUD.replace({'02-MAY-1651': '02-MAY-1951',
                           '21-DEC-0758': '21-DEC-1958',
                           '06-DEC-1057': '06-DEC-1957',
                           '11-JAN-0964': '11-JAN-1964',
                           '06-SEP-1852': '06-SEP-1952',
                           '29-JAN-1854': '29-JAN-1954',
                           '10-DEC-1864': '10-DEC-1964',
                           '19-SEP-1853': '19-SEP-1953'}, inplace=True)

ks_wells_crs.COMPLETION.replace({'08-APR-1847': '08-APR-1947',
                                 '05-AUG-1877': '05-AUG-1977',
                                 '01-DEC-1781': '01-DEC-1981',
                                 '17-NOV-1041': '17-NOV-1941',
                                 '19-NOV-1198': '19-NOV-1998',
                                 '27-JUN-0950': '27-JUN-1950',
                                 '21-MAR-0935': '21-MAR-1935'}, inplace=True)


# Convert the dd-MONTH-YYYY style dates to YYYY-MM-DD style.
# If any dates fail, coerce them into NaT / na type
ks_wells_crs['SPUD'] = pd.to_datetime(ks_wells_crs['SPUD'],
                                      format='%d-%b-%Y',
                                      errors='coerce')

ks_wells_crs['COMPLETION'] = pd.to_datetime(ks_wells_crs['COMPLETION'],
                                            format='%d-%b-%Y',
                                            errors='coerce')

ks_wells_crs['MODIFIED'] = pd.to_datetime(ks_wells_crs['MODIFIED'],
                                          format='%d-%b-%Y',
                                          errors='coerce')

clean_a_date_field(ks_wells_crs, 'SPUD')
clean_a_date_field(ks_wells_crs, 'COMPLETION')
clean_a_date_field(ks_wells_crs, 'MODIFIED')

create_concatenated_well_name(ks_wells_crs, 'LEASE', 'WELL', 'wellnamenew')

# In[  DEDUPLICATION  ]:

# Check API format
ks_wells_crs.API_NUMBER
# For APIs that don't have an ending -0000, append it (so that format of all
# APIs are the same)
condition = (ks_wells_crs.API_NUMBER.str.len() == 12)
ks_wells_crs.loc[condition, 'API_NUMBER'] = ks_wells_crs.API_NUMBER + '-0000'
# Create new column to contain the API-10 formatted number
ks_wells_crs['API10'] = ks_wells_crs.API_NUMBER.str[:-5]
ks_wells_crs['API10']

replace_missing_strings_with_na(ks_wells_crs,
                                ['API_NUMBER', 'API10'],
                                limit_acceptable_columns=False)

# Check for dupe records
dupes = get_duplicate_api_records(ks_wells_crs, 'API_NUMBER')
# Number of duplicate records: 6596
# Number of unique values: 3295
dupes = get_duplicate_api_records(ks_wells_crs, 'API10')
# Number of duplicate records: 82975
# Number of unique values: 37677

# Create df to contain all records with API-14s of N/A, AND a df of all non-null APIs.
# Deduplicate both separately, and append them together at the end.
ks_wells_crs_noAPI = ks_wells_crs[ks_wells_crs['API_NUMBER'] == "N/A"]  # 21,453
ks_wells_crs_yesAPI = ks_wells_crs[ks_wells_crs['API_NUMBER'] != "N/A"]  # 474,788


# -----------------------------------------------------------------------------
# First, REMOVE API-14 DUPLICATES
# Sort records by API-14 and MODIFIED and keep the newest modified. If there's
# a tie, keep the newest SPUD date.
ks_wells_crs_yesAPI_newestfirst = ks_wells_crs_yesAPI.sort_values(by=['API_NUMBER',
                                                                      'MODIFIED',
                                                                      'SPUD'],
                                                                  ascending=[True, False, False],
                                                                  na_position='last')
ks_wells_deduped1 = ks_wells_crs_yesAPI_newestfirst.drop_duplicates(subset=['API_NUMBER',
                                                                            'TOWNSHIP',
                                                                            'TWN_DIR',
                                                                            'RANGE',
                                                                            'RANGE_DIR',
                                                                            'SECTION'],
                                                                    keep='first')
# See how many duplicate API-14s still exist
dupes = get_duplicate_api_records(ks_wells_deduped1, 'API_NUMBER')
# Number of duplicate records: 1240
# Number of unique values: 619

# Next, dedupe API14s that have not-identical locations, but have the same
# lease name and well number
ks_wells_deduped2 = ks_wells_deduped1.drop_duplicates(subset=['API_NUMBER',
                                                              'LEASE',
                                                              'WELL'],
                                                      keep='first')
dupes = get_duplicate_api_records(ks_wells_deduped2, 'API_NUMBER')
# Number of duplicate records: 868
# Number of unique values: 433
# FIXME later, but for now, LEAVE the remaining API-14 duplicates since it's
# less straightforward to tell which wells truly lie at the same location.

# -----------------------------------------------------------------------------
# REMOVE API-10 DUPLICATES
# Sort API10 records by MODIFIED and keep the newest modified. If there's a
# tie, keep the newest SPUD date.
ks_wells_deduped2_newest = ks_wells_deduped2.sort_values(by=['API10',
                                                             'MODIFIED',
                                                             'SPUD'],
                                                         ascending=[True, False, False],
                                                         na_position='last')
# Remove API-10 duplicates with same lease/name and same township/range/section
ks_wells_deduped3 = ks_wells_deduped2_newest.drop_duplicates(subset=['API10',
                                                                     'LEASE',
                                                                     'WELL',
                                                                     'TOWNSHIP',
                                                                     'TWN_DIR',
                                                                     'RANGE',
                                                                     'RANGE_DIR',
                                                                     'SECTION'],
                                                             keep='first')
dupes = get_duplicate_api_records(ks_wells_deduped3, 'API10')
# Number of duplicate records: 31864
# Number of unique values: 15549

# Next, remove dupes with same township/range/section, same lease name but
# different well name
ks_wells_deduped4 = ks_wells_deduped3.drop_duplicates(subset=['API10',
                                                              'LEASE',
                                                              'TOWNSHIP',
                                                              'TWN_DIR',
                                                              'RANGE',
                                                              'RANGE_DIR',
                                                              'SECTION'],
                                                      keep='first')
dupes = get_duplicate_api_records(ks_wells_deduped4, 'API10')
# Number of duplicate records: 26071
# Number of unique values: 12794

# Next, remove dupes with same lat-long, same well number but diff lease name
# (I see many cases of slightly-different punctuation in the lease name column,
# but the record clearly refers to the same well)
ks_wells_deduped5 = ks_wells_deduped4.drop_duplicates(subset=['API10',
                                                              'WELL',
                                                              'TOWNSHIP',
                                                              'TWN_DIR',
                                                              'RANGE',
                                                              'RANGE_DIR',
                                                              'SECTION'],
                                                      keep='first')
dupes = get_duplicate_api_records(ks_wells_deduped5, 'API10')
# Number of duplicate records: 12057
# Number of unique values: 5989

# Next, remove records with the same API10, same lat-long, and same operator
ks_wells_deduped6 = ks_wells_deduped5.drop_duplicates(subset=['API10',
                                                              'CURR_OPERATOR',
                                                              'latitude_calc',
                                                              'longitude_calc'],
                                                      keep='first')
dupes = get_duplicate_api_records(ks_wells_deduped6, 'API10')
# Number of duplicate records: 9991
# Number of unique values: 4973

# FIXME later, but for now LEAVE the remaining API-10 duplicates since it's
# less straightforward to tell which wells truly lie at the same location.

# -----------------------------------------------------------------------------
# NOTE ABOUT DUPLICATE LAT-LONG PAIRS
# Though there are duplicate lat-long pairs remaining, they have different
# well numbers (like 1, 2, 3, etc.), and many of the lat-long locations are
# simply the center of a township section. DON'T DEDUPE THESE PAIRS
dupes = get_duplicate_api_records(ks_wells_deduped6, 'longitude_calc')
# Number of duplicate records: 53636
# Number of unique values: 17692

# -----------------------------------------------------------------------------
# Now move onto the wells with NO API information. Some duplicate records exist
# in here as well -- remove them
dupes = get_duplicate_api_records(ks_wells_crs_noAPI, 'longitude_calc')
# Number of duplicate records: 5667
# Number of unique values: 1658

# First, sort these records so that the newest is on top.
ks_wells_crs_noAPI_newestfirst = ks_wells_crs_noAPI.sort_values(by=['wellnamenew',
                                                                    'MODIFIED',
                                                                    'SPUD'],
                                                                ascending=[True, False, False],
                                                                na_position='last')
# Remove any duplicates that share a lat-long coord as well as lease/well num,
# spud, completion, status, and operator information.
ks_wells_noAPI_dedupe1 = ks_wells_crs_noAPI_newestfirst.drop_duplicates(subset=['API_NUMBER',
                                                                                'LEASE',
                                                                                'WELL',
                                                                                'CURR_OPERATOR',
                                                                                'SPUD',
                                                                                'COMPLETION',
                                                                                'STATUS',
                                                                                'longitude_calc',
                                                                                'latitude_calc'],
                                                                        keep='first')
dupes = get_duplicate_api_records(ks_wells_noAPI_dedupe1, 'wellnamenew')
# Number of duplicate records: 5271
# Number of unique values: 1670

# Next, remove wells that don't have exactly the same location, but are probably
# records that refer to the same well based on their other attributes, and are
# also in the same township and range
ks_wells_noAPI_dedupe2 = ks_wells_noAPI_dedupe1.drop_duplicates(subset=['API_NUMBER',
                                                                        'LEASE',
                                                                        'WELL',
                                                                        'TOWNSHIP',
                                                                        'TWN_DIR',
                                                                        'RANGE',
                                                                        'RANGE_DIR',
                                                                        'ORIG_OPERATOR',
                                                                        'CURR_OPERATOR',
                                                                        'SPUD',
                                                                        'COMPLETION',
                                                                        'STATUS'],
                                                                keep='first')
dupes = get_duplicate_api_records(ks_wells_noAPI_dedupe2, 'wellnamenew')
# Number of duplicate records: 5158
# Number of unique values: 1640

# Next, remove likely duplicate records that share a very similar location,
# even if their original operator is different
ks_wells_noAPI_dedupe3 = ks_wells_noAPI_dedupe2.drop_duplicates(subset=['API_NUMBER',
                                                                        'LEASE',
                                                                        'WELL',
                                                                        'TOWNSHIP',
                                                                        'TWN_DIR',
                                                                        'RANGE',
                                                                        'RANGE_DIR',
                                                                        'SECTION',
                                                                        # 'ORIG_OPERATOR',
                                                                        'CURR_OPERATOR',
                                                                        'SPUD',
                                                                        'COMPLETION',
                                                                        'STATUS'],
                                                                keep='first')
dupes = get_duplicate_api_records(ks_wells_noAPI_dedupe3, 'wellnamenew')
# Number of duplicate records: 5063
# Number of unique values: 1613

# FINALLY, combine yesAPI records with noAPI records, for final wells layer
ks_wells_crs = pd.concat([ks_wells_deduped6,
                          ks_wells_noAPI_dedupe1]).reset_index(drop=True)


# In[    PREPROCESSING    ]:

# Split existing STATUS field into separate FAC_STATUS and FAC_TYPE information
# https://www.kgs.ku.edu/Magellan/Elog/status.html
# https://www.kgs.ku.edu/Magellan/Qualified/ogwell_fgdc.html
'''
CBM = produced coalbed methane;
CBM-P&A = produced coalbed methane, since plugged and abandoned;
D&A = never produced, now plugged and abandoned;
EOR = enhanced oil recovery well;
EOR-P&A = enhanced oil recovery well, since plugged and abandoned;
GAS = produced natural gas;
GAS-P&A = produced natural gas, since plugged and abandoned;
INJ = salt water disposal well or other injection well;
INJ-P&A = salt water disposal well or other injection well, since plugged and abandoned;
INTENT = proposed well, not yet drilled;
LOC = well that was never actually drilled;
O&G = produced oil and gas;
O&G-P&A = produced oil and gas, since plugged and abandoned;
OIL = produced oil;
OIL-P&A = produced oil, since plugged and abandoned;
OTHER = may not be an energy well, since water research wells and road construction wells are in database under some conditions;
OTHER-P&A = miscellaneous well since plugged;
SWD = salt water disposal well;
SWD-P&A = salt water disposal well, since plugged and abandoned
'''

ks_wells_crs.STATUS2.replace({np.nan: 'N/A'},
                             inplace=True)

print(*sorted(ks_wells_crs.STATUS.unique()), sep='\n')
print(*sorted(ks_wells_crs.STATUS2.unique()), sep='\n')


# Create list of values in the STATUS column that can be associated with certain
# FAC_TYPE values
cbm = ['CBM',
       'CBM-P&A']
eor = ['EOR',
       'EOR-P&A']
gas = ['GAS',
       'GAS-P&A']
inj = ['INJ',
       'INJ-P&A']
oil = ['OIL',
       'OIL-P&A']
na = ['INTENT',
      'LOC',
      'OTHER(NULL)',
      'OTHER(Unknown)',
      'OTHER(unknown)',
      'OTHER-P&A(Unknown)']
oilandgas = ['O&G',
             'O&G-P&A']
service = ['SERVICE',
           'SERVICE-P&A',
           'OTHER(SERVICE)',
           'OTHER-P&A(SERVICE)']
swd = ['SWD',
       'SWD-P&A']
cathode = ['OTHER(CATH)',
           'OTHER-P&A(CATH)']
strattest = ['OTHER(CORE HOLE)',
             'OTHER(COREHOLE)',
             'OTHER(STRAT)',
             'OTHER-P&A(COREHOLE)',
             'OTHER-P&A(STRAT)']

abdloc = ['OTHER(ABD LOC)']

class1 = ['OTHER(CLASS ONE (OLD))',
          'OTHER(CLASS1)',
          'OTHER-P&A(CLASS ONE (OLD))']

dry = ['D&A',
       'OTHER(Dry)',
       'OTHER(dry)']

gasinj = ['OTHER(GAS INJ)',
          'OTHER(GAS-INJ)',
          'OTHER-P&A(GAS-INJ)']

lost = ['OTHER(LH)',
        'OTHER-P&A(LH)']

observation = ['OTHER(OBSERVATION)',
               'OTHER(OBS)',
               'OTHER(MONITOR)',
               'OTHER(Monitor)',
               'OTHER-P&A(OBS)']

# Create new FAC_TYPE field and populate it based on the contents of STATUS field
# and the information from Kansas's metadata
ks_wells_crs['FAC_TYPE'] = ks_wells_crs['STATUS']
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(cbm), 'FAC_TYPE'] = 'Coal bed methane'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(eor), 'FAC_TYPE'] = 'Enhanced oil recovery'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(gas), 'FAC_TYPE'] = 'Gas production'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(inj), 'FAC_TYPE'] = 'Injection well'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(oil), 'FAC_TYPE'] = 'Oil production'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(oilandgas), 'FAC_TYPE'] = 'Oil and gas'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(service), 'FAC_TYPE'] = 'Service well'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(swd), 'FAC_TYPE'] = 'Salt water disposal well'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(cathode), 'FAC_TYPE'] = 'Cathodic protection well'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(strattest), 'FAC_TYPE'] = 'Stratigraphic test'

ks_wells_crs.loc[ks_wells_crs.STATUS.isin(abdloc), 'FAC_TYPE'] = 'Abandoned location, never drilled'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(class1), 'FAC_TYPE'] = 'Class 1 injection well'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(dry), 'FAC_TYPE'] = 'Dry hole'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(gasinj), 'FAC_TYPE'] = 'Gas injection'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(lost), 'FAC_TYPE'] = 'Lost hole'
ks_wells_crs.loc[ks_wells_crs.STATUS.isin(observation), 'FAC_TYPE'] = 'Observation well'

# In[  integration  ]:

ks_wells_integrated, _errors = integrate_facs(
    ks_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Kansas",
    src_ref_id="192",
    src_date="2024-03-01",  # Monthly
    # on_offshore= None,
    fac_name='wellnamenew',
    fac_id="API_NUMBER",
    fac_type='FAC_TYPE',
    spud_date="SPUD",
    comp_date="COMPLETION",
    drill_type=None,
    fac_status="STATUS2",
    op_name="CURR_OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ks_wells_integrated.geojson'
ks_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Kentucky
print(datetime.datetime.now())
print('Kentucky')
# =============================================================================
ky_wells_fp = r'kentucky\wells\kyog_dd.shp'
ky_wells = gpd.read_file(ky_wells_fp)


ky_wells_crs = transform_CRS(ky_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ky_wells_crs, 'Kentucky')

# In[  PRE-PROCESSING  ]:

# Check API format  (records with API info all just end with '0000')
ky_wells_crs.API.head()
# Create new column with API-10
ky_wells_crs['API10'] = ky_wells_crs.API.str[:-4]
ky_wells_crs.API10.head()

dupes = get_duplicate_api_records(ky_wells_crs, 'API')
# Number of duplicate records: 568
# Number of unique values: 282

# Dedupe API14 - Sort records by API14 and Cmpl_Date, and keep the record with
# the newer Cmpl_Date. If two Cmpl_Date are the same, then pick the record with
# the most recent PlugDate
ky_wells_crs_newestfirst = ky_wells_crs.sort_values(by=['API',
                                                        'Cmpl_Date',
                                                        'Plug_Date'],
                                                    ascending=[True, False, False],
                                                    na_position='last')

ky_wells_crs = ky_wells_crs_newestfirst.drop_duplicates(subset=["API",
                                                                "Org_WellNo",
                                                                "Org_Farm",
                                                                "longitude_calc",
                                                                "latitude_calc"],
                                                        keep='first')

dupes = get_duplicate_api_records(ky_wells_crs, 'API')
# Number of duplicate records: 252
# Number of unique values: 126
dupes = get_duplicate_api_records(ky_wells_crs, 'API10')
# Number of duplicate records: 252
# Number of unique values: 126

# Dedupe API10 - DROP duplicate records based on API and values that will
# help determine whether this is the exact same well --
# farm name, well number, and number/section for appx. spatial location
ky_wells_crs = ky_wells_crs.drop_duplicates(subset=["API10",
                                                    "Number",
                                                    "Section",
                                                    "FNS",
                                                    # "FEW",
                                                    # "Org_WellNo"
                                                    # "Org_Farm"
                                                    "USGS_Quad"],
                                            keep="first")

dupes = get_duplicate_api_records(ky_wells_crs, 'API')
# Number of duplicate records: 60
# Number of unique values: 30

# KEEP these remaining "duplicate APIs" in there, because they refer to very
# different spatial locations


# In[  PRE-PROCESSING  ]:

ky_wells_crs = replace_missing_strings_with_na(ky_wells_crs,
                                               ['Org_Farm',
                                                'Org_Oper',
                                                'Org_WellNo'],
                                               limit_acceptable_columns=False)

# Concatenate Org_Farm and Org_WellNo to create a more complete Well Name
create_concatenated_well_name(ky_wells_crs,
                              'Org_Farm',
                              'Org_WellNo',
                              'wellnamenew')

# Ensure date objects are formatted correctly
clean_a_date_field(ky_wells_crs, 'Cmpl_Date')


# Convert Abbreviations -> Long form
ky_wells_crs['bore_type'].replace({'V': 'Vertical',
                                   'H': 'Horizontal',
                                   'D': 'Directional'},
                                  inplace=True)

ky_wells_crs['Org_WClass'].replace({'DEV': 'Development well',
                                    'Dev': 'Development well',
                                    'DPT': 'Deeper pool test',
                                    'DPTD': 'Deeper pool test resulting in development of existing production formation',
                                    'EXT': 'Extension well',
                                    'MSC': 'N/A',   # Miscellaneous well
                                    'NFW': 'New field wildcat',
                                    'NPW': 'New pool wildcat',
                                    'SPT': 'Shallower pool test',
                                    'SRV': 'Service well, EPA Class II injection',
                                    'STR': 'Stratigraphic test with records released to the public',
                                    'STRP': 'Stratigraphic test with records released to the public',
                                    'UNC': 'N/A',  # Unclassified
                                    'WSW': 'Water supply well'},
                                   inplace=True)

ky_wells_crs['Org_Result'].replace({'AB': 'Well known to be abandoned but with no plugging information on file',
                                    'AI': 'Air injection',
                                    'CBM': 'Coalbed methane gas producer',
                                    'COI': 'Carbon dioxide injection',
                                    'CP': 'Cathodic protection',
                                    'D&A': 'Dry & abandoned',
                                    'DG': 'Domestic gas',
                                    'ERI': 'Enhanced recovery injection (Class II)',
                                    'FF': 'Fire flood',
                                    'GAS': 'Gas producer',
                                    'GI': 'Gas injection',
                                    'GS': 'Gas storage',
                                    'IA': 'Improperly abandoned',
                                    'LOC': 'Location (new permit issued or insufficient data)',
                                    'N2I': 'Nitrogen injection',
                                    'O&G': 'Combined oil & gas producer',
                                    'OB': 'Observation',
                                    'OIL': 'Oil producer',
                                    'SI': 'Steam injection',
                                    'SRI': 'Secondary recovery injection (Class II)',
                                    'SWD': 'Salt water disposal',
                                    'TA': 'Temporarily abandoned',
                                    'TAI': 'Thermal & air injection',
                                    'TP': 'Termination pending',
                                    'TRI': 'Tertiary recovery injection (Class II)',
                                    'TRM': 'Terminated (permit expired or cancelled)',
                                    'UN': 'N/A',  # Unknown (as classified by KY DOGC)
                                    'WD': 'Water disposal',
                                    'WI': 'Water injection',
                                    'WS': 'Water supply',
                                    'WW': 'Water well'},
                                   inplace=True)


# Create a FAC_TYPE field that, at minimum, contains the value in the Org_WClass field
ky_wells_crs['factypenew'] = ky_wells_crs.Org_WClass
ky_wells_crs['factypenew'] = ky_wells_crs['factypenew'].fillna('N/A')
# Create a copy of the Org_Result (status) field that we will revise in the next step
ky_wells_crs['facstatusnew'] = ky_wells_crs.Org_Result
ky_wells_crs['facstatusnew'] = ky_wells_crs['facstatusnew'].fillna('N/A')

# If the Org_Result field contains information about the well's TYPE,
# infer what the well's status actually is
# These status mappings are based on Mark's / my previous mappings of well statuses
types_in_status_field = {'Oil producer': 'PRODUCING',
                         'Gas producer': 'PRODUCING',
                         'Secondary recovery injection (Class II)': 'INJECTING',
                         'Combined oil & gas producer': 'PRODUCING',
                         'Gas storage': 'Gas storage',
                         'Water injection': 'Water injection',
                         'Domestic gas': 'Domestic gas',
                         'Water supply': 'Water supply',
                         'Salt water disposal': 'Salt water disposal',
                         'Observation': 'Observation',
                         'Gas injection': 'INJECTING',
                         'Tertiary recovery injection (Class II)': 'INJECTING',
                         'Water well': 'Water well',
                         'Water disposal': 'Water disposal',
                         'Coalbed methane gas producer': 'PRODUCING',
                         'Thermal & air injection': 'INJECTING',
                         'Cathodic protection': 'Other',
                         'Enhanced recovery injection (Class II)': 'INJECTING',
                         'Steam injection': 'INJECTING',
                         'Air injection': 'INJECTING'
                         }

# Replace the well TYPE information in the status field with a well STATUS
ky_wells_crs.facstatusnew.replace(types_in_status_field,
                                  inplace=True)

# Append information about the well's TYPE that's present in the Org_Result field
# into my new 'factypenew' field.
ky_wells_crs.loc[ky_wells_crs.Org_Result.isin(types_in_status_field.keys()), 'factypenew'] = ky_wells_crs.factypenew + ' - ' + ky_wells_crs.Org_Result
ky_wells_crs.loc[ky_wells_crs.factypenew.str.contains('N/A - '), 'factypenew'] = ky_wells_crs.factypenew.str.lstrip('N/A - ')


# In[  INTEGRATION  ]:

ky_wells_integrated, _errors = integrate_facs(
    ky_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Kentucky",
    src_ref_id="193",
    src_date="2024-03-21",  # Quarterly
    on_offshore=None,
    fac_name="wellnamenew",
    fac_id="API10",
    fac_type="factypenew",
    spud_date=None,
    comp_date="Cmpl_Date",
    drill_type="bore_type",
    fac_status="facstatusnew",
    op_name="Org_Oper",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ky_wells_integrated.geojson'
ky_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Louisiana
print(datetime.datetime.now())
print('Louisiana')
# =============================================================================
# la_wells_csv = pd.read_excel('Louisiana/Location/LA_wells.xlsx',
#                              sheet_name='LA_wells')
la_wells_csv = pd.read_csv(r'louisiana/wells/Results.csv', encoding="latin-1")

# Drop rows with no lat-long information at all (here, the null value is simply "-")
# la_wells_csv['Latitude'] = la_wells_csv['Latitude'].replace('-', 0)
# la_wells_csv['Longitude'] = la_wells_csv['Longitude'].replace('-', 0)
la_wells_csv = la_wells_csv[la_wells_csv.Latitude != '-']
la_wells_csv = la_wells_csv[la_wells_csv.Longitude != '-']
# Cast lat long columns as numeric type
la_wells_csv.Latitude = la_wells_csv.Latitude.astype(float)
la_wells_csv.Longitude = la_wells_csv.Longitude.astype(float)

# Filter out points outside of the general state area

la_wells_csv = la_wells_csv[la_wells_csv['Latitude'].between(28, 34)]
la_wells_csv = la_wells_csv[la_wells_csv['Longitude'].between(-95, -86)]


# FIXME - Assuming that lat-long points are in wgs84 but that could be wrong
# Create GDF from DF
la_wells = gpd.GeoDataFrame(la_wells_csv,
                            geometry=gpd.points_from_xy(la_wells_csv['Longitude'],
                                                        la_wells_csv['Latitude']),
                            crs=4326)

la_wells_crs = transform_CRS(la_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(la_wells_crs, 'Louisiana')


# In[  DEDUPLICATION  ]:

la_wells_crs['API Num'] = la_wells_crs['API Num'].astype('string')
la_wells_crs['API Num'].replace({'0': 'N/A',
                                 '0000': 'N/A',
                                 '00000000000000': 'N/A',
                                 '-': 'N/A'},
                                inplace=True)

dupes = get_duplicate_api_records(la_wells_crs, 'API Num')
# Number of duplicate records: 40667
# Number of unique values: 19313

# Create df to contain all records with API of N/A, AND a df of all non-null APIs.
# Deduplicate both separately, and append them together at the end.
la_wells_noAPI = la_wells_crs[la_wells_crs['API Num'] == "N/A"]   # 17,374
la_wells_yesAPI = la_wells_crs[la_wells_crs['API Num'] != "N/A"]  # 227,698

# Sort rows so that records with same API are clustered together, and the newest
# Well Status Date (largest integer) appears first in the "cluster".
# Deduplicate the wells table by keeping only the most recent (aka the first)
# record for each API, according to Well Status Date
la_wells_yesAPI_newestfirst = la_wells_yesAPI.sort_values(by=['API Num',
                                                              'Well Status Date'],
                                                          ascending=[True, False],
                                                          na_position='last')
la_wells_yesAPI_deduped = la_wells_yesAPI_newestfirst.drop_duplicates(subset=['API Num'],
                                                                      keep='first')

# NOTE ABOUT DUPLICATE LAT-LONG PAIRS
# Though there are duplicate lat-long pairs remaining, they have different
# well numbers (like 1, 2, 3, etc.), APIs and field/operator names.
# DON'T DEDUPE THESE PAIRS RIGHT NOW
dupes = get_duplicate_api_records(la_wells_yesAPI_deduped, 'latitude_calc')
# Number of duplicate records: 2126
# Number of unique values: 928
dupes = get_duplicate_api_records(la_wells_noAPI, 'latitude_calc')
# Number of duplicate records: 802
# Number of unique values: 310


# Append two tables together (rows with de-duped API info and rows without API info)
la_wells_crs = pd.concat([la_wells_yesAPI_deduped,
                          la_wells_noAPI])


dupes = get_duplicate_api_records(la_wells_crs, 'API Num')
# Number of duplicate records: 0
# Number of unique values: 0


# In[    pre-processing     ]:

# Ensure date objects are formatted correctly
la_wells_crs['Spud Date'].replace({'-': '1900-01-01'},
                                  inplace=True)
clean_a_date_field(la_wells_crs, 'Spud Date')


# Replace NA data values throughout all columns at once
la_wells_crs.replace({'NO PRODUCT SPECIFIED': 'N/A',
                      'VIRTUAL/BAD DATA': 'N/A',
                      '* UNKNOWN *': 'N/A',
                      'INACTIVE OPERATOR': 'N/A',
                      'ZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZZ': 'N/A',
                      'ZZZZZZZ': 'N/A',
                      '-': 'N/A'},
                     inplace=True)


create_concatenated_well_name(la_wells_crs,
                              'Well Name',
                              'Well Num',
                              'wellnamenew')


# In[  INTEGRATION  ]:

la_wells_integrated, _errors = integrate_facs(
    la_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Louisiana",
    src_ref_id="194",
    src_date="2024-04-18",  # Monthly
    on_offshore=None,
    fac_name="wellnamenew",
    fac_id="API Num",
    fac_type='Product Type Code Description',
    spud_date="Spud Date",
    comp_date=None,
    drill_type=None,
    fac_status='Well Status Code Description',
    op_name="Operator Name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save as geojson
out_fp = outfolder + 'la_wells_integrated.geojson'
la_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Michigan
print(datetime.datetime.now())
print('Michigan')
# =============================================================================
mi_wells_fp = r'michigan\wells\Oil_%26_Gas_Test_Well_Surface_Hole_Locations.csv'
mi_wells = pd.read_csv(mi_wells_fp)

# DROP the one coordinate that I know is wrong (latitude of 57)
mi_wells = mi_wells[mi_wells.wh_lat < 50]
# Fix the longitude coordinates that should be negative
mi_wells.loc[mi_wells.wh_long > 0, 'wh_long'] = mi_wells.wh_long * -1

mi_wells_gdf = gpd.GeoDataFrame(mi_wells,
                                geometry=gpd.points_from_xy(mi_wells['wh_long'],
                                                            mi_wells['wh_lat']),
                                crs=4326)

mi_wells_crs = transform_CRS(mi_wells_gdf,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)


if print_maps:
    quickmap(mi_wells_crs, 'Michigan')


# In[  DEDUPLICATING  ]:

# Check API format
mi_wells_crs.api_wellno.head()
# Create new column with API-10
mi_wells_crs.api_wellno = mi_wells_crs.api_wellno.astype(str)
mi_wells_crs['API10'] = mi_wells_crs.api_wellno.str[:-4]
mi_wells_crs.API10.head()

# There seem to be no missing API values, so simply check for duplicates
dupes = get_duplicate_api_records(mi_wells_crs, 'api_wellno')
# Number of duplicate records: 4305
# Number of unique values: 1581
dupes = get_duplicate_api_records(mi_wells_crs, 'API10')
# Number of duplicate records: 15418
# Number of unique values: 6087

# Drop rows where we know there are "true API14 duplicates" with dupe values in all
# the attributes we care about.
mi_wells_crs = mi_wells_crs.drop_duplicates(subset=["api_wellno",
                                                    "well_type",
                                                    "well_stat",
                                                    "Slant",
                                                    "co_name",
                                                    "latitude_calc",
                                                    "longitude_calc"],
                                            keep="first")
dupes = get_duplicate_api_records(mi_wells_crs, 'api_wellno')
# Number of duplicate records: 0
# Number of unique values: 0

# Now, dedupe the API10 occurrences
dupes = get_duplicate_api_records(mi_wells_crs, 'API10')
# Number of duplicate records: 11505
# Number of unique values: 4898

# Sort records by API10 and PermDate, and keep the record with
# the newer PermDate date
mi_wells_crs_newestfirst = mi_wells_crs.sort_values(by=['API10',
                                                        'PermDate'],
                                                    ascending=[True, False],
                                                    na_position='last')
mi_wells_crs = mi_wells_crs_newestfirst.drop_duplicates(subset=['API10'],
                                                        keep='first')

dupes = get_duplicate_api_records(mi_wells_crs, 'API10')
# Number of duplicate records: 0
# Number of unique values: 0


# NOTE ABOUT DUPLICATE LAT-LONG PAIRS
# Though there are duplicate lat-long pairs remaining, they have different
# well numbers (like 1, 2, 3, etc.) and APIs
# DON'T DEDUPE THESE PAIRS RIGHT NOW
dupes = get_duplicate_api_records(mi_wells_crs, 'latitude_calc')
# Number of duplicate records: 11531
# Number of unique values: 4752


# In[  PRE-PROCESSING  ]:

# Abbreviations -> Long form
mi_wells_crs['well_type'].replace({'BDW': 'Brine Disposal Well',
                                   'COI': 'CO2 Injection Well',
                                   'DH': 'Dry Hole',
                                   'GAS': 'Natural Gas Well',
                                   'GASSHG': 'Combined Gas and Shale Gas',
                                   'GBD': 'Gas Production and Brine Disposal',
                                   'GC': 'Gas Condensate Well',
                                   'GIW': 'Gas Injection Well',
                                   'GS': 'Gas Storage',
                                   'GSO': 'Gas Storage Observation Well',
                                   'LH': 'Lost Hole',
                                   'LHL': 'Lost Hole',
                                   'LOC': 'N/A',  # Location
                                   'LPG': 'Liquified Petroleum Gas Storage',
                                   'MDW': 'Part 625 Disposal Well',
                                   'MNB': 'Part 625 Natural Brine',
                                   'MSM': 'Part 625 Solution Mining',
                                   'MSW': 'Part 625 Storage Well',
                                   'MTW': 'Part 625 Test Well',
                                   'NA': 'N/A',  # Not Available
                                   'OBS': 'Observation Well',
                                   'OGC': 'Combined Oil and Gas Condensate',
                                   'OIL': 'Oil Well',
                                   'OSHG': 'Combined Oil and Shale Gas',
                                   'OTH': 'N/A',  # Other Well
                                   'OTI': 'Other Injection Well',
                                   'WIW': 'Water Injection Well',
                                   'WSW': 'N/A'},  # WSW is probably Water Storage Well but not defined in metadata
                                  inplace=True)

mi_wells_crs['Slant'].replace({'V': 'Vertical',
                               'H': 'Horizontal',
                               'D': 'Directional',
                               ' ': 'N/A',
                               np.nan: 'N/A'},
                              inplace=True)

create_concatenated_well_name(mi_wells_crs,
                              'lease_name',
                              'well_no',
                              'wellnamenew')


# In[100]:

mi_wells_integrated, _errors = integrate_facs(
    mi_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Michigan",
    src_ref_id="195",
    src_date="2023-09-15",  # Irregularly
    on_offshore=None,
    fac_name='wellnamenew',
    fac_id="API10",
    fac_type="well_type",
    spud_date=None,
    comp_date=None,
    drill_type="Slant",
    fac_status='well_stat',
    op_name='co_name',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'mi_wells_integrated.geojson'
mi_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Mississippi
print(datetime.datetime.now())
print('Mississippi')
# TODO - revise input filename so that it auto-fills with the "latest date"
# =============================================================================
ms_wells_allinfo = pd.read_csv(r'mississippi\wells\20250203_All_WellInfoList.csv')

# Remove records that lack both a lath and a long coordinate
ms_wells_allinfo = ms_wells_allinfo[(ms_wells_allinfo['Lat(NAD83)'].notna() & ms_wells_allinfo['Long(NAD83)'].notna())]
# Remove some erroneous Latitude values - step not needed as of 2023-11-13
# ms_wells_allinfo = ms_wells_allinfo[ms_wells_allinfo['Lat(NAD83)'].between(30, 35.5)]

# originally in epsg:4269
ms_wells = gpd.GeoDataFrame(ms_wells_allinfo,
                            geometry=gpd.points_from_xy(ms_wells_allinfo['Long(NAD83)'],
                                                        ms_wells_allinfo['Lat(NAD83)']),
                            crs=4269)  # NAD 83

# Check or modify CRS
ms_wells_crs = transform_CRS(ms_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ms_wells_crs, 'Mississippi')


# In[ DE-DUPLICATING ]:

# Check API format
ms_wells_crs.API14.head()
ms_wells_crs.API10.head()

dupes = get_duplicate_api_records(ms_wells_crs, "API14")
# Number of duplicate records: 4
# Number of unique values: 2
dupes = get_duplicate_api_records(ms_wells_crs, "API10")
# Number of duplicate records: 483
# Number of unique values: 232

# DEDUPE API10's (the API14s will get dealt with during this process, too)
# There is no date field to sort on / tell which record is newer...
# First, remove API10 duplicates where the attributes we care about are the same.
ms_wells_crs_deduped1 = ms_wells_crs.drop_duplicates(subset=["API10",
                                                             "Type",
                                                             "Status",
                                                             "Name",
                                                             "Oper",
                                                             "Sec",
                                                             "Twn",
                                                             "Rng",
                                                             "WellSlant"],
                                                     keep="first")

dupes = get_duplicate_api_records(ms_wells_crs_deduped1, "API10")
# Number of duplicate records: 354
# Number of unique values: 176

# Then, sort records so that ACTIVE = YES wells are first, so that in cases of
# duplicate API10's with different statuses, err on the side of keeping the ACTIVE one.
# Also sort alphabetically by WellSlant, so in ties of API10 and Active, the
# WellSlant info of Directional or Horizontal is retained.
ms_wells_crs_active_first = ms_wells_crs_deduped1.sort_values(by=['API10',
                                                                  'Active',
                                                                  'WellSlant'],
                                                              ascending=[True, False, True],
                                                              na_position='last')

ms_wells_crs = ms_wells_crs_active_first.drop_duplicates(subset=["API10",
                                                                 "Name",
                                                                 "Oper",
                                                                 "Sec",
                                                                 "Twn",
                                                                 "Rng"],
                                                         keep="first")


dupes = get_duplicate_api_records(ms_wells_crs, "API10")
# Number of duplicate records: 2
# Number of unique values: 1


# In[  PRE-PROCESSING  ]:
# Next steps will strip the text in front of the first hyphen of the 'type'
# field; for oil production wells, add a dummy value in that will get stripped
ms_wells_crs['Type'].replace({'OIL - Production': 'x - Oil - Production'},
                             inplace=True)

# Strip the unnecessary leading abbreviation from 'Type' field. Split the string
# values in the 'Type' field  based on a delimiter, and only keep the substring
# coming after the first occurence of that delimiter.
ms_wells_crs['Type_new'] = ms_wells_crs['Type'].str.split(' - ', 1).str[1]
# Remove types that are not meaningful
ms_wells_crs.loc[ms_wells_crs.Type_new == 'No Type Required', 'Type_new'] = 'N/A'
ms_wells_crs.loc[ms_wells_crs.Type_new == 'Expired Location', 'Type_new'] = 'N/A'

# Strip remaining leading abbreviation from 'Type' field
ms_wells_crs['Type_new'].replace({'AG - Water Injection Disposal (Acid Gas)': 'Water Injection Disposal (Acid Gas)'},
                                 inplace=True)


# # Strip unnecessary leading abbreviation from 'Status' field. Split the string
# values in the 'Status' field based on a delimiter, and only keep the substring
# that comes after the first occurence of that delimiter
ms_wells_crs['Status_new'] = ms_wells_crs['Status'].str.split(' - ', 1).str[1]

ms_wells_crs['Status_new'].replace({'DWW Domestic Water Well': 'DWW - Domestic Water Well'},
                                   inplace=True)

# Remove unnecessary multiple spaces in well name,
# and replace with single spaces
ms_wells_crs['Name_new'] = ms_wells_crs['Name'].apply(lambda x: ' '.join(x.split()))


# In[63]:

ms_wells_integrated, _errors = integrate_facs(
    ms_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Mississippi",
    src_ref_id="207",
    src_date="2024-01-05",  # Quarterly
    on_offshore=None,
    fac_name="Name_new",
    fac_id="API10",
    fac_type="Type_new",
    spud_date=None,
    comp_date=None,
    drill_type="WellSlant",
    fac_status="Status_new",
    op_name="Oper",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ms_wells_integrated.geojson'
ms_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Missouri
print(datetime.datetime.now())
print('Missouri')
# =============================================================================
# mo_wells = gpd.read_file(r'missouri\wells\Oil_and_Gas_Wells.geojson')

mo_wells_csv = pd.read_excel(r'missouri\wells\Oil and Gas Well List Updated August 30,2024.xlsx',
                             # skiprows=[0],
                             sheet_name='Report 1')
# # Delete empty first column
# mo_wells_csv = mo_wells_csv.drop('Unnamed: 0', axis=1)

# Drop records with no lat-long information
mo_wells_csv = mo_wells_csv[mo_wells_csv['Well Latitude Decimal'].notna()]

mo_wells = gpd.GeoDataFrame(mo_wells_csv,
                            geometry=gpd.points_from_xy(mo_wells_csv['Well Longitude Decimal'],
                                                        mo_wells_csv['Well Latitude Decimal']),
                            crs=4326)  # ASSUMPTION

mo_wells_crs = transform_CRS(mo_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(mo_wells_crs, 'Missouri')


# In[  DE-DUPLICATION  ]:

mo_wells_crs['API Number'].head()    # API10

# CHeck for duplicates
dupes = get_duplicate_api_records(mo_wells_crs, "API Number")
# Number of duplicate records: 2
# Number of unique values: 1

# There is at least one instance of a duplicate API number
# After checking out the URL associated with the well, the more recent operator
# seems to be the first instance of the well in the original data
mo_wells_crs = mo_wells_crs.drop_duplicates(subset=["API Number"],
                                            keep="first")


# In[  PRE-PROCESSING  ]:
clean_a_date_field(mo_wells_crs, 'Spud Date')

mo_wells_crs.loc[mo_wells_crs['Well Type'] == 'Unknown Well Type', 'Well Type'] = 'N/A'

# Replace empty spaces in `wellnamenew` with N/A
mo_wells_crs['Well Name'].replace({' ': 'N/A',
                                   'nan': 'N/A'},
                                  inplace=True)

mo_wells_crs['Lease Name'].replace({'FEE': 'N/A'},
                                   inplace=True)

create_concatenated_well_name(mo_wells_crs,
                              'Lease Name',
                              'Well Name',
                              'wellnamenew')

clean_a_date_field(mo_wells_crs, 'Spud Date')

# There are many trailing spaces in some of these values; strip excess spaces
mo_wells_crs['Well Type'] = mo_wells_crs['Well Type'].str.strip()
mo_wells_crs['Well Status'] = mo_wells_crs['Well Status'].str.strip()


# In[105]:

mo_wells_integrated, _errors = integrate_facs(
    mo_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Missouri",
    src_ref_id="245",
    src_date="2024-02-14",
    on_offshore=None,
    fac_name="wellnamenew",
    fac_id="API Number",
    fac_type="Well Type",
    spud_date="Spud Date",
    comp_date=None,
    drill_type=None,
    fac_status='Well Status',
    op_name='Operator',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save as geojson
out_fp = outfolder + 'mo_wells_integrated.geojson'
mo_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Montana
print(datetime.datetime.now())
print('Montana')
# =============================================================================
# Read in wells -- no need to also import `wells_p.shp` as it's the same info in a different projection
mt_wells_fp = r"montana/wells/wells.shp"
mt_wells = gpd.read_file(mt_wells_fp)

mt_wells_crs = transform_CRS(mt_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)


if print_maps:
    quickmap(mt_wells_crs, 'Montana')

# In[  DEDUPLICATION  ]:

dupes = get_duplicate_api_records(mt_wells_crs, 'API_WellNo')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:
clean_a_date_field(mt_wells_crs, 'Completed')

# Remove unnecessary multiple spaces in well name,
# and replace with single spaces
mt_wells_crs['Well_Nm_new'] = mt_wells_crs['Well_Nm'].apply(lambda x: ' '.join(x.split()))

# In[109]:

mt_wells_integrated, _errors = integrate_facs(
    mt_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Montana",
    src_ref_id="209",
    src_date="2024-04-09",  # latest completion date, pub date unknown
    on_offshore=None,
    fac_name="Well_Nm_new",
    fac_id="API_WellNo",
    fac_type="Type",
    spud_date=None,
    comp_date="Completed",
    drill_type=None,
    fac_status="Status",
    op_name="CoName",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save as geojson
out_fp = outfolder + 'mt_wells_integrated.geojson'
mt_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Nebraska
print(datetime.datetime.now())
print('Nebraska')
# =============================================================================
ne_wells = gpd.read_file(r'nebraska\NE_WELLS.shp')

# Check or modify CRS
ne_wells_crs = transform_CRS(ne_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ne_wells_crs, 'Nebraska')

# In[  DEDUPLICATION  ]:

ne_wells_crs['API_10'] = ne_wells_crs.API_WellNo.str[0:10]

dupes = get_duplicate_api_records(ne_wells_crs, 'API_WellNo')
# Number of duplicate records: 0
# Number of unique values: 0

dupes = get_duplicate_api_records(ne_wells_crs, 'API_10')
# Number of duplicate records: 1159
# Number of unique values: 577


# Sort records by API_WellNo, with the larger number listed first.
# This means that for wells with a non-zero value in the 13th and 14th position
# of their API number (i.e., wells that have been re-completed or reworked), are
# listed first (and therefore the most recent status / owner of this well
# surface location can be retained.
ne_wells_crs_sorted = ne_wells_crs.sort_values(by=['API_WellNo'],
                                               ascending=[False],
                                               na_position='last')

# Drop wells with identical API_10 and location. First look at the lat-long
# columns; for any remaining dupe API-10 values with not exactly identical
# lat-long coordinates, use the Range and Footages to determine whether
# the location of both records is the same
ne_wells_crs = ne_wells_crs_sorted.drop_duplicates(subset=['API_10',
                                                           'latitude_calc',
                                                           'longitude_calc'],
                                                   keep='first').reset_index(drop=True)
dupes = get_duplicate_api_records(ne_wells_crs, 'API_10')
# Number of duplicate records: 74
# Number of unique values: 37
ne_wells_crs = ne_wells_crs.drop_duplicates(subset=['API_10',
                                                    'Rng_No',
                                                    'Rng_Dir',
                                                    'Ft_NS',
                                                    'Ft_NS_Dir',
                                                    'Ft_EW',
                                                    'Ft_EW_Dir'],
                                            keep='first').reset_index(drop=True)

# TODO - FOR NOW, LEAVE THESE DUPES IN UNTIL I FIGURE OUT WHAT TO DO WITH THEM.
dupes = get_duplicate_api_records(ne_wells_crs, 'API_10')
# Number of duplicate records: 24
# Number of unique values: 12

# In[  PREPROCESSING  ]:
# Remove excessive spaces in the middle of the Well Name strings
ne_wells_crs['wellnamenew'] = ne_wells_crs['Well_Name'].str.replace(r'\s+', ' ',
                                                                    regex=True)

# Un-abbreviate status values
ne_status_dict = {
    'AA': 'Intent to Abandon Received',
    'AI': 'Active Injection',
    'AL': 'Abandoned Location',
    'AU': 'Abandoned - Unapproved',
    'AX': 'P&A - Approved',
    'C': 'Completed',
    'CA': 'Cancelled',
    'CO': 'Converted to SWD/ER',
    'DA': 'Dry and Abandoned',
    'DC': 'Drilling Completed',
    'DG': 'Drilling',
    'DM': 'Domestic Well',
    'DR': 'Well Drilled',
    'DW': 'Domestic Well - Gas',
    'EX': 'Expired Permit',
    'FR': 'Final Restoration',
    'IA': 'Inactive',
    'JA': 'Junked and Abandoned',
    'MI': 'Pending MIT',
    'NC': 'Not Completed',
    'ND': 'Not Drilled',
    'PA': 'Plugged and Abandoned',
    'PB': 'Plugged Back',
    'PD': 'Pending Injection Application',
    'PR': 'Producing',
    'PW': 'Permitted Well',
    'RC': 'Recompleted into New Formation',
    'RW': 'Released - Water Well',
    'SI': 'Shut In',
    'SP': 'Spudded',
    'TA': 'Temporarily Abandoned',
    'TP': 'Terminated Permit',
    'UN': 'N/A',  # Unknown
    'VP': 'Verbal Plugging',
    'WC': 'Waiting Completion',
    'WS': 'Water Supply'
}
ne_wells_crs.Well_Statu = ne_wells_crs.Well_Statu.replace(ne_status_dict)

# Replace missing operator names
ne_wells_crs.Co_Name = ne_wells_crs.Co_Name.replace({'CURRENTLY UNASSIGNED': 'N/A'})

# In[  INTEGRATION  ]:
ne_wells_integrated, _errors = integrate_facs(
    ne_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Nebraska",
    src_ref_id="272",
    src_date="2025-02-10",
    on_offshore='ONSHORE',
    fac_name="wellnamenew",
    fac_id="API_WellNo",
    fac_type="Well_Type",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    fac_status='Well_Statu',
    op_name="Co_Name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ne_wells_integrated.geojson'
ne_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% New Mexico
print(datetime.datetime.now())
print('New Mexico')
# =============================================================================
# FIXME - the CSV from NM's ArcGIS site now has different column names.
# All of this code needs to be rewritten with new column names
nm_wells_fp = r'new_mexico\wells\Wells_Public.csv'
nm_wells = pd.read_csv(nm_wells_fp)

# This CSV contains lat-long pairs that use A MIX OF COORDINATE SYSTEMS
# (Seriously why would someone do that??)
# Create 3 separate GDFs using different CRS,
# reproject them to a consistent CRS, then append all 3 together.
# Make an assumption that records with null projection value are WGS84
nm_wells_nad83 = nm_wells[nm_wells.Datum == 'NAD83']
nm_wells_nad83_gdf = gpd.GeoDataFrame(nm_wells_nad83,
                                      geometry=gpd.points_from_xy(nm_wells_nad83.Longitude,
                                                                  nm_wells_nad83.Latitude),
                                      crs=4269)
nm_wells_nad83_gdf_ = transform_CRS(nm_wells_nad83_gdf, appendLatLon=True)


nm_wells_nad27 = nm_wells[nm_wells.Datum == 'NAD27']
nm_wells_nad27_gdf = gpd.GeoDataFrame(nm_wells_nad27,
                                      geometry=gpd.points_from_xy(nm_wells_nad27.Longitude,
                                                                  nm_wells_nad27.Latitude),
                                      crs=4267)
nm_wells_nad27_gdf_ = transform_CRS(nm_wells_nad27_gdf, appendLatLon=True)


nm_wells_wgs84 = nm_wells[(nm_wells.Datum == 'WGS83') | (nm_wells.Datum.isna())]
nm_wells_wgs84_gdf = gpd.GeoDataFrame(nm_wells_wgs84,
                                      geometry=gpd.points_from_xy(nm_wells_wgs84.Longitude,
                                                                  nm_wells_wgs84.Latitude),
                                      crs=4326)
nm_wells_wgs84_gdf_ = transform_CRS(nm_wells_wgs84_gdf, appendLatLon=True)

nm_wells_crs = pd.concat([nm_wells_nad83_gdf_,
                          nm_wells_nad27_gdf_,
                          nm_wells_wgs84_gdf_])


if print_maps:
    quickmap(nm_wells_crs, 'New Mexico')


# In[  DE-DUPLICATION  ]:

nm_wells_crs.API.head()  # API10

dupes = get_duplicate_api_records(nm_wells_crs, 'API')
# Number of duplicate records: 0
# Number of unique values: 0


# In[  PRE-PROCESSING  ]:
# Ensure date objects are formatted correctly
nm_wells_crs['year_spudded_new'] = nm_wells_crs['SPUD Year'].astype(str)
nm_wells_crs['year_spudded_new'].replace({'9999': '1900'},
                                         inplace=True)
# Since all we know is the YEAR of spud not the whole date,
# add a placeholder month and year to the spud date column
nm_wells_crs['year_spudded_new'] = nm_wells_crs['year_spudded_new'] + '-01-01'

nm_wells_crs['Well Type'].replace({'Miscellaneous': 'N/A'}, inplace=True)

nm_wells_crs['Well Bore Direction'].replace({'V': 'VERTICAL',
                                             'H': 'HORIZONTAL',
                                             ' ': 'N/A',
                                             'D': 'DIRECTIONAL'},
                                            inplace=True)

# NM has ~20k wells with an OPERATOR value of "PRE-ONGARD WELL OPERATOR".
# ONGARD is NM's database for tracking O&G revenues. "PRE-ONGARD WELL OPERATOR"
# refers to a historical operator before the ONGARD database system started tracking them.
# Change this operator value to N/A
nm_wells_crs['OGRID Name'].replace({'PRE-ONGARD WELL OPERATOR': 'N/A'},
                                   inplace=True)

# In this dataset, many very old wells have "PRE-ONGARD" as a prefix to their
# well name. For neatness, remove this prefix
nm_wells_crs['wellnamenew'] = nm_wells_crs['Well Name'].str.replace('PRE-ONGARD ', '')

# In[  integration  ]:

nm_wells_integrated, _errors = integrate_facs(
    nm_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="New Mexico",
    src_ref_id="210",
    src_date="2024-04-19",
    # on_offshore=None,
    fac_name="wellnamenew",
    fac_id="API",
    fac_type="Well Type",
    spud_date='year_spudded_new',
    # comp_date=None,
    drill_type='Well Bore Direction',
    fac_status="Well Status",
    op_name="OGRID Name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'nm_wells_integrated.geojson'
nm_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% New York
print(datetime.datetime.now())
print('New York')
# =============================================================================
ny_wells_fp = r'new_york/wells/wellspublic.csv'
ny_wells_csv = pd.read_csv(ny_wells_fp)

# Remove records with missing lat-long information
ny_wells_csv = ny_wells_csv[ny_wells_csv.Surface_Longitude.notna()]
ny_wells_csv = ny_wells_csv[ny_wells_csv.Surface_latitude.notna()]

ny_wells = gpd.GeoDataFrame(ny_wells_csv,
                            geometry=gpd.points_from_xy(ny_wells_csv['Surface_Longitude'],
                                                        ny_wells_csv['Surface_latitude']),
                            crs=4326)  # not sure, assuming wgs84

ny_wells_crs = transform_CRS(ny_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

# Remove missing geometries, a.k.a, those with coordinates of (0,0)
ny_wells_crs = ny_wells_crs[ny_wells_crs.latitude_calc != 0].reset_index()

if print_maps:
    quickmap(ny_wells_crs, 'New York')

# In[  DE-DUPLICATING  ]:

ny_wells_crs.API_WellNo.head()
ny_wells_crs.API_WellNo = ny_wells_crs.API_WellNo.astype(str)
ny_wells_crs['API10'] = ny_wells_crs.API_WellNo.str[:-4]
ny_wells_crs['API10'].head()

# Check for duplicate API-14 and API-10; if zero, no further action needed
dupes = get_duplicate_api_records(ny_wells_crs, 'API_WellNo')
# Number of duplicate records: 0
# Number of unique values: 0
dupes = get_duplicate_api_records(ny_wells_crs, 'API10')
# Number of duplicate records: 1681
# Number of unique values: 773

# Since the Date_Status column is what I will use to keep the most up-to-date
# record, fill in any null Date_Status values with that record's Permit_Issued value
ny_wells_crs.loc[ny_wells_crs.Date_Status.isna(), 'Date_Status'] = ny_wells_crs.Permit_Issued

# Sort records by API10 and Date_Status date, and keep the record with
# the newer Date_Status date (If two LastStatusChange are the same,
# then pick the record with the newer Completion Date)
ny_wells_crs_newestfirst = ny_wells_crs.sort_values(by=['API10',
                                                        'Date_Status',
                                                        'Date_Well_Completed'],
                                                    ascending=[True, False, False],
                                                    na_position='last')

ny_wells_crs = ny_wells_crs_newestfirst.drop_duplicates(subset=['API10'],
                                                        keep='first').reset_index(drop=True)

dupes = get_duplicate_api_records(ny_wells_crs, 'API10')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:
ny_wells_crs = replace_missing_strings_with_na(ny_wells_crs,
                                               ['Well_Name', 'Slant'])

# https://extapps.dec.ny.gov/cfmx/extapps/GasOil/help/codes.cfm?key=qLookupWellType
ny_wells_crs['Well_Type'].replace({'BR': 'Brine',
                                   'DH': 'Dry Hole',
                                   'DS': 'Disposal',
                                   'DW': 'Dry Wildcat',
                                   'GD': 'Gas Development',
                                   'GE': 'Gas Extension',
                                   'GW': 'Gas Wildcat',
                                   'IG': 'Gas Injection',
                                   'IW': 'Enhanced Oil Recovery - Injection',
                                   'LP': 'Liquified Petroleum Gas Storage',
                                   'MB': 'Monitoring Brine',
                                   'MM': 'Monitoring Miscellaneous',
                                   'MS': 'Monitoring Storage',
                                   'NL': 'N/A',  # "Not Listed"
                                   'OD': 'Oil Development',
                                   'OE': 'Oil Extension',
                                   'OW': 'Oil Wildcat',
                                   'SG': 'Stratigraphic',
                                   'ST': 'Storage',
                                   'TH': 'Geothermal',
                                   'UN': 'N/A'},
                                  inplace=True)

# https://extapps.dec.ny.gov/cfmx/extapps/GasOil/help/codes.cfm?key=qLookupWellStatus
ny_wells_crs['Well_Status'].replace({'AC': 'Active',
                                     'AR': 'Application Received to Drill/Plug/Convert',
                                     'CA': 'Cancelled',
                                     'CO': 'Converted to Other Well Type',
                                     'DC': 'Drilling Completed',
                                     'DD': 'Drilled Deeper',
                                     'DG': 'Drilling in Progress',
                                     'EX': 'Expired Permit',
                                     'IN': 'Inactive',
                                     'NL': 'N/A',  # NL is undefined in the metadata
                                     'NR': 'N/A',  # means "Not Reported on AWR"
                                     'PA': 'Plugged and Abandoned',
                                     'PB': 'Plugged Back',
                                     'PI': 'Permit Issued',
                                     'PM': 'Plugged Back Multilateral',
                                     'RE': 'Refunded Fee',
                                     'RW': 'Released - Water Well',
                                     'SI': 'Shut-In',
                                     'TA': 'Temporarily Abandoned',
                                     'TR': 'Transferred Permit',
                                     'UN': 'N/A',   # "Unknown"
                                     'UL': 'N/A',  # "Unknown Located"
                                     'UM': 'N/A',  # "Unknown Not Found"
                                     'VP': 'Voided Permit'},
                                    inplace=True)

clean_a_date_field(ny_wells_crs, 'Date_Spudded')
clean_a_date_field(ny_wells_crs, 'Date_Well_Completed')


# In[  INTEGRATION  ]:

ny_wells_integrated, _errors = integrate_facs(
    ny_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="New York",
    src_ref_id="211",
    src_date="2024-04-17",  # Daily
    # on_offshore=None,
    fac_name='Well_Name',
    fac_id="API_WellNo",
    fac_type='Well_Type',
    spud_date='Date_Spudded',
    comp_date='Date_Well_Completed',
    drill_type="Slant",
    fac_status='Well_Status',
    op_name="Company_name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ny_wells_integrated.geojson'
ny_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% North Dakota
print(datetime.datetime.now())
print('North Dakota')
# =============================================================================
nd_wells_fp = r'north_dakota\wells\NDOGD.gdb'
nd_wells = gpd.read_file(nd_wells_fp,
                         driver='FileGDB',
                         layer='OGD_Wells')

nd_wells_crs = transform_CRS(nd_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)


if print_maps:
    quickmap(nd_wells_crs, 'North Dakota')

# In[ DE-DUPLICATING  ]:

nd_wells_crs.api.head()
# Create new column with API-10
nd_wells_crs['API10'] = nd_wells_crs.api_no.str[:-6]
nd_wells_crs['API10'].head()

# Check for duplicate APIs; if zero, no further action needed
dupes = get_duplicate_api_records(nd_wells_crs, 'api')
# Number of duplicate records: 0
# Number of unique values: 0
dupes = get_duplicate_api_records(nd_wells_crs, 'API10')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:
# Ensure date objects are formatted correctly
clean_a_date_field(nd_wells_crs, 'spud_date')

# Change well type attribute to long form
# https://gis.dmr.nd.gov/OGD_MapViewer_LEGEND.pdf
nd_wells_crs['well_type'].replace({'AGD': 'Acid Gas Disposal',
                                   'AI': 'Air Injection',
                                   'CBM': 'Coal Bed Methane',
                                   'CO2I': 'CO2 Injection',
                                   'CO2S': 'CO2 Storage',
                                   'DF': 'Dump Flood Injector',
                                   'DFP': 'Dump Flood Injector/Producer',
                                   'GASN': 'Nitrogen Gas',
                                   'GASC': 'Gas Condensate',
                                   'GASD': 'Dry Gas',
                                   'GI': 'Gas Injection',
                                   'GS': 'Gas Storage',
                                   'INJP': 'Injector/Producer',
                                   'IT': 'Injectivity Test',
                                   # 'MWOG': 'Monitoring: Oil and Gas',
                                   # 'MWUIC': 'Monitoring: UIC',
                                   'MWUI': 'Monitoring Well Underground Injection',
                                   'NJ': 'Non-Jurisdictional',
                                   'OG': 'Oil and Gas',
                                   'SFI': 'Slurry Fracture Injection',
                                   'ST': 'Stratigraphic Test',
                                   'SWD': 'Salt Water Disposal',
                                   'WI': 'Water Injection',
                                   'WS': 'Water Source'},
                                  inplace=True)

# Change status attribute to long form
nd_wells_crs['status'].replace({'A': 'Active',
                                'AB': 'Abandoned (Shut-In > 12 months)',
                                'DRL': 'Drilling',
                                'DRY': 'Dry Hole',
                                'EXP': 'Expired',
                                'IA': 'Inactive (Shut-In >= 3 months and =< 12 months)',
                                'IAW': 'Inactive Well Waiver',
                                'IJ': 'Injection',
                                'LOC': 'N/A',
                                'LOCR': 'N/A',
                                'NC': 'Not Complete',
                                'NCW': 'Not Completed Waiver',
                                'NJ': 'Non-Jurisdictional',
                                'PA': 'Plugged and Abandoned',
                                'PANF': 'Plugged and Abandoned Not Finalized',
                                'PNC': 'Permit Now Cancelled',
                                'PNS': 'Permit Now Suspended',
                                'TA': 'Temporarily Abandoned',
                                'TAI': 'Temporarily Abandoned, Suspension of Drilling (Intermediate Casing Set)',
                                'TAO': 'Temporarily Abandoned, Observation',
                                'TASC': 'Temporarily Abandoned, Suspension of Drilling (Surface Casing Set)',
                                'TATD': 'Temporarily Abandoned, Drilled to Total Depth'},
                               inplace=True)


# In[  INTEGRATION  ]:

nd_wells_integrated, _errors = integrate_facs(
    nd_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="North Dakota",
    src_ref_id="212",
    src_date="2024-04-18",  # Daily
    on_offshore=None,
    fac_name="well_name",
    fac_id="api",
    fac_type="well_type",
    spud_date="spud_date",
    comp_date=None,
    drill_type=None,
    fac_status="status",
    op_name="operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

len(nd_wells_integrated)

# Save as geojson
out_fp = outfolder + 'nd_wells_integrated.geojson'
nd_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Ohio
print(datetime.datetime.now())
print('Ohio')
# TODO - revise input filename so that it auto-fills with the "latest date"
# =============================================================================
fp_oh = r"ohio\Rbdmsd97.mdb"
# Read in the MS Access tables that I need as dfs
# dfs are a list of dataframes
tableNamesIdx, tableNames, dfs = read_msAccess(fp_oh,
                                               table_subset=['Well',
                                                             'tblLocational',
                                                             'Company'])

# create a dictionary that maps the index (number) of each MS Access table to
# the table's name, so I can select dataframes from `dfs` by Name instead of
# just their index number. (The state of Ohio seems to have changed the order...)
index2name = dict(enumerate(tableNames))
name2index = {v: k for k, v in index2name.items()}

# What's the index number that refers to the 'Well' table?
well_index = name2index.get('Well')

# Extract Well-Level attribute data, and keep only the columns I need
well_oh = dfs[well_index][['API_WELLNO',
                           'WL_STATUS',
                           'DT_STATUS',
                           'DT_SPUD',
                           'DT_COMP',
                           'WELL_NM',
                           'WELL_TYP',
                           'WELL_NO',
                           'OPNO']]
# For merging with locational info
well_oh["LOCATION_ID"] = well_oh["API_WELLNO"]
well_oh['OPNO'] = well_oh['OPNO'].fillna(0).astype(int)


# Extract Well-Level Location data, and keep only the columns I need
# What's the index number that refers to the 'tblLocational' table?
tbl_loc_index = name2index.get('tblLocational')
tbl_loc = dfs[tbl_loc_index][["LOCATION_ID",
                              "WH_LAT",
                              "WH_LONG",
                              "SLANT"]]
# Only select for locations that are not null
tbl_loc1 = tbl_loc[(tbl_loc.WH_LAT.notnull() & tbl_loc.WH_LONG.notnull())]

# Merge the two datasets: location and attribute information
well_oh_merged_loc = pd.merge(tbl_loc1,
                              well_oh,
                              on="LOCATION_ID",
                              how="left")

print(f'Number of records in original locational dataset = {tbl_loc1.shape[0]}')
print(f'Compare: number of records in merged dataset = {well_oh_merged_loc.shape[0]}')


oh_wells_gdf = gpd.GeoDataFrame(well_oh_merged_loc,
                                geometry=gpd.points_from_xy(well_oh_merged_loc.WH_LONG,
                                                            well_oh_merged_loc.WH_LAT),
                                crs=4326)

oh_wells_crs = transform_CRS(oh_wells_gdf,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

# Filter out odd point in Pennsylvania
# TODO - should we attempt to filter out any other points in WV?
oh_wells_crs = oh_wells_crs[oh_wells_crs['WH_LONG'].between(-85, -80.2)]
oh_wells_crs = oh_wells_crs[oh_wells_crs['WH_LAT'].between(35, 42)]

if print_maps:
    quickmap(oh_wells_crs, 'Ohio')


# In[  DEDUPLICATING  ]:

oh_wells_crs.API_WELLNO.head()
# Create new column with API-10
oh_wells_crs['API10'] = oh_wells_crs.API_WELLNO.str[:-4]
oh_wells_crs['API10'].head()

dupes = get_duplicate_api_records(oh_wells_crs, 'API_WELLNO')
# Number of duplicate records: 0
# Number of unique values: 0

dupes = get_duplicate_api_records(oh_wells_crs, 'API10')
# Number of duplicate records: 685
# Number of unique values: 333

# Sort values to keep newest updated status.
# If status dates are the same, retain the one with newest completion date,
# then spud date. If NO Date info, retain the non-vertical well
oh_wells_crs_newestfirst = oh_wells_crs.sort_values(by=['API10', 'DT_STATUS',
                                                        'DT_COMP', 'DT_SPUD',
                                                        'SLANT'],
                                                    ascending=[True, False,
                                                               False, False,
                                                               True],
                                                    na_position='last')
oh_wells_crs = oh_wells_crs_newestfirst.drop_duplicates(subset=['API10',
                                                                'latitude_calc',
                                                                'longitude_calc'],
                                                        keep='first')

dupes = get_duplicate_api_records(oh_wells_crs, 'API10')
# Number of duplicate records: 160
# Number of unique values: 79

# For remaining duplicate API10s with not identical lat-long values,
# just keep the newer-status record
oh_wells_crs = oh_wells_crs.drop_duplicates(subset=['API10'],
                                            keep='first')

dupes = get_duplicate_api_records(oh_wells_crs, 'API10')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:

# What's the index number that refers to the 'Well' table?
company_index = name2index.get('Company')
# MERGE WELLS WITH COMPANY INFO FROM COMPANY TABLE IN ACCESS DATABASE  # FIXME
companies = dfs[company_index][["CONO", "CONAME"]]
# For merging with locational dataset
companies["OPNO"] = companies["CONO"].fillna(0).astype(int)
# Replace certain company values with N/A
companies['CONAME'].replace({'vacant': 'N/A',
                             np.nan: 'N/A',
                             'UNKNOWN OWNER': 'N/A',
                             'UNREGISTERED OWNER': 'N/A',
                             'HISTORIC OWNER': 'N/A',
                             'NOT LOGGED': 'N/A'},
                            inplace=True)

oh_wells_crs = pd.merge(oh_wells_crs,
                        companies,
                        on="OPNO",
                        how="left")


clean_a_date_field(oh_wells_crs, 'DT_SPUD')
clean_a_date_field(oh_wells_crs, 'DT_COMP')


# Change directional attribute to long form
oh_wells_crs['SLANT'].replace({"V": "VERTICAL",
                               "D": "DIRECTIONAL",
                               "H": "HORIZONTAL",
                               "v": "VERTICAL",
                               "O": "HORIZONTAL",
                               "G": "N/A",
                               None: "N/A"},
                              inplace=True)


oh_wells_crs['WL_STATUS'].replace({'AI': 'ACTIVE INJECTION',
                                   'CA': 'CANCELLED',
                                   'DA': 'DRY AND ABANDONED',
                                   'DG': 'DRILLING',
                                   'DM': 'DOMESTIC WELL',
                                   'DR': 'WELL DRILLED',
                                   'EM': 'EXEMPT MISSISSIPPIAN WELL',
                                   'EX': 'PERMIT EXPIRED',
                                   'FR': 'FINAL RESTORATION',
                                   'HP': 'HISTORICAL PRODUCTION WELL',
                                   'I1': 'TEMPORARILY INACTIVE - FIRST YEAR',
                                   'I2': 'TEMPORARILY INACTIVE - SECOND YEAR',
                                   'I3': 'TEMPORARILY INACTIVE - THIRD YEAR',
                                   'I4': 'TEMPORARILY INACTIVE - FOURTH YEAR',
                                   'IA': 'DRILLED, INACTIVE',
                                   'LH': 'LOST HOLE',
                                   'LU': 'LOCATION UNKNOWN',
                                   'ND': 'NOT DRILLED',
                                   'NF': 'FIELD INSPECTED, WELL NOT FOUND',
                                   'O': 'OTHER',
                                   'OP': 'ORPHAN WELL - PENDING',
                                   'OR': 'ORPHAN WELL - READY',
                                   'PA': 'PLUGGED AND ABANDONED',
                                   'PB': 'PLUGGED BACK',
                                   'PR': 'PRODUCING',
                                   'RO': 'REOPEN',
                                   'RP': 'REPLUGGED WELL',
                                   'SF': 'STIMULATION FINISHED',
                                   'SI': 'SHUT-IN',
                                   'SS': 'STIMULATION STARTED',
                                   'SW': 'STORAGE WELL',
                                   'TA': 'TEMPORARILY ABANDONED',
                                   'UN': 'N/A',  # UNKNOWN STATUS
                                   'WP': 'WELL PERMITTED',
                                   'WW': 'PLUGGED BACK FOR WATER WELL',
                                   np.nan: 'N/A'},
                                  inplace=True)


oh_wells_crs['WELL_TYP'].replace({'OG_R': 'OIL AND GAS',
                                  'UN_R': 'N/A',
                                  'ST_R': 'STRATIGRAPHIC TEST',
                                  'SW_R': 'CLASS II DISPOSAL WELLS BRINE/WASTE',
                                  'MN_R': 'MONITOR/OBSERVATION',
                                  'ER_R': 'CLASS II ENHANCED RECOVERY',
                                  'OG': 'OIL AND GAS',
                                  'SM_R': 'CLASS III SOLUTION MINING',
                                  'SW_R': 'CLASS II DISPOSAL WELLS BRINE/WASTE',
                                  'IW_R': 'CLASS I INDUSTRIAL WASTE',
                                  'GS_R': 'GAS STORAGE',
                                  'WS': 'WATER SUPPLY',
                                  'BR_R': 'BRINE PRODUCTION',
                                  'OG': 'OIL AND GAS',
                                  np.nan: 'N/A'},
                                 inplace=True)

# Change WELL_NM and WELL_NO values that are empty strings to N/A instead
oh_wells_crs.WELL_NM.replace({'': 'N/A'}, inplace=True)
oh_wells_crs['WELL_NM'] = oh_wells_crs['WELL_NM'].fillna('N/A')

# Remove excess spaces from well name
oh_wells_crs['WELL_NM_NEW'] = oh_wells_crs['WELL_NM'].apply(lambda x: ' '.join(x.split()))

# In[  INTEGRATION  ]:

oh_wells_integrated, _errors = integrate_facs(
    oh_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Ohio",
    src_ref_id="213",
    src_date="2024-03-29",  # Weekly
    on_offshore=None,
    fac_name='WELL_NM_NEW',
    fac_id="API10",
    fac_type="WELL_TYP",
    spud_date="DT_SPUD",
    comp_date="DT_COMP",
    drill_type="SLANT",
    fac_status="WL_STATUS",
    op_name='CONAME',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'oh_wells_integrated.geojson'
oh_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Oklahoma
print(datetime.datetime.now())
print('Oklahoma')
# =============================================================================
ok_wells_fp = r'oklahoma\wells\rbdms-wells.csv'
ok_wells_csv = gpd.read_file(ok_wells_fp)

# Drop cols I don't need
ok_wells_csv.drop(['WELL_BROWSE_LINK',
                   'WELL_RECORDS_DOCS',
                   'COUNTY',
                   'SECTION',
                   'TOWNSHIP',
                   'RANGE',
                   'QTR4',
                   'QTR3',
                   'QTR2',
                   'QTR1',
                   'PM',
                   'FOOTAGE_EW',
                   'EW',
                   'FOOTAGE_NS',
                   'NS',
                   'geometry'], axis=1, inplace=True)

# Convert the string-type latlong fields to numeric (this also has the effect
# of converting any empty strings to np.nan)
ok_wells_csv['SH_LON'] = pd.to_numeric(ok_wells_csv['SH_LON'], errors="coerce")
ok_wells_csv['SH_LAT'] = pd.to_numeric(ok_wells_csv['SH_LAT'], errors="coerce")

# Drop any wells with partially or fully missing coordinates
valid_coord_mask = (ok_wells_csv.SH_LON.notna() & ok_wells_csv.SH_LAT.notna())
ok_wells_csv = ok_wells_csv[valid_coord_mask]

# Repair erroneous coordinates
# For reasonable longitude values that are positive, make them negative
ok_wells_csv.loc[ok_wells_csv.SH_LON.between(94, 103), 'SH_LON'] = ok_wells_csv.SH_LON * -1
# Drop unreasonable latitude values, then same for longitude
ok_wells_csv = ok_wells_csv[ok_wells_csv.SH_LAT.between(25, 40)]
ok_wells_csv = ok_wells_csv[ok_wells_csv.SH_LON.between(-103, -94)].reset_index(drop=True)

# Convert df to gdf
ok_wells = gpd.GeoDataFrame(ok_wells_csv,
                            geometry=gpd.points_from_xy(ok_wells_csv['SH_LON'],
                                                        ok_wells_csv['SH_LAT']),
                            crs=4326)

ok_wells_crs = transform_CRS(ok_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(ok_wells_crs, 'Oklahoma')

# In[  DE-DUPLICATING  ]:

dupes = get_duplicate_api_records(ok_wells_crs, 'API')  # already API10 format
# Number of duplicate records: 0
# Number of unique values: 0


# In[  PRE-PROCESSING  ]:
# STATUS and TYPE codes here:
# https://oklahoma.gov/content/dam/ok/en/occ/documents/og/ogdatafiles/rbdms-wells-data-dictionary.xlsx

ok_wells_crs['OPERATOR'].replace({'OTC/OCC NOT ASSIGNED': 'N/A'},
                                 inplace=True)

ok_wells_crs['WELLSTATUS'].replace({'AC': 'Active',
                                    'ACRT': 'Active, red tagged ',  # red tagged for compliance issues
                                    'DUC': 'Drilled to total depth but not completed',
                                    'EX': 'Expired permit, not drilled',
                                    'ND': 'New drill',  # often missing completion report
                                    'NE': 'No evidence of well',
                                    'OR': 'Orphaned',
                                    'PA': 'Plugged and abandoned',
                                    'PAFF': 'Plugged with federal funds',
                                    'PASF': 'Plugged with state funds',
                                    'PASUR': 'Plugged with surety funds',
                                    'PASURSF': 'Plugged with surety and state funds',
                                    'SFAW': 'State Funds Award Letter',
                                    'SFFO': 'State Funds Final Order',
                                    'SP': 'Spudded well',  # Spud report but no completion report
                                    'STFD': 'On list to be plugged with state funds',
                                    'TA': 'Temporarily abandoned',
                                    'TM': 'Terminated'
                                    },
                                   inplace=True)

# #Abbreviations -> Long form
# (1) OK well type codes: https://oklahoma.gov/content/dam/ok/en/occ/documents/og/ogdatafiles/well-transfers-data-dictionary.xlsx
# (2) Other well type codes provided on LEGEND tab of this doc: https://oklahoma.gov/content/dam/ok/en/occ/documents/og/ogdatafiles/online-active-well-list.xlsx
ok_wells_crs['WELLTYPE'].replace({'2D': 'Class 2 Non-Commercial Disposal Well',  # 2
                                  '2DCm': 'Commerical UIC disposal well',  # 1
                                  '2DNC': 'Non-Commercial UIC disposal well',  # 1
                                  '2R': 'Class 2 Injection or Enhanced Recovery Well (Input)',  # 2
                                  '2RIn': 'Enhanced Recovery UIC injection well',  # 1
                                  '2RIN': 'Enhanced Recovery UIC injection well',  # 1
                                  '2RSI': 'UIC Simultaneous Injection well',  # also produces oil and/or gas to surface (1)
                                  'DRY': 'Dry Hole',  # 1
                                  'GAS': 'Gas production well',  # 1
                                  'Gas': 'Gas production well',  # 1
                                  'GSW': 'Natural Gas (refined) Storage well',  # 1
                                  # 'INJ': 'Legacy UIC well type',  # FIXME
                                  'LPSW': 'LPG storage well',  # 1
                                  'ND': 'N/A',  # No documents (1)
                                  'NT': 'N/A',  # No type--unknown old well likely without a completion report  (1)
                                  'OBS': 'Obervation well for Gas Storage facility',  # 1
                                  'OG': 'Oil/Gas production well',  # 1
                                  'OIL': 'Oil production well',  # 1
                                  'Oil': 'Oil production well',  # 1
                                  'ORPH': 'Orphaned well',  # 1
                                  'P&A': 'Plugged and abandoned well',  # 1
                                  'PA': 'Plugged and abandoned well',  # 1
                                  'STFD': 'State Funds well',  # 1
                                  'SW': 'Service Well (pressure maintenance, stratigraphic test holes, etc)',
                                  # 'SWD': 'Legacy UIC well type'  # FIXME
                                  'TA': 'Temporarily abandoned well',  # 1
                                  'TA ': 'Temporarily abandoned well',  # 1
                                  'TM': 'Terminated UIC well',  # voluntary or mandatory revocation of UIC authorization (1)
                                  # 'WIW': 'Legacy UIC well type'  # FIXME
                                  'WSW': 'Water Supply Well'},  # water well used for variety of purposes, may not be fresh (1)
                                 inplace=True)


ok_wells_crs = replace_missing_strings_with_na(ok_wells_crs, ['WELL_NUM',
                                                              'WELL_NAME',
                                                              'OPERATOR',
                                                              'WELLSTATUS',
                                                              'WELLTYPE'])


# Concatenate Org_Farm and Org_WellNo to create a more complete Well Name
create_concatenated_well_name(ok_wells_crs,
                              'WELL_NAME',
                              'WELL_NUM',
                              'wellnamenew')


# In[80]:

ok_wells_integrated, _errors = integrate_facs(
    ok_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Oklahoma",
    src_ref_id="259",
    src_date="2024-11-13",
    on_offshore='ONSHORE',
    fac_name="wellnamenew",
    fac_id="API",
    fac_type="WELLTYPE",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    fac_status="WELLSTATUS",
    op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'ok_wells_integrated.geojson'
ok_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Pennsylvania
print(datetime.datetime.now())
print('Pennsylvania')
# =============================================================================
# Read in CONVENTIONAL WELL production file
PA_conv = pd.read_csv(r'pennsylvania\wells\conventional\OilGasProduction.csv')

# Create a date-type reporting period field that will be properly sorted into
# chronological order. Example: '15APR' --> '2015-04-01'
PA_conv['period_id_new'] = PA_conv['PERIOD_ID'].str[:-2]  # remove random character at end of month name
PA_conv['period_id_new'] = PA_conv['period_id_new'] + '-01-01'
PA_conv['period_id_new'] = pd.to_datetime(PA_conv['period_id_new'],
                                          format='%Y-%m-%d')

# Sort records by permit number, then reporting period (from newest to oldest)
# NOTE: Eight digit Permit Number is last 8 digits of API number, not including
# the State code for PA
PA_conv_sorted = PA_conv.sort_values(by=['PERMIT_NUM', 'period_id_new'],
                                     ascending=[True, False],
                                     na_position='first')

# Drop duplicate well records based on permit number, and retain the first
# one that appears in the dataframe (the newest reporting period)
PA_conv_deduped = PA_conv_sorted.drop_duplicates(subset=['PERMIT_NUM',
                                                         'LATITUDE_DECIMAL',
                                                         'LONGITUDE_DECIMAL'],
                                                 keep='first')

dupes = get_duplicate_api_records(PA_conv_deduped, 'PERMIT_NUM')
# Number of duplicate records: 0
# Number of unique values: 0

# Read in UNCONVENTIONAL WELL production files
PA_unconv_2015 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2015.csv')
PA_unconv_2016 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2016.csv')
PA_unconv_2017 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2017.csv')
PA_unconv_2018 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2018.csv')
PA_unconv_2019 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2019.csv')
PA_unconv_2020 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2020.csv')
PA_unconv_2021 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2021.csv')
PA_unconv_2022 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2022.csv')
PA_unconv_2023 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2023.csv')
PA_unconv_2024 = pd.read_csv(r'pennsylvania\wells\unconventional\OilGasProduction_2024.csv')

PA_unconv = pd.concat([PA_unconv_2015,
                       PA_unconv_2016,
                       PA_unconv_2017,
                       PA_unconv_2018,
                       PA_unconv_2019,
                       PA_unconv_2020,
                       PA_unconv_2021,
                       PA_unconv_2022,
                       PA_unconv_2023,
                       PA_unconv_2024],
                      axis=0, ignore_index=True)
PA_unconv = PA_unconv.reset_index(drop=True)


# Create a date-type reporting period field that will be properly sorted into
# chronological order. Example: '15APR' --> '2015-04-01'
PA_unconv['period_id_new'] = PA_unconv['PERIOD_ID'].str[:-1]  # remove random character at end of month name
PA_unconv['period_id_new'] = pd.to_datetime(PA_unconv['period_id_new'],
                                            format='%y%b')


# Sort records by permit number, then reporting period (from newest to oldest)
PA_unconv_sorted = PA_unconv.sort_values(by=['PERMIT_NUM', 'period_id_new'],
                                         ascending=[True, False],
                                         na_position='first')

# Drop duplicate well records based on permit number, and retain the first
# one that appears in the dataframe (the newest reporting period)
PA_unconv_deduped = PA_unconv_sorted.drop_duplicates(subset=['PERMIT_NUM',
                                                             'LATITUDE_DECIMAL',
                                                             'LONGITUDE_DECIMAL'],
                                                     keep='first')

dupes = get_duplicate_api_records(PA_unconv_deduped, 'PERMIT_NUM')
# Number of duplicate records: 0
# Number of unique values: 0

# Concatenate UNCONVENTIONAL and CONVENTIONAL wells together
PA_wells = pd.concat([PA_conv_deduped, PA_unconv_deduped],
                     axis=0,
                     ignore_index=True)

# Remove one erroneous lat-long point (latitude is 49) and points with 'nan' coordinate info
PA_wells = PA_wells[PA_wells.LATITUDE_DECIMAL < 44]

pa_wells_crs = gpd.GeoDataFrame(PA_wells,
                                geometry=gpd.points_from_xy(PA_wells.LONGITUDE_DECIMAL,
                                                            PA_wells.LATITUDE_DECIMAL),
                                crs=4326)

pa_wells_crs = transform_CRS(pa_wells_crs,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(pa_wells_crs, 'Pennsylvania')


# In[  DE-DUPLICATING  ]:

# Check for duplicate APIs; if zero, no further action needed
# Wells that are duplicates here are those that appear in BOTH the
# Conventional AND Unconventional reports for some reason
dupes = get_duplicate_api_records(pa_wells_crs, 'PERMIT_NUM')
# Number of duplicate records: 22
# Number of unique values: 11

# Sort records by permit number, then reporting period (from newest to oldest)
pa_wells_crs_sort = pa_wells_crs.sort_values(by=['PERMIT_NUM',
                                                 'period_id_new'],
                                             ascending=[True, False],
                                             na_position='last')

# Drop duplicate well records based on permit number, and retain the first
# one that appears in the dataframe (the newest reporting period)
pa_wells_crs_sort = pa_wells_crs_sort.drop_duplicates(subset=['PERMIT_NUM'],
                                                      keep='first')
pa_wells_crs = pa_wells_crs_sort.copy().reset_index(drop=True)

dupes = get_duplicate_api_records(pa_wells_crs, 'PERMIT_NUM')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:
# Ensure date objects are formatted correctly
clean_a_date_field(pa_wells_crs, 'SPUD_DATE')

pa_wells_crs['CONFIG_CODE'] = pa_wells_crs['CONFIG_CODE'].str.rstrip('Well').str.strip()
pa_wells_crs['CONFIG_CODE'].replace({'Undetermined': 'N/A'},
                                    inplace=True)

pa_wells_crs['WELL_CODE_DESC'].replace({'UNDETERMINED': 'N/A'},
                                       inplace=True)


# In[  INTEGRATION  ]:

pa_wells_integrated, _errors = integrate_facs(
    pa_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Pennsylvania",
    src_ref_id="215",
    src_date="2023-12-01",  # Annually
    on_offshore=None,
    fac_name="FARM",
    fac_id="PERMIT_NUM",
    fac_type="WELL_CODE_DESC",
    spud_date="SPUD_DATE",
    comp_date=None,
    drill_type="CONFIG_CODE",
    fac_status="WELL_STATUS",
    op_name="CLIENT",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'pa_wells_integrated.geojson'
pa_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% South Dakota
# !!! TODO
print(datetime.datetime.now())
print('South Dakota')
# =============================================================================
sd_wells_csv = pd.read_excel(r'south_dakota\Wells.xlsx', sheet_name=0)

sd_wells = gpd.GeoDataFrame(sd_wells_csv,
                            geometry=gpd.points_from_xy(sd_wells_csv['Longitude (GCS83)'],
                                                        sd_wells_csv['Latitude (GCS83)']),
                            crs=4269)  # Assuming that by GCS83 they mean NAD83

# Check or modify CRS
sd_wells_crs = transform_CRS(sd_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(sd_wells_crs, 'South Dakota')

# In[  DEDUPLICATION  ]:

# Format API-12 numbers to include hyphens. Also, create a API_10 field.
sd_wells_crs['API'] = sd_wells_crs['API Number'].str.replace(' ', '-')
sd_wells_crs['API_10'] = sd_wells_crs['API'].str[0:12]

dupes = get_duplicate_api_records(sd_wells_crs, 'API')
# Number of duplicate records: 0
# Number of unique values: 0
dupes = get_duplicate_api_records(sd_wells_crs, 'API_10')
# Number of duplicate records: 262
# Number of unique values: 124

# Sort records by API-12 and Date Permit Issued, and keep the record with
# the newer Permit date
# This means that for wells with a non-zero value in the 11th and 12 position
# of their API number (i.e., wells that have been re-completed or reworked), are
# listed first (and therefore the most recent status / owner of this well
# surface location can be retained.
sd_wells_crs_sorted = sd_wells_crs.sort_values(by=['API',
                                                   'Date Permit Issued'],
                                               ascending=[False, False],
                                               na_position='last')

# Drop wells with identical API_10 and location. First look at the lat-long
# columns; for any remaining dupe API-10 values with not exactly identical
# lat-long coordinates, use the Range and Footages to determine whether
# the location of both records is the same
sd_wells_crs = sd_wells_crs_sorted.drop_duplicates(subset=['API_10',
                                                           'latitude_calc',
                                                           'longitude_calc'],
                                                   keep='first').reset_index(drop=True)
dupes = get_duplicate_api_records(sd_wells_crs, 'API_10')


sd_wells_crs = sd_wells_crs.drop_duplicates(subset=['API_10',
                                                    'Measured Directions',
                                                    'Location Description'],
                                            keep='first').reset_index(drop=True)

# TODO - FOR NOW, LEAVE THESE DUPES IN UNTIL I FIGURE OUT WHAT TO DO WITH THEM.
dupes = get_duplicate_api_records(sd_wells_crs, 'API_10')
# Number of duplicate records: 4
# Number of unique values: 2


# In[  PREPROCESSING  ]:

# Format dates
sd_wells_crs['spud'] = pd.to_datetime(sd_wells_crs['Spud Date']).dt.strftime("%Y-%m-%d")
sd_wells_crs['spud'] = sd_wells_crs['spud'].fillna('1900-01-01')
sd_wells_crs['comp'] = pd.to_datetime(sd_wells_crs['Completion Date']).dt.strftime("%Y-%m-%d")
sd_wells_crs['comp'] = sd_wells_crs['comp'].fillna('1900-01-01')

# In[  INTEGRATION  ]:
sd_wells_integrated, _errors = integrate_facs(
    sd_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="South Dakota",
    src_ref_id="271",
    src_date="2025-01-03",
    on_offshore='ONSHORE',
    fac_name="Well Name",
    fac_id='API',
    fac_type='Current Well Type',
    spud_date='spud',
    comp_date='comp',
    # drill_type=None,
    fac_status='Administrative Status',
    op_name="Operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'sd_wells_integrated.geojson'
sd_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Tennessee
print(datetime.datetime.now())
print('Tennessee')

# TODO - use the new csv I downloaded instead, source 260
# =============================================================================
tn_wells_csv = pd.read_csv(r'tennessee\wells\Oil and Gas Well Permits.csv')
# Drop records with no lat-long info
tn_wells_csv = tn_wells_csv[tn_wells_csv.Longitude.notna()]

# ASSUMING WGS84 MIGHT BE WRONG
tn_wells = gpd.GeoDataFrame(tn_wells_csv,
                            geometry=gpd.points_from_xy(tn_wells_csv['Longitude'],
                                                        tn_wells_csv['Latitude']),
                            crs=4326)
# Check or modify CRS
tn_wells_crs = transform_CRS(tn_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

if print_maps:
    quickmap(tn_wells_crs, 'Tennessee')

# In[  PREPROCESSING  ]:

dupes = get_duplicate_api_records(tn_wells_crs, 'API No')
# Number of duplicate records: 0
# Number of unique values: 0


# In[  INTEGRATION  ]:
tn_wells_integrated, _errors = integrate_facs(
    tn_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Tennessee",
    src_ref_id="216",
    src_date="2024-11-13",
    on_offshore='ONSHORE',
    fac_name="Well Name And No",
    fac_id="API No",
    fac_type="Purpose of Well",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    fac_status=None,
    op_name="Operator Name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'tn_wells_integrated.geojson'
tn_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Texas - USE `usa_texas_wells.py` FOR TEXAS WELLS
# # =============================================================================

# =============================================================================
# %% Utah
print(datetime.datetime.now())
print('Utah')
# =============================================================================
ut_wells_fp = r'utah/wells/viewAGRC_WellData_Surf.shp'
ut_wells = gpd.read_file(ut_wells_fp)

ut_wells_crs = transform_CRS(ut_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)


if print_maps:
    quickmap(ut_wells_crs, 'Utah')

# In[  DE-DUPLICATING  ]:

# Check for duplicate APIs; if zero, no further action needed
dupes = get_duplicate_api_records(ut_wells_crs, 'API')
# Number of duplicate records: 2
# Number of unique values: 1
ut_wells_crs = ut_wells_crs.drop_duplicates(subset=['API',
                                                    'Operator',
                                                    'WellStatus'],
                                            keep="last")
dupes = get_duplicate_api_records(ut_wells_crs, 'API')


# In[  PRE-PROCESSING  ]:
# Ensure date objects are formatted correctly
clean_a_date_field(ut_wells_crs, 'OrigComplD')

# Change status attribute to long form
ut_wells_crs['WellStatus'].replace({'NEW': 'New Application for Permit to Drill: Received but not yet approved',
                                    'RET': 'New Application for Permit to Drill: Returned unapproved',
                                    'APD': 'Approved Permit to Drill, Deepen, or Re-enter',
                                    'LA': 'Abandoned',
                                    'CP': 'Cancelled Permit',
                                    'DRL': 'Well Spudded and/or Currently Drilling',
                                    'OPS': 'Drilling Operations Suspended',
                                    'P': 'Producing',
                                    'S': 'Shut In',
                                    'PAI': 'Producing Zone or Lateral + Active Injection Zone or Lateral',
                                    'PII': 'Producing Zone or Lateral + Inactive Injection Zone or Lateral',
                                    'SAI': 'Shut-in Zone or Lateral + Active Injection Zone or Lateral',
                                    'SII': 'Shut-in Zone or Lateral + Inactive Injection Zone or Lateral',
                                    'TA': 'Temporarily-Abandoned',
                                    'A': 'Active',
                                    'I': 'Inactive',
                                    'PA': 'Plugged and Abandoned'},
                                   inplace=True)

ut_wells_crs['WellType'].replace({'OW': 'Oil Well',
                                  'GW': 'Natural Gas Well',
                                  'OGW': 'Combined Oil and Gas Well',
                                  'CBM': 'Coalbed Methane Well',
                                  # 'CO2': 'Carbon Dioxide Well',
                                  'CD': 'Carbon Dioxide Well',
                                  'HE': 'Helium Well',
                                  'OWI': 'Oil Well/Water Injection Well',
                                  'GWI': 'Gas Well/Water Injection Well',
                                  'OGI': 'Oil Well/Gas Injection Well',
                                  'GGI': 'Gas Well/Gas Injection Well',
                                  'GWD': 'Gas Well/Water Disposal Well',
                                  'WI': 'Water Injection Well',
                                  'WD': 'Water Disposal Well',
                                  'GI': 'Gas Injection Well',
                                  'GS': 'Gas Storage Well',
                                  'GD': 'Gas Disposal Well',
                                  'WS': 'Water Source Well',
                                  'TW': 'Test Well',
                                  'tw': 'Test Well',
                                  'D': 'Dry Hole',
                                  'NA': 'N/A',
                                  'LI': 'Lithium Well',
                                  'PO': 'Potash Well',
                                  'HS': 'Hydrocarbon Storage Well',
                                  'OWD': 'Oil Well/Water Disposal Well'},
                                 inplace=True)

ut_wells_crs['Dir_Horiz'].replace({'Y': 'Horizontal',
                                   'N': 'Vertical'},
                                  inplace=True)


# In[ integration ]:

ut_wells_integrated, _errors = integrate_facs(
    ut_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Utah",
    src_ref_id="218",
    src_date="2024-04-18",  # Daily
    on_offshore=None,
    fac_name="WellName",
    fac_id="API",
    fac_type="WellType",
    spud_date=None,
    comp_date='OrigComplD',
    drill_type="Dir_Horiz",
    fac_status="WellStatus",
    op_name="Operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save
out_fp = outfolder + 'ut_wells_integrated.geojson'
ut_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% West Virginia
print(datetime.datetime.now())
print('West Virginia')
# TODO - revise input filename so that it auto-fills with the "latest date"
# =============================================================================
wv_fp = r'west_virginia\wells\All_DEP_Wells.geojson'
wv_wells = gpd.read_file(wv_fp)

# Coordinates in geometry column are actually in Web Mercator;
# set the CRS of this gdf directly.
wv_wells = wv_wells.set_crs('epsg:3857', allow_override=True)

# Transform to epsg:4326
wv_wells_crs = transform_CRS(wv_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

# Because there are so many erroneous points, filter the wells
# and keep only those within the state boundary
wv_wells_crs = gpd.clip(wv_wells_crs,
                        states[states.name == 'West Virginia'].geometry)

if print_maps:
    quickmap(wv_wells_crs, 'West Virginia')


# In[  deduplication  ]:

dupes = get_duplicate_api_records(wv_wells_crs, 'api')  # API-10
# Number of duplicate records: 61757
# Number of unique values: 28764

# Sort records by API and issuedate (of permit), and keep the record with the
# NEWEST "issue date"
wv_wells_crs_newestfirst = wv_wells_crs.sort_values(by=['api', 'issuedate'],
                                                    ascending=[True, False],
                                                    na_position='last')

wv_wells_crs = wv_wells_crs_newestfirst.drop_duplicates(subset=['api'],
                                                        keep='first')

dupes = get_duplicate_api_records(wv_wells_crs, 'api')
# Number of duplicate records: 0
# Number of unique values: 0


# NOT removing duplicate lat-longs at this stage -- many of them look like
# they will be filtered out as "never drilled" locations
dupes = get_duplicate_api_records(wv_wells_crs, 'latitude_calc')
# Number of duplicate records: 5394
# Number of unique values: 694


# In[   pre-processing     ]:

replace_missing_strings_with_na(wv_wells_crs, ['welltype',
                                               'welluse'])

clean_a_date_field(wv_wells_crs, 'compdate')

# Replace values I know mean nothing with "N/A"
# In the `respparty`, `farmname`, and `wellnumber` column
wv_wells_crs['respparty'].replace({'OPERATOR UNKNOWN': 'N/A',
                                   'UNKNOWN - DEP PAID PLUGGING CONTRACT': 'N/A',
                                   'UNKNOWN': 'N/A',
                                   'UNKNOWN - WEST VIRGINIA DIVISION OF HIGHWAYS': 'N/A'},
                                  inplace=True)

wv_wells_crs['farmname'] = wv_wells_crs['farmname'].astype(str)
wv_wells_crs['farmname'].replace({'NO SUCH NUMBER': 'N/A',
                                  'nan': 'N/A',
                                  'NO PERMIT ISSUED': 'N/A',
                                  'UNKNOWN': 'N/A'},
                                 inplace=True)

wv_wells_crs['wellnumber'] = wv_wells_crs['wellnumber'].astype(str)
wv_wells_crs['wellnumber'].replace({'NO SUCH NUMBER': 'N/A',
                                    'nan': 'N/A',
                                    'NO PERMIT ISSUED': 'N/A',
                                    'UNKNOWN': 'N/A',
                                    '0': 'N/A'},
                                   inplace=True)


# Create a new Well Name column
create_concatenated_well_name(wv_wells_crs,
                              'farmname',
                              'wellnumber',
                              'wellnamenew')


# Create DRILL_TYPE field
wv_wells_crs['direction'] = wv_wells_crs['welltype']
wv_wells_crs.direction.replace({'HORIZONTAL 6A': 'HORIZONTAL',
                                'COAL BED METHANE WELL': 'N/A'},
                               inplace=True)


# Create a FAC_TYPE field that primarily captures the info in `welluse` column,
# but also indicates whether the well is a coal bed methane well
wv_wells_crs['welluse'].replace({'CBM - ARTICULATE': 'ARTICULATE WELL'},
                                inplace=True)
wv_wells_crs['FACTYPE'] = wv_wells_crs['welluse']
wv_wells_crs.loc[(wv_wells_crs.welltype == 'COAL BED METHANE WELL') & (wv_wells_crs.welluse != 'N/A'), 'FACTYPE'] = 'COAL BED METHANE - ' + wv_wells_crs.welluse


# In[  INTEGRATION  ]:
wv_wells_integrated, _errors = integrate_facs(
    wv_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="West Virginia",
    src_ref_id="219",
    src_date="2024-03-25",  # Weekly
    # on_offshore= None,
    fac_name="wellnamenew",
    fac_id="api",
    fac_type='FACTYPE',
    # spud_date= None,
    comp_date='compdate',
    drill_type='direction',
    fac_status="wellstatus",
    op_name="respparty",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'wv_wells_integrated.geojson'
wv_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')


# =============================================================================
# %% Wyoming
print(datetime.datetime.now())
print('Wyoming')
# TODO - revise input filename so that it auto-fills with the "latest date"
# =============================================================================
# Load in abandoned wells and all other wells, concatenate together
wy_wh = gpd.read_file(r'wyoming/wells/010325WH.DBF')
wy_pa = gpd.read_file(r'wyoming/wells/010325PA.DBF')

wy_wells = pd.concat([wy_wh, wy_pa], axis=0, ignore_index=True)
wy_wells = wy_wells.reset_index(drop=True)

wy_wells_crs = gpd.GeoDataFrame(wy_wells,
                                geometry=gpd.points_from_xy(wy_wells['LON'],
                                                            wy_wells['LAT']),
                                crs=4267)  # nad83 guess

wy_wells_crs = transform_CRS(wy_wells_crs,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)


if print_maps:
    quickmap(wy_wells_crs, 'Wyoming')


# In[  DE-DUPLICATION  ]:
# First, convert the API numbers from floats to strings with no decimal information
wy_wells_crs['APINO'] = wy_wells_crs['APINO'].map('{:.0f}'.format)

dupes = get_duplicate_api_records(wy_wells_crs, 'APINO')
# Number of duplicate records: 0
# Number of unique values: 0

# In[  PRE-PROCESSING  ]:
wy_wells_crs.WELL_CLASS.replace({'O': 'Oil Well',
                                 'G': 'Gas Well',
                                 'C': 'Condensate',
                                 'I': 'Injector Well',
                                 'S': 'Source Well',
                                 'AP': 'Active Permit',
                                 'D': 'Disposal',
                                 'M': 'Monitor Well',
                                 'MW': 'Monitor Well',
                                 'ST': 'Strat Test',
                                 'GS': 'Gas Storage',
                                 'GO': 'Gas Orphaned',
                                 'OO': 'Oil Orphaned',
                                 'DO': 'Disposal Orphaned',
                                 'IO': 'Injector Orphaned',
                                 'MO': 'Monitor Well Orphaned',
                                 'LW': 'LandOwner Water Well',
                                 'WS': 'N/A',  # Cannot find what this defition is
                                 'NA': 'N/A',  # Cannot find what this defition is
                                 '5': 'N/A',   # Cannot find what this defition is
                                 '05': 'N/A',  # Cannot find what this defition is
                                 '1': 'N/A',   # Cannot find what this defition is
                                 '01': 'N/A'},  # Cannot find what this defition is
                                inplace=True)

# wy_wells_crs.Coalbed = wy_wells_crs.Coalbed.str.upper()
# wy_wells_crs.loc[wy_wells_crs.Coalbed=='Y', 'Wellclass'] = wy_wells_crs['Wellclass'] + ' - Coalbed'

wy_wells_crs.STATUS = wy_wells_crs.STATUS.str.upper()
wy_wells_crs.STATUS.replace({'PO': 'Producing Oil Well',
                             'PG': 'Producing Gas Well',
                             'DH': 'Dry Hole',
                             'SI': 'Shut-in',
                             'TA': 'Temporarily Abandoned',
                             'PA': 'Permanently Abandoned',
                             'AI': 'Active Injector',
                             'DR': 'Dormant',
                             'NI': 'Notice of Intent to Abandon',
                             'SR': 'Subsequent Report of Abandonment',
                             'EP': 'Expired Permit',
                             'AP': 'Permit to Drill',
                             'SP': 'Well Spudded',
                             'WP': 'Waiting on Approval',
                             'UNK': 'N/A',
                             'SO': 'Suspended Operations',
                             'NO': 'Denied Permit',
                             'WD': 'Withdrawn Permit',
                             'DU': 'Drilled Uncompleted',
                             'FL': 'Flowing',
                             'GL': 'Gas Lift',
                             'PR': 'Pumping Rods',
                             'PS': 'Pumping Submersible',
                             'PH': 'Pumping Hydraulic',
                             'PL': 'Plunger Lift',
                             'NR': 'N/A',  # No Report
                             'WS': 'N/A',  # Cannot find what this defition is
                             'ND': 'N/A',  # Cannot find what this defition is
                             '02': 'N/A',  # Cannot find what this defition is
                             '03': 'N/A',  # Cannot find what this defition is
                             'MW': 'Monitor Well'},
                            inplace=True)


# Standardize missing values related to well name and number
wy_wells_crs.UNIT_LEASE = wy_wells_crs.UNIT_LEASE.fillna('N/A')
wy_wells_crs.WN = wy_wells_crs.WN.fillna('N/A')
wy_wells_crs.UNIT_LEASE.replace({'None': 'N/A'}, inplace=True)
wy_wells_crs.WN.replace({'None': 'N/A'}, inplace=True)

create_concatenated_well_name(wy_wells_crs,
                              'UNIT_LEASE',
                              'WN',
                              'wellnamenew')

wy_wells_crs.HORIZ_DIR.replace({'N': 'Vertical',  # N stands for Natural Drift on WY's GIS site
                                'H': 'Horizontal',
                                'D': 'Directional'},
                               inplace=True)


# For spud dates or completion dates that have "00" as their day value,
# or have a written-out date shorter than 8 characters,
# mark these dates as n/a
wy_wells_crs.FIRSTSPUD = wy_wells_crs.FIRSTSPUD.fillna('19000101')
wy_wells_crs = wy_wells_crs[~wy_wells_crs.FIRSTSPUD.str.len() < 8]
wy_wells_crs = wy_wells_crs[~wy_wells_crs.FIRSTSPUD.str.endswith('00')]
# Re-format dates so that they contain hyphens, i.e. YYYY-MM-DD
wy_wells_crs.FIRSTSPUD = pd.to_datetime(wy_wells_crs.FIRSTSPUD, format='%Y%m%d')


# Repeat this process for FIRSTCOMP dates
wy_wells_crs.FIRSTCOMP = wy_wells_crs.FIRSTCOMP.fillna('19000101')
wy_wells_crs = wy_wells_crs[~wy_wells_crs.FIRSTCOMP.str.endswith('00')]
wy_wells_crs = wy_wells_crs[~wy_wells_crs.FIRSTCOMP.str.len() < 8]
# Re-format dates so that they contain hyphens, i.e. YYYY-MM-DD
wy_wells_crs.FIRSTCOMP = pd.to_datetime(wy_wells_crs.FIRSTCOMP, format='%Y%m%d')


# Recast both columns as strings
wy_wells_crs.FIRSTSPUD = wy_wells_crs.FIRSTSPUD.astype(str)
wy_wells_crs.FIRSTCOMP = wy_wells_crs.FIRSTCOMP.astype(str)


# In[ INTEGRATION  ]:

wy_wells_integrated, _errors = integrate_facs(
    wy_wells_crs,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United States of America",
    state_prov="Wyoming",
    src_ref_id="220",
    src_date="2024-04-02",  # Monthly
    on_offshore=None,
    fac_name="wellnamenew",
    fac_id="APINO",
    fac_type='WELL_CLASS',
    spud_date='FIRSTSPUD',
    comp_date='FIRSTCOMP',
    drill_type=None,
    fac_status='STATUS',
    op_name="COMPANY",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save as geojson
out_fp = outfolder + 'wy_wells_integrated.geojson'
wy_wells_integrated.to_file(out_fp,
                            encoding='utf-8',
                            driver='GeoJSON')

# # =============================================================================
# # %% HIFLD National Wells  --  REMOVE FOR NOW SINCE HIFLD REMOVED ALMOST ALL THEIR ATTRIBUTE INFO
# print(datetime.datetime.now())
# print('HIFLD wells')
# # =============================================================================
# hifld_fp = r"national\HIFLD\Oil_and_Natural_Gas_Wells.geojson"
# # Takes about 4 minutes to read in and transform CRS of these ~1.5M records
# hifld_wells_usa_can = gpd.read_file(hifld_fp)

# # REMOVE ANY NON USA WELLS
# hifld_wells_usa_all = hifld_wells_usa_can.query("COUNTRY == 'USA'").reset_index(drop=True)
# print(f'Total # of wells in original dataset: {len(hifld_wells_usa_can)}')
# print(f'Total # of US well-level data only : {len(hifld_wells_usa_all)}')

# # REMOVE RECORDS FOR STATES THAT ALREADY HAVE STATE AGENCY WELL DATA
# hifld_wells_usa_all['STATE'] = hifld_wells_usa_all.STATE.map(us_abbrev_to_state)
# hifld_wells_usa_all['STATE'] = hifld_wells_usa_all['STATE'].str.upper()
# drophifld = [
#     'ALABAMA',
#     'ALASKA',
#     'ARKANSAS',
#     'CALIFORNIA',
#     'COLORADO',
#     'KANSAS',
#     'KENTUCKY',
#     'LOUISIANA',
#     'MICHIGAN',
#     'MISSISSIPPI',
#     'MISSOURI',
#     'MONTANA',
#     'NEW MEXICO',
#     'NEW YORK',
#     'NORTH DAKOTA',
#     'OHIO',
#     'OKLAHOMA',
#     'PENNSYLVANIA',
#     'TENNESSEE',
#     'TEXAS',
#     'UTAH',
#     'WEST VIRGINIA',
#     'WYOMING'
# ]
# hifld_wells = hifld_wells_usa_all[-hifld_wells_usa_all.STATE.isin(drophifld)].reset_index(drop=True)
# print(f'Total # of HIFLD wells not already provided by another state source: {len(hifld_wells)}')

# hifld_wells = transform_CRS(hifld_wells,
#                             target_epsg_code="epsg:4326",
#                             appendLatLon=True)

# if print_maps:
#     print(*hifld_wells.columns, sep='\n')
#     base = states.boundary.plot(color='black')
#     hifld_wells.plot(ax=base, markersize=2)


# # In[  PRE-PROCESSING ]:
# # Reduce repetition of the word 'well' in status values
# dict_stat = {
#     'NON-ACTIVE WELL': 'INACTIVE',
#     'PRODUCING WELL': 'PRODUCING',
#     'PRODUCING, NON-ACTIVE WELL': 'PRODUCING BUT INACTIVE',
#     'STORAGE WELL/MAINTENANCE WELL/OBSERVATION WELL': 'STORAGE/MAINTENANCE/OBSERVATION',
#     'UNKNOWN WELL': 'N/A',
#     'WELL DEVELOPMENT': 'DEVELOPMENT',
#     'ACTIVE WELL': 'ACTIVE'}
# hifld_wells.STATUS.replace(dict_stat, inplace=True)

# # Replace null-like values in various fields
# replace_missing_strings_with_na(hifld_wells, ['PRODTYPE', 'NAME', 'OPERATOR'])

# # Replace specific null-like values in specific fields
# hifld_wells.PRODTYPE.replace({'UNKNOWN WELL': 'N/A'}, inplace=True)

# # Operator names: Replace "HISTORIC OWNER" with "OTC/OCC NOT ASSIGNED"
# hifld_wells['OPERATOR'].replace({'HISTORIC OWNER': 'N/A',
#                                  'OTC/OCC NOT ASSIGNED': 'N/A'}, inplace=True)

# # Strip leading and trailing spaces in facility names and operator names
# hifld_wells['well_names'] = hifld_wells.NAME.str.strip()
# hifld_wells['OPERATOR'] = hifld_wells.OPERATOR.str.strip()

# # Fix date fields that will be retained in integrated result
# clean_a_date_field(hifld_wells, 'COMPDATE')
# # The completion date of '2014-12-31' is also NoData marker in HIFLD, remove it
# hifld_wells.COMPDATE.replace({'2014-12-03': '1900-01-01'}, inplace=True)

# #  Remove duplicate well records
# hifld_wells = hifld_wells.drop_duplicates(subset=['NAME',
#                                                   'STATE',
#                                                   'STATUS',
#                                                   'COUNTY',
#                                                   'SOURCE',
#                                                   'SOURCEDATE',
#                                                   'API',
#                                                   'OPERATOR',
#                                                   'PRODTYPE',
#                                                   'COMPDATE',
#                                                   'geometry']).reset_index()

# # In[  INTEGRATION  ]:

# hifld_wells_integrated, _errors = integrate_facs(
#     hifld_wells,
#     starting_ids=1,
#     category="Oil and natural gas wells",
#     fac_alias="WELLS",
#     country="United States of America",
#     state_prov="STATE",
#     src_ref_id="80",
#     src_date="2019-09-19",
#     # on_offshore="on_offshore",
#     fac_name="well_names",
#     fac_id="API",
#     fac_type="PRODTYPE",
#     # spud_date=None,
#     comp_date="COMPDATE",
#     # drill_type=None,
#     fac_status="STATUS",
#     op_name="OPERATOR",
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc")

# # Save as geojson
# out_fp = outfolder + 'hifld_wells_integrated.geojson'
# hifld_wells_integrated.to_file(out_fp,
#                                encoding='utf-8',
#                                driver='GeoJSON')
