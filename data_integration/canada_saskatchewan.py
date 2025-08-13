# -*- coding: utf-8 -*-
"""
Created on Wed Aug 23 07:56:42 2023

Data integration of SASKATCHEWAN, Canada OGIM data -- all categories
Based almost entirely on Mark's previous code:
    `OGIM_Data_Integration_Canada_Saskatchewan-v1.2.ipynb`

@author: maobrien, momara
"""
# =============================================================================
# Import libraries
# =============================================================================
import os
# from tqdm import tqdm
import pandas as pd
import geopandas as gpd
# import re
import numpy as np

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import (transform_CRS, integrate_facs,
                     read_spatial_data, save_spatial_data, schema_WELLS,
                     schema_LNG_STORAGE, schema_COMPR_PROC, schema_OTHER,
                     calculate_pipeline_length_km, get_duplicate_api_records,
                     integrate_pipelines, schema_PIPELINES, schema_REFINERY,
                     calculate_basin_area_km2, schema_BASINS, integrate_basins, check_invalid_geoms)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'canada', 'saskatchewan'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% Define custom functions
# =============================================================================


def flatten_gdf_geometry(gdf, geom_type):
    """Flatten multi-geometry collection (MultiPoint, 'MultiLineString', 'MultiPolygon')

    Inputs:
    ---
        gdf: GeoDataFrame with multi-geometry type
        geom_type: One of the following: MultiPoint, MultiLineString, MultiPolygon

    Returns:
    ---
        new_gdf_geom: Same dataframe as gdf with flattened geometries

    """
    # Geometry
    geometry = gdf.geometry
    flattened_geometry = []

    flattened_gdf = gpd.GeoDataFrame()

    for geom in geometry:
        if geom.type in ['GeometryCollection', 'MultiPoint', 'MultiLineString', 'MultiPolygon']:
            for subgeom in geom:
                if subgeom.type == geom_type:
                    flattened_geometry.append(subgeom)
        else:
            if geom.type == geom_type:
                flattened_geometry.append(geom)

    flattened_gdf.geometry = flattened_geometry

    # Then set gdf's geometry
    new_gdf_geom = gdf.set_geometry(flattened_gdf.geometry)

    return new_gdf_geom


def convert_LLD_to_LSD(lld):
    '''Convert a Legal Land Description I.D. to a Legal Sub-Division I.D.

    Add a leading zero to numeric sections of the legal land description, if it's missing
    Skip the portion of the LLD with cardinal directions (NE, SW, etc.)

    Parameters
    ----------
    lld : str
        A single LLD (Legal Land Description) value, in the format
        '00-AA-00-000-00-0'
        LSD Number - Quarter - Section - Township - Range - Meridian

    Returns
    -------
    lsd_new_ : str
        A single LSD (Legal SubDivision) value, in the format
        00-00-000-00W0
        LSD Number - Section - Township - Range + W + Meridian

    Example Usage
    -------
    convert_LLD_to_LSD('12-NW-10-71-23-3')
    >> 12-10-071-23W3

    '''
    # Some of the LLDs have a letter "a" in parts of them. For the purposes of
    # creating a LSD from the LLD, remove the "a" from the string
    lld = lld.replace('a', '')
    lld = lld.replace('A', '')

    split_ = lld.split("-")

    # LSD Number
    num_01 = split_[0]
    if len(num_01) == 1:
        num_01_ = "0" + num_01
    elif len(num_01) == 2:
        num_01_ = num_01

    # Section
    num_02 = split_[2]
    if len(num_02) == 1:
        num_02_ = "0" + num_02
    elif len(num_02) == 2:
        num_02_ = num_02

    # Township
    num_03 = split_[3]
    if len(num_03) == 2:
        num_03_ = "0" + num_03
    elif len(num_03) == 1:
        num_03_ = "00" + num_03
    else:
        num_03_ = num_03

    # Range
    num_04 = split_[4]
    if len(num_04) == 1:
        num_04_ = "0" + num_04
    elif len(num_04) == 2:
        num_04_ = num_04

    # Assemble new location description
    lsd_new_ = num_01_ + "-" + num_02_ + "-" + num_03_ + "-" + num_04_ + "W" + split_[5]

    return lsd_new_


def remove_trailing_parentheses(my_series,
                                parenthesis=True,
                                bracket=False):
    '''Remove the contents within parentheses at the end of a string, leaving other parentheses intact.'''
    if parenthesis and bracket:
        print("ERROR: Set only ONE of the parameters, parenthesis or bracket, to True.")
        return
    if parenthesis:
        s = my_series.str.rsplit('(', 1).str[0]
    if bracket:
        s = my_series.str.rsplit('[', 1).str[0]
    s = s.str.strip()  # strip whitespace
    return s


def convert_array_to_string(x):
    if isinstance(x, np.ndarray):
        return "; ".join(x)


# =============================================================================
# %% WELLS
# =============================================================================
# Read SK "vertical wells" data
fp_vert = 'wells\\Vertical_Wells.csv'
wellvert_csv = pd.read_csv(fp_vert)
wellvert_points = gpd.GeoDataFrame(wellvert_csv,
                                   geometry=gpd.points_from_xy(wellvert_csv.SURFACELONGITUDE,
                                                               wellvert_csv.SURFACELATITUDE),
                                   crs=4617)  # data uses NAD83(CSRS)

# Read SK "nonvertical wells" data. These data are polyline features that
# indicate the path of nonvertical wells. HOWEVER, the surface locations they
# represent are NOT present in the 'vertical_well.gdb' dataset. We will take
# the surface lat-long for these wells, create a point-type gdf, and append it
# to our previous vertical well gdf to get a complete catalog of all SK wells.
fp_nonvert = 'wells\\Non_Vertical_Wells.csv'
wellnonvert_csv = pd.read_csv(fp_nonvert)
wellnonvert_points = gpd.GeoDataFrame(wellnonvert_csv,
                                      geometry=gpd.points_from_xy(wellnonvert_csv.SURFACELONGITUDE,
                                                                  wellnonvert_csv.SURFACELATITUDE),
                                      crs=4617)  # data uses NAD83(CSRS)

# Append vertical and nonvertical well points into one gdf
sk_wells = pd.concat([wellvert_points, wellnonvert_points]).reset_index(drop=True)

# Plot to check locations of vert and nonvert wells.
sk_wells.plot(column='WELLDRILLINGTRAJECTORY', markersize=3, legend=True)


# Check CRS then transform it
print(sk_wells.crs)
sk_wells = transform_CRS(sk_wells,
                         target_epsg_code="epsg:4326",
                         appendLatLon=True)

# Select only the columns to keep
columns_to_keep = ["WELL_CWI",
                   "LEGACYWELLNAME",
                   "WELLLICENCEBUSINESSASSOCIATE",
                   "WELLDRILLINGTRAJECTORY",
                   "WELLBORECOMPLETION_CWI",  # Completion number
                   "WELLDERIVEDSPUDDATE",
                   "INITIALCOMPLETIONDATE",
                   "WELLBORECOMPSTATUS_FROMDATE",
                   "WELLSTATUS",
                   "WELLBORESTATUS",
                   "WELLSTATUSTYPECODE",
                   "CRUDEOILTYPE",
                   "WELLBORECOMP_CURRENTCOMPTYPE",
                   "longitude_calc",
                   "latitude_calc",
                   "geometry"]
sk_wells = sk_wells[columns_to_keep]

# Remove numbers in parentheses or brackets from the ends of strings
sk_wells['operator'] = remove_trailing_parentheses(sk_wells['WELLLICENCEBUSINESSASSOCIATE'],
                                                   parenthesis=False,
                                                   bracket=True)

sk_wells['welltype'] = remove_trailing_parentheses(sk_wells['WELLBORECOMP_CURRENTCOMPTYPE'],
                                                   parenthesis=True,
                                                   bracket=False)

# Replace the operator value "ABANDONED - OWNER OBSOLETE" with N/A
sk_wells.operator = sk_wells.operator.replace({'ABANDONED - OWNER OBSOLETE': 'N/A'})

# Reformat certain columns before de-duplicating/concatinating them
# TODO - is this necessary?
sk_wells['welltype'] = sk_wells['welltype'].str.replace(' Well', '')

# Properly format spud and (initial) completion date fields
sk_wells['spud'] = pd.to_datetime(sk_wells['WELLDERIVEDSPUDDATE']).dt.strftime("%Y-%m-%d")
sk_wells['completion'] = pd.to_datetime(sk_wells['INITIALCOMPLETIONDATE']).dt.strftime("%Y-%m-%d")

# Create a "completion number" column -- for wells with multiple boreholes or
# completions. Thiis number will help identify the oldest borehole (and
# therefore the first set of spud and completion dates) for deduplication
sk_wells['comp_num'] = sk_wells.WELLBORECOMPLETION_CWI.str.split('V').str[1].astype(int)

# Drop cols I don't need anymore
sk_wells.drop(['WELLLICENCEBUSINESSASSOCIATE',
               'WELLBORECOMP_CURRENTCOMPTYPE',
               'WELLDERIVEDSPUDDATE',
               'INITIALCOMPLETIONDATE'], axis=1, inplace=True)

# %% remove duplicate wells

dupes = get_duplicate_api_records(sk_wells, 'WELL_CWI')
# Number of duplicate records: 36505
# Number of unique values: 13649

# Sort values so that the oldest well completion is listed first
sk_wells_oldestfirst = sk_wells.sort_values(by=['WELL_CWI', 'comp_num'],
                                            ascending=[True, True],
                                            na_position='last')

# Groupby the WELL_CWI. Keep the columns I want. When they are grouped, keep
# the first / oldest spud and comp date. For well type, concatenate. For well
# status, pick the mode (most common)
sk_wells_grouped = sk_wells_oldestfirst.groupby(by=["WELL_CWI"]).agg(
    {"LEGACYWELLNAME": "first",
     "operator": "first",
     "welltype": np.unique,
     "WELLDRILLINGTRAJECTORY": pd.Series.mode,
     "spud": "first",
     "completion": "first",
     # "WELLBORECOMPSTATUS_FROMDATE": "first",
     "WELLSTATUS": pd.Series.mode,
     "WELLBORESTATUS": pd.Series.mode,
     "WELLSTATUSTYPECODE": pd.Series.mode,
     "longitude_calc": 'first',
     "latitude_calc": 'first',
     "geometry": "first"})

# Make the WELL_CWI a column again, not the index value
sk_wells_grouped = sk_wells_grouped.reset_index(drop=False)

# Groupby turns the gdf into a df -- convert the table to a gdf again
sk_wells_grouped = gpd.GeoDataFrame(sk_wells_grouped,
                                    crs=4326,
                                    geometry='geometry')

dupes = get_duplicate_api_records(sk_wells_grouped, 'WELL_CWI')
# Number of duplicate records: 0
# Number of unique values: 0

# During the groupby operation, some surface wells will be assigned a welltype
# that's an array. Change these to strings.
sk_wells_grouped['welltypenew'] = sk_wells_grouped.welltype.apply(lambda x: convert_array_to_string(x))
sk_wells_grouped.loc[sk_wells_grouped.welltypenew.notna(), 'welltype'] = sk_wells_grouped.welltypenew

# =============================================================================
# %% WELLS - Integration + Export
# =============================================================================
sk_wells_integrated, errors_3 = integrate_facs(
    sk_wells_grouped,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="39",
    src_date="2024-04-17",  # Daily
    on_offshore="Onshore",
    fac_name="LEGACYWELLNAME",
    fac_id="WELL_CWI",
    fac_type="welltype",
    spud_date="spud",
    comp_date="completion",
    drill_type="WELLDRILLINGTRAJECTORY",
    # install_date=None,
    fac_status="WELLSTATUS",  # TODO
    op_name="operator",
    # commodity=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# GeoJSON
save_spatial_data(
    sk_wells_integrated,
    file_name="canada_saskatchewan_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% FACILITIES - Read & preprocess Facility Licence Inventory data
# =============================================================================
fp_ = "facilities\\Saskatchewan_Facilities_Inventory_.xlsx"
sk_fac = pd.read_excel(fp_, sheet_name=0, header=3, dtype={'Built \nDate': str})

# Replace all line breaks in the column names with a space
# If after that, there are double-spaces, replace those with a single space
# print(sk_fac.columns)  # !!! This line throws an error when running integration code in bulk, so comment it out here!
sk_fac.columns = sk_fac.columns.str.replace("\n", " ")
sk_fac.columns = sk_fac.columns.str.replace("  ", " ")


# Add source ID and source date (use the "Run Date" at the top of the Excel doc)
sk_fac['src_id'] = '43'
sk_fac['src_date'] = '2024-03-21'

# preview
# print(sk_fac.head())
print("Total number of features in record = ", sk_fac.shape[0])


# Calculate oil and gas in bpd and mmcfd, respectively
# Add/calculate attributes for OIL and GAS design rates
m3_to_bbl = 6.2898
m3_to_ft3 = 35.314666721
sk_fac['capacity_oil_bpd'] = sk_fac['Oil Design Rate (m³/day)'] * m3_to_bbl
sk_fac['capacity_gas_mmcfd'] = sk_fac['Gas/ Associated Gas Design Rate (e³m³/day)'] * m3_to_ft3 / 1000

# Compute gas throughput as the sum of gas used as lease fuel use and
# gas delivered to market
leasefuel = sk_fac['Gas Used As Lease Fuel (e³m³/day)']
marketed = sk_fac['Gas Delivered to Market Injected (e³m³/day)']
sk_fac['throughput_gas_mmcfd'] = (leasefuel + marketed) * 35.314666721 / 1000


# Pre-processing specific fields
# -----------------------------------------------------------------------------
# new_fac_id = []
# licenses = []

# for idx, row in sk_fac.iterrows():

#     # Keep only the numeric part of facility ID (drop the letters specific to SK)
#     fac_id = row['Facility Infrastructure ID']
#     fac_id2 = fac_id.split(" ")
#     new_fac_id_ = fac_id2[0] + fac_id2[1] + fac_id2[2]
#     new_fac_id.append(new_fac_id_)

#     # Convert numeric licence number to a string
#     licenses.append(str(row['Licence #']))

# # Append newly calculated values to original dataframe
# sk_fac['FacilityId'] = new_fac_id
# sk_fac['LicenceNumber'] = licenses

# TODO - test if these changes affect the final product
# Remove spaces from Facility Infrastrucure ID
sk_fac['FacilityId'] = sk_fac['Facility Infrastructure ID'].str.replace(' ', '')
# Make the license ID a string instead of a number
sk_fac['LicenceNumber'] = sk_fac['Licence #'].astype(str)

# Convert dataframe to geodataframe
# !!! NOTE that original CRS isn't stated in metadata, WGS84 is assumed
sk_fac_gdf = gpd.GeoDataFrame(
    sk_fac,
    geometry=gpd.points_from_xy(sk_fac.Longitude, sk_fac.Latitude),
    crs="epsg:4326")

sk_fac_gdf.head()


# =============================================================================
# %% FACILITIES - NEW AND ACTIVE, also SUSPENDED
# =============================================================================
# "https://training.saskatchewan.ca/EnergyAndResources/files/Registry%20Downloads/NewAndActiveFacilitiesReport.csv
fac_data_new = pd.read_csv("facilities\\Saskatchewan_NewActiveFacilitiesReport_.csv")
fac_data_new = fac_data_new.rename(columns={"FacilityId": "FacilityID",
                                            "OPeratorName": "OperatorName"},
                                   errors="raise")
# fac_data_new['FacilityID'] = fac_data_new['FacilityId']
# fac_data_new['OperatorName'] = fac_data_new['OPeratorName']
fac_data_new['src_id'] = '41'

# https://training.saskatchewan.ca/EnergyAndResources/files/Registry%20Downloads/SuspendedFacilitiesReport.csv
fac_data_susp = pd.read_csv("facilities\\Saskatchewan_SuspendedFacilitiesReport_.csv")
fac_data_susp['src_id'] = '42'

# Concatenate facilities data
fac_data_all = pd.concat([fac_data_new, fac_data_susp]).reset_index(drop=True)
fac_data_all.head()
fac_data_all['src_date'] = '2024-04-17'  # Daily

print(fac_data_new.LicenceNumber.unique(), len(fac_data_new.LicenceNumber.unique()))
print(fac_data_susp.LicenceNumber.unique(), len(fac_data_susp.LicenceNumber.unique()))

# Some missing locations are written as "00-00-000-00W0" -- replace this with null
fac_data_all.Location = fac_data_all.Location.replace('00-00-000-00W0', np.nan)
# Exclude any facility record with invalid location identifier
registry_facs = fac_data_all[~fac_data_all.Location.isnull()]
print("Number of records in original registry dataset = {} \ncompared with records with valid location info = {}".format(fac_data_all.shape[0], registry_facs.shape[0]))


# =============================================================================
# %% LEGAL SUBDIVISION (CADASTRAL) - PREPROCESSING
# FYI: this code block takes 10+ minutes to run completely
# =============================================================================
# Read in LSD (legal sub-division) data for Saskatchewan,
# which was downloaded from a REST server via QGIS:
# https://gis.saskatchewan.ca/arcgis/rest/services/CadastreLegalSubdivision/MapServer

fp2 = "cadastral_LSD\\saskatchewan_legal_subdivision_.geojson"
cad_data_ = read_spatial_data(fp2, table_gradient=True)  # LARGE file, takes a while to load

print(cad_data_.LLD)
# Format of LLD (Legal Land Description)
# 00-AA-00-000-00-0
# LSD Number - Quarter - Section - Township - Range - Meridian

# Convert the Legal Land Description in each row of the cadastral data
# into a township-range-section format that can join with other tables.
# Also, get the lat-long centroid of each subdivision polygon.
# -----------------------------------------------------------------------------
cad_data_['LSD'] = cad_data_.LLD.apply(lambda x: convert_LLD_to_LSD(x))
cad_data_.LSD

cad_data_['geom_centroid'] = cad_data_.geometry.centroid
cad_data_['src_id'] = '40'

cad_data_.head()

# =============================================================================
# %% FACILITIES - PREPROCESSING AND MERGING
# =============================================================================
# Facilities Inventory dataset (~8k records) contains lat-longs and
# "Surface Location" in the format 00-00-000-00W0
# Facilities New/Active and Suspended (~36k records) ONLY contains "Location"
# in the format 00-00-000-00W0 -- no lat-long information.
# Note that DLS grid system has a spatial accuracy of +/-200 m


# Get unique licence numbers in the Facilities Inventory dataset
fac_id_unique = list(sk_fac_gdf['LicenceNumber'].unique())
# loc_unique = list(sk_fac_gdf['Location'].unique())

# From the Registry of New/Active and Suspended facilities, identify records with a
# licence num that matches the licence nums in the Facility Inventories dataset
# (a.k.a., records we can match a specific latitude and longitude to)
reg_sel_ids = registry_facs[registry_facs.LicenceNumber.isin(fac_id_unique)]
# reg_sel_locs = registry_facs[registry_facs.Location.isin(loc_unique)]
print(f'{len(reg_sel_ids)} of the {len(registry_facs)} Registry records can be matched with a lat-long location in the Facility Inventory dataset')
# print(f'Compare with total unique licences in facility inventory = {len(fac_id_unique)}')

# Merge the Facility Inventory with the Registry of New/Active and Suspended data
# based on identical Location values and Licence number.
sk_fac_gdf['Location'] = sk_fac_gdf['Surface Location']
sk_fac_merged_ = pd.merge(sk_fac_gdf,  # TODO check for dupes
                          registry_facs,
                          suffixes=('_inventory', '_registry'),
                          on=['LicenceNumber', 'Location'],
                          how='outer')
sk_fac_merged_.head()

# Finally, merge facility records with cadastral records, so that facilities
# without an existing lat-long can be associated with the lat-long centroid of
# the section-township-range they fall within

# Before merging any datasets, confirm that they all use the same CRS
if sk_fac_gdf.crs == cad_data_.crs:
    print('Facilities data and cadastral data use same CRS; OK to merge')
else:
    print('Facilities data and cadastral data DO NOT USE same CRS.')
    print('Re-project before merging geometry columns')

sel_loc_data_ = cad_data_[cad_data_.LSD.isin(sk_fac_merged_.Location.unique())]
print(f'# of Location codes present in both datasets = {len(sel_loc_data_)}')
print(f'compared with total facility records = {len(sk_fac_merged_.Location.unique())}')

cad_data_['Location'] = cad_data_.LSD
sk_fac_gdf2 = pd.merge(sk_fac_merged_,    # TODO check for dupes
                       cad_data_,
                       suffixes=('_facs', '_cadastral'),
                       on='Location',
                       how='left')  # left join because we want to retain all facilities

# -----------------------------------------------------------------------------
# Create a new point geometry column for the gdf. By default, use the coordinate
# provided by the Facilities Inventory dataset; if no coordinate is provided by
# that dataset, use the cadastral centroid geometry.
sk_fac_gdf2['geometry_final'] = sk_fac_gdf2.geometry_facs
sk_fac_gdf2.loc[sk_fac_gdf2.geometry_final.isna(), 'geometry_final'] = sk_fac_gdf2.geom_centroid

sk_fac_gdf2 = sk_fac_gdf2.set_geometry("geometry_final")
sk_fac_gdf2 = transform_CRS(sk_fac_gdf2, appendLatLon=True)
print(sk_fac_gdf2.crs)
sk_fac_gdf2.head()

# -----------------------------------------------------------------------------
# Amend 'Built Date' values to remove Timestamp part, leaving only the date.
# The "Built Date" of "2006-12-13" appears to be a no-data marker of some kind;
# Cast it (and any other nan cells) as NoData (1900-01-01)
sk_fac_gdf2['Built Date'] = pd.to_datetime(sk_fac_gdf2['Built Date']).dt.strftime("%Y-%m-%d")
sk_fac_gdf2['Built Date'].replace({'2006-12-13': '1900-01-01',
                                   'nan': '1900-01-01',
                                   np.nan: '1900-01-01'}, inplace=True)

# =============================================================================
# %% TANK BATTERIES
# Includes: multi-well group battery, effluent measurement battery, etc
# =============================================================================
batteries_id = [
    'Multi Well Oil Battery',
    'Multi Well Swabbing Oil Battery',
    'Multi Well Gas Battery'
]

# Subset data
sk_tanks_bat = sk_fac_gdf2[sk_fac_gdf2['Facility Type'].isin(batteries_id)].reset_index()
print(f'Total number of tank batteries in SK: {len(sk_tanks_bat)}')


sk_tanks_bat3, errors = integrate_facs(
    sk_tanks_bat,
    fac_alias="LNG_STORAGE",
    starting_ids=sk_wells_integrated.OGIM_ID.iloc[-1] + 1,
    category="Tank batteries",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="40,41,42,43",
    src_date="2024-04-17",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="Facility Infrastructure ID",
    fac_type='Facility Type',
    install_date='Built_Date',
    fac_status='Facility Infrastructure Status',
    op_name='Licensee Name',
    liq_capacity_bpd='capacity_oil_bpd',
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="capacity_gas_mmcfd",
    gas_throughput_mmcfd='throughput_gas_mmcfd',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    sk_tanks_bat3,
    file_name="canada_saskatchewan_tank_batteries",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% COMPRESSOR STATIONS
# =============================================================================
comps_ = ['Gas Compression Facility']

comp_stations = sk_fac_gdf2[sk_fac_gdf2['Facility Type'].isin(comps_)]
print(f'Total number of comp stations in SK: {len(comp_stations)}')

sk_comps3, errors = integrate_facs(
    comp_stations,
    fac_alias="COMPR_PROC",
    starting_ids=sk_tanks_bat3.OGIM_ID.iloc[-1] + 1,
    category="Natural gas compressor stations",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="40,41,42,43",
    src_date="2024-04-17",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="Facility Infrastructure ID",
    fac_type='Facility Type',
    install_date='Built_Date',
    fac_status='Facility Infrastructure Status',
    op_name='Licensee Name',
    liq_capacity_bpd='capacity_oil_bpd',
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="capacity_gas_mmcfd",
    gas_throughput_mmcfd='throughput_gas_mmcfd',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    sk_comps3,
    file_name="canada_saskatchewan_compressor_stations",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% GATHERING AND PROCESSING
# =============================================================================
proc_ = ['Gas Processing Plant',
         'Custom Treating Plant',
         'Cleaning Plant/Skim Oil']
proc_plts = sk_fac_gdf2[sk_fac_gdf2['Facility Type'].isin(proc_)]
print(f'Total number of gathering and processing facilities in SK: {len(proc_plts)}')


sk_proc_plts3, errors = integrate_facs(
    proc_plts,
    fac_alias="COMPR_PROC",
    starting_ids=sk_comps3.OGIM_ID.iloc[-1] + 1,
    category="Gathering and processing",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="40,41,42,43",
    src_date="2024-04-17",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="Facility Infrastructure ID",
    fac_type='Facility Type',
    install_date='Built_Date',
    fac_status='Facility Infrastructure Status',
    op_name='Licensee Name',
    liq_capacity_bpd='capacity_oil_bpd',
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="capacity_gas_mmcfd",
    gas_throughput_mmcfd='throughput_gas_mmcfd',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    sk_proc_plts3,
    file_name="canada_saskatchewan_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% PETROLEUM TERMINALS
# =============================================================================
stor_ = ['Oily Byproduct Storage',
         'LPG Storage Facility']

sk_stor = sk_fac_gdf2[sk_fac_gdf2['Facility Type'].isin(stor_)]
print(f'Total number of O&G storage facilities in SK: {len(sk_stor)}')

sk_terms3, errors = integrate_facs(
    sk_stor,
    fac_alias="LNG_STORAGE",
    starting_ids=sk_proc_plts3.OGIM_ID.iloc[-1] + 1,
    category="Petroleum Terminals",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="40,41,42,43",
    src_date="2024-04-17",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="Facility Infrastructure ID",
    fac_type='Facility Type',
    install_date='Built_Date',
    fac_status='Facility Infrastructure Status',
    op_name='Licensee Name',
    liq_capacity_bpd='capacity_oil_bpd',
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="capacity_gas_mmcfd",
    gas_throughput_mmcfd='throughput_gas_mmcfd',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    sk_terms3,
    file_name="canada_saskatchewan_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% INJECTION + DISPOSAL
# =============================================================================
inj_ = ['Water Injection/Disposal Facility',
        'Injection/Production Satellite',
        'EOR Injection Facility']
sk_inj = sk_fac_gdf2[sk_fac_gdf2['Facility Type'].isin(inj_)]
print(f'Total number of O&G injection facilities in SK: {len(sk_inj)}')


sk_inj3, errors = integrate_facs(
    sk_inj,
    fac_alias="LNG_STORAGE",
    starting_ids=sk_terms3.OGIM_ID.iloc[-1] + 1,
    category="Injection and Disposal",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="40,41,42,43",
    src_date="2024-04-17",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="Facility Infrastructure ID",
    fac_type='Facility Type',
    install_date='Built_Date',
    fac_status='Facility Infrastructure Status',
    op_name='Licensee Name',
    liq_capacity_bpd='capacity_oil_bpd',
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="capacity_gas_mmcfd",
    gas_throughput_mmcfd='throughput_gas_mmcfd',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    sk_inj3,
    file_name="canada_saskatchewan_injection_disposal",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% EQUIPMENT + COMPONENTS (CanVec)
# =============================================================================
# Read in valves: a device on a pipeline that controls flow.
fp_ = "canvec_data\\canvec_50K_SK_Res_MGT\\valve_0.shp"
valves_ = read_spatial_data(fp_)
valves_ = transform_CRS(valves_, appendLatLon=True)
valves_['fac_type'] = 'Valve'


sk_valves, errors = integrate_facs(
    valves_,
    fac_alias="OTHER",
    starting_ids=sk_inj3.OGIM_ID.iloc[-1] + 1,
    category="Equipment and components",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="44",  # CanVec 50K
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    fac_type='fac_type',
    fac_id=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    sk_valves,
    file_name="canada_saskatchewan_equipment_components",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% REFINERIES (CanVec)
# =============================================================================
fp2 = "canvec_data\\canvec_250K_SK_Res_MGT\\petroleum_refinery_0.shp"
refin_ = read_spatial_data(fp2)
refin_ = transform_CRS(refin_, appendLatLon=True)

# Calculate capacity in bpd. Capacity units here are in
# 1e3 m3 per day crude oil processed, per the CanVec metadata/documentation
refin_["capacity_bpd"] = refin_['capacity'] * 6.2898 * 1000


sk_ref, errors = integrate_facs(
    refin_,
    fac_alias="REFINERY",
    starting_ids=sk_valves.OGIM_ID.iloc[-1] + 1,
    category="Crude oil refinery",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="45",  # CanVec 250K
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name="facname",
    op_name="ownnames",
    fac_type='petrodscen',
    fac_id=None,
    liq_capacity_bpd="capacity_bpd",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    sk_ref,
    file_name="canada_saskatchewan_crude_oil_refinery",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% PIPELINES (not CanVec)
# TODO - integrate the geojson version from ArcGIS because it's acutally updated daily.
# =============================================================================
sk_pipelines_ = gpd.read_file('pipelines\\Pipelines.geojson')  # ~4600 records
sk_flowlines_ = gpd.read_file('pipelines\\Flowlines.geojson')  # ~92,000 records
sk_pipes = pd.concat([sk_pipelines_, sk_flowlines_])
sk_pipes.head()

# Transform CRS
sk_pipes = transform_CRS(sk_pipes, target_epsg_code="epsg:4326")
# Check if there are null geometries in the data
nulls_, _ = check_invalid_geoms(sk_pipes, id_attr="LICENCENUMBER")

# TODO - test what would happen if we didn't do this step?
# There are some features that are MultiLineString type.
# Flatten MultiLineString into LineString type, and confirm that the total
# number of features has remained the same after the transformation
before = len(sk_pipes)
sk_pipes = flatten_gdf_geometry(sk_pipes, geom_type='LineString')
after = len(sk_pipes)
print(f'Before flatten: {before} -- After flatten: {after}')

# Calculate pipeline length
sk_pipes = calculate_pipeline_length_km(sk_pipes)

# Replace none-like `PIPELINENAME` values with np.nan
sk_pipes.PIPELINENAME = sk_pipes.PIPELINENAME.fillna('N/A')
sk_pipes.PIPELINENAME.replace({'': 'N/A'}, inplace=True)

# If there are any exact duplicate geometries drop them (there are a handful of
# fresh water flowlines I think that are unintentionally duplicated)
sk_pipes = sk_pipes.drop_duplicates(subset=['LICENCENUMBER',
                                            'LICENCEISSUEDATE',
                                            'OWNERNAME',
                                            'PIPELINENAME',
                                            'SUBSTANCE',
                                            'SEGMENTSTATUSTYPECODE',
                                            'PIPELINE_LENGTH_KM',
                                            'geometry'],
                                    keep='first').reset_index()

# =============================================================================
# %% PIPELINES (not CanVec) - Integration & Export
# =============================================================================
sk_pipes_integrated, errors_pipes_6 = integrate_pipelines(
    sk_pipes.reset_index(),
    starting_ids=sk_ref.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="46",
    src_date="2024-04-16",  # Daily
    on_offshore="Onshore",
    fac_name='PIPELINENAME',
    fac_id="LICENCENUMBER",
    fac_type="LICENCETYPE",
    install_date="SEGMENTCONSTRUCTIONDATE",
    fac_status="SEGMENTSTATUS",
    op_name="OWNERNAME",
    commodity="SUBSTANCE",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)


save_spatial_data(
    sk_pipes_integrated,
    file_name="canada_saskatchewan_oil_natural_gas_pipelines",
    schema=schema_PIPELINES,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% FIELDS (aka, oil and natural gas pools)
# =============================================================================
sk_plays = read_spatial_data("pool_lands/pool_land__.shp")
sk_plays = transform_CRS(sk_plays)
sk_plays2 = calculate_basin_area_km2(sk_plays)


sk_fields_, sk_fields_errors = integrate_basins(
    sk_plays2,
    starting_ids=sk_pipes_integrated.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas fields",
    fac_alias="OIL_GAS_BASINS",
    country="Canada",
    state_prov="Saskatchewan",
    src_ref_id="47",
    src_date="2024-04-16",  # Monthly
    on_offshore="Onshore",
    _name="PoolName",
    reservoir_type="PoolType",
    op_name=None,
    _area_km2="AREA_KM2"
)

save_spatial_data(
    sk_fields_,
    file_name="canada_saskatchewan_oil_natural_gas_fields",
    schema=schema_BASINS,
    schema_def=False,  # accomodate multipolygons
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% PRODUCTION  # TODO
# =============================================================================

# # Read monthly production data
# fp_prod = "monthly_production_by_pool"
# all_files = glob.glob(fp_prod + "\\*.xlsx")
# all_data = []

# # Results
# for file in all_files:
#     df = pd.read_excel(file, header=5, sheet_name=4)
#     all_data.append(df)
# # ==============================================================================
# # Then concatenate
# dfs_ = pd.concat(all_data)


# dfs_.head()


# # Read .CSV data for pool codes
# petrinex_pools = pd.read_csv("petrinex_data//SK_PetrinexData-sk_horizon_pool_codes.csv")
# petrinex_pools.head()


# # Now merge this pool code data with the production data based on pool names
# petrinex_pools['Pool Name'] = petrinex_pools['PoolName']

# # Then merge
# dfs_merged = pd.merge(dfs_, petrinex_pools, on='Pool Name', how='left')
# dfs_merged.head()


# print (dfs_merged.columns, "\n ============= \n", dfs_merged['Product Type'].unique())


# # Now merge this production data with shapefile for specific pools
# # First group by Pool Name and sum production values
# aggFns_ = {
#     'Area':'first',
#     'Pool Type':'first',
#     'Crude Oil Type':'first',
#     'Unit Name':'first',
#     'Reported Production Volume':'sum',
#     'Unit of Measure':'first',
#     'Reported Production Hours':'sum',
#     'PoolCode':'first',
#     }


# tot_prod_sk = dfs_merged.groupby(by=['Pool Name', 'Product Type']).agg(aggFns_).reset_index()
# tot_prod_sk.head()


# # In[100]:


# # Pivot Table
# # =============================================================================
# tot_prod_sk2 = pd.pivot_table(tot_prod_sk, values=['Reported Production Volume'], index=['Pool Name', 'PoolCode'], columns=['Product Type']).reset_index()

# # Summary Pivot
# # =============================================================================
# tot_prod_sk2.head()


# # In[101]:


# # Calculate production volumes in Mcf for gas, barrels for oil and water
# # =============================================================================
# tot_prod_sk2['GAS_MCF'] = tot_prod_sk2['Reported Production Volume']['Gas']*35.314666721 # Convert 1e3 m3 to Mcf
# tot_prod_sk2['OIL_BBL'] = tot_prod_sk2['Reported Production Volume']['Oil']*6.2898 # Convert m3 to barrels
# tot_prod_sk2['WATER_BBL'] = tot_prod_sk2['Reported Production Volume']['Water']*6.2898 # Convert m3 to barrels
# tot_prod_sk2['CONDENSATE_BBL'] = tot_prod_sk2['Reported Production Volume']['Condensate']*6.2898 # Convert m3 to barrels


# # In[102]:


# # Merge with pool code shapefile
# # =============================================================================
# tot_prod_sk2['PoolCode2'] = [float(tot_prod_sk2['PoolCode'].iloc[x]) for x in range(tot_prod_sk2.shape[0])]
# sk_plays['PoolCode2'] = [float(sk_plays['PoolCode'].iloc[x]) for x in range(sk_plays.shape[0])]

# results_merged_prod = pd.merge(sk_plays, tot_prod_sk2, on='PoolCode2', how='left')
# results_merged_prod.head()


# # In[103]:


# # =============================================================================
# results_merged_prod.rename(columns={('GAS_MCF', ''):'GAS_MCF',
#                                     ('OIL_BBL', ''):'OIL_BBL',
#                                     ('WATER_BBL', ''):'WATER_BBL',
#                                     ('CONDENSATE_BBL', ''):'CONDENSATE_BBL'}, inplace=True)


# # In[104]:


# results_merged_prod.columns


# # In[105]:


# # Calculate centroid of polygon
# results_prod_ = transform_CRS(results_merged_prod, appendLatLon=True)


# # In[106]:


# # Integrate data

# sk_prod1, sk_prod_errors = integrate_production(
#     results_prod_,
#     starting_ids=sk_fields2.OGIM_ID.iloc[-1]+1,
#     category="Oil and natural gas production",
#     fac_alias="OIL_GAS_PROD",
#     country="Canada",
#     state_prov="British Columbia",
#     src_ref_id="48, 49",
#     src_date="2022-03-29",
#     on_offshore="Onshore",
#     fac_name="PoolName",
#     fac_id="PoolCode",
#     fac_type="PoolType",
#     spud_date=None,
#     comp_date=None,
#     drill_type=None,
#     fac_status=None,
#     op_name=None,
#     oil_bbl="OIL_BBL",
#     gas_mcf='GAS_MCF',
#     water_bbl='WATER_BBL',
#     condensate_bbl='CONDENSATE_BBL',
#     prod_days=None,
#     prod_year=2021,
#     entity_type='Pool',
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
#     )


# # In[107]:


# # Save data
# save_spatial_data(
#     sk_prod1,
#     file_name="canada_saskatchewan_oil_natural_gas_production",
#     schema=schema_OIL_GAS_PROD,
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path="results\\"
#     )
