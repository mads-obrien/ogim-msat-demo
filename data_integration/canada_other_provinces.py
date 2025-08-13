# -*- coding: utf-8 -*-
"""
Created on Fri Sept 1 2023

Code that will integrate OGIM-relevant data sources in
all Canadian provinces *besides* Alberta, BC, Saskatchewan, and Manitoba.

New Brunswick	NB
Newfoundland and Labrador	NL
Northwest Territories	NT
Nova Scotia	NS
Nunavut	NU
Ontario ON
Prince Edward Island	PE
Quebec	QC
Yukon	YT

Based almost entirely on Mark's previous code

@author: maobrien, momara
"""
import os
import pandas as pd
# import numpy as np
import geopandas as gpd
# from tqdm import trange
import glob

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     read_spatial_data, save_spatial_data, schema_WELLS,
                     schema_COMPR_PROC, schema_OTHER,
                     calculate_pipeline_length_km, transform_geom_3d_2d,
                     integrate_pipelines, schema_PIPELINES, schema_REFINERY,
                     calculate_basin_area_km2, schema_BASINS, integrate_basins)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'canada'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% Define custom functions
# =============================================================================


def fix_date_format_nfl(gdf, col_attr_name="SpudDate"):
    """Date format is, for example, "January, 01, 1990", we need to reformat to "1990-01-01" """

    months_mm = {
        'Januray': '01',
        'February': '02',
        'March': '03',
        'April': '04',
        'May': '05',
        'June': '06',
        'July': '07',
        'August': '08',
        'September': '09',
        'October': '10',
        'November': '11',
        'December': '12'
    }

    new_format_ = []

    for idx1, row1 in gdf.iterrows():
        try:
            date_ = row1[col_attr_name].split(",")  # split by ","
            # year [yyyy]
            year_ = date_[1].strip()
            # month [mm]
            month1 = date_[0].split(" ")[0]
            month_ = months_mm.get(month1)
            # day [dd]
            day_ = date_[0].split(" ")[1]
            if len(day_) == 1:
                day_ = "0" + day_
            try:
                yyy_mm_dd = year_ + "-" + month_ + "-" + day_

                new_format_.append(yyy_mm_dd)
            except:
                new_format_.append("1900-01-01")

        except:
            new_format_.append("1900-01-01")

    gdf[col_attr_name] = new_format_

    return gdf


def append_src_ref_id(prov_code="QC", _res="50K"):
    """ Return src_ref_id to append to gdf for CANVEC data

    Inputs:
    ---
        proc_code: 2-letter code for Canadian provice
        _res: data resolution for the Canvec data ("50K" or "250K")

    Returns:
    ----
        src_ref_id: reference id for the data as found in the OGIM standalone table
    """
    if prov_code == "QC" and "50K" in _res:
        src_ref_id = '63'
    elif prov_code == "QC" and "250K" in _res:
        src_ref_id = '64'
    elif prov_code == "NL" and "50K" in _res:
        src_ref_id = '65'
    elif prov_code == "NL" and "250K" in _res:
        src_ref_id = '66'
    elif prov_code == "NB" and "50K" in _res:
        src_ref_id = '67'
    elif prov_code == "NB" and "250K" in _res:
        src_ref_id = '68'
    elif prov_code == "NS" and "50K" in _res:
        src_ref_id = '69'
    elif prov_code == "NS" and "250K" in _res:
        src_ref_id = '70'
    elif prov_code == "NT" and "50K" in _res:
        src_ref_id = '71'
    elif prov_code == "NT" and "250K" in _res:
        src_ref_id = '72'
    elif prov_code == "NU" and "50K" in _res:
        src_ref_id = '73'
    elif prov_code == "NU" and "250K" in _res:
        src_ref_id = '74'
    elif prov_code == "PE" and "50K" in _res:
        src_ref_id = '75'
    elif prov_code == "PE" and "250K" in _res:
        src_ref_id = '76'
    elif prov_code == "YT" and "50K" in _res:
        src_ref_id = '77'
    elif prov_code == "YT" and "250K" in _res:
        src_ref_id = '78'

    return src_ref_id


# =============================================================================
# %% NEWFOUNDLAND AND LABRADOR - WELLS - done
# `Development wells`: https://home-cnlopb.hub.arcgis.com/datasets/28cac5ccdace47819a69e897c79ae729/about
# `Exploration wells`: https://home-cnlopb.hub.arcgis.com/datasets/d4174b3353b3436fa7b7d182aaf656f5/about
# `Dual classified wells`: https://home-cnlopb.hub.arcgis.com/datasets/04d2b5254f894984a59fe322abe98d65/about
# `Delineation wells`: https://home-cnlopb.hub.arcgis.com/datasets/6ff2b3c3d4924497bde5d25e7df6fcc9/about
# =============================================================================

# Read all separate well shapefiles and concatenate them together
all_files_ = glob.glob("newfoundland_labrador\\wells\\*.shp")
all_wells = []
for file in all_files_:
    gdf = gpd.read_file(file)
    all_wells.append(gdf)

all_wells_ = pd.concat(all_wells).reset_index()
all_wells_.head()

# Check CRS
wells2 = transform_CRS(all_wells_, appendLatLon=True)
# Drop wells with unreasonable coordinates
wells2 = wells2.query('longitude_calc > -100')

wells2.columns


# Fix format of date columns using our custom function
# -----------------------------------------------------------------------------
dd = all_wells_.SpudDate.iloc[0].split(",")
year_ = dd[1].strip()
month_ = dd[0].split(" ")[0]
day_ = dd[0].split(" ")[1]

# Fix Spud Date
wells3 = fix_date_format_nfl(wells2, col_attr_name="SpudDate")
# Fix Completion Dates
wells4 = fix_date_format_nfl(wells3, col_attr_name="TermDate")


# The 'Classifi' attribute corresponds with the name of the file downloaded from
# N&L's website; use the value in this column to assign a src_ref_id number to
# each
# -----------------------------------------------------------------------------
wells4.Classifi.unique()
class_dict_ = {
    'Delineation': '139',
    'Development': '136',
    'Exploration/Delineation': '138',
    'Development/Delineation': '138',
    'Exploration / Delineation': '138',
    'Exploration': '137'
}
wells4['src_ref_id'] = wells4['Classifi'].map(class_dict_)

# Based on the src_ref_id, assign the correct SRC_DATE
src_date_dict = {
    '136': '2024-04-17',  # Daily
    '137': '2024-03-27',  # Irregularly
    '138': '2022-11-02',
    '139': '2023-09-06'  # Irregularly
}
wells4['src_date_'] = wells4['src_ref_id'].map(src_date_dict)

# Use well type information from the status field to create a FAC_TYPE column
# that combines the hydrocarbon handled by the well with its deveopment/exploration classification
# -----------------------------------------------------------------------------
wells4.WellStatus.unique()

wells4['hydrocarb'] = wells4.WellStatus
# remove trailing whitespace from values so my `isin()` function works properly
wells4['hydrocarb'] = wells4['hydrocarb'].str.strip()
# If the factype_new value is only a status, no hydrocarbon info, make the value N/A instead
statusonly = ['Abandoned', 'Suspended', 'Drilling']
wells4.loc[wells4.hydrocarb.isin(statusonly), 'hydrocarb'] = None
# Strip the "preceding status info" away from the hydrocarbon info I want to retain
wells4['hydrocarb'] = wells4['hydrocarb'].str.lstrip('Abandoned ')
wells4['hydrocarb'] = wells4['hydrocarb'].str.lstrip('Suspended ')

wells4.loc[wells4.hydrocarb.isna(), 'factype_new'] = wells4.Classifi
wells4.loc[wells4.hydrocarb.notna(), 'factype_new'] = wells4.Classifi + ' - ' + wells4.hydrocarb


# Integration
# -----------------------------------------------------------------------------
nfl_wells_int, errors3 = integrate_facs(
    wells4,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="Newfoundland and Labrador",
    src_ref_id="src_ref_id",
    src_date="src_date_",
    on_offshore="OFFSHORE",
    fac_name="Wellname",
    fac_id="UWI",
    fac_type="factype_new",
    spud_date="SpudDate",
    comp_date="TermDate",
    drill_type=None,
    install_date=None,
    fac_status="WellStatus",
    op_name="Operator",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    nfl_wells_int,
    "canada_newfoundland_labrador_oil_gas_wells",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% NEWFOUNDLAND AND LABRADOR - LICENSE BLOCKS - done
# Active Exploration Licenses: https://home-cnlopb.hub.arcgis.com/datasets/b1f536a6a7554608aec2810c95821ccc/about
# Active Production Licenses: https://home-cnlopb.hub.arcgis.com/datasets/5d563b7108fc41d2add34170ea36edd4/about
# =============================================================================
block1 = read_spatial_data("newfoundland_labrador\\license_blocks\\Active-EL.shp")
block1['src_ref_id'] = '140'
block2 = read_spatial_data("newfoundland_labrador\\license_blocks\\ProductionLicences.shp")
block2['src_ref_id'] = '141'


all_blocks = pd.concat([block1, block2]).reset_index()
all_blocks.columns

# Based on the src_ref_id, assign the correct SRC_DATE
src_date_dict = {
    '140': '2024-03-27',  # updated roughly quarterly?
    '141': '2023-06-27'
}
all_blocks['src_date_'] = all_blocks['src_ref_id'].map(src_date_dict)

# Check and transform CRS
all_blocks = transform_CRS(all_blocks)

# Calculate block area
all_blocks = calculate_basin_area_km2(all_blocks)

# Strip Z coordinate
gdf4 = transform_geom_3d_2d(all_blocks)


# Integrate
# -----------------------------------------------------------------------------
nfl_blks, _ = integrate_basins(
    gdf4,
    starting_ids=nfl_wells_int.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas license blocks",
    fac_alias="OIL_GAS_BASINS",
    country="Canada",
    state_prov="Newfoundland and Labrador",
    src_ref_id="src_ref_id",
    src_date="src_date_",
    on_offshore="Offshore",
    _name="Licence",
    reservoir_type=None,
    op_name="LicenceRep",
    _area_km2="AREA_KM2"
)

save_spatial_data(
    nfl_blks,
    "canada_newfoundland_labrador_oil_gas_license_blocks",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% ONTARIO - WELLS
# `Data source`: https://geohub.lio.gov.on.ca/datasets/lio::petroleum-well/about
# `Data owner`: *Ontario GeoHub*
# =============================================================================
# fp = "ontario\\wells\\Non_Sensitive.gdb"
fp = "ontario\\wells\\Petroleum_Well.geojson"
on_wells = gpd.read_file(fp)
on_wells2 = transform_CRS(on_wells, appendLatLon=True)


# Use the contents of the OFFSHORE_BLOCK column to create a flag for whether a
# well is onshore or offshore; add this info to a new column
on_wells2['ON_OFFSHORE'] = 'Offshore'
on_wells2.loc[on_wells2.OFFSHORE_BLOCK.isna(), 'ON_OFFSHORE'] = 'Onshore'
on_wells2.loc[(on_wells2.OFFSHORE_BLOCK.notna() & on_wells2.OFFSHORE_BLOCK.str.contains(' ')), 'ON_OFFSHORE'] = 'Onshore'


# Use the contents of seperate attributes relating to well directionality to
# create a single column stating the well's directionality
on_wells2['TRAJECTORY'] = 'N/A'
on_wells2.loc[on_wells2.VERTICAL_FLG == 'Y', 'TRAJECTORY'] = 'VERTICAL'
on_wells2.loc[on_wells2.HORIZONTAL_FLG == 'Y', 'TRAJECTORY'] = 'HORIZONTAL'
on_wells2.loc[on_wells2.DIRECTIONAL_FLG == 'Y', 'TRAJECTORY'] = 'DIRECTIONAL'


# Remove the 'time' portion from the existing Spud_Date datetime format
on_wells2['SPUD_DATE_02'] = pd.to_datetime(on_wells2.SPUD_DATE).dt.strftime("%Y-%m-%d")
# Replace a known NA date value with our standard N/A value
on_wells2['SPUD_DATE_02'].replace({'1800-01-01': '1900-01-01'}, inplace=True)
on_wells2['SPUD_DATE_02'].value_counts()


# Modify WELL_MODE (aka well status) values
dict_ = {
    'Abandoned Well': 'ABANDONED',
    'Unknown': 'N/A',
    'Active Well': 'ACTIVE',
    'No Well Found': 'N/A',
    'Suspended Well': 'SUSPENDED',
    'Capped Well': 'CAPPED',
    'Potential': 'POTENTIAL',
    'Plugged back and whipstocked': 'PLUGGED AND WHIPSTOCKED',
    'Abandoned and Junked (Lost)': 'ABANDONED AND JUNKED'
}
on_wells3 = replace_row_names(on_wells2, colName="WELL_MODE", dict_names=dict_)


# Modify WELL_TYPE (aka FAC_TYPE) values
dict_ = {
    'Natural Gas Well': 'GAS',
    'Location': 'LOCATION',
    'Gas Show': 'GAS',
    'Private Gas Well': 'GAS',
    'Dry Hole': 'DRY HOLE',
    'Oil Well': 'OIL',
    'Oil and Gas Show': 'OIL AND GAS',
    'Oil Well Gas Show': 'OIL AND GAS',
    'Gas Well Oil Show': 'OIL AND GAS',
    'Stratigraphic Test': 'STRATIGRAPHIC TEST',
    'Natural Gas Storage Well': 'GAS STORAGE',
    'Brine Well': 'BRINE',
    'Historical Oil Well': 'OIL',
    'Oil and Gas Well': 'OIL AND GAS',
    'Disposal Well': 'DISPOSAL',
    'Oil Show': 'OIL',
    'Other': 'OTHER',
    'Injection Well': 'INJECTION',
    'Licensed': 'LICENCSED',
    'Historical Injection Well': 'INJECTION',
    'Brine Well Natural Gas Well': 'BRINE',
    'Cavern Storage Well': 'CAVERN STORAGE',
    'Solution Mining Well': 'SOLUTION MINING',
    'Observation Well': 'OBSERVATION',
    'Brine Well Oil Well': 'BRINE',
    'Source Well': 'SOURCE'
}
on_wells4 = replace_row_names(on_wells3, colName="WELL_TYPE", dict_names=dict_)

on_wells4['WELL_ID'] = on_wells4['WELL_ID'].astype(int)

on_wells4.WELL_NAME.replace({'Unknown': 'N/A',
                             '(private well name)': 'Confidential'},
                            inplace=True)

on_wells4.OPERATOR.replace({'(private operator)': 'Confidential'},
                           inplace=True)

# INTEGRATION
# -----------------------------------------------------------------------------
on_wells_int, errors3 = integrate_facs(
    on_wells4,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="Ontario",
    src_ref_id="58",
    src_date="2024-11-05",  # Daily
    on_offshore="ON_OFFSHORE",
    fac_name="WELL_NAME",
    fac_id="WELL_ID",
    fac_type="WELL_TYPE",
    spud_date="SPUD_DATE_02",
    # comp_date=None,
    drill_type="TRAJECTORY",
    # install_date=None,
    fac_status="WELL_MODE",
    op_name="OPERATOR",
    # commodity=None,
    # liq_capacity_bpd=None,
    # liq_throughput_bpd=None,
    # gas_capacity_mmcfd=None,
    # gas_throughput_mmcfd=None,
    # num_compr_units=None,
    # num_storage_tanks=None,
    # site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save data
save_spatial_data(
    on_wells_int,
    "canada_ontario_oil_gas_wells",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% ONTARIO - TANKS - done
# Data source: https://geohub.lio.gov.on.ca/documents/mnrf::tank-discontinued/about
# Data owner: Ontario GeoHub
# Online location: https://ws.gisetl.lrc.gov.on.ca/fmedatadownload/Packages/TANK.zip
# Name of resource: Tank - Data Download Package
# Description: Extracted as SHAPE file format June 20, 2014 -
# FINAL EXTRACT - data is no longer maintained or supported
# =============================================================================
fp_ = "ontario\\tanks_historical_data\\LIO-2014-06-20\\TANK.shp"
on_tanks_all = read_spatial_data(fp_)
on_tanks_all = transform_CRS(on_tanks_all, appendLatLon=True)

# Filter out water tanks, keep petroleum tanks only
on_tanks_all.SUBTYPE.unique()
on_tanks = on_tanks_all.query("SUBTYPE == 'Petroleum Tank'")

# Because these records are polygons, create centroid points to use as the
# geometry instead
on_tanks['centroid'] = on_tanks['geometry'].centroid
on_tanks_point = on_tanks.set_geometry('centroid')

# !!! NOTE: According to documentation, BUSINESS_EFFECTIVE_DATE means "Date
# that the record becomes effective in relation to the business i.e. the date
# MNR became aware of its existence." This isn't the same as INSTALL_DATE;
# don't include in OGIM

# !!! NOTE: Because these are individual tank locations, and not a point at a
# tank battery, set the category to EQUIPMENT and COMPONENTS category

# Create gdf
on_tanks_point_integrated, err_ = integrate_facs(
    on_tanks_point,
    starting_ids=on_wells_int.OGIM_ID.iloc[-1] + 1,
    category="Equipment and components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="Ontario",
    src_ref_id="59",
    src_date="2014-06-20",
    on_offshore="Onshore",
    # fac_name=None,
    # fac_id=None,
    fac_type="SUBTYPE",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # install_date="INSTALL_DATE",
    # fac_status=None,
    # op_name=None,
    # commodity=None,
    # liq_capacity_bpd=None,
    # liq_throughput_bpd=None,
    # gas_capacity_mmcfd=None,
    # gas_throughput_mmcfd=None,
    # num_compr_units=None,
    # num_storage_tanks=None,
    # site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save data
save_spatial_data(
    on_tanks_point_integrated,
    file_name="canada_ontario_equipment_components_tanks",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)

# =============================================================================
# %% ONTARIO - FIELDS - done
# =============================================================================
fp_ = "ontario\\oil_gas_fields\\Non_Sensitive.gdb"
on_fields = read_spatial_data(fp_, table_gradient=False)

# Transform CRS
on_fields3 = transform_CRS(on_fields)
# Calculate area
on_fields4 = calculate_basin_area_km2(on_fields3)

# Integrate
on_fields, _errors_ = integrate_basins(
    on_fields4,
    starting_ids=on_tanks_point_integrated.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas fields",
    fac_alias="OIL_GAS_BASINS",
    country="Canada",
    state_prov="Ontario",
    src_ref_id="60",
    src_date="2011-10-31",
    on_offshore="Onshore",
    _name="HISTORICAL_OIL_FIELD_NAME",
    reservoir_type=None,
    op_name=None,
    _area_km2="AREA_KM2"
)


# Save data
save_spatial_data(
    on_fields,
    file_name="canada_ontario_oil_gas_fields",
    schema=schema_BASINS,
    schema_def=False,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% ONTARIO - PIPELINES (CanVec) - done
# =============================================================================
pipe1 = read_spatial_data("ontario\\canvec_data\\canvec_50K_ON_Res_MGT\\pipeline_1.shp")
pipe1['src_ref_id'] = '61'
pipe2 = read_spatial_data("ontario\\canvec_data\\canvec_250K_ON_Res_MGT\\pipeline_1.shp")
pipe2['src_ref_id'] = '62'

# Concatenate the two datasets
pipe12_on = pd.concat([pipe1, pipe2])

# Transform CRS
pipes4 = transform_CRS(pipe12_on, target_epsg_code="epsg:4326")
# Calculate pipeline length in km
pipes5 = calculate_pipeline_length_km(pipes4)

# Change pipeline type values
pipes5.pippdt_en.unique()
dict_ = {
    'Multiuse': 'Multiuse',
    'Not Identified': 'N/A',
    'Natural Gas': 'NATURAL GAS',
    'Oil': 'OIL'
}
pipes6 = replace_row_names(pipes5, "pippdt_en", dict_names=dict_)


# Data integration
on_pipes, errors_pipes = integrate_pipelines(
    pipes6.reset_index(),
    starting_ids=on_fields.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="Ontario",
    src_ref_id="src_ref_id",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity="pippdt_en",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)


# Save results
save_spatial_data(
    on_pipes,
    file_name="canada_ontario_oil_natural_gas_pipelines",
    schema=schema_PIPELINES,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% ONTARIO - REFINERIES (CanVec) - done
# =============================================================================
ref_ = read_spatial_data("ontario\\canvec_data\\canvec_250K_ON_Res_MGT\\petroleum_refinery_0.shp")

# Transform CRS
ref2_ = transform_CRS(ref_, appendLatLon=True)

# Capcity information is provided in units of 1e3 m3 day of crude oil; convert to BPD
ref2_['capacity_bpd'] = ref2_['capacity'] * 6.2898 * 1000

# Integration
# -----------------------------------------------------------------------------
on_ref3, on_ref_errors = integrate_facs(
    ref2_,
    starting_ids=on_pipes.OGIM_ID.iloc[-1] + 1,
    category="Crude oil refinery",
    fac_alias="REFINERY",
    country="Canada",
    state_prov="Ontario",
    src_ref_id="62",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name="facname",
    # fac_id="feature_id",
    fac_type=None,
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name="ownnames",
    commodity=None,
    liq_capacity_bpd="capacity_bpd",
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save results
save_spatial_data(
    on_ref3,
    file_name="canada_ontario_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% ONTARIO - EQUIP & COMPONENTS (CanVec) - done
# =============================================================================
valves_ = read_spatial_data("ontario\\canvec_data\\canvec_50K_ON_Res_MGT\\valve_0.shp")

# Transform CRS
valves2_ = transform_CRS(valves_, appendLatLon=True)

# Equipment type
valves2_['type_'] = 'Valve'


# Integration
# -----------------------------------------------------------------------------
on_Eq3r, on_Eq3_errors = integrate_facs(
    valves2_,
    starting_ids=on_ref3.OGIM_ID.iloc[-1] + 1,
    category="Equipment and Components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="61",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type="type_",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save results
save_spatial_data(
    on_Eq3r,
    file_name="canada_ontario_equipment_components_valves",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path)


# =============================================================================
# %% ONTARIO - GATHERING AND PROCESSING (CanVec)
# !!! Question for Mark, are we sure these are oil and gas sites?
# =============================================================================
dat1 = read_spatial_data("ontario\\canvec_data\\canvec_50K_ON_Res_MGT\\oil_gas_site_0.shp")  # Point
dat1['src_ref_id'] = '61'
dat3 = read_spatial_data("ontario\\canvec_data\\canvec_250K_ON_Res_MGT\\oil_gas_site_0.shp")  # Point
dat3['src_ref_id'] = '62'

# Concatenate
fac_data_on = pd.concat([dat1, dat3])

# Transform CRS
fac_data_on3 = transform_CRS(fac_data_on, appendLatLon=True)

fac_data_on3.ogs_dsc_en.unique()


# Integration
# -----------------------------------------------------------------------------
fac_on_, _ = integrate_facs(
    fac_data_on3,
    fac_alias="COMPR_PROC",
    starting_ids=on_Eq3r.OGIM_ID.iloc[-1] + 1,
    category="Gathering and processing",
    country="Canada",
    state_prov="Ontario",
    src_ref_id="src_ref_id",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    op_name=None,
    fac_type='ogs_dsc_en',
    # fac_id='feature_id',
    liq_capacity_bpd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save data
save_spatial_data(
    fac_on_,
    file_name="canada_ontario_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% OTHER PROVINCES - WELLS, REFINERIES, PIPELINES, VALVES, O&G SITES (CanVec)
# =============================================================================
# Map province abbreviations that appear in filenames to their full name
prov_dict_ = {
    "QC": "Quebec",
    "NL": "Newfoundland and Labrador",
    "NB": "New Brunswick",
    "NS": "Nova Scotia",
    "NT": "Northwest Territories",
    "NU": "Nunavut",
    "PE": "Prince Edward Island",
    "YT": "Yukon"
}

# Within the `data\\canada` folder, make a list of all subfolders specific to
# each province so I can iterate over them
folder_names = ['new_brunswick\\canvec_data',
                'newfoundland_labrador\\canvec_data',
                'northwest_territories\\canvec_data',
                'nova_scotia\\canvec_data',
                'nunavut\\canvec_data',
                'pei\\canvec_data',
                'quebec\\canvec_data',
                'yukon\\canvec_data']


# Read CanVec data so that there's one list per infrastructure type
# (i.e. `data_wells`), and that list contains multiple dataframes (one dataframe
# per shapefile that's loaded), with each df pertaining to a province.
# -----------------------------------------------------------------------------
well_data_fn, data_wells = [], []
oil_gas_site_fn, data_oil_gas_site = [], []
refinery_fn, data_refineries = [], []
pipeline_fn, data_pipelines = [], []
valves_fn, data_valves = [], []


# Loop through each province's canvec_data folder, and identify each shapefile
# wthin the prov's canvec_data folder (and the folder's subdirectories).
# For each shapefile, determine what infrastructure types it contains based on
# the filename. Read the shp in and append its records to the proper list.
for root in folder_names:
    print(root)
    for path, subdirs, files in os.walk(root):
        for name in files:
            if name.endswith(".shp"):
                print(os.path.join(path, name))
                path_to_a_shp = os.path.join(path, name)

                if "petroleum_well_0" in path_to_a_shp:
                    well_data_fn.append(path_to_a_shp)

                    # Read data
                    gdf = gpd.read_file(path_to_a_shp)
                    gdf = transform_CRS(gdf, appendLatLon=True)

                    # Use the province abbreviation in the subfolder name
                    # to set the STATE_PROV attribute
                    prov_code = path_to_a_shp.split("\\")[2].split("_")[2]
                    gdf['STATE_PROV'] = prov_dict_.get(prov_code)

                    # use resolution in the subfolder name to assign SRC_REF_ID
                    _res = path_to_a_shp.split("\\")[2].split("_")[1]
                    ref_id_ = append_src_ref_id(prov_code=prov_code, _res=_res)
                    gdf['src_ref_id1'] = ref_id_

                    # Append GDF
                    data_wells.append(gdf)

                elif "oil_gas_site_0" in path_to_a_shp or "oil_gas_site_1" in path_to_a_shp:
                    oil_gas_site_fn.append(path_to_a_shp)

                    # Read data
                    gdf = gpd.read_file(path_to_a_shp)
                    gdf = transform_CRS(gdf, appendLatLon=True)

                    # Use the province abbreviation in the subfolder name
                    # to set the STATE_PROV attribute
                    prov_code = path_to_a_shp.split("\\")[2].split("_")[2]
                    gdf['STATE_PROV'] = prov_dict_.get(prov_code)

                    # use resolution in the subfolder name to assign SRC_REF_ID
                    _res = path_to_a_shp.split("\\")[2].split("_")[1]
                    ref_id_ = append_src_ref_id(prov_code=prov_code, _res=_res)
                    gdf['src_ref_id1'] = ref_id_

                    # Append GDF
                    data_oil_gas_site.append(gdf)

                elif "petroleum_refinery_0" in path_to_a_shp:
                    refinery_fn.append(path_to_a_shp)

                    # Read data
                    gdf = gpd.read_file(path_to_a_shp)
                    gdf = transform_CRS(gdf, appendLatLon=True)

                    # Use the province abbreviation in the subfolder name
                    # to set the STATE_PROV attribute
                    prov_code = path_to_a_shp.split("\\")[2].split("_")[2]
                    gdf['STATE_PROV'] = prov_dict_.get(prov_code)

                    # use resolution in the subfolder name to assign SRC_REF_ID
                    _res = path_to_a_shp.split("\\")[2].split("_")[1]
                    ref_id_ = append_src_ref_id(prov_code=prov_code, _res=_res)
                    gdf['src_ref_id1'] = ref_id_

                    # Append GDF
                    data_refineries.append(gdf)

                elif "pipeline_1" in path_to_a_shp:
                    pipeline_fn.append(path_to_a_shp)

                    # Read data
                    gdf = gpd.read_file(path_to_a_shp)
                    gdf = transform_CRS(gdf, appendLatLon=True)

                    # Use the province abbreviation in the subfolder name
                    # to set the STATE_PROV attribute
                    prov_code = path_to_a_shp.split("\\")[2].split("_")[2]
                    gdf['STATE_PROV'] = prov_dict_.get(prov_code)

                    # use resolution in the subfolder name to assign SRC_REF_ID
                    _res = path_to_a_shp.split("\\")[2].split("_")[1]
                    ref_id_ = append_src_ref_id(prov_code=prov_code, _res=_res)
                    gdf['src_ref_id1'] = ref_id_

                    # Append GDF
                    data_pipelines.append(gdf)

                elif "valve_0" in path_to_a_shp:
                    valves_fn.append(path_to_a_shp)

                    # Read data
                    gdf = gpd.read_file(path_to_a_shp)
                    gdf = transform_CRS(gdf, appendLatLon=True)

                    # Use the province abbreviation in the subfolder name
                    # to set the STATE_PROV attribute
                    prov_code = path_to_a_shp.split("\\")[2].split("_")[2]
                    gdf['STATE_PROV'] = prov_dict_.get(prov_code)

                    # use resolution in the subfolder name to assign SRC_REF_ID
                    _res = path_to_a_shp.split("\\")[2].split("_")[1]
                    ref_id_ = append_src_ref_id(prov_code=prov_code, _res=_res)
                    gdf['src_ref_id1'] = ref_id_

                    # Append GDF
                    data_valves.append(gdf)


# =============================================================================
# %% OTHER PROVINCES (CanVec) - Integrate + Export
# =============================================================================
# For each infra category, concatenate all separate dataframes
# into a single dataframe, and use that df as input for the integration fxn


# WELLS integration
# -----------------------------------------------------------------------------
data_wells_ = pd.concat(data_wells).reset_index()
print("Total # of records = ", data_wells_.shape[0])

wells_gdf, errors_3 = integrate_facs(
    data_wells_,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="STATE_PROV",
    src_ref_id="src_ref_id1",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type=None,
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    wells_gdf,
    file_name="canada_other_provinces_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# OIL AND GAS FACILITY (gathering and processing) integration
# -----------------------------------------------------------------------------
data_fac_ = pd.concat(data_oil_gas_site).reset_index()
print("Total # of records = ", data_fac_.shape[0])

fac_gdf, errors_3 = integrate_facs(
    data_fac_,
    starting_ids=wells_gdf.OGIM_ID.iloc[-1] + 1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Canada",
    state_prov="STATE_PROV",
    src_ref_id="src_ref_id1",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type="ogs_dsc_en",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    fac_gdf,
    file_name="canada_other_provinces_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# VALVES integration
# -----------------------------------------------------------------------------
data_valves_ = pd.concat(data_valves).reset_index()
print("Total # of records = ", data_valves_.shape[0])

# Specify facility types
data_valves_['fac_type'] = "VALVES"

valves_gdf, errors_3 = integrate_facs(
    data_valves_,
    starting_ids=fac_gdf.OGIM_ID.iloc[-1] + 1,
    category="Equipment and components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="STATE_PROV",
    src_ref_id="src_ref_id1",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type="fac_type",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    valves_gdf,
    file_name="canada_other_provinces_equipment_components",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# REFINERIES integration
# -----------------------------------------------------------------------------
data_refineries_ = pd.concat(data_refineries).reset_index()
print("Total # of records = ", data_refineries_.shape[0])

ref_gdf, errors_3 = integrate_facs(
    data_refineries_,
    starting_ids=valves_gdf.OGIM_ID.iloc[-1] + 1,
    category="Crude oil refinery",
    fac_alias="REFINERY",
    country="Canada",
    state_prov="STATE_PROV",
    src_ref_id="src_ref_id1",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name="facname",
    # fac_id="feature_id",
    fac_type="petrodscen",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name="ownnames",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    ref_gdf,
    file_name="canada_other_provinces_crude_oil_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# PIPELINES integration
# -----------------------------------------------------------------------------
data_pipes_ = pd.concat(data_pipelines).reset_index()
print("Total # of records = ", data_pipes_.shape[0])

# Calculate pipeline length in km
data_pipes2 = calculate_pipeline_length_km(data_pipes_)

data_pipes2.pippdt_en.unique()
data_pipes3 = replace_row_names(data_pipes2,
                                "pippdt_en",
                                dict_names={'Oil': 'OIL',
                                            'Natural Gas': 'NATURAL GAS',
                                            'Multiuse': 'MULTIUSE',
                                            'Not Identified': 'N/A'})

pipes_gdf, errors_3 = integrate_pipelines(
    data_pipes3,
    starting_ids=ref_gdf.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="STATE_PROV",
    src_ref_id="src_ref_id1",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type="piprgrd_en",
    commodity="pippdt_en",
    pipe_length_km="PIPELINE_LENGTH_KM"
)

save_spatial_data(
    pipes_gdf,
    file_name="canada_other_provinces_oil_natural_gas_pipelines",
    schema=schema_PIPELINES,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)
