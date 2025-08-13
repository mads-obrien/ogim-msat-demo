# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 11:49:43 2023

Data integration of BRITISH COLUMBIA, Canada OGIM data -- all categories
Based almost entirely on Mark's previous code:
    `Refresh_and_Data_Integration_Canada_British_Columbia.ipynb`

NOTE that most British Columbia data must be downloaded/refreshed MANUALLY from
https://data-bc-er.opendata.arcgis.com/
As such, there is no `data_refresh/canada_british_columbia.py` file.

@author: maobrien, momara
"""
# =============================================================================
# Import libraries
# =============================================================================
import os
# from tqdm import trange
import pandas as pd
import geopandas as gpd

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     read_spatial_data, save_spatial_data, schema_WELLS,
                     schema_LNG_STORAGE, schema_COMPR_PROC, schema_OTHER,
                     schema_REFINERY, calculate_pipeline_length_km,
                     integrate_pipelines, schema_PIPELINES,
                     calculate_basin_area_km2, schema_BASINS, integrate_basins)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'canada', 'british_columbia'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% WELLS
# Data source: https://data-bc-er.opendata.arcgis.com/
# Data owner: British Columbia Energy Regulator
# Well-level data are included in the `Well_Surface_Hole_Locations_(Permitted).shp` file
#  - Facility data are included in the `Facility_Locations_(Permitted).shp` data
#  - Most of the data are updated nightly
# =============================================================================
fp = r"wells/Well_Surface_Hole_(Permitted).geojson"
bc_wells = read_spatial_data(fp, table_gradient=True)
bc_wellsv2 = transform_CRS(bc_wells, target_epsg_code='epsg:4326', appendLatLon=True)

# Replace abbreviations with full names
# https://www.bc-er.ca/files/gis/WELLCODE_METADATA.pdf

# Well Status
dict_names = {
    'ABAN': 'Abandoned',
    'ACT': 'Active',
    'CANC': 'Cancelled',
    'CASE': 'Cased',
    'COMP': 'Completed',
    'DRIL': 'Drilling',
    'DSUS': 'Drilling suspended',
    'GAST': 'Gas testing',
    'PRES': 'Prep to resume',
    'PSPD': 'Prep to spud',
    'SUSP': 'Suspended',
    'WAG': 'Well authorization granted',
    'ABNZ': 'Abandoned zone',
    'XXXX': 'N/A',
    "NOT AVAILABLE": 'N/A'}
bc_wellsv3 = replace_row_names(bc_wellsv2, "WELL_ACTIVITY", dict_names)

# Fluid type
dict_names = {
    'GAS': 'Gas',
    'MGAS': 'Multiple gas',
    'MOG': 'Multiple oil and gas',
    'MOIL': 'Multiple oil',
    'OIL': 'Oil',
    'SOLV': 'Solvent',
    'UND': 'N/A',
    'WATR': 'Water',
    'CO2': 'Carbon dioxide',
    'LPG': 'Liquefied petroleum gas',
    'AGAS': 'Acid gas',
    'GEOT': 'Geothermal fluid',
    'XXXX': 'N/A'}
bc_wellsv3b = replace_row_names(bc_wellsv3, 'BORE_FLUID_TYPE', dict_names)

# Operations type
dict_names = {
    'DISP': 'Disposal',
    'INJ': 'Injection',
    'OBS': 'Observation',
    'PROD': 'Production',
    'SRC': 'Source',
    'STOR': 'Storage',
    'UND': 'N/A',
    'CYCL': 'Cyclical',
    'XXXX': 'N/A',
    'Undefined': 'N/A',
    'NOT AVAILABLE': 'N/A'}
bc_wellsv3c = replace_row_names(bc_wellsv3b, 'OPERATION_TYPE', dict_names)

# =============================================================================
# %% WELLS - INTEGRATION
# =============================================================================
bc_wells3, errors_3 = integrate_facs(
    bc_wellsv3c,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="25",
    src_date="2024-04-19",
    on_offshore="Onshore",
    fac_name="WELL_NAME",
    fac_id="WELL_AUTHORITY_NUMBER",
    fac_type="OPERATION_TYPE",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status="WELL_ACTIVITY",
    op_name="OPERATOR_ABBREVIATION",
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
    bc_wells3,
    file_name="canada_british_columbia_oil_gas_wells",
    schema=schema_WELLS,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% Load in general facility shapefile
# `Data source`: https://data-bc-er.opendata.arcgis.com/
# Facility Type Codes:
# https://data-bc-er.opendata.arcgis.com/datasets/ef4b939c21454c8399771863b0491fdf_0/explore
# =============================================================================
fp_ = "Facility_Locations_(Permitted).geojson"
bc_fac = read_spatial_data(fp_, table_gradient=True)
bc_fac = transform_CRS(bc_fac, target_epsg_code='epsg:4326', appendLatLon=True)

# Inspect what facility types are present
bc_fac.FACILITY_TYPE_DESC.value_counts()

# Remove trailing 'T00:00:00+00:00' from date fields
bc_fac.ACTIVITY_APPROVAL_DATE = pd.to_datetime(bc_fac.ACTIVITY_APPROVAL_DATE).dt.strftime("%Y-%m-%d")
bc_fac.ACTIVITY_CANCEL_DATE = pd.to_datetime(bc_fac.ACTIVITY_CANCEL_DATE).dt.strftime("%Y-%m-%d")

# We assume the ACTIVITY_APPROVAL date as an approximte installation date for the facility
bc_fac[bc_fac.ACTIVITY_APPROVAL_DATE.notna()].ACTIVITY_APPROVAL_DATE.min()
bc_fac[bc_fac.ACTIVITY_APPROVAL_DATE.notna()].ACTIVITY_APPROVAL_DATE.max()

# =============================================================================
# %% TANK BATTERIES
# These refer to a collection of tanks in oil and gas gathering and processing
# and are going to be collocated with other major facility types such as
# compressor stations, well pads, and processing plants, or could be standalone facilities
# =============================================================================
batteries_id = ['Battery Site',
                'Satellite Battery',
                'Processing Battery']
bc_tanks_bat = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(batteries_id)].reset_index()
print(f"Total number of tank batteries in BC = {len(bc_tanks_bat)}")

# Integration
bc_tanks_bat3, bc_tanks_errors = integrate_facs(
    bc_tanks_bat,
    starting_ids=1,
    category="Tank batteries",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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
    bc_tanks_bat3,
    file_name="canada_british_columbia_tank_batteries",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% COMPRESSOR STATIONS
# =============================================================================
comps_ = ['Compressor Station']
comp_stations = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(comps_)]
print(f"Total number of comp stations in dataset = {len(comp_stations)}")

# Integration
bc_comps3, errors_comps = integrate_facs(
    comp_stations,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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
    bc_comps3,
    file_name="canada_british_columbia_compressor_stations",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% INJECTION AND DISPOSAL
# =============================================================================
inj_ = ['Disposal Station',
        'Injection Station']
inj = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(inj_)]
print(f"Total number of injection and disposal facilities in dataset = {len(inj)}")

# Integration
bc_inj, errors_inj = integrate_facs(
    inj,
    starting_ids=1,
    category="Injection and disposal",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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
    bc_inj,
    file_name="canada_british_columbia_injection_disposal",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% STATIONS-OTHER
# Includes pumping stations and metering and regulating stations
# =============================================================================
stns_ = ['Pump Station',
         'Oil Sales Meter',
         'Gas Sales Meter']
stns = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(stns_)]
print(f"Total number of stations - other in dataset = {len(stns)}")

# Integration
bc_stns, errors_stns = integrate_facs(
    stns,
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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
    bc_stns,
    file_name="canada_british_columbia_stations_other",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% WELL FACILITIES
# ??? - are we counting these as wells, how can we be sure what they represent?
# =============================================================================
wells_fac_ = ['Well Facility']
wells_fac = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(wells_fac_)]
print(f"Total number of well facilities in dataset = {len(wells_fac)}")

# Integration
bc_well_facs, errors_well_facs = integrate_facs(
    wells_fac,
    starting_ids=1,
    category="OIL AND NATURAL GAS WELL FACILITY",
    fac_alias="WELLS",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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
    bc_well_facs,
    file_name="canada_british_columbia_well_facilities",
    schema=schema_WELLS,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% GATHERING AND PROCESSING
# =============================================================================
proc_ = ['Gas Processing Plant',
         'NGL Fractionation Facility',
         'Pipeline Gathering']
proc_plts = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(proc_)]
print(f"Total number of processing plants in dataset = {len(proc_plts)}")

# Read gas processing plant facility throughput data
# Data source: https://iris.bc-er.ca/download/facility_capacity.csv
proc_thru = pd.read_csv("facility_capacity.csv", header=2)
proc_thru.head()
proc_thru.columns

# Calculate plant throughput in MMcfd
proc_thru['throughput_mmcfd'] = proc_thru['Raw Gas (e3m3/d)'] * 35.3146666721 / 1000
print("Max, min processing throughput = ", [proc_thru.throughput_mmcfd.min(), proc_thru.throughput_mmcfd.max()])

# Merge throughput df with plant location gdf, based on plant name
names_ = proc_thru['Plant']
print("Length of throughput data = {} and includes {} facilities in locational data".format(len(names_), proc_plts[proc_plts.FACILITY_NAME.isin(names_)].shape[0]))
proc_thru['FACILITY_NAME'] = proc_thru['Plant']
proc_merged = pd.merge(proc_plts, proc_thru, on='FACILITY_NAME', how='left')

# =============================================================================
# %% GATHERING AND PROCESSING - INTEGRATION
# =============================================================================
bc_proc3, errors_proc = integrate_facs(
    proc_merged,
    starting_ids=1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26, 27",
    src_date="2024-04-19",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="throughput_mmcfd",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    bc_proc3,
    file_name="canada_british_columbia_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% EQUIPMENT AND COMPONENTS
# =============================================================================
# Subset equip & components from facilities dataset
equip_ = ['Compressor Dehydrator',
          'Gas Dehydrator',
          'Pipeline Equipment']
equip = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(equip_)]
print(f"Total number of equipment data in dataset = {len(equip)}")

# Read in pipeline installations dataset, which also contains equipment records.
# Include features such as flare stacks, generators, line heaters, pumps, risers, tanks, etc.
pipe_instl = read_spatial_data("Pipeline_Installations_(Permitted).geojson")
pipe_instl.INSTALLATION_TYPE_DESC.unique()
pipe_instl = transform_CRS(pipe_instl,
                           target_epsg_code='epsg:4326',
                           appendLatLon=True)
# Remove trailing 'T00:00:00+00:00' from date fields
pipe_instl.ACTIVITY_APPROVAL_DATE = pd.to_datetime(pipe_instl.ACTIVITY_APPROVAL_DATE).dt.strftime("%Y-%m-%d")
pipe_instl.ACTIVITY_CANCEL_DATE = pd.to_datetime(pipe_instl.ACTIVITY_CANCEL_DATE).dt.strftime("%Y-%m-%d")

# The "Pipeline Installations" dataset can list multiple pieces of equip on the
# same facility, and assign them identical lat-longs. Create a unique FAC_ID
# number for every record (Project number plus installation number) to identify
# "true" duplicates from false ones
pipe_instl['fac_id'] = pipe_instl.PROJECT_NUMBER.astype(str) + '-' + pipe_instl.SEGMENT_NUMBER.astype(str) + '-' + pipe_instl.INSTALLATION_NUMBER.astype(str)

# Add source_id info to these two GDFs
equip["source_id"] = "26"
pipe_instl['source_id'] = "29"

# =============================================================================
# %% EQUIPMENT AND COMPONENTS - INTEGRATION
# =============================================================================
# Integrate equip & components from general facilities dataset
bc_equip1, bc_equip_errors = integrate_facs(
    equip,
    starting_ids=1,
    category="Equipment and components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="source_id",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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

# Integrate equip & components from pipeline installations dataset
bc_equip2, bc_equip_errors2 = integrate_facs(
    pipe_instl,
    # starting_ids=bc_equip1.OGIM_ID.iloc[-1] + 1,
    category="Equipment and components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="source_id",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name=None,
    fac_id="fac_id",
    fac_type="INSTALLATION_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status=None,
    op_name="PROPONENT",
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

# Merge the two geodataframes, then export
bc_equip_merged_ = pd.concat([bc_equip1, bc_equip2])
bc_equip_merged_.head()

save_spatial_data(
    bc_equip_merged_,
    file_name="canada_british_columbia_equipment_components",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% TERMINALS
# =============================================================================
terminals_ = ['Tank Terminal']
bc_terminals = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(terminals_)]
print(f"Total number of O&G terminals in dataset = {len(bc_terminals)}")

# Integration
bc_terms, bc_terms_errors = integrate_facs(
    bc_terminals,
    starting_ids=1,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="26",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name="FACILITY_NAME",
    fac_id="FACILITY_ID",
    fac_type="FACILITY_TYPE_DESC",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name="PROPONENT",
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
    bc_terms,
    file_name="canada_british_columbia_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% LNG - REMOVE, use GEM instead
# =============================================================================
# lng_ = ['LNG Facility']
# bc_lng = bc_fac[bc_fac.FACILITY_TYPE_DESC.isin(lng_)]
# print(f"Total number of LNG facilities in dataset = {len(bc_lng)}")

# # Integration
# bc_lng2, bc_lng_errors = integrate_facs(
#     bc_lng,
#     starting_ids=bc_terms.OGIM_ID.iloc[-1] + 1,
#     category="LNG Facility",
#     fac_alias="LNG_STORAGE",
#     country="Canada",
#     state_prov="British Columbia",
#     src_ref_id="26",
#     src_date="2023-12-08",
#     on_offshore="Onshore",
#     fac_name="FACILITY_NAME",
#     fac_id="FACILITY_ID",
#     fac_type="FACILITY_TYPE_DESC",
#     spud_date=None,
#     comp_date=None,
#     drill_type=None,
#     install_date="ACTIVITY_APPROVAL_DATE",
#     fac_status="STATUS",
#     op_name="PROPONENT",
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd=None,
#     gas_throughput_mmcfd=None,
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     bc_lng2,
#     file_name="canada_british_columbia_lng",
#     schema=schema_LNG_STORAGE,
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path=integration_out_path
# )

# =============================================================================
# %% PIPELINES
# =============================================================================
# Read in BCER pipeline segments
fp_pipes = "Pipeline_Segments_(Permitted).geojson"
bc_pipes = read_spatial_data(fp_pipes)
bc_pipes = transform_CRS(bc_pipes, target_epsg_code="epsg:4326")
bc_pipes = calculate_pipeline_length_km(bc_pipes)
# Inspect what pipeline types and statuses are present
bc_pipes.LINE_TYPE_DESC.unique()
bc_pipes.STATUS.unique()
# Remove trailing 'T00:00:00+00:00' from date fields
bc_pipes.ACTIVITY_APPROVAL_DATE = pd.to_datetime(bc_pipes.ACTIVITY_APPROVAL_DATE).dt.strftime("%Y-%m-%d")
bc_pipes.ACTIVITY_CANCEL_DATE = pd.to_datetime(bc_pipes.ACTIVITY_CANCEL_DATE).dt.strftime("%Y-%m-%d")

# Drop duplicate pipeline features (because I know there are some)
bc_pipes = bc_pipes.drop_duplicates(subset=['APPLICATION_DETERMINATION_NUM',
                                            'PROJECT_NUMBER',
                                            'ACTIVITY_APPROVAL_DATE',
                                            'STATUS',
                                            'PROPONENT',
                                            'LINE_TYPE_DESC',
                                            'PHYSICAL_PIPE_LENGTH',
                                            'geometry'],
                                    keep=False)

# Read data from CANVEC -- concatenate the 50K and 250K shapefiles
canvec_1 = read_spatial_data("canvec_data\\canvec_50K_BC_Res_MGT\\pipeline_1.shp")
canvec_1['source_id'] = "31"
canvec_2 = read_spatial_data("canvec_data\\canvec_250K_BC_Res_MGT\\pipeline_1.shp")
canvec_2['source_id'] = "32"
canvec_pipes = pd.concat([canvec_1, canvec_2]).reset_index()
canvec_pipes = transform_CRS(canvec_pipes, target_epsg_code='epsg:4326')
canvec_pipes = calculate_pipeline_length_km(canvec_pipes)

# Replace none-like commodity value with N/A
canvec_pipes.pippdt_en.replace({'Not Identified': 'N/A'}, inplace=True)

# =============================================================================
# %% PIPELINES - INTEGRATION
# =============================================================================
# Integrate BCER pipelines
bc_pipes2, errors_pipes_ = integrate_pipelines(
    bc_pipes.reset_index(),
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="33",
    src_date="2024-04-18",
    on_offshore="Onshore",
    fac_name='SEGMENT_NUMBER',
    fac_id="APPLICATION_DETERMINATION_NUM",
    fac_type="LINE_TYPE_DESC",
    install_date="ACTIVITY_APPROVAL_DATE",
    fac_status="STATUS",
    op_name='PROPONENT',
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

# Integrate Canvec pipelines
bc_pipes4, errors_pipes_4 = integrate_pipelines(
    canvec_pipes,
    starting_ids=bc_pipes2.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="source_id",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type="piprgrd_en",
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity='pippdt_en',
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km='PIPELINE_LENGTH_KM',
    pipe_material=None
)

# Merge the pipeline datasets and then export
bc_pipes_merged_ = pd.concat([bc_pipes2, bc_pipes4]).reset_index(drop=True)
bc_pipes_merged_.head()

save_spatial_data(
    bc_pipes_merged_,
    file_name="canada_british_columbia_oil_gas_pipelines",
    schema=schema_PIPELINES,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% FIELDS
# =============================================================================
bc_plays = read_spatial_data("Oil_and_Gas_Fields.geojson")
bc_plays = transform_CRS(bc_plays)
bc_plays = calculate_basin_area_km2(bc_plays, attrName="AREA_KM2")

# Replace field type "Overlap" with "Oil and gas" designation
bc_plays.FIELD_TYPE.replace({'Overlap': 'Oil and gas'},
                            inplace=True)

# Integrate
bc_fields_, bc_fields_errors = integrate_basins(
    bc_plays,
    starting_ids=1,
    category="Oil and natural gas fields",
    fac_alias="OIL_GAS_BASINS",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="35",
    src_date="2024-04-18",
    on_offshore="Onshore",
    _name="FIELD_AREA_NAME",
    reservoir_type="FIELD_TYPE",
    op_name=None,
    _area_km2="AREA_KM2"
)

save_spatial_data(
    bc_fields_,
    file_name="canada_british_columbia_oil_gas_fields",
    schema=schema_BASINS,
    schema_def=False,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% REFINERIES
# =============================================================================
# Refineries from CANVEC
ref_ = read_spatial_data("canvec_data\\canvec_250K_BC_Res_MGT\\petroleum_refinery_0.shp")
ref_ = transform_CRS(ref_, target_epsg_code="epsg:4326", appendLatLon=True)

# Capacity information is in units of 1000 m3 per day
# https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/doc/CanVec_Catalogue_50K_Res_MGT/SRDB_EXPL_ESSIM_50K_Res_MGT-sd-en.html#div-2130000
# Convert to barrels per day
ref_['capacity_bpd'] = ref_['capacity'] * 1000 * 6.2898  # https://apps.cer-rec.gc.ca/Conversion/conversion-tables.aspx?GoCTemplateCulture=fr-CA

# Integrate
bc_ref3, bc_ref_errors = integrate_facs(
    ref_,
    starting_ids=1,
    category="Crude oil refinery",
    fac_alias="REFINERY",
    country="Canada",
    state_prov="British Columbia",
    src_ref_id="32",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name="facname",
    fac_id="feature_id",
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

save_spatial_data(
    bc_ref3,
    file_name="canada_british_columbia_crude_oil_refinery",
    schema=schema_REFINERY,
    schema_def=False,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% # TODO -  PRODUCTION
# =============================================================================

# # Monthly production data from BCOGC
# fpProd = "production\\BC Total Production.xlsx"
# bc_prod_data_ = pd.read_excel(fpProd)
# bc_prod_data_.head()


# # Attributes
# bc_prod_data_.columns

# # Unique Well Authorization #s
# print("Number of unique well authorization #s = {} compared with # of records in dataset of {}".format(len(bc_prod_data_['Well Authorization Number'].unique()),bc_prod_data_.shape[0]))

# # Add columns for OIL [BBL], GAS [MCF], WATER [BBL] and COND [BBL]
# # Convert to barrels and mcf per year
# bc_prod_data_['OIL_BBL'] = bc_prod_data_['Oil Production (m3)'] * 6.2898
# bc_prod_data_['COND_BBL'] = bc_prod_data_['Condensate Production (m3)'] * 6.2898
# bc_prod_data_['WATER_BBL'] = bc_prod_data_['Water Production (m3)'] * 6.2898
# bc_prod_data_['GAS_MCF'] = bc_prod_data_['Gas Production (e3m3)'] * 35.314666721


# # Look at wells location from the permitted wells dataset
# well_loc = bc_wellsv3c
# well_loc.columns


# # Merge the data with WELL LOCATION data based on Well authorization number
# well_loc['Well Authorization Number'] = [float(well_loc['WELL_AUTHO'].iloc[x]) for x in range(well_loc.shape[0])]
# bc_prod_data_['Well Authorization Number'] = [float(bc_prod_data_['Well Authorization Number'].iloc[x]) for x in range(bc_prod_data_.shape[0])]
# # ============================================================
# merged_data_prod = pd.merge(bc_prod_data_, well_loc, on='Well Authorization Number', how='left')

# # Check if there are any null lat/lons in merged dataset
# nulls_ = merged_data_prod[merged_data_prod.longitude_calc.isnull()]
# nulls_

# merged_data_prod.columns

# merged_data_prod.OPERATION_.unique()

# # Group by Well Authorization # and sum production over the year
# # ============================================================
# aggFns7 = {
#     'UWI': 'first',
#     'Production Days': 'sum',
#     'Gas Production (e3m3)': 'sum',
#     'Oil Production (m3)': 'sum',
#     'Condensate Production (m3)': 'sum',
#     'Water Production (m3)': 'sum',
#     'Marketable Gas Volume (e3m3)': 'sum',
#     'Production Days': 'sum',
#     'OIL_BBL': 'sum',
#     'COND_BBL': 'sum',
#     'WATER_BBL': 'sum',
#     'GAS_MCF': 'sum',
#     'WELL_AUTHO': 'first',
#     'OPERATOR_A': 'first',
#     'OPERATOR_1': 'first',
#     'WELL_NAME': 'first',
#     'WELL_ACTIV': 'first',
#     'BORE_FLUID': 'first',
#     'OPERATION_': 'first',
#     'STATUS_EFF': 'first',
#     'longitude_calc': 'first',
#     'latitude_calc': 'first'
# }

# merged_prod_gp = merged_data_prod.groupby(by='Well Authorization Number').agg(aggFns7).reset_index()


# # Create GeoDataFrame
# # ============================================================
# bc_prod_data3 = gpd.GeoDataFrame(merged_prod_gp, geometry=gpd.points_from_xy(merged_prod_gp.longitude_calc, merged_prod_gp.latitude_calc), crs="epsg:4326")

# bc_prod_data3['prod_year'] = 2021

# # Create database
# bc_prod1, bc_prod_errors = integrate_production(
#     bc_prod_data3,
#     starting_ids=bc_ref3.OGIM_ID.iloc[-1]+1,
#     category="Oil and natural gas production",
#     fac_alias = "OIL_GAS_PROD",
#     country="Canada",
#     state_prov="British Columbia",
#     src_ref_id="38",
#     src_date="2022-04-09",
#     on_offshore="Onshore",
#     fac_name="WELL_NAME",
#     fac_id="UWI",
#     fac_type="OPERATION_",
#     spud_date=None,
#     comp_date=None,
#     drill_type=None,
#     fac_status=None,
#     op_name="OPERATOR_A",
#     oil_bbl="OIL_BBL",
#     gas_mcf='GAS_MCF',
#     water_bbl='WATER_BBL',
#     condensate_bbl='COND_BBL',
#     prod_days='Production Days',
#     prod_year="prod_year",
#     entity_type='Well',
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
#     )

# # Export GeoJSON
# save_spatial_data(
#     bc_prod1,
#     file_name="canada_british_columbia_oil_gas_production",
#     schema=schema_OIL_GAS_PROD,
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path="results\\"
#     )
