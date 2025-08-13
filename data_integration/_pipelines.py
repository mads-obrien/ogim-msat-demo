# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global OIL AND NATURAL GAS PIPELINES.

# TODO:
[x] standardize import statements and CWD setting
[] standardize spacing between sections
[] alphabetize countries
[] update all file paths

@authors: momara, maobrien, ahimmelberger
"""
import os
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
# import glob

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_pipelines,
                     save_spatial_data, schema_PIPELINES, read_spatial_data,
                     check_invalid_geoms, explode_multi_geoms,
                     calculate_pipeline_length_km, NULL_NUMERIC, strip_z_coord)
# from data_quality_checks import *
# from data_quality_scores import *
# from standardize_countries import *
# from assign_offshore_attribute import assign_offshore_attribute
from assign_countries_to_feature_2 import assign_countries_to_feature

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Set destination folder for exported SHP and JSON outputs
results_folder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% COUNTRY BOUNDARIES
# Load in country + maritime zone shapes, for assigning country names to features later
# Use most up-to-date, "seamless" land and marine area shapefile
# =============================================================================
os.chdir(pubdata)
path_to_boundary_geoms = r'International_data_sets\National_Maritime_Boundaries\marine_and_land_boundaries_seamless.shp'
my_boundary_geoms = gpd.read_file(path_to_boundary_geoms)

# =============================================================================
# %% USA - NATIONAL
# =============================================================================
os.chdir(v24data)
# Hydrocarbon liquids pipelines
pipes_01 = read_spatial_data(r"united_states\national\HIFLD\HGL_Pipelines.geojson")
# Natural gas pipelines
pipes_02 = read_spatial_data(r"united_states\national\HIFLD\NaturalGas_InterIntrastate_Pipelines.geojson")

# Transform CRS
pipes_01a = transform_CRS(pipes_01)
pipes_02a = transform_CRS(pipes_02)

# Check for invalid geoms
pipes_01c_list, pipes_01c_nulls = check_invalid_geoms(pipes_01a, id_attr="FID")
pipes_02c_list, pipes_02c_nulls = check_invalid_geoms(pipes_02a, id_attr="FID")

# Exclude results for pipes with Null geometries
pipes_02c_ = pipes_02a[~pipes_02a.FID.isin(pipes_02c_list)]
pipes_02c_list2, pipes_02c_nulls2 = check_invalid_geoms(pipes_02c_, id_attr="FID")

# pipes_02c_
lineStringIDs = []
multiLineStringIDs = []

for idx, row in pipes_02c_.iterrows():
    geom_ = row.geometry
    if geom_.type == "LineString":
        lineStringIDs.append(row.FID)
    elif geom_.type == "MultiLineString":
        multiLineStringIDs.append(row.FID)

# Transform geometries to LineString
data_lineStr_02c_ = pipes_02c_[pipes_02c_.FID.isin(lineStringIDs)]
data_multiLineStr_02c_ = pipes_02c_[pipes_02c_.FID.isin(multiLineStringIDs)]

data_multiLineStr_02d_ = explode_multi_geoms(data_multiLineStr_02c_)

# Then concatenate
pipes_03c = pd.concat([data_lineStr_02c_, data_multiLineStr_02d_]).reset_index()

# Check for mixed geometries
# pipes_01c_
lineStringIDs = []
multiLineStringIDs = []

for idx, row in pipes_01a.iterrows():
    geom_ = row.geometry
    if geom_.type == "LineString":
        lineStringIDs.append(row.FID)
    elif geom_.type == "MultiLineString":
        multiLineStringIDs.append(row.FID)

print(len(lineStringIDs), ", ", len(multiLineStringIDs))

# Transform geometries to LineString
data_lineStr_01a = pipes_01a[pipes_01a.FID.isin(lineStringIDs)]
data_multiLineStr_01a = pipes_01a[pipes_01a.FID.isin(multiLineStringIDs)]

data_multiLineStr_01d = explode_multi_geoms(data_multiLineStr_01a)

# Then concatenate
pipes_01c = pd.concat([data_lineStr_01a, data_multiLineStr_01d]).reset_index()

pipes_01c.head()

# Data include pipeline segments in Canada. Need to clip to US boundaries
# USA Country
os.chdir(pubdata)
country_ = read_spatial_data("North_America\\United_States_v1.2\\country\\_usa_country_boundary_wgs1984_.shp")

# Check and transform CRS
us_country = transform_CRS(country_)

# Clip pipeline data to US boundary
pipes_01d = gpd.clip(pipes_01c, us_country)
pipes_01d = explode_multi_geoms(pipes_01d)

# Clip pipeline data to US boundary
pipes_02d = gpd.clip(pipes_03c, us_country)
pipes_02d = explode_multi_geoms(pipes_02d)

# Calculate pipeline length in km
pipes_01d = calculate_pipeline_length_km(pipes_01d)
pipes_02d = calculate_pipeline_length_km(pipes_02d)

# Assign ON and OFFSHORE labels
pipes_02d['on_offshore'] = 'Onshore'

# The other pipeline dataset are therefore offshore
pipes_02e = pipes_03c[~pipes_03c.FID.isin(pipes_02d.FID)]
pipes_02e['on_offshore'] = 'Offshore'

# Concatenate
pipes_02f = pd.concat([pipes_02d, pipes_02e])

# Add commodity field
pipes_01d['commodity'] = 'Hydrocarbon gas liquid'
pipes_02f['commodity'] = 'Natural gas'

# =============================================================================
# %% USA - NATIONAL - Integration
# =============================================================================
_pipes_us, _pipes_us_list = integrate_pipelines(
    pipes_01d.reset_index(),
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="United States",
    state_prov=None,
    src_ref_id="95",
    src_date="2022-01-12",
    on_offshore="Onshore",
    fac_name='Pipename',
    fac_id="GlobalID",
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name="Opername",
    commodity="commodity",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

_pipes_us2, _pipes_us_list2 = integrate_pipelines(
    pipes_02f.reset_index(),
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="United States",
    state_prov=None,
    src_ref_id="96",
    src_date="2021-12-14",
    on_offshore="on_offshore",
    fac_name=None,
    fac_id="FID",
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name="Operator",
    commodity="commodity",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

# =============================================================================
# %% USA - TEXAS
# =============================================================================
os.chdir(v24data)
# List all files in the directory, then read in the county-specific pipelines.
# Will take 5-ish minutes to read and concatenate them all.
fp = 'united_states\\texas\\pipelines\\'
files = os.listdir(fp)

# Empty dictionary to hold county-specific gdfs
p = {}

for file in tqdm(files):
    fips = file[4:7]
    if file.endswith('.shp'):
        p[fips] = gpd.read_file(fp + file)

# Concatenate all county-specific locations into one state-wide gdf
tex_pipes = pd.concat(p.values())
tex_pipes = tex_pipes.reset_index(drop=True)

tex_pipes = transform_CRS(tex_pipes,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# User guide containing attribute definitions
# https://www.rrc.texas.gov/media/kmld3uzj/digital-map-information-user-guide.pdf

# DIAMETER: convert millimeters to inches
tex_pipes["diameter_inch"] = tex_pipes["DIAMETER"] * 25.4

commodity_dict = {
    'AA': 'ANYHDROUS AMMONIA, TRANSMISSION',
    'CO2': 'CARBON DIOXIDE, TRANSMISSION',
    'CRO': 'CRUDE OIL, TRANSMISSION',
    'CRL': 'CRUDE OIL, GATHERING',
    'CFL': 'CRUDE OIL, FULL WELL STREAM GATHERING',
    'CRA': 'CRUDE OIL, OFFSHORE GATHERING',
    'HVL': 'HIGHLY VOLATILE LIQUID, TRANSMISSION',
    'PRD': 'REFINED LIQUID PRODUCT, TRANSMISSION',
    'NGT': 'NATURAL GAS, TRANSMISSION',
    'NGG': 'NATURAL GAS, GATHERING',
    'NFG': 'NATURAL GAS, FULL WELL STREAM GATHERING',
    'NGZ': 'NATURAL GAS, OFFSHORE GATHERING',
    'NG': 'NATURAL GAS',
    'OGT': 'OTHER GAS, TRANSMISSION'
}
tex_pipes["commodity"] = tex_pipes["COMMODITY1"].replace(commodity_dict)

# Status
status_dict = {
    'I': 'IN SERVICE',
    'B': 'ABANDONED',
    'R': 'IN REPAIR'  # Not defined in TX RRC User Guide
}
tex_pipes["status"] = tex_pipes["STATUS_CD"].replace(status_dict)

# Facility type
fac_type_dict = {
    'A': 'CRUDE OIL OFFSHORE',
    'C': 'ANHYDROUS AMMONIA',
    'G': 'GAS GATHERING',
    'K': 'CARBON DIOXIDE',
    'L': 'CRUDE OIL GATHERING',
    'O': 'CRUDE OIL TRANSMISSION',
    'P': 'REFINED LIQUID PRODUCT',
    'Q': 'HIGHLY VOLATILE LIQUID PRODUCT',
    'T': 'GAS TRANSMISSION',
    'Z': 'GAS OFFSHORE',
    'D': 'N/A',  # Not defined in TX RRC User Guide
    None: 'N/A'
}

tex_pipes["fac_type"] = tex_pipes["SYSTYPE"].replace(fac_type_dict)

pipes_tex = calculate_pipeline_length_km(tex_pipes)

pipes_tex["state_prov"] = "TEXAS"

# Deduplicate features
# dupes = pipes_tex[pipes_tex.duplicated(subset=['geometry'], keep=False)]
pipes_tex = pipes_tex.drop_duplicates(subset=['P5_NUM',
                                              'OPER_NM',
                                              'SYS_NM',
                                              'SUBSYS_NM',
                                              'T4PERMIT',
                                              'DIAMETER',
                                              'PLINE_ID',
                                              'status',
                                              'fac_type',
                                              'geometry'], keep='first')

# =============================================================================
# %% USA - TEXAS - Integration
# =============================================================================
_pipes_us3, _pipes_us_list3 = integrate_pipelines(
    pipes_tex.reset_index(),
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="United States",
    state_prov="state_prov",
    src_ref_id="197",
    src_date="2024-04-19",  # Weekly
    on_offshore=None,
    fac_name="SYS_NM",
    fac_id="TPMS_ID",
    fac_type="fac_type",
    install_date=None,
    fac_status="status",
    op_name="OPER_NM",
    commodity="commodity",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm="diameter_inch",
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

# =============================================================================
# %% USA - Combine and export all
# =============================================================================
# Concatenate the two national datasets with the Texas dataset
pipes_us_all = pd.concat([_pipes_us, _pipes_us2, _pipes_us3])

# Preview plot
# pipes_us_all.plot()

# Save results
save_spatial_data(
    pipes_us_all,
    "united_states_oil_natural_gas_pipelines",
    schema_def=False,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% VENEZUELA
# =============================================================================
os.chdir(v24data)
# pipes_data = read_spatial_data(r'venezuela/DUCTOS_FPO_F.geojson')
pipes_data = read_spatial_data(r'venezuela/DUCTOS_F_CarlosGonzales.shp')
pipes_data = transform_CRS(pipes_data, appendLatLon=False)
pipes_data = calculate_pipeline_length_km(pipes_data)

dicts_ = {
    'OLEODUCTO': 'OIL PIPELINE',
    'POLIDUCTO': 'OIL PIPELINE',
    'ORIMULSION': 'ORIMULSION PIPELINE',
    'DILUENDUCTO': 'DILUENT PIPELINE',
    'OLEODUCTO (PROYECTO A FUTURO)': 'OIL PIPELINE (PROPOSED)',
    'GASODUCTO (OPERATIVO)': 'GAS PIPELINE (OPERATING)',
    'GASODUCTO (REHABILITACION)': 'GAS PIPELINE (REHABILITATION)',
    'GASODUCTO (FUTURO)': 'GAS PIPELINE (PROPOSED)'
}

pipes4 = replace_row_names(pipes_data, colName="TIPO", dict_names=dicts_)

# Commodity
pipes5 = replace_row_names(pipes4,
                           colName="CARACTERIS",
                           dict_names={'PETROLEO': 'OIL', 'GAS': 'GAS'})

# Create status field based on descriptions in TIPO
pipes5['status'] = 'N/A'
pipes5.loc[pipes5.TIPO.str.contains('PROPOSED'), 'status'] = 'PROPOSED'
pipes5.loc[pipes5.TIPO.str.contains('OPERATING'), 'status'] = 'OPERATING'
pipes5.loc[pipes5.TIPO.str.contains('REHABILITATION'), 'status'] = 'REHABILITATION'

# Remove redundant values from TIPO field that I've already ported into
# the commodity field or status field
values2drop = ['OIL PIPELINE',
               'OIL PIPELINE (PROPOSED)',
               'GAS PIPELINE (OPERATING)',
               'GAS PIPELINE (REHABILITATION)',
               'GAS PIPELINE (PROPOSED)'
               ]

pipes5.loc[pipes5.TIPO.isin(values2drop), 'TIPO'] = 'N/A'


# =============================================================================
# %% VENEZUELA - Integration + Export
# =============================================================================
pipes6, err6 = integrate_pipelines(
    pipes5,
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Venezuela",
    state_prov=None,
    src_ref_id="261",
    src_date="2017-01-01",
    on_offshore=None,
    fac_name=None,
    fac_id=None,
    fac_type="TIPO",
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity="CARACTERIS",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

save_spatial_data(
    pipes6,
    "venezuela_oil_gas_pipelines",
    schema_def=False,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% ARGENTINA
# See this pdf for metadata, particularly page 16 for attribute definitions
# https://apps.energia.gob.ar/upload/planosb/instructivo/instructivo_319.pdf
# =============================================================================
os.chdir(v24data)
# Read GATHERING AND FLOW PIPELINES
# -----------------------------------------------------------------------------
fp_gather = r"argentina\pipelines\instalaciones-hidrocarburos-ductos-res-319-93-shp.shp"
arg_pipe_gather = read_spatial_data(fp_gather, specify_encoding=True, data_encoding="utf-8")
arg_pipe_gather = transform_CRS(arg_pipe_gather, appendLatLon=False)
arg_pipe_gather = calculate_pipeline_length_km(arg_pipe_gather)

# Translate fac type, and add additional information that these are gathering lines
dict_types_ = {
    'GASODUCTO': 'GAS GATHERING PIPELINE',
    'OLEODUCTO': 'OIL GATHERING PIPELINE',
    'OTROS': 'OTHER GATHERING PIPELINE',
    'ACUEDUCTO': 'WATER GATHERING PIPELINE',
    'POLIDUCTO': 'POLYDUCT',
    None: 'N/A'
}

arg_pipe_gather = replace_row_names(arg_pipe_gather,
                                    colName='TIPO',
                                    dict_names=dict_types_)

# Replace / standardize all the missing value strings
arg_pipe_gather.replace({'Sin Datos': np.nan,
                         'SIN DATOS': np.nan,
                         'sin datos': np.nan,
                         'Sin Dato': np.nan,
                         'SIN DATO': np.nan,
                         'sin dato': np.nan,
                         'S/D': np.nan,
                         's/d': np.nan,
                         '#N/A': np.nan}, inplace=True)

# Create new "commodity" field, informed by commodity info in TIPO field
arg_pipe_gather['commodity'] = 'N/A'
arg_pipe_gather.loc[arg_pipe_gather.TIPO == 'GAS PIPELINE', 'commodity'] = 'GAS'
arg_pipe_gather.loc[arg_pipe_gather.TIPO == 'OIL PIPELINE', 'commodity'] = 'OIL'
arg_pipe_gather.loc[arg_pipe_gather.TIPO == 'WATER PIPELINE', 'commodity'] = 'WATER'

# Translate pipeline materials to English
dict_mat = {
    'ACERO': 'STEEL',
    'ACERO REVESTIDO': 'Coated steel',
    'ACERO ROSCADO': 'Threaded steel',
    'PRFV': 'PRFV',
    'ERFV': 'ERFV',
    'Acero': 'Steel',
    'Acero ASTM A53 Grado A-Revestido': 'ASTM A53 Steel A-Revestite',
    'B': 'B',
    'B/X52': 'B/X52',
    'X42': 'X42',
    'X 52': 'X 52',
    'X60': 'X60',
    'Acero ASTM A53 grado B': 'ASTM A53 grade B steel',
    'acero': 'steel',
    'aereo': 'aerial',
    'POLIETILENO DE ALTA DENSIDAD': 'HIGH DENSITY POLYETHYLENE',
    'J55': 'J55',
    'ASTM A53 Gr B': 'ASTM A53 GR B',
    'API 5L Gr B': 'API 5L GR B',
    'API 5CT': 'API 5CT',
    'API 5L Gr X42': 'API 5L GR X42',
    'API 5L X42': 'API 5L X42',
    'Acero al carbono API 5L/ASTM A53 Grado B': 'Carbon steel 5L/ASTM A53 Grade B',
    'ACERO y ERFV': 'Steel and ERFV',
    'ACERO API 5L Gr.B': 'API 5L GR.B Steel',
    'ASTM A53-70': 'ASTM A53-70',
    'ASTM-A-53-70': 'ASTM-A-53-70',
    'ACERO API 5L X-52': 'API Steel 5L X-52',
    'ACERO API 5LX-242 CAÂ¥ERIA CON COSTURA': 'API 5LX-242 Caâ ¥ ERIA',
    'ACERO API 5L Gr X-56': 'API 5L GR X-56 Steel',
    'Acero ASTM A53GR': 'ASTM A53GR steel',
    'Acero ASTM A53 A 106 GR AB': 'ASTM A53 A 106 GR AB',
    'Acero ASTM A53GR B': 'ASTM A53GR B Steel',
    'Acero ASTM A53GR AB': 'ASTM A53GR AB STEEL',
    'ERFV S 600': 'ERFV S 600',
    'ERFV S 800': 'ERFV S 800',
    'Polietileno': 'Polyethylene',
    'Acero ASTM A53 Gr B': 'ASTM A53 GR B Steel',
    'TUBING': 'TUBING',
    'PLASTICO': 'PLASTIC',
    'Fibra': 'Fiber',
    'Manguera': 'Hose',
    'ASTM A-106 Gr.B': 'ASTM A-106 GR.B',
    'ASTM A53/106': 'ASTM A53/106',
    'ASTM A53 Gr A': 'ASTM A53 GR A',
    'ASTM A106 Gr B': 'ASTM A106 GR B',
    'Acero al Carbono': 'Carbon Steel',
    'Acero Carbono': 'Carbon steel',
    'API J55 - 9.2 lb/feet': 'API J55 - 9.2 lb/feet',
    'API 5L GR B': 'API 5L GR B',
    'API 5L X65': 'API 5L X65',
    'API 5L  SCH40': 'API 5L Sch40',
    'API 5L GR X52': 'API 5L GR X52',
    'API 5L GR X42': 'API 5L GR X42',
    'API 5L GR X56': 'API 5L GR X56',
    'API 5L SCH80': 'API 5L SCH80',
    'ERFV AMINA': 'ERFV Amina',
    'API 5L GR X60': 'API 5L GR X60',
    'API 5LX  X-42': 'API 5LX X-42',
    'API 5LX X-52': 'API 5LX X-52',
    'ERFV API 15HR': 'ERFV API 15HR',
    'CS ASTM A 106 GR. B': 'CS ASTM at 106 gr.B',
    'API 5L GRADO X52': 'API 5L Grade X52',
    'PAD': 'PAD',
    'ACERO ASTM-A53\n': 'ASTM-A53 steel',
    'PLASTICO ERFV': 'ERFV plastic',
    'API 5L': 'API 5L',
    'ACERO API 5L X 46': 'API steel 5l x 46',
    'ACERO ASTM-A53': 'ASTM-A53 steel',
    'PEAD 250': 'Pead 250',
    'PE': 'Pe',
    'Galvanizado': 'Galvanized',
    'erfv': 'ERFV',
    'ERFV\r\n': 'ERFV',
    'OTROS(DETALLAR)': 'N/A',
    'API 5L Gr X52': 'API 5L GR X52',
    'PEAD': 'Pead',
    'prfv': 'PRFV',
    'polietileno': 'polyethylene',
    'Acero al carbono API 5L Grado X 65': 'Carbon steel 5L grade x 65',
    'Acero al carbono API 5L / ASTM A 53 Grado B': 'Carbon steel 5L / ASTM at 53 grade B',
    'Acero al carbono API 5L Grado X 52': 'Carbon steel 5L grade x 52',
    'Acero al carbono API 5L Grado X 42': 'Carbon steel 5L grade x 42',
    'Acero al carbono API 5L Grado X65': 'Carbon steel 5L grade x65',
    'Acero al carbono API 5L Grado X70': 'Carbon steel 5L grade x70',
    'Acero Recuperado': 'Recovered steel',
    'Acero grado X-56 - Norma API 5L': 'Steel X -56 - API 5L standard',
    'Acero Nuevo': 'New steel',
    'Api5I GradoA': 'API5I degrees',
    'Aldyl': 'Aldyl',
    'ACERO al carbono API 5L X-52': 'Carbon steel 5L x-52',
    'TUBING J55 Grado 4': 'TUBING J55 Grade 4',
    'API 5L GR X-42': 'API 5L GR X-42',
    'Acero SC J-55': 'SC J-55 steel',
    'SIN DEFINIR': 'UNDEFINED',
    'API 5L SCH160': 'API 5L Sch160',
    'Fiberglass': 'Fiberglass',
    'Schedule 40 Carbon-Steel': 'Schedule 40 Carbon-Steel',
    'Acero API5L 80': 'API5L 80 steel',
    'Acero al carbono ASTM A 53 Grado B': 'ASTM to 53 grade B carbon steel',
    'ERFV (Centron #2000)': 'ERFV (Centron #2000)',
    'Otros': 'N/A',
    'Polietileno de alta densidad': 'High density polyethylene',
    'API 5L J55': 'API 5L J55',
    'AISI 316': 'AISI 316',
    'API 5L N80': 'API 5L N80',
    'Casing J55': 'Casing J55',
    'API 5L A25': 'API 5L A25',
    'API 5L Gr X56': 'API 5L GR X56',
    'API5L Gr B': 'API5L gr b',
    'Acero API5LB': 'API5LB steel',
    'Aldil': 'Aldil',
    'Acero API 5L B': 'API 5L B steel',
    'ERFV-Acero Carbono': 'ERFV-carbonacero',
    'Acero X42': 'X42 steel',
    'API 5L, X65': 'API 5L, X65',
    'API 5L X70.': 'API 5L X70.',
    'API 5L X56': 'API 5L X56',
    'X56': 'X56',
    'GR B': 'Gr b',
    'API 5L, X56': 'API 5L, X56',
    'GRB': 'GRB',
    'ERFV (Repsa)': 'ERFV (REPSA)',
    'API 5L GR X65': 'API 5L GR X65',
    'sin dato': 'N/A',
    'OTRO': 'OTHER',
    'API 5L X52': 'API 5L X52',
    'API 5L GrB': 'API 5L GRB',
    'PLASTICO (PEAD)': 'Plastic (Pead)'
}

arg_pipe_gather = replace_row_names(arg_pipe_gather,
                                    colName="MATERIAL",
                                    dict_names=dict_mat)

# Handle all variations of null values in DIAMETRO column
arg_pipe_gather.DIAMETRO.replace({0: np.nan,
                                  9999: np.nan,
                                  999: np.nan}, inplace=True)

# Pipeline diameter is provided in inches; convert to millimeters
arg_pipe_gather['diameter_mm'] = arg_pipe_gather.DIAMETRO * 25.4

# Quite a few duplicate records for gathering pipelines exist -- remove them.
# Sort the gathering pipeline records from newest "ALTA_PLANO" date to lowest.
# If a record has duplicate information in the fields we care about, then keep
# the newest / most recent copy of that record.

dupes = arg_pipe_gather[arg_pipe_gather.duplicated(subset=['geometry'], keep=False)]

arg_pipe_gather_newestfirst = arg_pipe_gather.sort_values(by=['ALTA_PLANO', 'MODIFICACI'],
                                                          ascending=[False, False],
                                                          na_position='last')

arg_pipe_gather = arg_pipe_gather_newestfirst.drop_duplicates(subset=['DU',
                                                                      'TRDU',
                                                                      'EMPRESA_IN',
                                                                      'TIPO',
                                                                      'TIPO_TRAMO',
                                                                      'DIAMETRO',
                                                                      'PROF',
                                                                      'ESPESOR',
                                                                      'MATERIAL',
                                                                      'REVESTIM',
                                                                      'geometry'],
                                                              keep='first').reset_index(drop=True)

# In some cases, all attributes except operator are the same -- drop these
# duplicate pipeline segments based on keeping the newest one
arg_pipe_gather = arg_pipe_gather.drop_duplicates(subset=['DU',
                                                          'TRDU',
                                                          # 'EMPRESA_IN',
                                                          'TIPO',
                                                          'TIPO_TRAMO',
                                                          'DIAMETRO',
                                                          'PROF',
                                                          'ESPESOR',
                                                          'MATERIAL',
                                                          'REVESTIM',
                                                          'geometry'],
                                                  keep='first').reset_index(drop=True)

# Same thing, but sometimes "less important" fields like ESPESOR or PROF or
# MATERIAL or DIAMETRO are different... ignore these
arg_pipe_gather = arg_pipe_gather.drop_duplicates(subset=['DU',
                                                          'TRDU',
                                                          # 'EMPRESA_IN',
                                                          'TIPO',
                                                          'TIPO_TRAMO',
                                                          # 'DIAMETRO',
                                                          # 'PROF',
                                                          # 'ESPESOR',
                                                          # 'MATERIAL',
                                                          'REVESTIM',
                                                          'geometry'],
                                                  keep='first').reset_index(drop=True)

# In cases where TRDU (the pipeline system) oddly has a different
# name between two records that are otherwise identical where it matters, keep
# the newest copy of the record
arg_pipe_gather = arg_pipe_gather.drop_duplicates(subset=['DU',
                                                          # 'TRDU',
                                                          # 'EMPRESA_IN',
                                                          'TIPO',
                                                          'TIPO_TRAMO',
                                                          'DIAMETRO',
                                                          'PROF',
                                                          'ESPESOR',
                                                          'MATERIAL',
                                                          'REVESTIM',
                                                          'geometry'],
                                                  keep='first').reset_index(drop=True)

# Remove more of the remaining duplicates, based on ignoring very minor fields
# that we don't care about in OGIM
arg_pipe_gather = arg_pipe_gather.drop_duplicates(subset=['DU',
                                                          # 'TRDU',
                                                          # 'EMPRESA_IN',
                                                          'TIPO',
                                                          'TIPO_TRAMO',
                                                          'DIAMETRO',
                                                          # 'PROF',
                                                          # 'ESPESOR',
                                                          'MATERIAL',
                                                          # 'REVESTIM',
                                                          'geometry'],
                                                  keep='first').reset_index(drop=True)

# Read in TRANSPORTATION GAS PIPELINES
# -----------------------------------------------------------------------------
fp_trans = r"argentina\pipelines\gasoductos-de-transporte-enargas--shp.shp"
arg_pipe_trans = read_spatial_data(fp_trans, specify_encoding=True, data_encoding="utf-8")
arg_pipe_trans = transform_CRS(arg_pipe_trans, appendLatLon=False)
arg_pipe_trans = calculate_pipeline_length_km(arg_pipe_trans)
arg_pipe_trans['commodity'] = 'Gas'

# Translate SUBTIPO_DE column, and add additional information that these
# are transport lines
arg_pipe_trans['factype'] = arg_pipe_trans.SUBTIPO_DE.replace(
    {'Troncal': 'Transport pipeline - Trunkline',
     'Loop': 'Transport pipeline - Loop',
     'Paralelo': 'Transport pipeline - Parallel',
     'Sin determinar': 'Transport pipeline',
     'Proyecto': 'Transport pipeline - Project'})


# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Integrate gathering pipelines
arg_pipe_gather_integrated, gath_err = integrate_pipelines(
    arg_pipe_gather,
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Argentina",
    # state_prov=None,
    src_ref_id="115",
    src_date="2024-04-01",
    # on_offshore=None,
    fac_name="DU",
    # fac_id=None,
    fac_type="TIPO",
    # install_date=None,
    # fac_status=None,
    op_name="EMPRESA_IN",
    commodity='commodity',
    # liq_capacity_bpd= None,
    # liq_throughput_bpd= None,
    # gas_capacity_mmcfd= None,
    # gas_throughput_mmcfd= None,
    pipe_diameter_mm="diameter_mm",
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material="MATERIAL"
)

# Integrate transport gas pipelines
arg_pipe_trans_integrated, trans_err = integrate_pipelines(
    arg_pipe_trans,
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Argentina",
    # state_prov=None,
    src_ref_id="116",
    src_date="2023-06-29",
    # on_offshore=None,
    fac_name="NOMBRE",
    # fac_id=None,
    fac_type="factype",
    # install_date=None,
    # fac_status=None,
    op_name="EMPRESA_LI",
    commodity='commodity',
    # liq_capacity_bpd=None,
    # liq_throughput_bpd=None,
    # gas_capacity_mmcfd=None,
    # gas_throughput_mmcfd=None,
    # pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material=None
)

# Concatenate and save as one dataset for pipelines
pipes_arg = pd.concat([arg_pipe_gather_integrated,
                       arg_pipe_trans_integrated]).reset_index(drop=True)

save_spatial_data(
    pipes_arg,
    "argentina_oil_natural_gas_pipelines",
    schema_def=True,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# # %% BRAZIL
# # =============================================================================
# os.chdir(pubdata)
# pipes_ = read_spatial_data("South_America\\Brazil_v1.2\\pipelines\\Gasodutos_Transporte_15062021.shp")

# # Check and transform CRS
# pipes2 = transform_CRS(pipes_, appendLatLon=False)

# # Calculate pipeline length
# pipes3 = calculate_pipeline_length_km(pipes2)

# pipes4 = replace_row_names(pipes3, colName="FLUIDO", dict_names={'Gás': 'GAS'})

# pipes5 = replace_row_names(pipes4, colName="TIPO_AUT", dict_names={None: 'N/A', 'Operação': 'OPERATIONAL'})

# # Change capacity to MMcfd
# m3_to_ft3 = 35.314666721
# pipes5['gas_mmcfd'] = pipes5['CAP_Mm3d'] * m3_to_ft3 / 1000

# # =============================================================================
# # %% BRAZIL - Integration + Export
# # =============================================================================
# pipes6, err6 = integrate_pipelines(
#     pipes5,
#     starting_ids=1,
#     category="Oil and natural gas pipelines",
#     fac_alias="PIPELINES",
#     country="Brazil",
#     state_prov=None,
#     src_ref_id="126",
#     src_date="2022-05-15",
#     on_offshore=None,
#     fac_name="GASODUTO",
#     fac_id="ID",
#     fac_type=None,
#     install_date=None,
#     fac_status="TIPO_AUT",
#     op_name="OPERADOR",
#     commodity="FLUIDO",
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd="gas_mmcfd",
#     gas_throughput_mmcfd=None,
#     pipe_diameter_mm=None,
#     pipe_length_km="EXTENS_KM",
#     pipe_material=None
# )

# save_spatial_data(
#     pipes6,
#     "brazil_oil_natural_gas_pipelines",
#     schema_def=False,
#     schema=schema_PIPELINES,
#     file_type="GeoJSON",
#     out_path=results_folder
# )

# =============================================================================
# %% BOLIVIA
# =============================================================================
os.chdir(v24data)
fp2 = r'bolivia\geobolivia\ductos.shp'
bol_pipes = gpd.read_file(fp2, encoding='utf-8')
bol_pipes = bol_pipes[['nombre',
                       'createddat',
                       'operationa',
                       'stationser',
                       'linedescri',
                       'product',
                       'geometry']]
bol_pipes = transform_CRS(bol_pipes, appendLatLon=False)
bol_pipes = calculate_pipeline_length_km(bol_pipes)

# Populate the mostly empty "nombre" column
bol_pipes.loc[bol_pipes.nombre.isna(), 'nombre'] = bol_pipes.linedescri

# Revise some of the translations in the 'product' column which describes the pipeline type
# bol_pipes['product'].value_counts()
bol_pipes['product'].replace({'Oil Export exterior': 'Oil Export'}, inplace=True)

# Create a commodity column
bol_pipes['commodity'] = bol_pipes['product']
bol_pipes.commodity = bol_pipes.commodity.str.replace(' Domestic', '')
bol_pipes.commodity = bol_pipes.commodity.str.replace(' Export', '')

# =============================================================================
# %% BOLIVIA - Integration + Export
# =============================================================================
bol_pipes_integrated, err5 = integrate_pipelines(
    bol_pipes,
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Bolivia",
    # state_prov=None,
    src_ref_id="124",
    src_date="2016-11-23",
    on_offshore='ONSHORE',
    fac_name="nombre",
    # fac_id=None,
    fac_type="product",
    # install_date=None,
    fac_status='operationa',
    # op_name=None,
    commodity="commodity",
    # liq_capacity_bpd= None,
    # liq_throughput_bpd= None,
    # gas_capacity_mmcfd= None,
    # gas_throughput_mmcfd= None,
    # pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material=None
)

# Remove z values from geometry
bol_pipes_integrated = strip_z_coord(bol_pipes_integrated)

save_spatial_data(
    bol_pipes_integrated,
    "bolivia_oil_natural_gas_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% SOUTH AMERICA - Other pipeline datasets
# =============================================================================
os.chdir(pubdata)
# Read data
pipe_ = read_spatial_data("South_America\\international_pipelines\\_south_america_other_pipelines_.shp")

# Check and transform CRS
pipe2 = transform_CRS(pipe_, appendLatLon=True)

# Calculate pipeline length in km
pipe4 = calculate_pipeline_length_km(pipe2)

pipes5, pipe_err = integrate_pipelines(
    pipe4,
    starting_ids=1,
    category="OIL AND NATURAL GAS PIPELINES",
    fac_alias="PIPELINES",
    country="Country",
    state_prov=None,
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore=None,
    fac_name=None,
    fac_id=None,
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None
)

save_spatial_data(
    pipes5,
    "south_america_other_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AFRICA - ArcGIS Online and USGS
# =============================================================================
os.chdir(pubdata)

usgs_pipes = read_spatial_data(r"Africa\Continent\Africa_GIS.gdb\AFR_Infra_OG_Pipelines.shp")
libya_pipes = read_spatial_data(r"Africa\Libya\Libya_AGO_kmitchell\Libya_Pipelines.shp")
# Remove Nigeria pipelines from SRC_ID 159, no longer public
# nigeria_pipes = read_spatial_data(r'Africa\Nigeria\Nigeria\Gas_Pipeline.shp')

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
usgs_pipes = transform_CRS(usgs_pipes, target_epsg_code="epsg:4326", appendLatLon=True)
# nigeria_pipes = transform_CRS(nigeria_pipes, target_epsg_code="epsg:4326", appendLatLon=True)
libya_pipes = transform_CRS(libya_pipes, target_epsg_code="epsg:4326", appendLatLon=True)

# Calculate the length of each polyline, and add the results in a new column
usgs_pipes = calculate_pipeline_length_km(usgs_pipes, attrName="PIPELINE_LENGTH_KM")
# nigeria_pipes = calculate_pipeline_length_km(nigeria_pipes, attrName="PIPELINE_LENGTH_KM")
libya_pipes = calculate_pipeline_length_km(libya_pipes, attrName="PIPELINE_LENGTH_KM")


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Move pipeline diameter information into its own field, marking NAN values properly
# nigeria_pipes['diam_inches'] = nigeria_pipes['Name'].str.strip(' "')
# nigeria_pipes.loc[nigeria_pipes.diam_inches == 'Placemark', 'diam_inches'] = None
# # Convert inches to millimeters
# nigeria_pipes['diam_mm'] = nigeria_pipes['diam_inches'].astype('float') * 25.4   # 1 in = 25.4 mm
# # Fill in N/A values
# nigeria_pipes['diam_mm'] = nigeria_pipes['diam_mm'].fillna(NULL_NUMERIC)

# Convert cm diameter to mm
libya_pipes['dimaeter_mm'] = libya_pipes['DIAMETER_C'] * 10


# Assign countries to pipelines
libya_pipes_with_countries = assign_countries_to_feature(libya_pipes,
                                                         # gdf_country_colname='COUNTRY',
                                                         gdf_uniqueid_field='OBJECTID',
                                                         boundary_geoms=my_boundary_geoms,
                                                         overwrite_country_field=True)


# For some reason, function below won't run properly if 'Shape_Leng' field is present...
# Debug this later, but for now just delete the field
usgs_pipes = usgs_pipes.drop('Shape_Leng', axis=1)

# Assign countries to pipelines
usgs_pipes_with_countries = assign_countries_to_feature(usgs_pipes,
                                                        # gdf_country_colname='COUNTRY',
                                                        gdf_uniqueid_field='OBJECTID',
                                                        boundary_geoms=my_boundary_geoms,
                                                        overwrite_country_field=True)

# Remove pipes from the USGS dataset that are purely in Libya, because we already have
# a satisfactory pipeline dataset from the AGO source
usgs_pipes_with_countries = usgs_pipes_with_countries.query("COUNTRY != 'Libya'").reset_index()

# ========================================================================
# %% AFRICA - ArcGIS Online and USGS - Integration + Export
# ========================================================================
# nigeria_pipes_, errors = integrate_pipelines(
#     nigeria_pipes,
#     starting_ids=0,
#     category='Oil and natural gas pipelines',
#     fac_alias="PIPELINES",
#     country='Nigeria',
#     # state_prov = None,
#     src_ref_id='159',
#     src_date='2020-11-01',
#     on_offshore='Onshore',
#     # fac_name = None,
#     # fac_id = None,
#     # fac_type = None,
#     # install_date = None,
#     # fac_status = None,
#     # op_name = None,
#     # commodity = None,
#     # liq_capacity_bpd = None,
#     # liq_throughput_bpd = None,
#     # gas_capacity_mmcfd = None,
#     # gas_throughput_mmcfd = None,
#     pipe_diameter_mm='diam_mm',
#     pipe_length_km='PIPELINE_LENGTH_KM',
#     # pipe_material = None
# )

libya_pipes_with_countries_, errors = integrate_pipelines(
    libya_pipes_with_countries,
    starting_ids=0,
    category='Oil and natural gas pipelines',
    fac_alias="PIPELINES",
    country='COUNTRY',
    # state_prov = None,
    src_ref_id='166',
    src_date='2017-06-01',
    on_offshore='ONS_OFFSHO',
    fac_name='PIPELINE_N',
    # fac_id = None,
    # fac_type = None,
    install_date='DATE_ONLIN',
    fac_status='STATUS',
    op_name='OPERATOR_N',
    commodity='CONTENT',
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    pipe_diameter_mm='dimaeter_mm',
    pipe_length_km='PIPELINE_LENGTH_KM',
    # pipe_material = None
)

usgs_pipes_with_countries_, errors = integrate_pipelines(
    usgs_pipes_with_countries,
    starting_ids=0,
    category='Oil and natural gas pipelines',
    fac_alias="PIPELINES",
    country='COUNTRY',
    # state_prov = None,
    src_ref_id='158',
    src_date='2021-08-01',
    # on_offshore = None,
    # fac_name = None,
    fac_id='FeatureUID',
    # fac_type = None,
    # install_date = None,
    # fac_status = None,
    # op_name = None,
    commodity='DsgAttr01',
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    # pipe_diameter_mm = None,
    pipe_length_km='PIPELINE_LENGTH_KM',
    # pipe_material = None
)

# Export data to GeoJSON
# -----------------------------------------------------------------------------
# LIBYA
save_spatial_data(
    libya_pipes_with_countries_,
    file_name="libya_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# # NIGERIA
# save_spatial_data(
#     nigeria_pipes_,
#     file_name="nigeria_pipelines",
#     schema_def=False,  # to accomodate multilinestring type
#     schema=schema_PIPELINES,
#     file_type="GeoJSON",
#     out_path=results_folder)


# USGS / CONTINENTAL
save_spatial_data(
    usgs_pipes_with_countries_,
    file_name="africa_usgs_pipelines",
    schema_def=False,  # to accomodate multilinestring type
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% AUSTRALIA
# =============================================================================
os.chdir(pubdata)
fp1 = "Australia+NewZealand\\Australia\\GAS_pipeline_Australia.kml.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
pipelines1['COMMODITY'] = 'GAS'
pipelines11 = calculate_pipeline_length_km(pipelines1, attrName="PIPELINE_LENGTH_KM")
fp2 = "Australia+NewZealand\\Australia\\OIL_pipeline_Australia.kml.shp"
pipelines2 = read_spatial_data(fp2, table_gradient=True)
pipelines2['COMMODITY'] = 'OIL'
pipelines22 = calculate_pipeline_length_km(pipelines2, attrName="PIPELINE_LENGTH_KM")
fp3 = "Australia+NewZealand\\Australia\\CONDENSATE_pipeline_Australia.kml.shp"
pipelines3 = read_spatial_data(fp3, table_gradient=True)
pipelines3['COMMODITY'] = 'CONDENSATE'
pipelines33 = calculate_pipeline_length_km(pipelines3, attrName="PIPELINE_LENGTH_KM")

pipelines_concat = gpd.GeoDataFrame(pd.concat([pipelines11, pipelines22, pipelines33], ignore_index=True))
pipelines_concat_1 = pipelines_concat.set_crs(4326)

# =============================================================================
# %% AUSTRALIA - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat_1,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="Australia",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    # on_offshore="Offshore",
    # fac_name="NAMES",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "STATUS",
    # op_name = "",
    commodity="COMMODITY",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)
# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)


save_spatial_data(
    pipelines_final_no_z,
    file_name="australia_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% AUSTRALIA - Queensland
# =============================================================================
os.chdir(v24data)
fp = r'australia\queensland\Petroleum_pipeline_licences\data.gdb'
queen_pipes = gpd.read_file(fp,
                            layer='Petroleum_pipeline_licences',
                            driver='FileGDB')
queen_pipes = transform_CRS(queen_pipes,
                            target_epsg_code="epsg:4326",
                            appendLatLon=True)

# Un-abbreviate the permit purpose, and permit minerals, that I know the meaning of
queen_pipes['typenew'] = queen_pipes.permitpurpose
queen_pipes.typenew.replace({'PETPIP': 'PETROLEUM',
                             'PROWTR': 'WATER',
                             '': np.nan,
                             ' ': np.nan},
                            inplace=True)
queen_pipes.loc[queen_pipes.typenew.notna(), 'typenew'] = queen_pipes.typenew + ' PIPELINE'

queen_pipes['commoditynew'] = queen_pipes.permitminerals
queen_pipes.commoditynew.replace({'CSG': 'COAL SEAM GAS',
                                  'PETRO': 'PETROLEUM',
                                  # 'CONGAS'
                                  # 'INCCSG'
                                  'C2H6': 'ETHANE',
                                  # 'OIL'
                                  # 'LPG'
                                  # 'WSTGAS'
                                  'CSG,PETRO': 'COAL SEAM GAS, PETROLEUM'},
                                 inplace=True)

# don't use 'PERMITSTATUS' field, it doesn't state the status of the pipeline itself, just the license application
# However, we can include whether an application in still pending and assume the pipeline is proposed
queen_pipes['statusnew'] = 'N/A'
queen_pipes.loc[queen_pipes.permitstatus == 'Application', 'statusnew'] = 'PROPOSED'

# Reformat approvedate to strip the time portion away
queen_pipes.approvedate = pd.to_datetime(queen_pipes.approvedate).dt.strftime("%Y-%m-%d")

# =============================================================================
# %% AUSTRALIA - Queensland - Integration + Export
# =============================================================================
queen_pipes_integrated, errors = integrate_pipelines(
    queen_pipes,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="Australia",
    state_prov="Queensland",
    src_ref_id="241",
    src_date="2024-04-16",  # Weekly
    # on_offshore="Offshore",
    fac_name='permitname',
    fac_id='permitid',
    fac_type='typenew',
    install_date='approvedate',  # date that permit was approved, pipeline likely constructed later than this
    fac_status='statusnew',
    op_name='authorisedholdername',
    commodity="commoditynew",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

save_spatial_data(
    queen_pipes_integrated,
    file_name="australia_queensland_pipelines",
    # schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% AZERBAIJAN
# =============================================================================
os.chdir(pubdata)
fp1 = "Middle_East+Caspian\\Azerbaijan\\Oil_Gas_Infra_.com\\Pipelines\\Gas_pipelines_Azerbajan.kml.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
pipelines1['COMMODITY'] = 'GAS'
pipelines11 = calculate_pipeline_length_km(pipelines1, attrName="PIPELINE_LENGTH_KM")
fp2 = "Middle_East+Caspian\\Azerbaijan\\Oil_Gas_Infra_.com\\Pipelines\\Oil_pipelines_Azerbajan.kml.shp"
pipelines2 = read_spatial_data(fp2, table_gradient=True)
pipelines2['COMMODITY'] = 'OIL'
pipelines22 = calculate_pipeline_length_km(pipelines2, attrName="PIPELINE_LENGTH_KM")

pipelines_concat = gpd.GeoDataFrame(pd.concat([pipelines11, pipelines22], ignore_index=True))
pipelines_concat_1 = pipelines_concat.set_crs(4326)


names = []

for idx1_, row1_ in tqdm(pipelines_concat_1.iterrows(), total=pipelines_concat_1.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if name == 'Gas':
        names_null = "N/A"
        names.append(names_null)
    elif name == 'Oil':
        names_null = "N/A"
        names.append(names_null)
    elif name == 'Untitled Path':
        names_null = "N/A"
        names.append(names_null)
    else:
        names.append(name)

pipelines_concat_1['NAMES'] = names


# Assign countries to pipelines - changed on 1/20/2023 to use new function
pipelines_concat_1 = pipelines_concat_1.reset_index(drop=False)
pipelines_concat_11 = assign_countries_to_feature(pipelines_concat_1,
                                                  # gdf_country_colname = 'COUNTRY',
                                                  gdf_uniqueid_field='index',
                                                  boundary_geoms=my_boundary_geoms,
                                                  overwrite_country_field=True)


# =============================================================================
# %% AZERBAIJAN - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat_11,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="Offshore",
    fac_name="NAMES",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "STATUS",
    # op_name = "",
    commodity="COMMODITY",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

save_spatial_data(
    pipelines_final,
    file_name="azerbaijan_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% BANGLADESH
# =============================================================================
os.chdir(pubdata)
fp1 = "China+SE_Asia\\Bangladesh\\Gas_Pipelines_Bangladesh.kml.shp"
pipelines = read_spatial_data(fp1, table_gradient=True)
pipelines1 = transform_CRS(pipelines, target_epsg_code="epsg:4326", appendLatLon=True)
pipelines2 = calculate_pipeline_length_km(pipelines1, attrName="PIPELINE_LENGTH_KM")
pipelines2['COMMODITY'] = 'GAS'


# =============================================================================
# %% BANGLADESH - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines2,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # on_offshore="",
    # fac_name="",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    commodity="COMMODITY",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

save_spatial_data(
    pipelines_final,
    file_name="bangladesh_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% BRAZIL
# =============================================================================
os.chdir(v24data)
br_dist = gpd.read_file(r'brazil/WebMap_EPE/Distribution_pipelines.shp')
br_out = gpd.read_file(r'brazil/WebMap_EPE/Outflow_pipelines.shp')
br_trans = gpd.read_file(r'brazil/WebMap_EPE/Transmission_pipelines.shp')
# DO NOT include this one, because it maps "potential / planned" pipelines that don't exist yet
# br_oilpipe = gpd.read_file(r'brazil/WebMap_EPE/Pipelines_(Oil_Pipeline_Plan)_.shp')

br_dist = transform_CRS(br_dist, target_epsg_code="epsg:4326", appendLatLon=True)
br_out = transform_CRS(br_out, target_epsg_code="epsg:4326", appendLatLon=True)
br_trans = transform_CRS(br_trans, target_epsg_code="epsg:4326", appendLatLon=True)

# Remove the pipelines which don't exist yet, for the tables that indicate such
br_out = br_out.query("Categoria == 'Existente'").reset_index(drop=True)
br_trans = br_trans.query("Categoria == 'Existente'").reset_index(drop=True)

# -----------------------------------------------------------------------------
br_dist['factype'] = 'Distribution pipeline'
br_dist = br_dist.rename(columns={'Distrib': 'operator',
                                  'last_edi_1': 'last_edit_date'})

br_out['factype'] = 'Outflow pipeline'
br_out = br_out.rename(columns={'Transporta': 'operator',
                                'DUTO_ID': 'facname',
                                'DIAM_POL': 'diam_inches'})
br_out.FLUIDO.replace({'Gás Natural': 'Natural Gas',
                       'Gás Natural e Condensado': 'Natural Gas and Condensate',
                       'Petróleo e Gás': 'Oil and Gas'},
                      inplace=True)

br_trans['factype'] = 'Transmission pipeline'
br_trans = br_trans.rename(columns={'Transporta': 'operator',
                                    'Nome_Dut_1': 'facname',
                                    'Diam_Pol_x': 'diam_inches',
                                    'last_edi_1': 'last_edit_date'
                                    })

# Concatenate into a single table
br_pipes = pd.concat([br_trans, br_out, br_dist]).reset_index(drop=True)
br_pipes = br_pipes[['factype',
                     'facname',
                     'operator',
                     'diam_inches',
                     'Categoria',
                     'FLUIDO',
                     'last_edit_date',
                     'geometry']]

# Clean up diameter values which are strings, then convert inches to mm
# Some values appear to list multiple diameter values, such as "4/6/8/10";
# For OGIM, use the largest value  # TODO confirm whether this is the best choice
br_pipes.diam_inches.replace({'3 1/2': '3.5',
                              '9.13': '13',
                              '10/12': '12',
                              '8/10': '10',
                              '11.13': '13',
                              '11.13': '13',
                              '4/6/8/10': '10'}, inplace=True)
br_pipes.diam_inches = br_pipes.diam_inches.astype(float)
br_pipes["diameter_mm"] = br_pipes.diam_inches * 25.4

# calculate pipeline length attribute
br_pipes = calculate_pipeline_length_km(br_pipes,
                                        attrName="PIPELINE_LENGTH_KM")

# Remove any duplicate pipelines
br_pipes_ = br_pipes.drop_duplicates(subset=['factype',
                                             'facname',
                                             'operator',
                                             'diam_inches',
                                             'FLUIDO',
                                             'PIPELINE_LENGTH_KM',
                                             'geometry'],
                                     keep='first').reset_index()

# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
br_pipes_integrated, errors = integrate_pipelines(
    br_pipes_,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="BRAZIL",
    # state_prov="",
    src_ref_id="267",
    src_date="2011-01-01",
    # on_offshore="",
    fac_name="facname",
    # fac_id = "",
    fac_type="factype",
    # install_date = "",
    # fac_status = "",
    op_name="operator",
    commodity="FLUIDO",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    pipe_diameter_mm="diameter_mm",
    pipe_length_km="PIPELINE_LENGTH_KM"
    # pipe_material = "",
)

br_pipes_integrated_no_z = strip_z_coord(br_pipes_integrated)

save_spatial_data(
    br_pipes_integrated_no_z,
    file_name="brazil_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% DENMARK
# =============================================================================
os.chdir(pubdata)
fp1 = "Europe\\Denmark\\Denmark\\Denmark_Pipelines.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)

# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

pipelines_cleaned2 = calculate_pipeline_length_km(
    pipelines11,
    attrName="PIPELINE_LENGTH_KM"
)


names = []
commodity = []

# Reformat names, calculate pipeline diameter
# Splitting the names at the inch marker, then appending names list with just name
# Then using the split name to recalculate diameter to mm
for idx1_, row1_ in tqdm(pipelines_cleaned2.iterrows(), total=pipelines_cleaned2.shape[0]):
    name = row1_.Name
    if name == "gas":
        names.append("N/A")
        commodity.append("Gas")
    elif name == "oil":
        names.append("N/A")
        commodity.append("Oil")
    elif name == "Nogat pipeline":
        names.append(name)
        commodity.append("N/A")

    else:
        pass


pipelines_cleaned2['FORMATTED_NAME'] = names
pipelines_cleaned2['Commodity'] = commodity


# Assign countries to pipelines - changed on 1/20/2023 to use new function
pipelines_cleaned2 = pipelines_cleaned2.reset_index(drop=False)
pipelines_cleaned3 = assign_countries_to_feature(pipelines_cleaned2,
                                                 # gdf_country_colname = 'COUNTRY',
                                                 gdf_uniqueid_field='index',
                                                 boundary_geoms=my_boundary_geoms,
                                                 overwrite_country_field=True)

# =============================================================================
# %% DENMARK - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_cleaned3,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    # on_offshore="Offshore",
    fac_name="FORMATTED_NAME",
    # fac_id = "FORMATTED_PIPE",
    # fac_type = "",
    # install_date = "curPhDate",
    # fac_status = "curPhase",
    # op_name = "curOperNam",
    commodity="Commodity",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "FORMATTED_DIAMETER_MM",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = "",
)

pipelines_final_no_z = strip_z_coord(pipelines_final)


save_spatial_data(
    pipelines_final_no_z,
    file_name="denmark_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% EUROPE  # TODO
# =============================================================================
os.chdir(pubdata)
fp1 = "Europe\\Pipelines\\Europe_Pipelines_final.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

pipelines111 = calculate_pipeline_length_km(
    pipelines11,
    attrName="PIPELINE_LENGTH_KM"
)


# Assign countries to pipelines - changed on 1/20/2023 to use new function
pipelines111 = pipelines111.reset_index(drop=False)
pipelines1111 = assign_countries_to_feature(pipelines111,
                                            # gdf_country_colname = 'COUNTRY',
                                            gdf_uniqueid_field='index',
                                            boundary_geoms=my_boundary_geoms,
                                            overwrite_country_field=True)


# =============================================================================
# %% EUROPE - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines1111,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    # fac_name="Name",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )


save_spatial_data(
    pipelines_final_no_z,
    file_name="europe_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% INDIA
# =============================================================================
os.chdir(pubdata)
fp1 = "China+SE_Asia\\India\\GAS_pipelines_offshore_India.kml.shp"
gasoffshore = read_spatial_data(fp1, table_gradient=True)
gasoffshore['TYPE1'] = 'GAS'
gasoffshore['ONOFF'] = 'OFFSHORE'
gasoffshore1 = transform_CRS(gasoffshore, target_epsg_code="epsg:4326", appendLatLon=True)
gasoffshore2 = calculate_pipeline_length_km(gasoffshore1, attrName="PIPELINE_LENGTH_KM")

fp2 = "China+SE_Asia\\India\\Onshore_Pipelines_India.kml.shp"
onshore = read_spatial_data(fp2, table_gradient=True)
onshore['TYPE1'] = 'N/A'
onshore['ONOFF'] = 'ONSHORE'
onshore1 = transform_CRS(onshore, target_epsg_code="epsg:4326", appendLatLon=True)
onshore2 = calculate_pipeline_length_km(onshore1, attrName="PIPELINE_LENGTH_KM")

fp3 = "China+SE_Asia\\India\\OIL_pipelines_offshore_India.kml.shp"
oiloffshore = read_spatial_data(fp3, table_gradient=True)
oiloffshore['TYPE1'] = 'OIL'
oiloffshore['ONOFF'] = 'OFFSHORE'
oiloffshore1 = transform_CRS(oiloffshore, target_epsg_code="epsg:4326", appendLatLon=True)
oiloffshore2 = calculate_pipeline_length_km(oiloffshore1, attrName="PIPELINE_LENGTH_KM")

pipelines_concat = gpd.GeoDataFrame(pd.concat([gasoffshore2, onshore2, oiloffshore2], ignore_index=True))


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []

for idx1_, row1_ in tqdm(pipelines_concat.iterrows(), total=pipelines_concat.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if 'Mukta Panna oil and gas' in name:
        mukta = "Mukta Panna"
        names.append(mukta)
    elif 'Mukta Panna export' in name:
        mukta = "Mukta Panna"
        names.append(mukta)
    elif 'Tapti' in name:
        tapti = "Tapti"
        names.append(tapti)
    else:
        names.append('N/A')

pipelines_concat['NAME1'] = names
pipelines_concat2 = pipelines_concat.set_crs(4326)

# Assign countries to pipelines - changed on 1/20/2023 to use new function
pipelines_concat2 = pipelines_concat2.reset_index(drop=False)
pipelines_concat3 = assign_countries_to_feature(pipelines_concat2,
                                                # gdf_country_colname = 'COUNTRY',
                                                gdf_uniqueid_field='index',
                                                boundary_geoms=my_boundary_geoms,
                                                overwrite_country_field=True)


# =============================================================================
# %% INDIA - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat3,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    on_offshore="ONOFF",
    fac_name="NAME1",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    commodity="TYPE1",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

pipelines_final_no_z = strip_z_coord(pipelines_final)


save_spatial_data(
    pipelines_final_no_z,
    file_name="india_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% KAZAKHSTAN
# =============================================================================
os.chdir(pubdata)
fp1 = "Middle_East+Caspian\\Kazakhstan\\Oil_Gas_Infra_.com\\Pipelines\\Gas_pipeline_Kazakhsta.kml.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
pipelines1['COMMODITY'] = 'GAS'
pipelines11 = calculate_pipeline_length_km(pipelines1, attrName="PIPELINE_LENGTH_KM")
fp2 = "Middle_East+Caspian\\Kazakhstan\\Oil_Gas_Infra_.com\\Pipelines\\Oil_pipeline_Kazakhstan.kml.shp"
pipelines2 = read_spatial_data(fp2, table_gradient=True)
pipelines2['COMMODITY'] = 'OIL'
pipelines22 = calculate_pipeline_length_km(pipelines2, attrName="PIPELINE_LENGTH_KM")


pipelines_concat = gpd.GeoDataFrame(pd.concat([pipelines11, pipelines22], ignore_index=True))
pipelines_concat_1 = pipelines_concat.set_crs(4326)


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []

for idx1_, row1_ in tqdm(pipelines_concat_1.iterrows(), total=pipelines_concat_1.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if name == 'Gas Pipeline':
        names_null = "Gas"
        names.append(names_null)
    elif name == 'Oil Pipeline':
        names_null = "Oil"
        names.append(names_null)
    else:
        names.append(name)

pipelines_concat_1['NAMES'] = names

# Assign countries to pipelines - changed on 1/20/2023 to use new function
pipelines_concat_1 = pipelines_concat_1.reset_index(drop=False)
pipelines_concat_11 = assign_countries_to_feature(pipelines_concat_1,
                                                  # gdf_country_colname = 'COUNTRY',
                                                  gdf_uniqueid_field='index',
                                                  boundary_geoms=my_boundary_geoms,
                                                  overwrite_country_field=True)


# =============================================================================
# %% KAZAKHSTAN - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat_11,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="Offshore",
    # fac_name="NAMES",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "STATUS",
    # op_name = "",
    commodity="NAMES",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)


save_spatial_data(
    pipelines_final,
    file_name="kazakhstan_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% MIDDLE EAST
# =============================================================================
os.chdir(pubdata)
fp = r"Middle_East+Caspian\Iran\other_data\Pipelines\Existing_and_Proposed_Persian_Guld_Gas_Oil_Pipelines.shp"
data = read_spatial_data(fp, table_gradient=False)
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Calculate the length of each polyline, and add the results in a new column
data = calculate_pipeline_length_km(data, attrName="PIPELINE_LENGTH_KM")

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Fill in unknown 'Name' values, for subsequent string searches
data.loc[data.Name.isnull(), 'Name'] = 'Unknown'

# Add status column to indicate which pipelines are "proposed" or "operating"
# based on details in 'Name' column
data['stat'] = 'N/A'
data.loc[data.Name.str.contains('|'.join(['Proposal', 'Proposed'])), 'stat'] = 'Proposed'
data.loc[data.Name.str.contains('Operating'), 'stat'] = 'Operating'

# add commodity column, based on details in 'Name' column
data['comm'] = 'N/A'
data.loc[data.Name.str.contains('Gas'), 'comm'] = 'Gas'
data.loc[data.Name.str.contains('Oil'), 'comm'] = 'Oil'

# Some pipeline Names indicate an operator;
# after Googling the acronym, I've included the operator's full name
data['operator'] = 'N/A'
data.loc[data.Name.str.contains('SNGPL'), 'operator'] = 'Sui Northern Gas Pipelines Limited'
data.loc[data.Name.str.contains('SSGC'), 'operator'] = 'Sui Southern Gas Company'


# Remove unhelpful pipeline names like "Operating Egypt Pipelines"
names2drop = ['Unknown',
              'Oman Gas Pipelines',
              'Operating Egypt Oil Pipelines',
              'Operating Egypt Gas Pipelines',
              'Iran Gas Pipelines',
              'Operating Iran Oil Pipelines',
              'Operating Saudi Arabia Oil Pipelines',
              'Operating Iraq Oil Pipelines',
              'Operating Saudia Arabia Gas Pipelines',
              'Egypt Gas Pipeline'
              ]
data.loc[(data.Name.isin(names2drop)), 'Name'] = 'N/A'

# Assign countries to pipelines - changed on 1/20/2023 to use new function
data = data.reset_index(drop=False)
data_with_countries = assign_countries_to_feature(data,
                                                  # gdf_country_colname = 'COUNTRY',
                                                  gdf_uniqueid_field='index',
                                                  boundary_geoms=my_boundary_geoms,
                                                  overwrite_country_field=True)


# =============================================================================
# %% MIDDLE EAST  - Integration + Export
# =============================================================================
data_with_countries_, errors = integrate_pipelines(
    data_with_countries,
    starting_ids=0,
    category='Oil and natural gas pipelines',
    fac_alias="PIPELINES",
    country='COUNTRY',
    # state_prov = None,
    src_ref_id='164',
    src_date='2014-04-01',
    on_offshore='Onshore',
    fac_name='Name',
    # fac_id = None,
    # fac_type = None,
    # install_date = None,
    fac_status='stat',
    op_name='operator',
    commodity='comm',
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    # pipe_diameter_mm = None,
    pipe_length_km='PIPELINE_LENGTH_KM',
    # pipe_material = None
)


save_spatial_data(
    data_with_countries_,
    file_name="middleeast_pipelines",
    schema_def=False,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% MYANMAR
# =============================================================================
os.chdir(pubdata)
fp1 = "China+SE_Asia\\Myanmar\\Gas_pipeline_Myanmar.kml.shp"
gas = read_spatial_data(fp1, table_gradient=False)
gas['TYPE1'] = 'GAS'
gas1 = transform_CRS(gas, target_epsg_code="epsg:4326", appendLatLon=True)
gas2 = calculate_pipeline_length_km(gas1, attrName="PIPELINE_LENGTH_KM")

fp1 = "China+SE_Asia\\Myanmar\\Oil_pipeline_Myanmar.kml.shp"
oil = read_spatial_data(fp1, table_gradient=False)
oil['TYPE1'] = 'OIL'
oil1 = transform_CRS(oil, target_epsg_code="epsg:4326", appendLatLon=True)
oil2 = calculate_pipeline_length_km(oil1, attrName="PIPELINE_LENGTH_KM")

pipelines_concat = gpd.GeoDataFrame(pd.concat([gas2, oil2], ignore_index=True))


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# retain single row that does have a name
names = []

for idx1_, row1_ in tqdm(pipelines_concat.iterrows(), total=pipelines_concat.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if 'Sino' in name:
        mukta = "Sino Burma"
        names.append(mukta)
    else:
        names.append('N/A')

pipelines_concat['NAME1'] = names
pipelines_concat2 = pipelines_concat.set_crs(4326)

# =============================================================================
# %% MYANMAR - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat2,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="Myanmar",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="ONOFF",
    fac_name="NAME1",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "TYPE1",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)


save_spatial_data(
    pipelines_final_no_z,
    file_name="myanmar_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NETHERLANDS
# =============================================================================
os.chdir(pubdata)
fp1 = "Europe\\Netherlands\\Pipelines\\ABAND_Pipelines_Netherlands.kml.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
pipelines1['STATUS'] = "Abandoned"
pipelines11 = calculate_pipeline_length_km(pipelines1, attrName="PIPELINE_LENGTH_KM")
fp2 = "Europe\\Netherlands\\Pipelines\\AUX_Pipelines_Netherlands.kml.shp"
pipelines2 = read_spatial_data(fp2, table_gradient=True)
pipelines2['TYPE'] = "Auxiliary"
pipelines22 = calculate_pipeline_length_km(pipelines2, attrName="PIPELINE_LENGTH_KM")
fp3 = "Europe\\Netherlands\\Pipelines\\GAS_Pipelines_Netherlands.kml.shp"
pipelines3 = read_spatial_data(fp3, table_gradient=True)
pipelines3['TYPE'] = "Gas"
pipelines33 = calculate_pipeline_length_km(pipelines3, attrName="PIPELINE_LENGTH_KM")
fp4 = "Europe\\Netherlands\\Pipelines\\OIL_Pipelines_Netherlands.kml.shp"
pipelines4 = read_spatial_data(fp4, table_gradient=True)
pipelines4['TYPE'] = "Oil"
pipelines44 = calculate_pipeline_length_km(pipelines4, attrName="PIPELINE_LENGTH_KM")
fp5 = "Europe\\Netherlands\\Pipelines\\WATER_Pipelines_Netherlands.kml.shp"
pipelines5 = read_spatial_data(fp5, table_gradient=True)
pipelines5['TYPE'] = "Water"
pipelines55 = calculate_pipeline_length_km(pipelines5, attrName="PIPELINE_LENGTH_KM")


pipelines_concat = gpd.GeoDataFrame(pd.concat([pipelines11, pipelines22, pipelines33, pipelines44, pipelines55], ignore_index=True))
pipelines_concat_1 = pipelines_concat.set_crs(4326)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# #If needed, these queries can select/remove multistring geometries
# multilinestring_pipes = pipelines_concat_1.loc[pipelines_concat_1.geometry.geometry.type=='MultiLineString']
# multilinestring_pipes_final_no_z = strip_z_coord(multilinestring_pipes)

# linestring_pipes = pipelines_concat_1.loc[pipelines_concat_1.geometry.geometry.type=='LineString']
# linestring_pipes_final_no_z = strip_z_coord(linestring_pipes)
# # flattened_pipelines= flatten_gdf_geometry(linestring_pipes_final_no_z, 'MultiLineString' )

# pipelines_concat_1 = strip_z_coord(pipelines_concat_1)


# Assign countries to pipelines - changed on 1/11/2023 to use new function
pipelines_concat_11 = assign_countries_to_feature(pipelines_concat_1,
                                                  # gdf_country_colname = 'COUNTRY',
                                                  gdf_uniqueid_field='Name',
                                                  boundary_geoms=my_boundary_geoms,
                                                  overwrite_country_field=True)

# =============================================================================
# %% NETHERLANDS - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat_11,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    on_offshore="Offshore",
    fac_name="Name",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    fac_status="STATUS",
    # op_name = "",
    commodity="TYPE",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )
pipelines_final_no_z_ = replace_row_names(pipelines_final_no_z, colName="FAC_STATUS", dict_names={'NAN': 'N/A'})
pipelines_final_no_z__ = replace_row_names(pipelines_final_no_z_, colName="COMMODITY", dict_names={'NAN': 'N/A'})


save_spatial_data(
    pipelines_final_no_z__,
    file_name="netherlands_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NEW ZEALAND
# =============================================================================
os.chdir(pubdata)
fp1 = r"Australia+NewZealand\New_Zealand\NZ_pipelines.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

pipelines111 = calculate_pipeline_length_km(
    pipelines11,
    attrName="PIPELINE_LENGTH_KM"
)

# =============================================================================
# %% NEW ZEALAND - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines111,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="NEW ZEALAND",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    # fac_name="Name",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )


save_spatial_data(
    pipelines_final_no_z,
    file_name="new_zealand_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NORWAY -- TEMPORARILY DROP bc oginfra.com duplicates these a lot
# =============================================================================
# os.chdir(v24data)
# fp1 = r"norway\pipelines\pipLine.shp"
# pipelines1 = read_spatial_data(fp1, table_gradient=False)

# # Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
# pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

# pipelines_cleaned = pipelines11[pipelines11['geometry'] != None]

# pipelines_cleaned2 = calculate_pipeline_length_km(
#     pipelines_cleaned,
#     attrName="PIPELINE_LENGTH_KM"
# )
# pipelines_cleaned3 = pipelines_cleaned2.reset_index(drop=True)

# # Data manipulation / processing if needed
# # -----------------------------------------------------------------------------
# names = []
# diameter = []
# pipelines = []

# # Reformat names, calculate pipeline diameter
# # Splitting the names at the inch marker, then appending names list with just name
# # Then using the split name to recalculate diameter to mm
# for idx1_, row1_ in tqdm(pipelines_cleaned3.iterrows(), total=pipelines_cleaned3.shape[0]):
#     name = row1_.pipName
#     if names is None:
#         names.append(name)

#     else:
#         names_split = name.split("\" ")
#         names.append(names_split[1])
#         diameter_calc = float(names_split[0]) * 25.4
#         diameter.append(diameter_calc)
#     pipe = row1_.idPipeline
#     if pipe is None:
#         pass
#     else:
#         pipe1 = str(pipe)
#         pipelines.append(pipe1)


# pipelines_cleaned3['FORMATTED_NAME'] = names
# pipelines_cleaned3['FORMATTED_DIAMETER_MM'] = diameter
# pipelines_cleaned3['FORMATTED_PIPE'] = pipelines


# # Assign countries to pipelines - changed on 1/11/2023 to use new function
# pipelines_cleaned3_ = assign_countries_to_feature(pipelines_cleaned3,
#                                                   # gdf_country_colname = 'COUNTRY',
#                                                   gdf_uniqueid_field="idPipeline",
#                                                   boundary_geoms=my_boundary_geoms,
#                                                   overwrite_country_field=True)

# # =============================================================================
# # %% NORWAY  - Integration + Export
# # =============================================================================
# pipelines_final, errors = integrate_pipelines(
#     pipelines_cleaned3_,
#     starting_ids=0,
#     category="Oil and natural gas pipelines",
#     fac_alias="PIPELINES",
#     country="COUNTRY",
#     # state_prov="",
#     src_ref_id="24",
#     src_date="2022-03-14",
#     on_offshore="Offshore",
#     fac_name="FORMATTED_NAME",
#     fac_id="FORMATTED_PIPE",
#     # fac_type = "",
#     install_date="curPhDate",
#     fac_status="curPhase",
#     op_name="curOperNam",
#     commodity="medium",
#     # liq_capacity_bpd = "",
#     # liq_throughput_bpd = "",
#     # gas_capacity_mmcfd = "",
#     # gas_throughput_mmcfd = "",
#     pipe_diameter_mm="FORMATTED_DIAMETER_MM",
#     pipe_length_km="PIPELINE_LENGTH_KM",
#     # pipe_material = "",
# )


# save_spatial_data(
#     pipelines_final,
#     file_name="norway_pipelines",
#     # schema_def = True,
#     schema=schema_PIPELINES,
#     file_type="GeoJSON",
#     out_path=results_folder)


# =============================================================================
# %% AFRICA - oginfra.com - TEMPORARILY DROP bc USGS duplicates these a lot
# =============================================================================
# os.chdir(pubdata)

# CAM_pipes = gpd.read_file("Africa/Cameroon/Oil Pipelines.kml.shp").to_crs("epsg:4326")

# DRC_pipes1 = gpd.read_file("Africa/DRC/Oil Pipelines_DRC.kml.shp").to_crs("epsg:4326")
# DRC_pipes2 = gpd.read_file("Africa/DRC/Gas Pipelines_DRC.kml.shp").to_crs("epsg:4326")

# GAN_pipes = gpd.read_file("Africa/Ghana/Gas_Pipelines_Ghana.kml.shp").to_crs("epsg:4326")

# IVC_pipes1 = gpd.read_file("Africa/Ivory_Coast/Gas_Pipelines_IvoryCoast.kml.shp").to_crs("epsg:4326")
# IVC_pipes2 = gpd.read_file("Africa/Ivory_Coast/Oil_Pipelines_Ivory_Coast.kml.shp").to_crs("epsg:4326")

# LIB_pipes1 = gpd.read_file("Africa/Libya/Pipeline_Offshore_Libya.kml.shp").to_crs("epsg:4326")
# LIB_pipes2 = gpd.read_file("Africa/Libya/Pipelines_Exact_Onshore_Libya.kml.shp").to_crs("epsg:4326")

# MOZ_pipes1 = gpd.read_file("Africa/Mozambique/Gas_Pipeline_Mozambique.kml.shp").to_crs("epsg:4326")

# NIG_pipes1 = gpd.read_file("Africa/Nigeria/Gas_Pipeline_Nigeria.kml.shp").to_crs("epsg:4326")
# NIG_pipes2 = gpd.read_file("Africa/Nigeria/Oil_Pipelines_Nigeria.kml.shp").to_crs("epsg:4326")
# NIG_pipes3 = gpd.read_file("Africa/Nigeria/Condensate_Pipelines_Nigeria.kml.shp").to_crs("epsg:4326")

# SAFR_pipes1 = gpd.read_file("Africa/South_Africa/Gas_Pipeline_SouthAfrica.kml.shp").to_crs("epsg:4326")
# SAFR_pipes2 = gpd.read_file("Africa/South_Africa/Oil_Pipeline_SouthAfrica.kml.shp").to_crs("epsg:4326")

# SUD_pipes1 = gpd.read_file("Africa/Sudan/Sudan/_Pipelines_Sudan_.shp").to_crs("epsg:4326")

# # Data manipulation / processing if needed
# # -----------------------------------------------------------------------------
# # add commodity info based on original filename
# CAM_pipes['Commodity'] = 'Oil'
# DRC_pipes1['Commodity'] = 'Oil'
# DRC_pipes2['Commodity'] = 'Gas'
# GAN_pipes['Commodity'] = 'Gas'
# IVC_pipes1['Commodity'] = 'Gas'
# IVC_pipes2['Commodity'] = 'Oil'
# LIB_pipes1['onoff'] = 'Offshore'
# LIB_pipes2['onoff'] = 'Onshore'
# MOZ_pipes1['Commodity'] = 'Gas'
# NIG_pipes1['Commodity'] = 'Gas'
# NIG_pipes2['Commodity'] = 'Oil'
# NIG_pipes3['Commodity'] = 'Condensate'
# SAFR_pipes1['Commodity'] = 'Gas'
# SAFR_pipes2['Commodity'] = 'Oil'

# # Append country-specific gdfs into one gdf
# all_dfs_final = [CAM_pipes,
#                  DRC_pipes1,
#                  DRC_pipes2,
#                  GAN_pipes,
#                  IVC_pipes1,
#                  IVC_pipes2,
#                  LIB_pipes1,
#                  LIB_pipes2,
#                  MOZ_pipes1,
#                  NIG_pipes1,
#                  NIG_pipes2,
#                  NIG_pipes3,
#                  SAFR_pipes1,
#                  SUD_pipes1,
#                  SAFR_pipes2]

# data = pd.concat(all_dfs_final)
# # Drop empty geometries
# data = data[data.geometry != None].reset_index(drop=True)
# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)
# # Calculate the length of each polyline, and add the results in a new column
# data = calculate_pipeline_length_km(data, attrName="PIPELINE_LENGTH_KM")


# # Insert N/A where needed in Name and Descriptio field
# data.loc[data.Name == 'Untitled Path', 'Name'] = 'N/A'
# data.Descriptio = data.Descriptio.fillna('N/A')

# # Extract useful information from the 'Descriptio' field
# # Pipe diameter, then convert to millimeters
# data.loc[data.Descriptio.str.contains('0"'), 'pipe_diam_inch'] = data['Descriptio'].str.strip('"')
# data['pipe_diam_inch'] = data[data.pipe_diam_inch.notnull()]['pipe_diam_inch'].astype('int')
# data.loc[data.pipe_diam_inch.notnull(), 'pipe_diam_mm'] = data['pipe_diam_inch'] * 25.4
# # data.pipe_diam_mm = data.pipe_diam_mm.fillna(-999).astype('int')


# # Names of pipelines
# data.loc[data.Descriptio.str.contains('Cameroon_Petroleum_Development_'), 'Name'] = 'Chad-Cameroon Petroleum Development and Pipeline Project'
# data.loc[data.Descriptio.str.contains('mozambique-south-africa'), 'Name'] = 'Mozambique South Africa Gas Pipeline'
# data.loc[data.Name == 'WAGP', 'Name'] = 'West African Gas Pipeline'


# # Associate country names with pipeline features
# # Not sure why I need to run this in order for below function to work -- fix this in my function later
# data = data.drop('Shape_Leng', axis=1)

# # Assign countries to pipelines - changed on 1/11/2023 to use new function
# data = data.reset_index(drop=False)  # create a unique ID column for use in next step
# data_with_countries = assign_countries_to_feature(data,
#                                                   # gdf_country_colname = 'COUNTRY',
#                                                   gdf_uniqueid_field='index',
#                                                   boundary_geoms=my_boundary_geoms,
#                                                   overwrite_country_field=True)

# =============================================================================
# %% AFRICA - oginfra.com - Integration + Export
# =============================================================================
# data_with_countries_, errors = integrate_pipelines(
#     data_with_countries,
#     starting_ids=0,
#     category='Oil and natural gas pipelines',
#     fac_alias="PIPELINES",
#     country='COUNTRY',
#     # state_prov = None,
#     src_ref_id='22',
#     src_date='2014-01-01',
#     on_offshore='onoff',
#     fac_name='Name',
#     # fac_id = None,
#     # fac_type = None,
#     # install_date = None,
#     # fac_status = None,
#     # op_name = None,
#     commodity='Commodity',
#     # liq_capacity_bpd = None,
#     # liq_throughput_bpd = None,
#     # gas_capacity_mmcfd = None,
#     # gas_throughput_mmcfd = None,
#     pipe_diameter_mm='pipe_diam_mm',
#     pipe_length_km='PIPELINE_LENGTH_KM',
#     # pipe_material = None
# )


# save_spatial_data(
#     data_with_countries_,
#     file_name="oginfracom_africa_pipelines",
#     schema_def=False,
#     schema=schema_PIPELINES,
#     file_type="GeoJSON",
#     out_path=results_folder)

# =============================================================================
# %% MIDDLE EAST - oginfra.com
# =============================================================================
os.chdir(pubdata)

# Read in original shapefiles as geodataframes
# Individual shapefiles are specific to a country, specific to a commodity,
# specific to onshore/offshore, or some combination of the three.
afghan = gpd.read_file(r"Middle_East+Caspian\Afghanistan\Oil_Gas_Infra_.com\Pipelines\_Afghanistan_Pipeline_OilGasInfra.com.shp").to_crs("epsg:4326")

iran_gas = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Pipelines\Gas Pipelines.kml.shp').to_crs("epsg:4326")
iran_crude = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Pipelines\Major Crude Oil Export Pipeline.kml.shp').to_crs("epsg:4326")

iraq_gas = gpd.read_file(r'Middle_East+Caspian\Iraq\Oil_Gas_Infra_.com\Pipelines\Gas_Pipeline_Iraq.kml.shp').to_crs("epsg:4326")
iraq_oil = gpd.read_file(r'Middle_East+Caspian\Iraq\Oil_Gas_Infra_.com\Pipelines\Oil_Pipelines_Iraq.kml.shp').to_crs("epsg:4326")

kuwait = gpd.read_file(r'Middle_East+Caspian\Kuwait\Oil_Gas_Infra_.com\Pipelines\Oil_Pipelines_Kuwait.kml.shp').to_crs("epsg:4326")

oman_gas = gpd.read_file(r'Middle_East+Caspian\Oman\Oil_Gas_Infra_.com\Pipelines\Gas_pipelines_Oman.kml.shp').to_crs("epsg:4326")
oman_oil = gpd.read_file(r'Middle_East+Caspian\Oman\Oil_Gas_Infra_.com\Pipelines\Oil_pipelines_Oman.kml.shp').to_crs("epsg:4326")

pak_gas = gpd.read_file(r'Middle_East+Caspian\Pakistan\Oil_Gas_Infra_.com\Pipelines\gas_pipeline_Pakistan.kml.shp').to_crs("epsg:4326")
pak_oil = gpd.read_file(r'Middle_East+Caspian\Pakistan\Oil_Gas_Infra_.com\Pipelines\oil_pipeline_Pakistan.kml.shp').to_crs("epsg:4326")

qatar_gas = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Pipelines\Gas_Pipeline_Qatar.kml.shp').to_crs("epsg:4326")
qatar_oil = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Pipelines\Oil_Pipeline_Qatar.kml.shp').to_crs("epsg:4326")

saudi_gas = gpd.read_file(r'Middle_East+Caspian\Saudi_Arabia\Oil_Gas_Infra_.com\Pipelines\Gas_Wells_Pipelines_Saudi_Arabia.kml.shp').to_crs("epsg:4326")
saudi_oil = gpd.read_file(r'Middle_East+Caspian\Saudi_Arabia\Oil_Gas_Infra_.com\Pipelines\Oil_Pipelines_Saudi_Arabia.kml.shp').to_crs("epsg:4326")

uae_gas_off = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Pipelines\Gas_Pipelines_UAE.kml.shp').to_crs("epsg:4326")
uae_oil_off = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Pipelines\Oil_Pipelines_Offshore_UAE.kml.shp').to_crs("epsg:4326")
uae_gas_on = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Pipelines\Onshore_Gas_Pipelines_UAE.kml.shp').to_crs("epsg:4326")
uae_on = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Pipelines\Onshore_Pipelines_UAE.kml.shp')  # possibly OIL??

yemen_gas = gpd.read_file(r'Middle_East+Caspian\Yemen\Oil_Gas_Infra_.com\Pipelines\Gas_Pipelines_Yemen.kml.shp').to_crs("epsg:4326")
yemen_oil = gpd.read_file(r'Middle_East+Caspian\Yemen\Oil_Gas_Infra_.com\Pipelines\Oil_Pipelines_Yemen.kml.shp').to_crs("epsg:4326")


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Make list of all gdfs containing GAS pipelines
list_of_gas = [
    iran_gas,
    iraq_gas,
    oman_gas,
    pak_gas,
    qatar_gas,
    saudi_gas,
    uae_gas_off,
    uae_gas_on,
    yemen_gas
]
# Make list of all gdfs containing OIL pipelines
list_of_oil = [
    iran_crude,
    iraq_oil,
    oman_oil,
    pak_oil,
    qatar_oil,
    saudi_oil,
    uae_oil_off,
    yemen_oil
]
# Make list of all gdfs containing UNKNOWN commodity
list_of_unknown = [
    afghan,
    kuwait,
    uae_on
]

# Add 'commodity' column to each group of gdfs
for df in list_of_gas:
    df['commodity'] = 'Gas'

for df in list_of_oil:
    df['commodity'] = 'Oil'

for df in list_of_unknown:
    df['commodity'] = 'N/A'


# In all dataframes, If a feature's name is None / empty, replace with 'NOT AVAILABLE'
all_dfs = list_of_gas + list_of_oil + list_of_unknown

for df in all_dfs:
    df.loc[df.Name.isna(), 'Name'] = 'N/A'
    df.loc[df.Name.str.contains('Untitled'), 'Name'] = 'N/A'


# Append gdfs covering the same country into a single gdf
# (and reset index of the output)
iran = iran_gas.append(iran_crude).reset_index(drop=True)
iraq = iraq_gas.append(iraq_oil).reset_index(drop=True)
oman = oman_gas.append(oman_oil).reset_index(drop=True)
pak = pak_gas.append(pak_oil).reset_index(drop=True)
qatar = qatar_gas.append(qatar_oil).reset_index(drop=True)
saudi = saudi_gas.append(saudi_oil).reset_index(drop=True)
uae = uae_gas_off.append(uae_gas_on).append(uae_oil_off).append(uae_on).reset_index(drop=True)
yemen = yemen_gas.append(yemen_oil).reset_index(drop=True)

# Create and populate COUNTRY column in single-country gdfs
afghan['country'] = 'Afghanistan'
iran['country'] = 'Iran'
iraq['country'] = 'Iraq'
kuwait['country'] = 'Kuwait'
oman['country'] = 'Oman'
pak['country'] = 'Pakistan'
qatar['country'] = 'Qatar'
saudi['country'] = 'Saudi Arabia'
uae['country'] = 'UAE'
yemen['country'] = 'Yemen'


# Some Iran pipelines appear in the wrong place... fix them
# Import built-in geopandas countries dataset
world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
iran_country = world[world.name == 'Iran']

# quick plot of where pipelines are
# base = iran_country.boundary.plot(color='black')
# iran.plot(ax=base)

# It looks like some Iran coordinates may have gotten their X and Y values flip-flopped
# Map the ones I think are incorrect based on the geodataframe values
# base = iran_country.boundary.plot(color='black')
# iran.iloc[0:3].plot(ax=base)

# # Once confirmed, delete the incorrect rows
iran = iran.drop([0, 1, 2]).reset_index(drop=True)

# Where possible, extract the TYPE of pipeline into its own column, based on details in the 'Name' field
# Only UAE and Yemen have some records with this detail

# Make list of sub-strings that would indicate pipeline type
terms_export = ['Export', 'export']
terms_trunk = ['Trunk', 'trunk', 'trrunk']

# Create a new 'type' column, and for any record whose Name contains one of my
# 'export' or 'trunkline' keywords, specify the pipeline type in the new column
uae['type'] = None
uae.loc[uae.Name.str.contains('|'.join(terms_export)), 'type'] = 'Export pipeline'
uae.loc[uae.Name.str.contains('|'.join(terms_trunk)), 'type'] = 'Trunkline'

yemen['type'] = None
yemen.loc[yemen.Name.str.contains('|'.join(terms_trunk)), 'type'] = 'Trunkline'

# Since I've now captured the pipeline type as Export or Trunkline in another column,
# I can change the generic pipeline names "Trunkline" etc. to NOT AVAILABLE instead.
yemen.loc[yemen.Name.str.contains('|'.join(terms_trunk)), 'Name'] = 'N/A'
uae.loc[(uae.Name == 'export line') | (uae.Name == 'oil export') | (uae.Name == 'trunkline'), 'Name'] = 'N/A'


# Fix quirks specific to particular countries
# Replace unhelpful feature names like "oil" or "Gas Pipeline 2" with 'NOT AVAILABLE', while retaining helpful/real values
pak['Name'] = 'N/A'
oman.loc[oman.Name == 'NULL', 'Name'] = 'N/A'
# Specify that this pipeline carries LNG in its Commodity field, rather than Name field
yemen.loc[yemen.Name == 'LNG', 'commodity'] = 'LNG'
# Some UAE records have empty geometries -- drop these records
uae = uae[uae.geometry.notnull()]


# Join all of the dfs together into one pipelines GDF,
# since the only attributes present (besides the ones I added) are Name and geometry

# re-define the list of dataframes so it reflects all my changes
all_dfs_final = [
    afghan,
    iran,
    iraq,
    kuwait,
    oman,
    pak,
    qatar,
    saudi,
    uae,
    yemen
]

data = pd.concat(all_dfs_final)
data = data.drop('Descriptio', axis=1)
data = data.reset_index(drop=True)
data['type'] = data['type'].fillna('N/A')

# Calculate the length of each polyline, and add the results in a new column
data = calculate_pipeline_length_km(data, attrName="PIPELINE_LENGTH_KM")

# Drop duplicate pipelines
data = data.drop_duplicates(subset=['Name',
                                    'commodity',
                                    'geometry'],
                            keep='first')

# =============================================================================
# %% MIDDLE EAST - oginfra.com-  Integration + Export
# =============================================================================
data_, errors = integrate_pipelines(
    data.reset_index(),
    starting_ids=0,
    category='Oil and natural gas pipelines',
    fac_alias="PIPELINES",
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    # on_offshore = None,
    fac_name='Name',
    # fac_id = None,
    fac_type='type',
    # install_date = None,
    # fac_status = None,
    # op_name = None,
    commodity='commodity',
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    # pipe_diameter_mm = None,
    pipe_length_km='PIPELINE_LENGTH_KM',
    # pipe_material = None
)


save_spatial_data(
    data_,
    file_name="middleeast_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% RUSSIA CASPIAN  # TODO
# =============================================================================
os.chdir(pubdata)
fp1 = (r"China+SE_Asia\Asia_Pipelines\Russia_Caspian_Pipelines_Final.shp")
pipelines1 = read_spatial_data(fp1, table_gradient=True)
# Transform CRS to EPSG 4326 and calculate and append lat and lon vvalues [latitude_calc, longitude_calc]
pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

pipelines111 = calculate_pipeline_length_km(
    pipelines11,
    attrName="PIPELINE_LENGTH_KM"
)


# Assign countries to pipelines - changed on 1/11/2023 to use new function
pipelines1111 = assign_countries_to_feature(pipelines111,
                                            gdf_country_colname='COUNTRY_',
                                            gdf_uniqueid_field='OBJECTID',
                                            boundary_geoms=my_boundary_geoms,
                                            overwrite_country_field=True)

# =============================================================================
# %% RUSSIA CASPIAN - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines1111,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    # fac_name="Name",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )


save_spatial_data(
    pipelines_final_no_z,
    file_name="russia_caspian_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% SAUDI ARABIA -- TEMPORARILY DROP bc oginfra.com duplicates these a lot
# =============================================================================
# os.chdir(pubdata)
# fp = r"Middle_East+Caspian\Saudi_Arabia\other_data\SAUDI_pipelines_.shp"
# data = read_spatial_data(fp, table_gradient=True)

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)
# # Calculate the length of each polyline, and add the results in a new column
# data = calculate_pipeline_length_km(data, attrName="PIPELINE_LENGTH_KM")

# # Data manipulation / processing if needed
# # -----------------------------------------------------------------------------
# # Create a status column to capture "Operating" or "Proposal" details provided in the 'Name' column (where available)
# data['stat'] = None
# data.loc[data.Name.str.contains('Proposal'), 'stat'] = 'Proposed'
# data.loc[data.Name.str.contains('Operating'), 'stat'] = 'Operating'

# # Create commodity column
# data['comm'] = None
# data.loc[data.Name.str.contains('Gas'), 'comm'] = 'Gas'
# data.loc[data.Name.str.contains('Oil'), 'comm'] = 'Oil'

# # Create new Name column, that keeps all pipeline names EXCEPT for the very
# # generic one "Operating Saudi Arabia Pipelines"
# data['name_new'] = None
# data.loc[(data.stat != 'Operating'), 'name_new'] = data.Name

# =============================================================================
# %% SAUDI ARABIA - Integration + Export
# =============================================================================
# data_, errors = integrate_pipelines(
#     data,
#     starting_ids=0,
#     category='Oil and natural gas pipelines',
#     fac_alias="PIPELINES",
#     country='Saudi Arabia',
#     # state_prov = None,
#     src_ref_id='161',
#     src_date='2018-04-01',
#     on_offshore='Onshore',  # pipelines appeared onshore in plot
#     fac_name='name_new',
#     # fac_id = None,
#     # fac_type = None,
#     # install_date = None,
#     fac_status='stat',
#     # op_name = None,
#     commodity='comm',
#     # liq_capacity_bpd = None,
#     # liq_throughput_bpd = None,
#     # gas_capacity_mmcfd = None,
#     # gas_throughput_mmcfd = None,
#     # pipe_diameter_mm = None,
#     pipe_length_km='PIPELINE_LENGTH_KM',
#     # pipe_material = None
# )


# save_spatial_data(
#     data_,
#     file_name="saudiarabia_pipelines",
#     schema_def=True,
#     schema=schema_PIPELINES,
#     file_type="GeoJSON",
#     out_path=results_folder)


# =============================================================================
# %% SOUTHEAST ASIA  # TODO
# =============================================================================
os.chdir(pubdata)
fp1 = r"China+SE_Asia\Asia_Pipelines\SE_Asia_Pipelines_Final.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)

# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

pipelines111 = calculate_pipeline_length_km(
    pipelines11,
    attrName="PIPELINE_LENGTH_KM"
)


# Assign countries to pipelines - changed on 1/11/2023 to use new function
pipelines1111 = assign_countries_to_feature(pipelines111,
                                            gdf_country_colname='COUNTRY_',
                                            gdf_uniqueid_field='OBJECTID',
                                            boundary_geoms=my_boundary_geoms,
                                            overwrite_country_field=True)

# pipelines1111.plot()


# =============================================================================
# %% SOUTHEAST ASIA - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines1111,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    # fac_name="Name",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )


save_spatial_data(
    pipelines_final_no_z,
    file_name="se_asia_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% THAILAND
# =============================================================================
os.chdir(pubdata)
fp1 = r"China+SE_Asia\Thailand\Oil_pipeline_Thailand.kml.shp"
oil = read_spatial_data(fp1, table_gradient=True)
oil['TYPE1'] = 'OIL'
oil1 = transform_CRS(oil, target_epsg_code="epsg:4326", appendLatLon=True)
oil2 = calculate_pipeline_length_km(oil1, attrName="PIPELINE_LENGTH_KM")

# =============================================================================
# %% THAILAND  - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    oil2,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Thailand",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="ONOFF",
    # fac_name="NAME1",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "TYPE1",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)
# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)

save_spatial_data(
    pipelines_final_no_z,
    file_name="thailand_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)

# =============================================================================
# %% TURKMENISTAN
# =============================================================================
os.chdir(pubdata)
fp1 = "Middle_East+Caspian\\Turkmenistan\\Oil_Gas_Infra_.com\\Pipelines\\Gas_pipelines_Turkmenistan.kml.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
pipelines1['COMMODITY'] = 'GAS'
pipelines11 = calculate_pipeline_length_km(pipelines1, attrName="PIPELINE_LENGTH_KM")
fp2 = "Middle_East+Caspian\\Turkmenistan\\Oil_Gas_Infra_.com\\Pipelines\\Oil_pipelines_Turkmenistan.kml.shp"
pipelines2 = read_spatial_data(fp2, table_gradient=True)
pipelines2['COMMODITY'] = 'OIL'
pipelines22 = calculate_pipeline_length_km(pipelines2, attrName="PIPELINE_LENGTH_KM")


pipelines_concat = gpd.GeoDataFrame(pd.concat([pipelines11, pipelines22], ignore_index=True))
pipelines_concat_1 = pipelines_concat.set_crs(4326)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []

for idx1_, row1_ in tqdm(pipelines_concat_1.iterrows(), total=pipelines_concat_1.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if name == 'Gas Pipeline':
        names_null = "Gas"
        names.append(names_null)
    elif name == 'Oil Pipeline':
        names_null = "Oil"
        names.append(names_null)
    elif name == 'Untitled Path':
        names_null = "N/A"
        names.append(names_null)
    else:
        names.append(name)

pipelines_concat_1['NAMES'] = names

# Assign countries to pipelines - changed on 1/11/2023 to use new function
pipelines_concat_1 = pipelines_concat_1.reset_index(drop=False)  # create unique ID for next step

pipelines_concat_11 = assign_countries_to_feature(pipelines_concat_1,
                                                  # gdf_country_colname = 'COUNTRY',
                                                  gdf_uniqueid_field='index',
                                                  boundary_geoms=my_boundary_geoms,
                                                  overwrite_country_field=True)

# =============================================================================
# %% TURKMENISTAN - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines_concat_11,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="Offshore",
    fac_name="NAMES",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "STATUS",
    # op_name = "",
    commodity="COMMODITY",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)
# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )

save_spatial_data(
    pipelines_final_no_z,
    file_name="turkmenistan_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% UNITED KINGDOM
# =============================================================================
os.chdir(pubdata)
fp1 = "Europe\\United_Kingdom\\Pipelines\\Layer_pipeline_eab_UnitedKingdom.kml.shp"
pipelines1 = read_spatial_data(fp1, table_gradient=True)
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
pipelines11 = transform_CRS(pipelines1, target_epsg_code="epsg:4326", appendLatLon=True)

pipelines111 = calculate_pipeline_length_km(
    pipelines11,
    attrName="PIPELINE_LENGTH_KM"
)

# Assign countries to pipelines - changed on 1/11/2023 to use new function
pipelines1111 = assign_countries_to_feature(pipelines111,
                                            # gdf_country_colname = 'COUNTRY',
                                            gdf_uniqueid_field='Name',
                                            boundary_geoms=my_boundary_geoms,
                                            overwrite_country_field=True)

# =============================================================================
# %% UNITED KINGDOM - Integration + Export
# =============================================================================
pipelines_final, errors = integrate_pipelines(
    pipelines1111,
    starting_ids=0,
    category="Oil and natural gas pipelines",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    on_offshore="Offshore",
    fac_name="Name",
    # fac_id = "",
    # fac_type = "",
    # install_date = "",
    # fac_status = "",
    # op_name = "",
    # commodity = "",
    # liq_capacity_bpd = "",
    # liq_throughput_bpd = "",
    # gas_capacity_mmcfd = "",
    # gas_throughput_mmcfd = "",
    # pipe_diameter_mm = "",
    pipe_length_km="PIPELINE_LENGTH_KM",
    # pipe_material = ""
)

# Remove z values from geometry
pipelines_final_no_z = strip_z_coord(pipelines_final)
# flattened_pipelines= flatten_gdf_geometry(pipelines_final_no_z, 'MultiLineString' )


save_spatial_data(
    pipelines_final_no_z,
    file_name="united_kingdom_pipelines",
    # schema_def = True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% SE ASIA: INDONESIA, MALAYSIA  # TODO
# =============================================================================
os.chdir(pubdata)
# Read data
fpasia = r"China+SE_Asia\SE_Asia_Indonesia_Malaysia_Pipelines\se_asia_indonesia_malaysis_pipelines.shp"
pipes_se = read_spatial_data(fpasia)

pipes_se.Name.unique()

# Transform CRS
pipes_se2 = transform_CRS(pipes_se)

pipes_v2 = calculate_pipeline_length_km(pipes_se2)

pipes_se, _pipes = integrate_pipelines(
    pipes_v2.reset_index(drop=True),
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country=None,
    state_prov=None,
    src_ref_id="22",
    src_date="2014-11-01",
    on_offshore=None,
    fac_name=None,
    fac_id=None,
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

save_spatial_data(
    pipes_se,
    "se_asia_ind_malay_pipelines",
    schema_def=False,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=results_folder
)
