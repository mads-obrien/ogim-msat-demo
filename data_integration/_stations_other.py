# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global STATIONS - OTHER.

# TODO:
[x] standardize import statements and CWD setting
[] standardize spacing between sections
[] alphabetize countries
[] update all file paths

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
# import numpy as np
# from tqdm import tqdm
# import datetime
import shapely.wkt

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, standardize_dates_hifld_us,
                     transform_CRS, integrate_facs, save_spatial_data,
                     schema_OTHER, read_spatial_data, dict_us_states,
                     deduplicate_with_rounded_geoms)
from ogim_translation_utils import (translate_argentina_installations_fac_types,
                                    replace_special_chars_in_column_argentina)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
# pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
save_path_stns_other = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% UNITED STATES
# =============================================================================
os.chdir(v24data)
# NOTE - this dataset is no longer hosted by HIFLD online but we'll keep it
# and attribute the last visited date to when it was last live.
pump_stn = read_spatial_data(r"united_states\national\HIFLD\POL_Pumping_Station.geojson")
pump_stn = transform_CRS(pump_stn, appendLatLon=True)
pump_stn = pump_stn.query("COUNTRY=='USA'")

pump_stn = replace_row_names(pump_stn, "STATE", dict_names=dict_us_states)
# pump_stn = standardize_dates_hifld_us(pump_stn,
#                                       attrName="SOURCEDATE",
#                                       newAttrName="src_dates")

# Fix type descriptions
pump_stn = replace_row_names(pump_stn,
                             "TYPE",
                             dict_names={'POL PUMPING STATION': 'PETROLEUM, OIL, AND LUBRICANTS PUMPING STATION'})

# =============================================================================
# %% UNITED STATES - Integration + Export
# =============================================================================
# ---------------------------------------------------------------------------
pump_stn_integrated, stn_errors_3 = integrate_facs(
    pump_stn,
    starting_ids=1,
    category="STATIONS - OTHER",
    fac_alias="OTHER",
    country="United States",
    state_prov="STATE",
    src_ref_id="94",
    src_date="2021-04-23",  # most recent SOURCEDATE
    on_offshore="Onshore",
    fac_name="NAME",
    fac_id="PUMPID",
    fac_type="TYPE",
    # install_date=None,
    # fac_status=None,
    op_name="OPERATOR",
    commodity="COMMODITY",
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
    pump_stn_integrated,
    "united_states_stations_other",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=save_path_stns_other
)


# ===========================================================================
# %% ARGENTINA
# There are two datasets for facilities in Argentina, one that includes mostly
# facilities and another that includes mostly equipment on the site of
# facilities (e.g., batteries, tanks, valves, etc.)
# ===========================================================================
os.chdir(v24data)

# Read "pumping stations" dataset
fp0_ = r"argentina\facilities\estaciones-de-bombeo-shp.shp"
pstations = read_spatial_data(fp0_, specify_encoding=True, data_encoding="utf-8")
pstations = transform_CRS(pstations, appendLatLon=True)
pstations['src_ref'] = "260"


# ---------------------------------------------------------------------------
# Read "hydrocarbon installations" dataset
fp1_ = r"argentina\facilities\instalaciones-hidrocarburos-instalaciones-res-318-shp.shp"
arg_fac1 = read_spatial_data(fp1_, specify_encoding=True, data_encoding="utf-8")
arg_fac1 = transform_CRS(arg_fac1, appendLatLon=True)
arg_fac1['src_ref'] = "109"
# Only keep facilities with valid geometries
arg_fac1 = arg_fac1[arg_fac1.geometry.notna()]

# Set facility name column
arg_fac1['fac_name'] = arg_fac1['INSTALACIO']

# Translate contents of "TIPO" field, based on the translations we've previously done
arg_fac1['new_fac_type'] = translate_argentina_installations_fac_types(arg_fac1,
                                                                       fac_type_col_spanish='TIPO')

# Drop duplicate records (same attributes & identical or nearly-identical geometries),
# just those records that are indentical in the fields that we care about.
arg_fac1 = deduplicate_with_rounded_geoms(arg_fac1,
                                          column_subset=['fac_name',
                                                         'IDINSTALAC',
                                                         'new_fac_type',
                                                         'EMPRESA'],
                                          desired_precision=5)

# ---------------------------------------------------------------------------
# Read "installation characteristics" dataset
fp2_ = r"argentina\facilities\instalaciones-hidrocarburos-instalaciones-res-319-93-p-caractersticos--shp.shp"
arg_fac2 = read_spatial_data(fp2_, specify_encoding=True, data_encoding="utf-8")
arg_fac2 = transform_CRS(arg_fac2, appendLatLon=True)
arg_fac2['src_ref'] = "110"
# Only keep facilities with valid geometries
arg_fac2 = arg_fac2[arg_fac2.geometry.notna()]

# Set facility name column, and replace missing values
arg_fac2['fac_name'] = arg_fac2['NPC']
arg_fac2['fac_name'].replace({'Sin Nombre': 'N/A',
                              'Sin Dato': 'N/A'}, inplace=True)

# Translate contents of "TIPO" field, based on the translations we've previously done
arg_fac2['new_fac_type'] = translate_argentina_installations_fac_types(arg_fac2,
                                                                       fac_type_col_spanish='TIPO')

# Replace characters that are incorrectly encoded in this dataset
cols2fix = ['NPC', 'fac_name', 'DESCPC']
for col in cols2fix:
    arg_fac2[col] = replace_special_chars_in_column_argentina(arg_fac2, col)
# Test for what rows have special characters
# spec_char_rows = arg_fac2[arg_fac2.select_dtypes('object').apply(lambda x: x.str.contains('[\^|\Ã|\~]', regex=True)).any(axis=1)]


# Drop duplicate records (same attributes & identical or nearly-identical geometries),
# just those records that are indentical in the fields that we care about.
arg_fac2 = deduplicate_with_rounded_geoms(arg_fac2,
                                          column_subset=['fac_name',
                                                         'new_fac_type',
                                                         # 'TIPO',
                                                         'EMPRESA_IN',
                                                         'DESCPC'],
                                          desired_precision=5)

# After this deduplication, check for additional duplicates that have the same
# attributes and near-identical geometries EXCLUDING the 'DESCPC' field
arg_fac2 = deduplicate_with_rounded_geoms(arg_fac2,
                                          column_subset=['fac_name',
                                                         'new_fac_type',
                                                         'EMPRESA_IN'],
                                          desired_precision=5)

# TODO - fix encoding issues!
# # Manually fix encoding issues with FAC_NAME
# names_ = {
#     'CERRO DRAGÃ³N 01': 'CERRO DRAGON 01',
#     'CERRO DRAGÃ³N 02': 'CERRO DRAGON 02',
#     'PUENTE DE REGULACIÃ³N NEUBA EFO': 'PUENTE DE REGULACION NEUBA EFO'
# }
# stns_oth_ARG1["FAC_NAME"] = stns_oth_ARG1["FAC_NAME"].replace(names_)


# ---------------------------------------------------------------------------
# Select "OTHER" type facility records from datasets 1 and 2
stn_other_keywords = [
    'GAS FISCAL MEASUREMENT POINT',  # dataset 1
    'LEASE AUTOMATIC CUSTODY TRANSFER UNIT',  # dataset 1
    'Regulatory plant',  # dataset 2
    'CRUDE FISCAL MEASUREMENT POINT (LACT)',  # dataset 2
    'GAS FISCAL MEASUREMENT POINT (PIST)',  # dataset 2
    'Gasoline and condensate measurement point',  # dataset 2
    'LPG measurement point'  # dataset 2
]

arg_oth_1 = arg_fac1[arg_fac1['new_fac_type'].isin(stn_other_keywords)]
arg_oth_2 = arg_fac2[arg_fac2['new_fac_type'].isin(stn_other_keywords)]


# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Integrate pumping stations
pstations_integrated, err_ = integrate_facs(
    pstations.reset_index(),
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2023-07-13",
    # on_offshore=None,
    # fac_name=None,
    fac_id="NRO_REGIST",
    # fac_type=None,
    # install_date=None,
    # fac_status=None,
    op_name="RAZON_SOCI",
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

# Integrate dataset 1
arg_oth_1_integrated, err_ = integrate_facs(
    arg_oth_1.reset_index(),
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2023-05-02",
    # on_offshore=None,
    fac_name="fac_name",
    fac_id="IDINSTALAC",
    fac_type="new_fac_type",
    # install_date=None,
    # fac_status=None,
    op_name="EMPRESA",
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


# Integrate dataset 2
arg_oth_2_integrated, err_ = integrate_facs(
    arg_oth_2.reset_index(),
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2024-04-01",
    # on_offshore=None,
    fac_name="fac_name",
    # fac_id=None,
    fac_type="new_fac_type",
    # install_date=None,
    # fac_status=None,
    op_name="EMPRESA_IN",
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


# Concatenate two datasets
arg_oth_final = pd.concat([arg_oth_1_integrated,
                           arg_oth_2_integrated]).reset_index(drop=True)

save_spatial_data(
    arg_oth_final,
    file_name="argentina_stations_other",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_path_stns_other
)


# =============================================================================
# %% BOLIVIA
# =============================================================================
os.chdir(v24data)
fp = r'bolivia\geobolivia\estaciones2013.csv'
bol_stations_csv = pd.read_csv(fp)
bol_stations = gpd.GeoDataFrame(bol_stations_csv,
                                geometry=bol_stations_csv.geometry.map(shapely.wkt.loads),
                                crs=4326)
bol_stations = transform_CRS(bol_stations, appendLatLon=True)

# Subset to other stations only
bol_stations_other = bol_stations.query("sitetype == ['Regulator Station', 'Meter Station', 'Pump Station']")

# =============================================================================
# %% BOLIVIA - Integration + Export
# =============================================================================
bol_stations_other_integrated, stns_err = integrate_facs(
    bol_stations_other,
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Bolivia",
    # state_prov=None,
    src_ref_id="123",
    src_date="2013-03-06",
    on_offshore='ONSHORE',
    fac_name="sitename",
    # fac_id="gml_id",
    fac_type="sitetype",
    # install_date=None,
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

save_spatial_data(
    bol_stations_other_integrated,
    "bolivia_stations_other",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=save_path_stns_other
)


# =============================================================================
# %% BRAZIL  # DONE
# =============================================================================
os.chdir(v24data)
bz_deliv = gpd.read_file(r'brazil\WebMap_EPE\Citygates.shp')
bz_deliv = transform_CRS(bz_deliv,
                         target_epsg_code="epsg:4326",
                         appendLatLon=True)
# Rename a few columns; I've confirmed that "Transporta" means company here
bz_deliv = bz_deliv.rename(columns={"Transporta": "company"}, errors="raise")

# Remove the stations which don't exist yet
bz_deliv = bz_deliv.query("Classifica == 'Existente'").reset_index(drop=True)

# Translate "Segmento" values
bz_deliv.Segmento.replace({
    'Industrial, Comercial e outros': 'Industrial, commercial, and other use',
    'Termeletrico': 'Thermoelectric',
    # 'Downstream': 'Downstream',
    'Industrial (Fertilizantes)': 'Industrial (Fertilizers)'},
    inplace=True)

bz_deliv['factype'] = 'Natural gas delivery point - ' + bz_deliv.Segmento

# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
bz_deliv_integrated, err_3 = integrate_facs(
    bz_deliv,
    starting_ids=1,
    category="STATIONS - OTHER",
    fac_alias="OTHER",
    country="BRAZIL",
    # state_prov="STATE",
    src_ref_id="267",
    src_date="2024-01-01",
    on_offshore="Onshore",
    fac_name="Nome",
    # fac_id="PUMPID",
    fac_type="factype",
    # install_date=None,
    # fac_status=None,
    op_name="company",
    # commodity="COMMODITY",
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

save_spatial_data(
    bz_deliv_integrated,
    "brazil_stations_other",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=save_path_stns_other
)

# ===========================================================================
# %% VENEZUELA
# ===========================================================================
os.chdir(v24data)
fp_ = r"venezuela\INSTALACIONES_F_CarlosGonzales.shp"
vez_fac = read_spatial_data(fp_)
vez_fac = transform_CRS(vez_fac, appendLatLon=True)

# Translate commodity type
vez_fac.MAPA.replace({'PETROLERO': 'OIL',
                      'GASIFERO': 'GAS'}, inplace=True)

# Translate facility type descriptions
dict_ = {
    'PLANTA PETROQUIMICA': 'PETROCHEMICAL PLANT',
    'PLANTA DE DISTRIBUCION DEL PRODUCTO': 'PRODUCT DISTRIBUTION PLANT',
    'REFINERIA': 'REFINERY',
    'TERMINAL DE EMBARQUE': 'SHIPPING TERMINAL',
    'MONOBOYA': 'MONOBOYA',
    'REFINERIA (PROYECTO A FUTURO)': 'REFINERY (FUTURE PROJECT)',
    'ESTACION DE REBOMBEO (PROYECTO A FUTURO)': 'REPUMP STATION (FUTURE PROJECT)',
    'PATIO TANQUE': 'TANK YARD',
    'COMPLEJO MEJORADOR DE CRUDO (PROYECTO A FUTURO)': 'CRUDE IMPROVEMENT COMPLEX (FUTURE PROJECT)',
    'COMPLEJO MEJORADOR DE CRUDO': 'CRUDE IMPROVEMENT COMPLEX',
    'PLANTA ELECTRICA A GAS': 'GAS POWER PLANT',
    'PLANTA PROCESADORA DE GAS': 'GAS PROCESSING PLANT',
    'PLANTA PROCESADORA DE GAS (DESINCORPORACION)': 'GAS PROCESSING PLANT (DISINCORPORATION)',
    'PLANTA DE GENERACION TERMOELECTRICA': 'THERMOELECTRIC GENERATION PLANT',
    'PLANTA COMPRESORA': 'COMPRESSOR PLANT',
    'ESTACION DE RECOLECCION': 'COLLECTION STATION',
    'PLANTA DE FRACCIONAMIENTO': 'FRACTIONATION PLANT',
    'CENTRO DE OPERACIONES': 'OPERATIONS CENTER',
    'ESTACIONES DE VALVULAS': 'VALVE STATIONS',
    'PLANTA COMPRESORA (PROYECTO A FUTURO)': 'COMPRESSOR PLANT (FUTURE PROJECT)',
    'PLANTA INDUSTRIAL': 'INDUSTRIAL PLANT',
    'COMPLEJO MEJORADOR': 'IMPROVING COMPLEX',
    'PLANTA DE FRACCIONAMIENTO (PROYECTO A FUTURO)': 'FRACTIONATION PLANT (FUTURE PROJECT)',
    'PLANTA PROCESADORA DE GAS (PROYECTO A FUTURO)': 'GAS PROCESSING PLANT (FUTURE PROJECT)',
    'COMPLEJO MEJORADOR (PROYECTO A FUTURO)': 'IMPROVING COMPLEX (FUTURE PROJECT)',
    'PLATAFORMA': 'PLATFORM'
}

vez_fac = replace_row_names(vez_fac, colName="TIPO", dict_names=dict_)

# Stations - Other
stns_ = [
    'REPUMP STATION (FUTURE PROJECT)',
    'VALVE STATIONS'
]
oth_stns_VEZ = vez_fac[vez_fac['TIPO'].isin(stns_)]


# =============================================================================
# %%VENEZUELA - Integration + Export
# =============================================================================
stns3_VEZ, stns_err = integrate_facs(
    oth_stns_VEZ,
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Venezuela",
    # state_prov=None,
    src_ref_id="132",
    src_date="2017-01-01",
    # on_offshore=None,
    fac_name="NOMBRE",
    fac_id="Id",
    fac_type="TIPO",
    # install_date=None,
    # fac_status=None,
    # op_name=None,
    commodity='MAPA',
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

save_spatial_data(
    stns3_VEZ,
    "venezuela_stations_other",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=save_path_stns_other
)


# =============================================================================
# %% NIGERIA - REMOVE SRC_ID 159
# =============================================================================
# os.chdir(pubdata)
# fp = r"Africa\Nigeria\Nigeria\StationGas.shp"
# data = read_spatial_data(fp, table_gradient=True)

# # drop all columns except two useful ones
# data = data.filter(['Name', 'geometry'])

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# '''
# Types of facilities mentioned in the 'Name' column
#     Metering Station
#     Compressor Station or C/S
#     Pigging Station
#     LBV (Line Break Valve control pressure)
#     Flare Stack Station
#     Valve Station
#     Helipad
#     Tie-in
# '''
# # https://stackoverflow.com/questions/11350770/filter-pandas-dataframe-by-substring-criteria/55335207#55335207

# # Create a new factype field to populate
# # ---------------------------------------------------------------------------
# data['factype'] = None
# # Create lowercase version of of the 'Name' field, for easier comparison of strings
# data['Name_casefold'] = data.Name.str.lower()

# # Make a list of the multiple substrings that indicate a particular facility type
# # Include mis-spellings and abbreviations I've seen in the raw data
# # ---------------------------------------------------------------------------
# terms_meter = ['metering', 'metetring', 'mertering', 'm/s']
# terms_cs = ['compressor', 'c/s']
# terms_lbv = ['lbv', 'lvb']

# # Populate 'factype' field with stations that are a KNOWN TYPE, based on what's in their 'Name' field
# # ---------------------------------------------------------------------------
# data.loc[data['Name_casefold'].str.contains('|'.join(terms_lbv)), 'factype'] = 'Line break valve (LBV) control pressure'
# data.loc[data['Name_casefold'].str.contains('pigging'), 'factype'] = 'Pigging station'
# data.loc[data['Name_casefold'].str.contains('node'), 'factype'] = 'Node'
# data.loc[data['Name_casefold'].str.contains('tie-in'), 'factype'] = 'Tie-in'
# data.loc[data['Name_casefold'].str.contains('|'.join(terms_cs)), 'factype'] = 'Compressor station'
# data.loc[data['Name_casefold'].str.contains('|'.join(terms_meter)), 'factype'] = 'Metering station'
# data.loc[data['Name_casefold'].str.contains('flare stack'), 'factype'] = 'Flare stack station'
# data.loc[data['Name_casefold'].str.contains('valve station'), 'factype'] = 'Valve station'
# data.loc[data['Name_casefold'].str.contains('regulating'), 'factype'] = 'Regulating station'
# data.loc[data['Name_casefold'].str.contains('condensate tank'), 'factype'] = 'Condensate tank'

# # Add proper OGIM categories for each record
# # First, set default 'category' value to "other" .....most things like metering stations fall in this category
# # ---------------------------------------------------------------------------
# data['category_new'] = 'Stations (other)'
# # Change the record's CATEGORY value for compressor station points only
# data.loc[data.factype == 'Compressor station', 'category_new'] = 'Natural gas compressor stations'
# # data.loc[data.factype=='Condensate tank', 'category_new'] = 'Petroleum storage and terminals'

# # Create two separate gdfs for Compressor Stations and Other Stations
# # ---------------------------------------------------------------------------
# nigeria_cs = data[data.category_new == 'Natural gas compressor stations']
# # Retain only "Other Stations" where some O&G facility type info is known
# # This removes points for things like helipads, housing complexes, offices
# nigeria_stations = data[(data.category_new == 'Stations (other)') & (data.factype.notnull()) & (~data.factype.isin(['Line break valve (LBV) control pressure', 'Node', 'Tie-in', 'Condensate tank']))]

# # Equipment and components
# # ---------------------------------------------------------------------------
# nigeria_components = data.query("factype == ['Line break valve (LBV) control pressure', 'Node', 'Tie-in', 'Condensate tank']")

# nigeria_cs = nigeria_cs.reset_index(drop=True)
# nigeria_stations = nigeria_stations.reset_index(drop=True)

# # =============================================================================
# # %% NIGERIA - Integration + Export
# # =============================================================================
# nigeria_stations_, errors = integrate_facs(
#     nigeria_stations,
#     starting_ids=0,
#     category='STATIONS - OTHER',
#     fac_alias='OTHER',
#     country='Nigeria',
#     # state_prov = None,
#     src_ref_id='159',
#     src_date='2020-11-01',
#     on_offshore='Onshore',
#     fac_name='Name',
#     # fac_id=None,
#     fac_type='factype',
#     # spud_date=None,
#     # comp_date=None,
#     # drill_type=None,
#     # install_date=None,
#     # fac_status=None,
#     # op_name=None,
#     # commodity=None,
#     # liq_capacity_bpd=None,
#     # liq_throughput_bpd=None,
#     # gas_capacity_mmcfd=None,
#     # gas_throughput_mmcfd=None,
#     # num_compr_units=None,
#     # num_storage_tanks=None,
#     # site_hp=None,
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )

# save_spatial_data(
#     nigeria_stations_,
#     "nigeria_stations_other",
#     schema_def=True,
#     schema=schema_OTHER,
#     file_type="GeoJSON",
#     out_path=save_path_stns_other
# )


# ===========================================================================
# %% NIGERIA - OIL SPILL MONITOR DATASET
# ===========================================================================
os.chdir(v24data)
fp = r"nigeria/ArcGISOnline/OilSpillDatabase.shp"
data = read_spatial_data(fp, table_gradient=False)
# Drop records with missing / NAN geometries
data = data[~(data.geometry.is_empty | data.geometry.isna())].reset_index(drop=True)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

print("====================")
print(data.columns)

# First, inspect the attributes and see what we know about these records
# data.Condensatn.value_counts()
'''
Crude Oil                    6399
Gas                           604
Other                         389
Condensat                     274
Refined Products              205
Chemicals or Drilling Mud      18
'''

# Whoever originally cleaned this data used a reckless find-and-replace, which
# replaced every instance of the letters "st" with "Storage Tank", and every
# instance of "pl" with "Pipeline",  even when "st" or "pl" was
# part of a substring....
# I confirmed on the Nigeria Oil Spill Monitor website (https://oilspillmonitor.ng/)
# that these abbreviations are used in their 'type of facility' field
# data.Type_of_Fa.value_counts()
'''
Pipeline                                                                              2933
Flow Line                                                                             1253
Well Head                                                                              289
Other                                                                                  186  # drop for now until we learn more about this category
mf                                                                                      66  # change to manifold
Flow Storage Tankation                                                                  60  # change to flow station
Floating or Production or Storage TankoRigae  or Offloading PipelineaTank Farmorms      18  # change to FPSO
Storage Tank                                                                            17  # KEEP this
Tanker                                                                                   9  # in this dataset, Tanker means truck, so remove these records
Rig                                                                                      3
Tank Farm                                                                                2
Pumping Storage Tankation                                                                1 # change to Pumping station
Compressor Pipelineant                                                                   1 # compressor plant.... in this case, this one IS a compressor station.
Fuel Dispensation Storage Tankation                                                      1 # fuel dispensation station, aka gas station. REMOVE this record
'''
# ---------------------------------------------------------------------------
# Drop data rows of Facility Types I don't want to include
types2drop = ['Other', 'Tanker', 'Fuel Dispensation Storage Tankation']

data["Type_of_Fa"] = data["Type_of_fa"]
data = data[-data.Type_of_Fa.isin(types2drop)]

# Fix and re-name FAC_TYPE values I know to be wrong (and RETAIN the ones I don't want to change)
# ---------------------------------------------------------------------------
# First, create dictionary of the old fac_type (key) and the new fac_type (value)
# ---------------------------------------------------------------------------
factypedict = {
    'mf': 'Manifold',
    'Flow Storage Tankation': 'Flow Station',
    'Floating or Production or Storage TankoRigae  or Offloading PipelineaTank Farmorms': 'Floating production storage and offloading',
    'Pumping Storage Tankation': 'Pumping Station',
    'Compressor Pipelineant': 'Compressor Station'
}

data['Type_of_Fa'] = data['Type_of_Fa'].map(factypedict).fillna(data['Type_of_Fa'])

# % Fill in null fac_type values, based on other clues elsewhere in the table
# ---------------------------------------------------------------------------
# Location description
data["Site_Locat"] = data["Site_locat"]

# Spill area
data["Spill_Area"] = data["Spill_area"]

# Create lowercase version of Site_Locat attribute, for easier string comparison
data['Site_Locat_casefold'] = data.Site_Locat.str.lower()
# ---------------------------------------------------------------------------
# lists of multiple substrings present in the Site_Locat field that indicate facility type
# include all permutations and mis-spellings
terms_platform = ['pp', 'platform']
terms_tank = ['tank']
terms_well = ['well', 'wellhead']
terms_flowline = ['flow line', 'flowline', 'f/l', ' fl', 'delievery line', 'delivery line', 'd/l', 't/l', 'trunk line', 'trunkline', ' tl']
terms_pipe = ['pipeline', 'p/l', ' pl ']
terms_flowstat = ['flow station', 'flowstation', 'f/s']
# terms_manifold = ['maniford','manifold']
# ---------------------------------------------------------------------------
# Drop cells where Site_Locat is null or else the subsequent section won't work
data = data[-data.Site_Locat.isna()]
# ---------------------------------------------------------------------------
# Populate 'factype' field for points where 'Site_Locat' field describes the facility type
data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_platform)), 'Type_of_Fa'] = 'Offshore platforms'
data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_tank)), 'Type_of_Fa'] = 'Storage Tank'
data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_well)), 'Type_of_Fa'] = 'Well Head'
data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_flowline)), 'Type_of_Fa'] = 'Flow Line'
data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_pipe)), 'Type_of_Fa'] = 'Pipeline'
data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_flowstat)), 'Type_of_Fa'] = 'Flow Station'
data.loc[data['Site_Locat_casefold'].str.contains('fpso'), 'Type_of_Fa'] = 'Floating production storage and offloading'

print(data.Type_of_Fa.value_counts())
print("Total number of null facility types:", data.Type_of_Fa.isnull().sum())
# ---------------------------------------------------------------------------
# Drop cells where Type_of_Fa is null -- we simply don't know enough about what these points are to include them in OGIM
data = data[-data.Type_of_Fa.isna()]
# Drop records that are simply point locations along a pipeline -- not the data format we need for this infra type
types2drop = ['Pipeline', 'Flow Line']
data = data[-data.Type_of_Fa.isin(types2drop)]
# ---------------------------------------------------------------------------
# Create CATEGORY column that corresponds to my OGIM categories,
# and assign a CATEGORY attribute to records based on their FAC_TYPE values

data['category_new'] = None
data.loc[data.Type_of_Fa == 'Well Head', 'category_new'] = 'Oil and gas wells'
data.loc[(data.Type_of_Fa == 'Floating production storage and offloading') | (data.Type_of_Fa == 'Offshore platforms'), 'category_new'] = 'Offshore platforms'
data.loc[data.Type_of_Fa == 'other:compressor plant', 'category_new'] = 'Natural gas compressor stations'
data.loc[(data.Type_of_Fa == 'Storage Tank') | (data.Type_of_Fa == 'Tank Farm'), 'category_new'] = 'Petroleum storage and terminals'

# Create category for "Stations - Other"
otherfaclist = ['Manifold', 'Flow Station', 'Pumping Station']  # Excluding "RIG" from "Stations - Other"
data.loc[data.Type_of_Fa.str.contains('|'.join(otherfaclist)), 'category_new'] = 'Stations - other'

print(data.category_new.value_counts())
print("Num. of records with null CATEGORY (should be zero):", data.category_new.isnull().sum())

# Create 'on_off' shore category
data['on_off'] = 'Onshore'
data.loc[data.Spill_Area == 'of', 'on_off'] = 'Offshore'

# Create separate gdfs for each infra category
statoth = data[data.category_new == 'Stations - other'].reset_index(drop=True)

# Replace 'Site_locat" values containing '' with " (denotes inches)
statoth.Site_locat = statoth.Site_locat.str.replace("''", '"')

# TODO - Strip special characters from the 'Site_Locat' field

# Drop records that are duplicated in the fields we care about
statoth = deduplicate_with_rounded_geoms(statoth,
                                         column_subset=['Company',
                                                        'Type_of_Fa',
                                                        'Site_Locat_casefold'],
                                         desired_precision=5)

# =============================================================================
# %% NIGERIA - OIL SPILL MONITOR DATASET - Integration + Export
# =============================================================================
statoth_NIG2, errors = integrate_facs(
    statoth.reset_index(drop=True),
    starting_ids=0,
    category='Stations - other',
    fac_alias='OTHER',
    country='Nigeria',
    # state_prov = None,
    src_ref_id='165',
    src_date='2019-11-01',
    on_offshore='on_off',
    fac_name='Site_Locat',
    # fac_id = None,
    fac_type='Type_of_Fa',
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    # install_date = None,
    # fac_status = None,
    op_name='Company',
    # commodity = None,
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    # num_compr_units = None,
    # num_storage_tanks = None,
    # site_hp = None,
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    statoth_NIG2,
    "nigeria_stations_other_part_02",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=save_path_stns_other
)
