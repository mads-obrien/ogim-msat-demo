# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global INJECTION AND DISPOSAL FACILITIES.

@author: maobrien, momara, ahimmelberger
"""
import os
# import re
import pandas as pd
import geopandas as gpd
import numpy as np
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     save_spatial_data, schema_LNG_STORAGE, read_spatial_data,
                     dict_us_states, deduplicate_with_rounded_geoms)
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
results_folder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'


# ===========================================================================
# %% ARGENTINA
# There are two datasets for facilities in Argentina, one that includes mostly
# facilities and another that includes mostly equipment on the site of
# facilities (e.g., batteries, tanks, valves, etc.)
# ===========================================================================
os.chdir(v24data)
# Read facilities data
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
                                                         # 'DESCPC'  # Don't use this field
                                                         'EMPRESA_IN'],
                                          desired_precision=5)

# Select only records that have a "injection or disposal" TYPE value
# NOTE there are no injection/disposal related keywords in dataset 1
inj_keywords = [
    'SALT WATER INJECTION PLANT',  # present in dataset 2
    'SATELLITE GAS INJECTOR',  # dataset 2
    'SATELLITE WATER INJECTOR',  # dataset 2
    'FRESH WATER INJECTION PLANT',  # dataset 2
    'Gas injection plant'   # dataset 2
]

arg_inj_2 = arg_fac2.query("new_fac_type == @inj_keywords")


# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Only need to integrate dataset #2 because only it has injection/disposal records
arg_inj_2_integrated, err_ = integrate_facs(
    arg_inj_2.reset_index(),
    starting_ids=1,
    category="Injection and disposal",
    fac_alias="LNG_STORAGE",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2024-04-01",
    # on_offshore=None,
    fac_name="fac_name",
    # fac_id=None,
    fac_type="new_fac_type",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
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

# TODO - clean this up, there are still special characters to fix!
replace_special_chars = {'Ã©': 'É'}

specialchars2replace = {'PLANTA INY DE AGUA NÃ\x82Â°001 EOR': 'PLANTA INY DE AGUA 82°001 EOR',
                        'PLANTA INYECCIÃ\x83Â\x93N DE AGUA - PIA LACH SUR': 'PLANTA INYECCIÃ 93N DE AGUA - PIA LACH SUR',
                        'PLANTA DE INYECCIÃ\x83Â³N DE AGUA 001 ADA': 'PLANTA DE INYECCIÃ 83N DE AGUA 001 ADA',
                        'CERRO DRAGÃ\x83Â³N 2': 'CERRO DRAGÃN 2', 'CAÃ\x83Â±ADÃ\x83Â³N GRANDE 08': 'N/A',
                        'EL TRIÃ\x83Â¡NGULO 01': 'EL TRIÃNGULO 01', 'CERRO DRAGÃ\x83Â³N': 'CERRO DRAGÃN',
                        'PLANTA DE INYECCIÃ³N DE AGUA': 'PLANTA DE INYECCIÃN DE AGUA', 'PLANTA DE INYECCIÃ³N SUMIDEROS': 'PLANTA DE INYECCIÃN SUMIDEROS',
                        'PLANTA DE INYECCIÃ³N 3U': 'PLANTA DE INYECCIÃN 3U', 'PLANTA DE INYECCIÃ³N': 'PLANTA DE INYECCIÃN',
                        'PLANTA DE INYECCIÃ³N CF': 'PLANTA DE INYECCIÃN CF', 'PLANTA DE INYECCIÃ³N 3U': 'PLANTA DE INYECCIÃN 3U',
                        'PLANTA DE INYECCION CAÃ±ADA DURA': 'PLANTA DE INYECCION CÃNADA DURA', 'PLANTA DE INYECCIÃ³N DE AGUA SALADA': 'PLANTA DE INYECCIÃN DE AGUA SALADA',
                        'COMPAÃ\x91Ã\x8dAS ASOCIADAS PETROLERAS S.A.': 'COMPANIÃS AS ASOCIADAS PETROLERAS S.A.',
                        'YSUR ENERGÃ\x8dA ARGENTINA S.R.L.': 'YSUR ENERGÃ ARGENTINA S.R.L.'}

arg_inj_2_integrated.FAC_NAME.replace(specialchars2replace, inplace=True)
arg_inj_2_integrated.OPERATOR.replace(specialchars2replace, inplace=True)


# arg_inj_2_integrated = replace_row_names(arg_inj_2_integrated,
#                                          colName="FAC_NAME",
#                              dict_names=)

# eqm_inj2 = replace_row_names(eqm_inj1, colName="OPERATOR",
#                              dict_names={})


save_spatial_data(
    arg_inj_2_integrated,
    "argentina_injection_disposal",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% SAUDI ARABIA
# =============================================================================
os.chdir(v24data)
saudi = gpd.read_file(r'saudi_arabia\WI_Saudi_Arabia.kml.shp')
# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
saudi = transform_CRS(saudi, target_epsg_code="epsg:4326", appendLatLon=True)

# Change entries named 'Untitled Placemark' to NOT AVAILABLE
saudi.loc[saudi.Name.str.contains('Untitled'), 'Name'] = 'NOT AVAILABLE'

# =============================================================================
# %% SAUDI ARABIA - Integration + Export
# =============================================================================
saudi_, errors = integrate_facs(
    saudi,
    starting_ids=0,
    category='Injection and Disposal',
    fac_alias='LNG_STORAGE',
    country='Saudi Arabia',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    # on_offshore = None,
    fac_name='Name',
    # fac_id = None,
    # fac_type = None,
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    # install_date = None,
    # fac_status = None,
    # op_name = None,
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

saudi_1 = replace_row_names(saudi_, colName="FAC_NAME", dict_names={'': 'N/A', 'NAN': 'N/A', "NOT AVAILABLE": 'N/A'})


save_spatial_data(
    saudi_1,
    file_name="saudiarabia_injectiondisposal",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% UNITED STATES
# =============================================================================
os.chdir(v24data)
stor_ = read_spatial_data(r"united_states\national\Natural_Gas_Underground_Storage.geojson")

# Transform CRS
stor2 = transform_CRS(stor_, appendLatLon=True)

# Fix state names
stor2 = replace_row_names(stor2, "State", dict_names=dict_us_states)

# =============================================================================
# %% UNITED STATES - Integration + Export
# =============================================================================
us_stor, errors = integrate_facs(
    stor2,
    fac_alias="LNG_STORAGE",
    starting_ids=1,
    category="Injection and disposal",
    country="United States",
    state_prov="State",
    src_ref_id="100",
    src_date="2023-09-22",  # Annually
    on_offshore="Onshore",
    fac_name="Field",
    op_name="Company",
    fac_type='Field_Type',
    fac_status='Status',
    fac_id='ID',
    liq_capacity_bpd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    us_stor,
    "united_states_injection_disposal",
    schema_def=False,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=results_folder
)
