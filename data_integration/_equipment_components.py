# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global EQUIPMENT AND COMPONENTS.

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
# import numpy as np
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (transform_CRS, integrate_facs, save_spatial_data,
                     read_spatial_data, schema_OTHER,
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

# ---------------------------------------------------------------------------
# Select only records that have an equipment or component TYPE value
# NOTE there are no injection/disposal related keywords in dataset 1
eqpmt_keywords = [
    'VENTS',  # present in dataset 1
    'VALVE',  # present in dataset 2
    'SCRAPPER',  # dataset 2
    'CRUDE OIL COLLECTOR',  # dataset 2
    'GAS MANIFOLD',  # dataset 2
    'WATER COLLECTOR',  # dataset 2
    'BLOCK VALVE',  # dataset 2
    'PURGE VALVE'  # dataset 2
]

arg_eqm_1 = arg_fac1.query("new_fac_type == @eqpmt_keywords")
arg_eqm_2 = arg_fac2.query("new_fac_type == @eqpmt_keywords")

# For records that have a very undescriptive fac_name that is used for a lot of
# different records, use the "INSTALACIO" field instead
arg_eqm_1.loc[arg_eqm_1.fac_name == 'MEDANITO', 'fac_name'] = arg_eqm_1.DESCRIPCIO
arg_eqm_1.loc[arg_eqm_1.fac_name == 'MI', 'fac_name'] = arg_eqm_1.DESCRIPCIO


# =============================================================================
# %% ARGENTINA - Integrate + Export
# =============================================================================
# Integrate dataset 1
arg_eqm_1_integrated, err_ = integrate_facs(
    arg_eqm_1.reset_index(),
    starting_ids=1,
    category="Equipment and components",
    fac_alias="OTHER",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2023-05-02",
    # on_offshore=None,
    fac_name="fac_name",
    fac_id="IDINSTALAC",
    fac_type="new_fac_type",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
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
arg_eqm_2_integrated, err2_ = integrate_facs(
    arg_eqm_2,
    starting_ids=1,
    category="Equipment and components",
    fac_alias="OTHER",
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

# Concatenate two datasets
arg_eqm_final = pd.concat([arg_eqm_1_integrated,
                           arg_eqm_2_integrated]).reset_index(drop=True)


save_spatial_data(
    arg_eqm_final,
    "argentina_equipment_components",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=results_folder
)
