# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global TANK BATTERIES.

# TODO:
[x] standardize import statements and CWD setting
[] standardize spacing between sections
[] alphabetize countries
[] update all file paths

@author: maobrien, momara, ahimmelberger
"""
import os
# import re
import pandas as pd
import geopandas as gpd
# import numpy as np
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (transform_CRS, integrate_facs, save_spatial_data,
                     schema_LNG_STORAGE, read_spatial_data,
                     deduplicate_with_rounded_geoms, create_concatenated_well_name)
from ogim_translation_utils import (translate_argentina_installations_fac_types,
                                    replace_special_chars_in_column_argentina)
from abbreviation_utils import abbrev2name

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
# pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
save_batt = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% UNITED STATES - LNG ABOVE GROUND STORAGE
# =============================================================================
os.chdir(v24data)
fp_usa = "united_states/national/HIFLD/Above__Ground__LNG__Storage__Facilities_.geojson"
data_usa = read_spatial_data(fp_usa)

# Check and transform CRS
data_usa = transform_CRS(data_usa, appendLatLon=True)

# Create new "factype" field
data_usa.CON_TYPE.replace({'NOT AVAILABLE': 'N/A'}, inplace=True)
data_usa.TYPE.replace({'NOT AVAILABLE': 'N/A'}, inplace=True)

create_concatenated_well_name(data_usa,
                              'CON_TYPE',
                              'TYPE',
                              'factype')

# state abbreviation
abbrev2name(data_usa, "STATE", usa=True, can=False)


# =============================================================================
# %% UNITED STATES - Integration + Export
# =============================================================================
data_usa_int, errors = integrate_facs(
    data_usa,
    starting_ids=1,
    category="Tank battery",
    fac_alias="LNG_STORAGE",
    country="United States of America",
    state_prov="STATE",
    src_ref_id="244",
    src_date="2022-10-6",
    on_offshore="Onshore",
    fac_name="NAME",
    fac_id="AGLNGID",
    fac_type='TYPE',
    install_date=None,
    fac_status="STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd='TOTALCAP',
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks='NUMTANKS',
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    data_usa_int,
    file_name="united_states_above_ground_batteries",  # must include word 'batteries' for data consolidation
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_batt
)

# ===========================================================================
# %% COLORADO, USA
# ===========================================================================
os.chdir(v24data)
fp_co = r"united_states/colorado/Tank_Batteries.shp"
data_CO = read_spatial_data(fp_co)

# Check and transform CRS
data_CO = transform_CRS(data_CO, appendLatLon=True)

# For status codes
# https://cogcc.state.co.us/documents/about/COGIS_Help/Status_Codes.pdf
data_CO.fac_status.unique()
dict_status = {
    None: 'N/A',
    'AC': 'ACTIVE',
    'CL': 'CLOSED',
    'AL': 'ABANDONED',
    'PR': 'PRODUCING',
    'SI': 'SHUT IN'
}

data_CO["fac_status2"] = data_CO["fac_status"].replace(dict_status)

# =============================================================================
# %% COLORADO, USA - Integration + Export
# =============================================================================
co_tanks_bat3, errors = integrate_facs(
    data_CO,
    starting_ids=1,
    category="Tank battery",
    fac_alias="LNG_STORAGE",
    country="United States of America",
    state_prov="Colorado",
    src_ref_id="196",
    src_date="2024-04-18",  # Daily
    on_offshore="Onshore",
    fac_name="fac_name",
    fac_id="fac_id",
    fac_type=None,
    install_date=None,
    fac_status="fac_status2",
    op_name="oper_name",
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
    co_tanks_bat3,
    file_name="united_states_colorado_battery",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_batt
)

# ===========================================================================
# %% INDIANA, USA
# ===========================================================================
os.chdir(v24data)
fp_in = r"united_states/indiana/Tank_Batteries.shp"
data_IN = read_spatial_data(fp_in)

# Check and transform CRS
# Assume UTM Zone 16, EPSG:32616 and verify, since CRS is not set on original data
data_IN2 = data_IN.set_crs("epsg:32616")
data_IN3 = transform_CRS(data_IN2, appendLatLon=True)

# # Accuracy of location data
# interactive_map(
#     data_IN3,
#     random_sample=True,
#     num_samples=100
# )

# Check lease names
print(data_IN3.LEASE_NAME.unique())

# Check fac status
print(data_IN3.STATUS.unique())
data_IN3["fac_status"] = data_IN3["STATUS"].replace({'Plugd & Abandnd': 'Plugged and abandoned'})


# ===========================================================================
# %% INDIANA, USA - Integration + Export
# ===========================================================================
in_tanks_bat3, errors = integrate_facs(
    data_IN3,
    starting_ids=1,
    category="Tank battery",
    fac_alias="LNG_STORAGE",
    country="United States of America",
    state_prov="Indiana",
    src_ref_id="191",
    src_date="2022-10-01",
    on_offshore="Onshore",
    fac_name="LEASE_NAME",
    fac_id="PERMIT_NUM",
    fac_type=None,
    install_date=None,
    fac_status="fac_status",
    op_name="OPERATOR_N",
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
    in_tanks_bat3,
    file_name="united_states_indiana_battery",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_batt
)

# ===========================================================================
# %% MICHIGAN, USA
# ===========================================================================
os.chdir(v24data)
mi_fp = r"united_states/michigan/_facilities_michigan_.csv"
data_mi = pd.read_csv(mi_fp)
print(data_mi.head())
# DROP any records with no location information
data_mi = data_mi[data_mi.x.notna()]

# Create GDF
data_mi = gpd.GeoDataFrame(data_mi,
                           geometry=gpd.points_from_xy(data_mi.x,
                                                       data_mi.y,
                                                       crs="epsg:4326"))
data_mi = transform_CRS(data_mi, appendLatLon=True)

# Extract data for only tank batteries
print(data_mi.FacilityType.unique())
mi_bat = data_mi.query("FacilityType == ['Common Tank Battery', 'Tank Battery']")
print("Total # of records for tank batteries = ", mi_bat.shape[0])

# Check facility status
print(data_mi.FacilityStatus.unique())
data_mi["fac_status"] = data_mi["FacilityStatus"].replace({'Unknown': "N/A"})
# Drop tanks that have been marked as 'Removed'
data_mi = data_mi[data_mi.fac_status != 'Removed'].reset_index()

# ===========================================================================
# %% MICHIGAN, USA - Integration + Export
# ===========================================================================
mi_tanks_bat3, errors = integrate_facs(
    data_mi,
    starting_ids=1,
    category="Tank battery",
    fac_alias="LNG_STORAGE",
    country="United States of America",
    state_prov="Michigan",
    src_ref_id="195",
    src_date="2021-10-26",
    on_offshore="Onshore",
    fac_name="facilityname",
    fac_id="facilityno",
    fac_type=None,
    install_date=None,
    fac_status="fac_status",
    op_name="CurrentOperator",
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
    mi_tanks_bat3,
    file_name="united_states_michigan_battery",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_batt
)

# ===========================================================================
# %% ARGENTINA
# There are two datasets for facilities in Argentina, one that includes mostly
# facilities and another that includes mostly equipment on the site of
# facilities (e.g., batteries, tanks, valves, etc.)
# ===========================================================================
os.chdir(v24data)
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

# ---------------------------------------------------------------------------
# Select tank type facility records from datasets 1 and 2
tanks_keywords = [
    'TANK',  # dataset 2
    'TANK BATTERY',  # dataset 1
    'OIL BATTERY',  # dataset 2
    'GAS BATTERY'  # dataset 2
]

arg_tank_1 = arg_fac1[arg_fac1.new_fac_type.isin(tanks_keywords)]  # Dataset 01
arg_tank_2 = arg_fac2[arg_fac2.new_fac_type.isin(tanks_keywords)]  # Dataset 02

# TODO - fix encoding!
# Because Dataset #2 uses a mixture of character encodings,
# manually find and replace characters that were read incorrectly
# chardict = {'Ã±': 'ñ',
#             'Ã³': 'ó',
#             'Ã¡': 'á',
#             'Â°': '°',
#             'Ã©': 'é'
#             }
# tanks2 = tanks2.replace(chardict, regex=True)

# Specific instances where only Ã is present, but I know what the correct
# character should be
# tanks3 = tanks2.replace({'PDC CHALLACÃ': 'PDC CHALLACÓ'}, regex=True)
# tanks4 = tanks3.replace({'Ã': 'í'}, regex=True)

# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Data integration: 1st dataset
# ---------------------------------------------------------------------------
arg_tank_1_integrated, tanks_ArgEr = integrate_facs(
    arg_tank_1.reset_index(),
    starting_ids=1,
    category="Tank Battery",
    fac_alias="LNG_STORAGE",
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

# Data integration: 2nd dataset
# ---------------------------------------------------------------------------
arg_tank_2_integrated, tanks_ArgEr = integrate_facs(
    arg_tank_2,
    starting_ids=1 + arg_tank_1_integrated.shape[0],
    category="Tank Battery",
    fac_alias="LNG_STORAGE",
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

# Concatenate
arg_tanks_final = pd.concat([arg_tank_1_integrated,
                             arg_tank_2_integrated]).reset_index(drop=True)

save_spatial_data(
    arg_tanks_final,
    file_name="argentina_battery",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_batt
)

# =============================================================================
# %% CHINA
# =============================================================================
os.chdir(v24data)
fp = r"china\storage_tanks_of_China_in_2000-2021.shp"
china = read_spatial_data(fp, specify_encoding=True, data_encoding="utf-8")
china = transform_CRS(china, appendLatLon=True)

# Turn "estimated construction year" value (Year_2) into a date
china['installyear'] = china.Year_2 + '-01-01'

# There are some duplicate polygons in the original data set; delete them.
china = china.drop_duplicates(subset=['geometry'], keep='first')

# =============================================================================
# %% CHINA - Integration + Export
# =============================================================================
china_integrated, china_err = integrate_facs(
    china.reset_index(),
    starting_ids=1,
    category="Tank Battery",
    fac_alias="LNG_STORAGE",
    country="China",
    # state_prov=None,
    src_ref_id="242",
    src_date="2024-01-15",
    # on_offshore=None,
    # fac_name="fac_name",
    # fac_id="IDINSTALAC",
    # fac_type="new_fac_type",
    install_date='installyear',
    # fac_status=None,
    # op_name="EMPRESA",
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
    china_integrated,
    file_name="china_tank_battery",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_batt
)
