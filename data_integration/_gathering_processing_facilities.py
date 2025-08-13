# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global GATHERING AND PROCESSING FACILITIES.

@author: maobrien, momara, ahimmelberger
"""
import os
# import re
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
import shapely.wkt
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     save_spatial_data, schema_COMPR_PROC, read_spatial_data,
                     dict_us_states, deduplicate_with_rounded_geoms)
from ogim_translation_utils import (translate_argentina_installations_fac_types,
                                    replace_special_chars_in_column_argentina)
from capacity_conversions import convert_MMm3d_to_mmcfd

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
# pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
save_proc = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% ITALY
# =============================================================================
os.chdir(v24data)
fp = r'italy\centrali-idrocarburi.csv'
italy_fac = pd.read_csv(fp, sep=';', encoding='windows-1252', decimal=',')
italy_fac.columns = italy_fac.columns.str.lstrip()

# Drop any records with no lat-long coordinates
italy_fac = italy_fac.dropna(subset=['Longitudine', 'Latitudine'])

# TODO - confirm whether coords are in WGS84
italy_fac = gpd.GeoDataFrame(italy_fac,
                             geometry=gpd.points_from_xy(italy_fac.Longitudine,
                                                         italy_fac.Latitudine),
                             crs=4326)
italy_fac = transform_CRS(italy_fac,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Translate "Minerale" column
italy_fac['Minerale'].replace({'Olio/Gas': 'Oil/Gas',
                               'Olio': 'Oil',
                               np.nan: 'N/A'}, inplace=True)

# =============================================================================
# %% ITALY - Integration + Export
# =============================================================================
italy_fac_integrated, errors = integrate_facs(
    italy_fac,
    starting_ids=0,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Italy",
    state_prov="Provincia",
    src_ref_id="235",
    src_date="2023-05-18",
    fac_name="Nome centrale",
    commodity="Minerale",
    op_name="Operatore",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    italy_fac_integrated,
    file_name="italy_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


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

# Select oil and gas treatment facility records from datasets 1 and 2
proc_keywords = [
    'GAS PLANT',  # present in dataset 1
    'CONDITIONING PLANT',  # dataset 1
    'GAS TREATMENT PLANT',  # datset 2
    'OIL TREATMENT PLANT',  # datset 2
    'GAS SEPARATION PLANT',  # dataset 1 and 2
    'PROCESSING PLANT',  # dataset 2
    'Gas sweetening plant',  # dataset 2
    'Gas conditioning plant',  # dataset 2
    'Dehydrating plant'  # dataset 2
]

arg_proc_1 = arg_fac1[arg_fac1['new_fac_type'].isin(proc_keywords)]
arg_proc_2 = arg_fac2[arg_fac2['new_fac_type'].isin(proc_keywords)]


# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Integrate dataset 1
arg_proc_1_integrated, err_ = integrate_facs(
    arg_proc_1.reset_index(),
    starting_ids=1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
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
arg_proc_2_integrated, err_ = integrate_facs(
    arg_proc_2,
    starting_ids=1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
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
arg_proc_final = pd.concat([arg_proc_1_integrated,
                            arg_proc_2_integrated]).reset_index(drop=True)

save_spatial_data(
    arg_proc_final,
    file_name="argentina_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# =============================================================================
# %% AUSTRALIA
# =============================================================================
# os.chdir(pubdata)
# fp6 = r"Australia+NewZealand\Australia\Onshore_Processing_Plants_Australia.kml.shp"
os.chdir(v24data)
fp6 = r'australia/oginfra/Onshore_Processing_Plants_Australia.kml.shp'
gathering_processing_facility = read_spatial_data(fp6, table_gradient=True)
gathering_processing_facility1 = transform_CRS(gathering_processing_facility, target_epsg_code="epsg:4326", appendLatLon=True)

names = []

for idx1_, row1_ in tqdm(gathering_processing_facility1.iterrows(), total=gathering_processing_facility1.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if "Untitled" in name:
        names.append('N/A')

    else:
        names.append(name)

gathering_processing_facility1['NAME_'] = names

# =============================================================================
# %% AUSTRALIA - Integration + Export
# =============================================================================
gathering_proc_final_AUS, errors = integrate_facs(
    gathering_processing_facility1,
    starting_ids=0,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Australia",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    on_offshore="ONSHORE",
    fac_name="NAME_",
    # fac_id="",
    # fac_type="FAC_TYPE",
    # fac_status="",
    # operator="",
    # install_date='',
    # commodity = '',
    # liq_capacity_bpd = "",
    # liq_throughput_bpd="",
    # gas_capacity_mmcfd="",
    # gas_throughput_mmcfd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    gathering_proc_final_AUS,
    file_name="australia_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# =============================================================================
# %% BRAZIL  # DONE
# =============================================================================
os.chdir(v24data)
bz_proplant = gpd.read_file(r'brazil\WebMap_EPE\Processing_plants.shp')
bz_proplant = transform_CRS(bz_proplant,
                            target_epsg_code="epsg:4326",
                            appendLatLon=True)

# Remove the plants which don't exist yet
bz_proplant = bz_proplant.query("Classifica == 'Existente'").reset_index(drop=True)

# If number field contains any commas as decimals, replace with periods before conversion
bz_proplant.MMm3d = bz_proplant.MMm3d.str.replace(',', '.')
bz_proplant.MMm3d = pd.to_numeric(bz_proplant.MMm3d)
bz_proplant['gas_cap_mmcfd'] = bz_proplant.MMm3d.apply(lambda x: convert_MMm3d_to_mmcfd(x))

# Create properly formatted start date
bz_proplant['yr'] = bz_proplant.Entrada.astype(int).astype(str)
bz_proplant['startdate'] = bz_proplant.yr + '-01-01'

# translate statuses from Portuguese
bz_proplant.Situacao.replace({'Em operação': 'Operating',
                              'Fora de operação': 'Out of operation',
                              'Previsto': 'Planned'},
                             inplace=True)

# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
bz_proplant_integrated, errors = integrate_facs(
    bz_proplant,
    starting_ids=0,
    category='Gathering and processing',
    fac_alias='COMPR_PROC',
    country='BRAZIL',
    # state_prov=None,
    src_ref_id='267',
    src_date='2024-01-01',
    # on_offshore=None,
    fac_name='Nome',
    # fac_id=None,
    fac_type='fac_type',
    install_date='startdate',
    fac_status='Situacao',
    op_name='Proprietar',
    gas_capacity_mmcfd='gas_cap_mmcfd',
    # gas_throughput_mmcfd=None,
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)


save_spatial_data(
    bz_proplant_integrated,
    file_name="brazil_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)

# =============================================================================
# %% VENEZUELA
# =============================================================================
os.chdir(v24data)
fp_ = r"venezuela\INSTALACIONES_F_CarlosGonzales.shp"
vez_fac = read_spatial_data(fp_)
vez_fac = transform_CRS(vez_fac, appendLatLon=True)

# Translate commodity type
vez_fac.MAPA.replace({'PETROLERO': 'OIL',
                      'GASIFERO': 'GAS'}, inplace=True)

# ---------------------------------------------------------------------------
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
    'COMPLEJO MEJORADOR DE CRUDO (PROYECTO A FUTURO)': 'CRUDE UPGRADING COMPLEX (FUTURE PROJECT)',
    'COMPLEJO MEJORADOR DE CRUDO': 'CRUDE UPGRADING COMPLEX',
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

proc_ = [
    'GAS PROCESSING PLANT',
    'GAS PROCESSING PLANT (DISINCORPORATION)',
    'FRACTIONATION PLANT',
    'FRACTIONATION PLANT (FUTURE PROJECT)',
    'GAS PROCESSING PLANT (FUTURE PROJECT)',
    'COLLECTION STATION',
    'CRUDE UPGRADING COMPLEX (FUTURE PROJECT)',
    'CRUDE UPGRADING COMPLEX'
]
# ---------------------------------------------------------------------------
proc_plts_VEZ = vez_fac[vez_fac['TIPO'].isin(proc_)]
print("==============================")
print("Total number of gathering and processing plants in dataset = ", proc_plts_VEZ.shape[0])
proc_plts_VEZ.head()

# remove decimal from ID number
proc_plts_VEZ.Id = proc_plts_VEZ.Id.astype(int)

# =============================================================================
# %% VENEZUELA - Integration + Export
# =============================================================================
proc_plts_VEZ_integrated, _err = integrate_facs(
    proc_plts_VEZ,
    starting_ids=1,
    category="Gathering and Processing",
    fac_alias="COMPR_PROC",
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
    proc_plts_VEZ_integrated,
    file_name="venezuela_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)

# =============================================================================
# %% BOLIVIA
# =============================================================================
os.chdir(v24data)
fp = r'bolivia\geobolivia\plantas.csv'
bol_gath_csv = pd.read_csv(fp)
bol_gath = gpd.GeoDataFrame(bol_gath_csv,
                            geometry=bol_gath_csv.geometry.map(shapely.wkt.loads),
                            crs=4326)
bol_gath = transform_CRS(bol_gath, appendLatLon=True)

# =============================================================================
# %% BOLIVIA - Integration + Export
# =============================================================================
bol_gath_integrated, gath3_err = integrate_facs(
    bol_gath,
    starting_ids=1,
    category="Gathering and Processing",
    fac_alias="COMPR_PROC",
    country="Bolivia",
    # state_prov=None,
    src_ref_id="122",
    src_date="2017-09-13",
    on_offshore='ONSHORE',
    fac_name="fac_name",
    # fac_id="ogc_fid",
    # fac_type=None,
    # install_date=None,
    # fac_status=None,
    op_name="ba_name",
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
    bol_gath_integrated,
    file_name="bolivia_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# =============================================================================
# %% AFRICA (oginfra.com)
# =============================================================================
os.chdir(v24data)

LIB_cpf = gpd.read_file(r"libya/oginfrastructure_dot_com/CPF_Central_Pocessing_Facilities_Onshore_Libya.kml.shp")
MOZ_fac = gpd.read_file(r"mozambique/Gas_Facilities_Mozambique.kml.shp")
NIG_proc = gpd.read_file(r"nigeria/Gas_Processing_Nigeria.kml.shp")
SAFR_proc = gpd.read_file(r"south_africa/Gas_Processing_SouthAfrica.kml.shp")
SUD = gpd.read_file(r"sudan/_sudan_facilities_.shp")

# add fac type where known
LIB_cpf['fac_type'] = "Central Processing Facility"
MOZ_fac['fac_type'] = "Gas Processing Plant"
NIG_proc['fac_type'] = "Gas Processing Plant"
SAFR_proc['fac_type'] = "Gas Processing Plant"
SUD['fac_type'] = "N/A"  # Unknown, no detail in original filename which is just 'facilities'

# Add country
LIB_cpf['country'] = "Libya"
MOZ_fac['country'] = "Mozambique"
NIG_proc['country'] = "Nigeria"
SAFR_proc['country'] = "South Africa"
SUD['country'] = "Sudan"

# Concatenate all processing facilities into one df
# Confirm that all dataframes are the same CRS before appending into one gdf
all_dfs_final_proc = [
    LIB_cpf,
    MOZ_fac,
    NIG_proc,
    SAFR_proc,
    SUD
]
data_cpf_af = pd.concat(all_dfs_final_proc)
data_cpf_af = transform_CRS(data_cpf_af,
                            target_epsg_code="epsg:4326",
                            appendLatLon=True)

# Change certain values in Name column to NA
data_cpf_af['Name'] = data_cpf_af['Name'].fillna('N/A')
data_cpf_af.loc[data_cpf_af.Name == 'Untitled Placemark', 'Name'] = 'N/A'
data_cpf_af.loc[data_cpf_af.Name == 'Placemark', 'Name'] = 'N/A'
data_cpf_af.loc[data_cpf_af.Name == '58', 'Name'] = 'N/A'  # one record only has a number as its name, might be an error

# Remove duplicate point after manual checking
data_cpf_af2 = data_cpf_af.drop(index=15)  # TODO - check this

# =============================================================================
# %% AFRICA (oginfra.com) - Integration + Export
# =============================================================================
data_cp_af2, errors = integrate_facs(
    data_cpf_af2,
    starting_ids=0,
    category='Gathering and processing',
    fac_alias='COMPR_PROC',
    country='country',
    # state_prov=None,
    src_ref_id='22',
    src_date='2014-01-01',
    # on_offshore=None,
    fac_name='Name',
    # fac_id=None,
    fac_type='fac_type',
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
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
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    data_cp_af2,
    file_name="africa_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# ===========================================================================
# %% LIBYA (ArcGIS Online)
# ===========================================================================
os.chdir(v24data)
fp = r"libya\Libya_AGO_kmitchell\Libya_Infrastructure_Gas_Processing.geojson"
data = read_spatial_data(fp, table_gradient=True)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Fix "online year" field to match our desired date format
data.on_line_year = data.on_line_year.fillna('1900')
data['on_line_year'] = data['on_line_year'].astype('str') + '-01-01'

# Remove tilde ~ from plant_types and replace with "and"
data.loc[data.plant_types == 'Cond Separation~Treatment', 'plant_types'] = 'Condensate Separation and Treatment'


# ===========================================================================
# %% LIBYA (ArcGIS Online) - Integration + Export
# ===========================================================================
data_proc_LIB, errors = integrate_facs(
    data,
    starting_ids=0,
    category='Gathering and processing',
    fac_alias='COMPR_PROC',
    country='Libya',
    # state_prov = None,
    src_ref_id='166',
    src_date='2017-06-01',
    # on_offshore=None,
    fac_name='plant_name',
    # fac_id=None,
    fac_type='plant_types',
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    install_date='on_line_year',
    fac_status='status',
    op_name='operator_name',
    # commodity = None,
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    gas_capacity_mmcfd='cap_op_gas_intake_mmscfd',  # according to AGO webpage this field is "CAP_OP_GAS_INTAKE_MMSCFD" # million standard cubic feet per day
    # gas_throughput_mmcfd = None,
    # num_compr_units = None,
    # num_storage_tanks = None,
    # site_hp = None,
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    data_proc_LIB,
    file_name="libya_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)

# ===========================================================================
# %% MIDDLE EAST (oginfra.com)
# Note: Central Processing Facilities (CPFs) from oginfra.com are all offshore,
# these are integrated in the `offshore_platforms.py` file
# ===========================================================================
os.chdir(v24data)

afghan = gpd.read_file(r'afghanistan\oginfra_dotcom\_Afghanistan_GasPlant_OilGasInfra.com.shp')
afghan["type_"] = "Gas processing plant"

iran_plant = gpd.read_file(r'iran\Gas Processing Plant.kml.shp')
iran_plant["type_"] = "Gas processing plant"

oman = gpd.read_file(r'oman\Gas_Processing_Plant_Oman.kml.shp')
oman["type_"] = "Gas processing plant"

pak = gpd.read_file(r'pakistan\gas_plants_Pakistan.kml.shp')
pak["type_"] = "Gas processing plant"

qatar_plant = gpd.read_file(r'qatar\Gas_Plant_Qatar.kml.shp')
qatar_plant["type_"] = "Gas processing plant"

saudi_plant = gpd.read_file(r'saudi_arabia\Gas_Plant_Saudi_Arabia.kml.shp')
saudi_plant["type_"] = "Gas processing plant"

uae_plant = gpd.read_file(r'uae\Plant_Gas_Onshore_UAE.kml.shp')
uae_plant["type_"] = "Gas processing plant"

yemen = gpd.read_file(r'yemen\Gas_Treatment_Yemen.kml.shp')
yemen["type_"] = "Gas treatment plant"

# ---------------------------------------------------------------------------
# Indicate onshore and offshore for UAE facilities (from filename)
# Plant_Gas_Onshore_UAE.kml.shp
uae_plant['onoff'] = 'Onshore'

# ---------------------------------------------------------------------------
# Add 'country' column to all country-specific gdfs
afghan['country'] = 'Afghanistan'
iran_plant['country'] = 'Iran'
# iran_cpf['country'] = 'Iran'
oman['country'] = 'Oman'
pak['country'] = 'Pakistan'
qatar_plant['country'] = 'Qatar'
# qatar_cpf['country'] = 'Qatar'
saudi_plant['country'] = 'Saudi Arabia'
# saudi_cpf['country'] = 'Saudi Arabia'
uae_plant['country'] = 'UAE'
# uae_cpf['country'] = 'UAE'
yemen['country'] = 'Yemen'
# ---------------------------------------------------------------------------
# Combine all countries into one processing plant gdf
# Onshore facilities
all_dfs_final_me_proc = [
    afghan,
    iran_plant,
    oman,
    pak,
    qatar_plant,
    saudi_plant,
    uae_plant,
    yemen
]

# ---------------------------------------------------------------------------
data_cpf_me = pd.concat(all_dfs_final_me_proc)
data_cpf_me = data_cpf_me.reset_index(drop=True)
# Delete mostly empty Descriptio column for convenience
data_cpf_me = data_cpf_me.drop('Descriptio', axis=1)

# Replace all instances of NAN, and 'Untitled' in Name column, with NOT AVAILABLE
data_cpf_me['onoff'] = data_cpf_me.onoff.fillna('N/A')
# data_cpf_me['type_'] = data_cpf_me.type.fillna('N/A')
# data_cpf_me['Name'] = data_cpf_me.Name.fillna('N/A')
# data_cpf_me.loc[data_cpf_me.Name.str.contains('untitled',case=False),'Name'] = 'N/A'
data_cpf_me["Name"] = data_cpf_me["Name"].replace({None: "N/A", np.nan: "N/A", "Untitled Placemark": "N/A"})

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data_cpf_me = transform_CRS(data_cpf_me, target_epsg_code="epsg:4326", appendLatLon=True)

# ===========================================================================
# %% MIDDLE EAST (oginfra.com) - Integration + Export
# ===========================================================================
data_cpf_me2, errors = integrate_facs(
    data_cpf_me,
    starting_ids=0,
    category='Gathering and processing',
    fac_alias='COMPR_PROC',
    country='country',
    # state_prov=None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='onoff',
    fac_name='Name',
    # fac_id=None,
    fac_type='type_',
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
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
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    data_cpf_me2,
    file_name="middle_east_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# ===========================================================================
# %% AZERBAIJAN
# ===========================================================================
os.chdir(v24data)
fp1 = r"azerbaijan\Gas_handling_Azerbajan.kml.shp"
gas_handling = read_spatial_data(fp1, table_gradient=False)
# ---------------------------------------------------------------------------
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
gas_handling1 = transform_CRS(gas_handling, target_epsg_code="epsg:4326", appendLatLon=True)
# ---------------------------------------------------------------------------

gas_handling1["fac_type"] = "Gas handling"

# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------
processing_facility_final_AZER, errors = integrate_facs(
    gas_handling1,
    starting_ids=0,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Azerbaijan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    fac_name="Name",
    # fac_id="",
    fac_type="fac_type",
    # drill_type="",
    # spud_date = "",
    # comp_date="",
    # fac_status="",
    # op_name="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    processing_facility_final_AZER,
    file_name="azerbaijan_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# ===========================================================================
# %% KAZAKHSTAN
# ===========================================================================
os.chdir(v24data)
fp1 = r"kazakhstan\Gas_handling_Kazakhsta.kml.shp"
gas_handling2 = read_spatial_data(fp1, table_gradient=False)
gas_handling2 = transform_CRS(gas_handling2, target_epsg_code="epsg:4326", appendLatLon=True)


# ---------------------------------------------------------------------------
# Data processing
# ---------------------------------------------------------------------------
processing_facility_final_KZK, errors = integrate_facs(
    gas_handling2,
    starting_ids=0,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Kazakhstan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
    # drill_type="",
    # spud_date = "",
    # comp_date="",
    # fac_status="",
    # op_name="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)
# ---------------------------------------------------------------------------
# Save data
# GeoJSON
# ---------------------------------------------------------------------------
save_spatial_data(
    processing_facility_final_KZK,
    file_name="kazakhstan_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)

# ===========================================================================
# %% TURKMENISTAN
# ===========================================================================
os.chdir(v24data)
fp1 = r"turkmenistan\Gas_handling_Turkmenistan.kml.shp"
gas_handling3 = read_spatial_data(fp1, table_gradient=True)

# ---------------------------------------------------------------------------
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
gas_handling3 = transform_CRS(gas_handling3, target_epsg_code="epsg:4326", appendLatLon=True)

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------
gas_handling3["fac_type"] = "Gas handling"  # Assign fac type

processing_facility_final_TURK, errors = integrate_facs(
    gas_handling3,
    starting_ids=0,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Turkmenistan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    # fac_name="Name",
    # fac_id="",
    fac_type="fac_type",
    # drill_type="",
    # spud_date = "",
    # comp_date="",
    # fac_status="",
    # op_name="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# ---------------------------------------------------------------------------
# Save data
# GeoJSON
# ---------------------------------------------------------------------------
save_spatial_data(
    processing_facility_final_TURK,
    file_name="turkmenistan_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# ===========================================================================
# %% BANGLADESH
# ===========================================================================
os.chdir(v24data)
fp4 = r"bangladesh\GasPlant_Bangladesh.kml.shp"
cpf = read_spatial_data(fp4, table_gradient=True)
cpf1 = transform_CRS(cpf, target_epsg_code="epsg:4326", appendLatLon=True)


cpf1["fac_type"] = "Gas plant"
# ---------------------------------------------------------------------------
# Integration
# ---------------------------------------------------------------------------
comp_proc_BANG, errors = integrate_facs(
    cpf1,
    starting_ids=0,
    category="Natural gas processing facilities",
    fac_alias="COMPR_PROC",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="fac_type",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    # liq_capacity_bpd = "",
    # liq_throughput_bpd="",
    # gas_capacity_mmcfd="",
    # gas_throughput_mmcfd="",
    # num_storage_tanks="",
    # site_hp = "",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)
# ---------------------------------------------------------------------------
# Save data
# GeoJSON
# ---------------------------------------------------------------------------
save_spatial_data(
    comp_proc_BANG,
    file_name="bangladesh_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# ===========================================================================
# %% MYANMAR
# ===========================================================================
os.chdir(v24data)
fp4 = r"myanmar\Gas_Plant_Myanmar.kml.shp"
cpf = read_spatial_data(fp4, table_gradient=True)
cpf1 = transform_CRS(cpf, target_epsg_code="epsg:4326", appendLatLon=True)

cpf1["fac_type"] = "Gas plant"
# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------
comp_proc_MYAN, errors = integrate_facs(
    cpf1,
    starting_ids=0,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Myanmar",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="ONSHORE",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    # liq_capacity_bpd = "",
    # liq_throughput_bpd="",
    # gas_capacity_mmcfd="",
    # gas_throughput_mmcfd="",
    # num_storage_tanks="",
    # site_hp = "",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# ---------------------------------------------------------------------------
# Save data
# GeoJSON
# ---------------------------------------------------------------------------
save_spatial_data(
    comp_proc_MYAN,
    file_name="myanmar_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)


# ===========================================================================
# %% UNITED STATES (HIFLD)
# ===========================================================================
os.chdir(v24data)
proc_us = read_spatial_data(r"united_states\national\HIFLD\NaturalGas_ProcessingPlants.geojson")
proc_us = transform_CRS(proc_us, appendLatLon=True)

# Standardize state names
# ---------------------------------------------------------------------------
proc_us = replace_row_names(proc_us, "State", dict_names=dict_us_states)

# Fix STATUS - field no longer available as of 2025
# proc_us = replace_row_names(proc_us,
#                             "STATUS",
#                             dict_names={'NOT AVAILABLE': 'N/A'})

# Plant type - field no longer available as of 2025
# proc_us = replace_row_names(proc_us,
#                             "TYPE",
#                             dict_names={'NOT AVAILABLE': 'N/A'})

# Check plant throughput and capacity
# Units here are MMcfd based on comparison with data from EIA # https://atlas.eia.gov/apps/3652f0f1860d45beb0fed27dc8a6fc8d/explore
print(sorted(proc_us.Cap_MMcfd.unique()[0:10]))
print(sorted(proc_us.Plant_Flow.unique()[0:10]))


# =============================================================================
# %% UNITED STATES - Integration + Export
# =============================================================================
proc_us_integrated, proc_errors_3 = integrate_facs(
    proc_us,
    starting_ids=1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="United States",
    state_prov="State",
    src_ref_id="91",
    src_date="2017-01-01",
    on_offshore="Onshore",
    fac_name="Plant_Name",
    # fac_id="",
    # fac_type="",
    install_date=None,
    fac_status=None,
    op_name="Operator",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="Cap_MMcfd",
    gas_throughput_mmcfd="Plant_Flow",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    proc_us_integrated,
    "united_states_gathering_processing",
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_proc
)
