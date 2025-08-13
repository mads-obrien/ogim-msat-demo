# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global OFFSHORE PLATFORMS.

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
# from ogimlib import *
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     save_spatial_data, schema_OTHER, read_spatial_data,
                     standardize_dates_hifld_us, deduplicate_with_rounded_geoms)
from ogim_translation_utils import (translate_argentina_installations_fac_types,
                                    replace_special_chars_in_column_argentina)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
save_plats = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'


# =============================================================================
# %% ITALY
# =============================================================================
os.chdir(v24data)
fp = r'italy\piattaforme.csv'
italyplats = pd.read_csv(fp, sep=';', encoding='windows-1252', decimal=',')
italyplats.columns = italyplats.columns.str.strip()  # strip spaces from column names

# TODO - confirm whether coords are in WGS84
italyplats = gpd.GeoDataFrame(italyplats,
                              geometry=gpd.points_from_xy(italyplats.Longitudine,
                                                          italyplats.Latitudine),
                              crs=4326)
italyplats = transform_CRS(italyplats,
                           target_epsg_code="epsg:4326",
                           appendLatLon=True)

# Translate "olio" in mineral / commodity column
italyplats.Minerale.replace({'OLIO': 'OIL'}, inplace=True)

# Translate "type" field
italyplats['Tipo struttura'].replace(
    {'monotubolare': 'monotubular',
     'bitubolare': 'bitubular',
     'struttura reticolare 3 gambe': '3-legged lattice structure',
     'struttura reticolare 4 gambe': '4-legged lattice structure',
     'struttura reticolare 5 gambe': '5-legged lattice structure',
     'struttura reticolare 6 gambe': '6-legged lattice structure',
     'struttura reticolare 8 gambe': '8-legged lattice structure',
     'struttura reticolare 12 gambe': '12-legged lattice structure',
     'struttura reticolare 20 gambe': '20-legged lattice structure',
     'cluster 3 gambe': '3-legged cluster',
     'cluster 4 gambe': '4-legged cluster',
     'testa pozzo sottomarina': 'subsea wellhead'}, inplace=True)

# Convert installation year to installation date
italyplats['installyear'] = italyplats['Anno di installazione'].astype(str)
italyplats['installdate'] = italyplats['installyear'] + '-01-01'

italyplats['Status'].replace({'Inattiva': 'Inactive',
                              np.nan: 'N/A'}, inplace=True)

# =============================================================================
# %% ITALY - Integration + Export
# =============================================================================
italyplats_integrated, _ = integrate_facs(
    italyplats,
    starting_ids=1,
    category="Offshore platforms",
    fac_alias="OTHER",
    country="Italy",
    # state_prov="REGION",
    src_ref_id="37",
    src_date="2023-05-18",
    on_offshore="Offshore",
    fac_name='Nome struttura',
    fac_id="Id",
    fac_type='Tipo struttura',
    install_date="installdate",
    fac_status="Status",
    op_name="Operatore",
    commodity="Minerale",
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
    italyplats_integrated,
    file_name="italy_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# =============================================================================
# %% UNITED STATES (BOEM)
# =============================================================================
os.chdir(v24data)

# Paths to your files
us_offshore_folder = 'united_states\\offshore\\'
spec_file_path = os.path.join(us_offshore_folder,
                              'BOEM_fixed_widths.xlsx')  # defines character width of each attrib column
files_list = ['platmast', 'platstru', 'platloc', 'compactfixed']

# Load the column specifications tables into a dictionary
# This excel doc lists the start character position and character widths for
# each attribute column in my BOEM data files.
specs = {}
for f in files_list:
    specs[f] = pd.read_excel(spec_file_path, sheet_name=f, header=1)

# -----------------------------------------------------------------------------
# Read each of the seperate BOEM tables one at at time. These tables will be
# joined later on to produce my final dataset for integrating, so keep only
# the columns I need for the joins.

for f in files_list:
    print(f'Now reading in {f}...')
    # Calculate the column start positions and widths
    col_starts = specs[f]['Position'] - 1  # subtract one to use zero-indexing
    col_widths = specs[f]['Length']
    col_names = specs[f]['Alias']

    # Read the fixed-width DAT file using pandas
    # The 'colspecs' argument is a list of tuples, where each tuple is
    # (start, end) for each column
    colspecs = [(start, start + length) for start, length in zip(col_starts, col_widths)]

    # Field definitions: https://www.data.boem.gov/Main/HtmlPage.aspx?page=platformMasters
    if f == 'platmast':
        filepath = os.path.join(us_offshore_folder, f'{f}.DAT')
        platmast = pd.read_fwf(filepath,
                               colspecs=colspecs,
                               names=col_names,
                               dtype={'Mms Company Num': 'Int64'})
        # Reduce to columns I need
        platmast = platmast[['Complex Id Num',
                             'Mms Company Num',
                             'Abandon Flag',
                             'Condn Prod Flag',
                             'Drilling Flag',
                             'Gas Prod Flag',
                             'Injection Code',
                             # 'Sulfur Prod Flag',
                             'Production Flag',
                             'Oil Prod Flag'
                             ]]
        platmast = platmast.rename(columns={'Mms Company Num': 'MMS Company Num'})
        # Drop Complex IDs that don't have a Company associated with them
        platmast = platmast[platmast['MMS Company Num'].notna()].reset_index(drop=True)

    if f == 'platstru':
        filepath = os.path.join(us_offshore_folder, f'{f}.DAT')
        platstru = pd.read_fwf(filepath,
                               colspecs=colspecs,
                               names=col_names)
        # Reduce to columns I need
        platstru = platstru[['Complex Id Num',
                             'Install Date',
                             'Last Revision Date',
                             'Removal Date',
                             'Structure Name',
                             'Structure Number',
                             'Structure Type Code',
                             'Authority Type',
                             'Authority Status'
                             ]]

    if f == 'platloc':
        filepath = os.path.join(us_offshore_folder, f'{f}.DAT')
        platloc = pd.read_fwf(filepath,
                              colspecs=colspecs,
                              encoding="windows-1252",
                              names=col_names)
        # Reduce to columns I need
        platloc = platloc[['COMPLEX ID NUMBER',
                           'STRUCTURE NUMBER',
                           'STRUCTURE NAME',
                           'LONGITUDE',
                           'LATITUDE']]
        platloc.columns = platloc.columns.str.title()
        platloc = platloc.rename(columns={'Complex Id Number': 'Complex Id Num'})

    if f == 'compactfixed':
        filepath = os.path.join(us_offshore_folder, f'{f}.txt')
        compactfixed = pd.read_fwf(filepath,
                                   encoding='latin-1',
                                   colspecs=colspecs,
                                   names=col_names)
        # Reduce to columns I need
        compactfixed = compactfixed[['MMS Company Num',
                                     'Bus Asc Name']]

        compactfixed = compactfixed.drop_duplicates(subset=['MMS Company Num',
                                                            'Bus Asc Name'],
                                                    keep='last')

# for df in [platmast, platstru, platloc, compactfixed]:
#     print(df.dtypes)
#     print('= = = = = = = = = ')

# -----------------------------------------------------------------------------
# Join lat-long locations with facility info
platforms = platloc.merge(platstru,
                          how='inner',
                          on=['Complex Id Num',
                              'Structure Number',
                              'Structure Name'],
                          suffixes=('_x', '_y'))

# Join Complex ID number with Company Name
companies = platmast.merge(compactfixed,
                           how='inner',
                           on='MMS Company Num')

# Join spatially-explicit facilities with their company name
platforms_joined = platforms.merge(companies,
                                   how='left',
                                   on='Complex Id Num')

# -----------------------------------------------------------------------------
# Only keep platforms that have NOT been removed
platforms_standing = platforms_joined[platforms_joined['Removal Date'].isna()]
platforms_standing = platforms_standing.reset_index(drop=True)

# Unabbreviate type codes
# https://www.data.boem.gov/Main/HtmlPage.aspx?page=platformStructuresFields#Structure%20Type%20Code
structype_code = {
    'CAIS': 'Caisson',
    'CT': 'Compliant tower',
    'FIXED': 'Fixed Leg Platform',
    'FPSO': 'Floating production, storage and offloading',
    'MOPU': 'Mobile Production Unit',
    'MTLP': 'Mini Tension Leg Platform',
    'SEMI': 'Semi Submersible Floating Production System',  # Semi Submersible (Column Stabilized Unit) Floating Production System
    'SPAR': 'SPAR Platform floating production system',
    'SSANC': 'Subsea Fixed anchors',
    'SSMNF': 'Subsea Manifold',
    'SSTMP': 'Subsea templates',
    'TLP': 'Tension leg platform',
    'UCOMP': 'Underwater completion or subsea caisson',
    'WP': 'Well Protector',
    np.nan: 'N/A',
    None: 'N/A'
}
platforms_standing['structype'] = platforms_standing['Structure Type Code'].replace(structype_code)

# Un-abbreviate status codes
# https://www.data.boem.gov/Main/HtmlPage.aspx?page=leaseDataFields
auth_status_code = {
    'UNIT': 'ACTIVE - APPROVED UNIT AGREEMENT',
    'PROD': 'PRODUCTION',
    'TERMIN': 'TERMINATED',
    'RELINQ': 'RELINQUISHED',
    'ACT': 'ACTIVE',
    'Approved': 'APPROVED',
    'SOO': 'SUSPENSION OF OPERATIONS',
    'SOP': 'SUSPENSION OF PRODUCTION',
    'DSO': 'DIRECTED SUSPENSION OF OPERATIONS',
    'PRIMRY': 'ACTIVE - INITIAL CONTRACT',
    'EXP': 'EXPIRED',
    'EXPIR': 'EXPIRED',
    'RELQ': 'RELINQUISHED',
    np.nan: 'N/A',
    None: 'N/A'
}
platforms_standing['status'] = platforms_standing['Authority Status'].replace(auth_status_code)


# Convert Install date into the date format I need (string type)
platforms_standing['install'] = pd.to_datetime(platforms_standing['Install Date'],
                                               infer_datetime_format=True).dt.strftime("%Y-%m-%d")

# -----------------------------------------------------------------------------
# Create a commodity field based on the "flag" fields
platforms_standing['Condn Prod Flag'] = platforms_standing['Condn Prod Flag'].where(platforms_standing['Condn Prod Flag'] == 'Y', None)
platforms_standing['Gas Prod Flag'] = platforms_standing['Gas Prod Flag'].where(platforms_standing['Gas Prod Flag'] == 'Y', None)
platforms_standing['Oil Prod Flag'] = platforms_standing['Oil Prod Flag'].where(platforms_standing['Oil Prod Flag'] == 'Y', None)
platforms_standing['Condn Prod Flag'].replace({'Y': 'CONDENSATE'}, inplace=True)
platforms_standing['Gas Prod Flag'].replace({'Y': 'GAS'}, inplace=True)
platforms_standing['Oil Prod Flag'].replace({'Y': 'OIL'}, inplace=True)

platforms_standing['commodity'] = platforms_standing[['Oil Prod Flag',
                                                      'Gas Prod Flag',
                                                      'Condn Prod Flag']].apply(lambda x: ', '.join(x.dropna().astype(str)), axis=1)
platforms_standing['commodity'].replace({'': 'N/A'}, inplace=True)

# -----------------------------------------------------------------------------
# TURN DF INTO GDF
platforms_gdf = gpd.GeoDataFrame(platforms_standing,
                                 geometry=gpd.points_from_xy(platforms_standing.Longitude,
                                                             platforms_standing.Latitude),
                                 crs=4326)

platforms_gdf = transform_CRS(platforms_gdf, appendLatLon=True)


# =============================================================================
# %% UNITED STATES (BOEM) - Integration + Export
# =============================================================================
plats_boem_integrated, _err = integrate_facs(
    platforms_gdf,
    starting_ids=1,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="UNITED STATES OF AMERICA",
    state_prov='GULF OF MEXICO',
    src_ref_id="266",
    src_date="2024-11-01",
    on_offshore='OFFSHORE',
    fac_name="Structure Name",
    fac_id="Complex Id Num",
    fac_type="structype",
    install_date='install',
    fac_status='status',
    op_name='Bus Asc Name',
    commodity='commodity',
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    plats_boem_integrated,
    file_name="united_states_boem_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
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
# ---------------------------------------------------------------------------
# Offshore platforms
plats_VZ = vez_fac[vez_fac['TIPO'].isin(['PLATFORM'])]

# =============================================================================
# %% VENEZUELA - Integration + Export
# =============================================================================
plats_VZ_integrated, _err = integrate_facs(
    plats_VZ,
    starting_ids=1,
    category="Offshore Platforms",
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
    plats_VZ_integrated,
    file_name="venezuela_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
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

# -----------------------------------------------------------------------------
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
# -----------------------------------------------------------------------------
plat_keywords = [
    'OFFSHORE PLATFORM',  # present in dataset 1
    'PLATFORM'  # dataset 2
]

arg_plat_1 = arg_fac1[arg_fac1['new_fac_type'].isin(plat_keywords)]
arg_plat_2 = arg_fac2[arg_fac2['new_fac_type'].isin(plat_keywords)]

# There are 5 entries for the "Poseidon" platform with identical lat-longs.
# Just keep one
arg_plat_1 = arg_plat_1.drop_duplicates(subset=['INSTALACIO'], keep='first')

# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Integrate dataset 1
arg_plat_1_integrated, err_ = integrate_facs(
    arg_plat_1.reset_index(),
    starting_ids=1,
    category="Offshore platforms",
    fac_alias="OTHER",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2023-05-02",
    on_offshore='OFFSHORE',
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
arg_plat_2_integrated, err_ = integrate_facs(
    arg_plat_2.reset_index(),
    starting_ids=1,
    category="Offshore platforms",
    fac_alias="OTHER",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2024-04-01",
    on_offshore='OFFSHORE',
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
arg_plat_final = pd.concat([arg_plat_1_integrated,
                            arg_plat_2_integrated]).reset_index(drop=True)


save_spatial_data(
    arg_plat_final,
    file_name="argentina_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# =============================================================================
# %% NIGERIA - oil spills NEW - # TODO WORK IN PROGRESS
# =============================================================================
# os.chdir(v24data)
# fp = r'nigeria/NOSDRA/nosdra_2024-12-17_19_30_57UTC_complete.csv'
# data = pd.read_csv(fp, on_bad_lines='warn')
# # Only keep the columns I care about, for my sanity
# data = data[['id',
#              'company',
#              'incidentdate',
#              'reportdate',
#              'lastupdatedby',
#              'contaminant',
#              'typeoffacility',
#              'sitelocationname',
#              'latitude',
#              'longitude'
#              ]]
# # Some of the latitude and longitude values are not valid numbers, so
# # cast the columns to numeric, and set any values that fail to null
# data['long_num'] = pd.to_numeric(data['longitude'], errors='coerce')
# data['lat_num'] = pd.to_numeric(data['latitude'], errors='coerce')

# # Drop all records with NO lat-long
# data = data[data.longitude.notna()].reset_index(drop=True)
# data = gpd.GeoDataFrame(data,
#                         geometry=gpd.points_from_xy(data.longitude,
#                                                     data.latitude),
#                         crs=4326)
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# # Convert abbreviations that I know refer to a commodity of interest
# contaminant_dict = {'ch': 'chemicals or drilling mud',
#                     'co': 'condensate',
#                     'CON': 'condensate',
#                     'Condensate': 'condensate',
#                     'cr': 'crude oil',
#                     'Cr': 'crude oil',
#                     'crude oil': 'crude oil',
#                     'Crude oil': 'crude oil',
#                     'crude': 'crude oil',
#                     'ch, cr': 'crude oil',  # drilling mud plus crude oil
#                     'ga': 'gas',
#                     'gas': 'gas',
#                     'GAS': 'gas',
#                     # 'gs': 'gas',  # TODO check this
#                     're': 'refined products',
#                     'other:': 'other'
#                     }
# data['contaminant_new'] = data.contaminant.map(contaminant_dict).fillna('N/A')

# # Un-abbreviate all facility types that are not simply points along a pipeline / flowline
# fac_type_dict = {'cp': 'compressor plant',
#                  'fd': 'fuel dispensation station',
#                  # 'fl': 'flow line',
#                  'fp': 'FPSO platform',
#                  'fs': 'flow station',
#                  # 'gl': 'gas line',
#                  'mf': 'manifold',
#                  'platform': 'platform',
#                  # 'pl': 'pipeline',
#                  # 'Pl': 'pipeline',
#                  # 'PL': 'pipeline',
#                  'ps': 'pumping station',
#                  'rg': 'rig',
#                  'st': 'storage tank',
#                  'tf': 'tank farm',
#                  'tf,st': 'tank farm',
#                  'tk': 'tanker',
#                  'wh': 'well head',
#                  'Well Head': 'well head',
#                  'wh, flanges': 'well head',
#                  'other: Wellhead location': 'well head',
#                  'other:illegal refinery': 'illegal refinery',
#                  'other:Activities of illegal refinery.': 'illegal refinery',
#                  'other: Illegal Refinery': 'illegal refinery',
#                  'other:(sump tank)': 'sump tank',
#                  'other: Sump Tank': 'sump tank',
#                  'other: STOCK TANK': 'stock tank',
#                  'other: TREATMENT PLANT': 'treatment plant',
#                  'other: Crude Processing Facility': 'crude processing facility',
#                  'other: SBM': 'single buoy mooring',
#                  'other:sbm': 'single buoy mooring',
#                  'other: SPM': 'single point mooring',
#                  'other:SPM': 'single point mooring',
#                  'SPM': 'single point mooring',
#                  'other:single point mooring (SPM)1': 'single point mooring',
#                  'other: Single Point Mooring': 'single point mooring',
#                  'other:compressor plant': 'compressor plant',
#                  'other:manifold': 'manifold',
#                  'FPSO': 'FPSO platform'
#                  }
# data['factype_new'] = data.typeoffacility.map(fac_type_dict).fillna('N/A')

# # Create CATEGORY column that corresponds to my OGIM categories,
# # and assign a CATEGORY attribute to records based on their FAC_TYPE values
# data['category_new'] = None
# data.loc[data.factype_new == 'well head', 'category_new'] = 'oil and gas wells'
# data.loc[data.factype_new == 'compressor plant', 'category_new'] = 'natural gas compressor stations'
# offshore_terms = ['offshore platforms',
#                   'FPSO platform',
#                   'platform',
#                   'single buoy mooring',
#                   'single point mooring']
# data.loc[data.factype_new.isin(offshore_terms), 'category_new'] = 'offshore platforms'
# storage_terms = ['storage tank',
#                  'tank farm',
#                  'sump tank',
#                  'stock tank']
# data.loc[data.factype_new.isin(storage_terms), 'category_new'] = 'Petroleum storage and terminals'

# # Create category for "Stations - Other"
# otherfaclist = ['flow station',
#                 'manifold',
#                 'rig',
#                 'pumping station']
# data.loc[data.factype_new.str.contains('|'.join(otherfaclist)), 'category_new'] = 'Stations - other'

# ===========================================================================
# %% NIGERIA - oil spills dataset
# ===========================================================================
os.chdir(pubdata)
fp = r"Africa\Nigeria\Nigeria\NGA_Oil_Spill_Monitor_.shp"
data = read_spatial_data(fp, table_gradient=False)
# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

print("====================")
print(data.columns)
# ---------------------------------------------------------------------------
# % Data manipulation / processing if needed
# ---------------------------------------------------------------------------

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
# ---------------------------------------------------------------------------
# Whoever originally cleaned this data used a reckless find-and-replace, which
# replaced every instance of the letters "st" with "Storage Tank", and every
# instance of "pl" with "Pipeline",  even when "st" or "pl" was
# part of a substring....
# I confirmed on the Nigeria Oil Spill Monitor website (https://oilspillmonitor.ng/)
# that these abbreviations are used in their 'type of facility' field
# data.Type_of_Fa.value_counts()
# ---------------------------------------------------------------------------
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

# data["Type_of_Fa"] = data["Type_of_fa"]
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
# ---------------------------------------------------------------------------
data['Type_of_Fa'] = data['Type_of_Fa'].map(factypedict).fillna(data['Type_of_Fa'])

# %Fill in null fac_type values, based on other clues elsewhere in the table
# ---------------------------------------------------------------------------

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

# Drop cells where Type_of_Fa is null -- we simply don't know enough about what these points are to include them in OGIM
data = data[-data.Type_of_Fa.isna()]
# Drop records that are simply point locations along a pipeline -- not the data format we need for this infra type
types2drop = ['Pipeline', 'Flow Line']
data = data[-data.Type_of_Fa.isin(types2drop)]

# Create CATEGORY column that corresponds to my OGIM categories,
# and assign a CATEGORY attribute to records based on their FAC_TYPE values

data['category_new'] = None
data.loc[data.Type_of_Fa == 'Well Head', 'category_new'] = 'Oil and gas wells'
data.loc[(data.Type_of_Fa == 'Floating production storage and offloading') | (data.Type_of_Fa == 'Offshore platforms'), 'category_new'] = 'Offshore platforms'
data.loc[data.Type_of_Fa == 'other:compressor plant', 'category_new'] = 'Natural gas compressor stations'
data.loc[(data.Type_of_Fa == 'Storage Tank') | (data.Type_of_Fa == 'Tank Farm'), 'category_new'] = 'Petroleum storage and terminals'

# Create category for "Stations - Other"
otherfaclist = ['Manifold', 'Flow Station', 'Rig', 'Pumping Station']
data.loc[data.Type_of_Fa.str.contains('|'.join(otherfaclist)), 'category_new'] = 'Stations - other'

print(data.category_new.value_counts())
print("Num. of records with null CATEGORY (should be zero):", data.category_new.isnull().sum())

# Create 'on_off' shore category
data['on_off'] = 'Onshore'
data.loc[data.Spill_Area == 'of', 'on_off'] = 'Offshore'

# Create separate gdfs for each infra category
platforms = data[data.category_new == 'Offshore platforms'].reset_index(drop=True)

# Replace 'Site_locat" values containing '' with " (denotes inches)
platforms.Site_locat = platforms.Site_Locat.str.replace("''", '"')

# TODO - Strip special characters from the 'Site_Locat' field

# Drop duplicate records (same attributes & identical or nearly-identical geometries),
# just those records that are indentical in the fields that we care about.
platforms = deduplicate_with_rounded_geoms(platforms,
                                           column_subset=['Company',
                                                          'Type_of_Fa',
                                                          'Site_Locat_casefold'],
                                           desired_precision=5)


# =============================================================================
# %% NIGERIA - oil spills dataset - Integration + Export
# =============================================================================
platforms_NIG2, errors = integrate_facs(
    platforms.reset_index(drop=True),
    starting_ids=0,
    category='Offshore platforms',
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
    platforms_NIG2,
    file_name="nigeria_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% AFRICA - Offshore CPFs
# ===========================================================================
os.chdir(pubdata)

CAM_cpf = gpd.read_file("Africa/Cameroon/CPF.kml.shp")
DRC_cpf = gpd.read_file("Africa/DRC/CPF_Central_Processing_Facility_DRC.kml.shp")
GAN_cpf = gpd.read_file("Africa/Ghana/CPF_Central_Processing_Facility_Ghana.kml.shp")
IVC_cpf = gpd.read_file("Africa/Ivory_Coast/CPF_Central_Processing_Facility_IvoryCoast.kml.shp")
NIG_cpf = gpd.read_file("Africa/Nigeria/CPF_Central_Processing_Facility_Nigeria.kml.shp")
SAFR_cpf = gpd.read_file("Africa/South_Africa/CPF_Central_Processing_Facility_SouthAfrica.kml.shp")

# add fac type where known
CAM_cpf['fac_type'] = "Central Processing Facility"
DRC_cpf['fac_type'] = "Central Processing Facility"
GAN_cpf['fac_type'] = "Central Processing Facility"
IVC_cpf['fac_type'] = "Central Processing Facility"
NIG_cpf['fac_type'] = "Central Processing Facility"
SAFR_cpf['fac_type'] = "Central Processing Facility"

# Add country
CAM_cpf['country'] = "Cameroon"
DRC_cpf['country'] = "Democratic Republic of the Congo"
GAN_cpf['country'] = "Ghana"
IVC_cpf['country'] = "Ivory Coast"
NIG_cpf['country'] = "Nigeria"
SAFR_cpf['country'] = "South Africa"

# Concatenate all offshore platforms into one df
# Confirm that all dataframes are the same CRS before appending into one gdf
all_dfs_final_offshore = [
    CAM_cpf,
    DRC_cpf,
    GAN_cpf,
    IVC_cpf,
    NIG_cpf,
    SAFR_cpf
]
data_cpf_af_plats = pd.concat(all_dfs_final_offshore)
data_cpf_af_plats = transform_CRS(data_cpf_af_plats,
                                  target_epsg_code="epsg:4326",
                                  appendLatLon=True)

# Change certain values in Name column to NA
data_cpf_af_plats['Name'] = data_cpf_af_plats['Name'].fillna('N/A')
data_cpf_af_plats.loc[data_cpf_af_plats.Name == 'Untitled Placemark', 'Name'] = 'N/A'
data_cpf_af_plats.loc[data_cpf_af_plats.Name == 'Placemark', 'Name'] = 'N/A'
data_cpf_af_plats.loc[data_cpf_af_plats.Name == '58', 'Name'] = 'N/A'  # one record only has a number as its name, might be an error

# =============================================================================
# %% AFRICA - Offshore CPFs / platforms - Integration + Export
# =============================================================================
data_cp_off_plats, errors = integrate_facs(
    data_cpf_af_plats,
    starting_ids=0,
    category='Offshore platforms',
    fac_alias='OTHER',
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
    data_cp_off_plats,
    file_name="africa_other_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% AFRICA - Wellhead platforms and FPSOs
# ===========================================================================
os.chdir(pubdata)

CAM_fpso = gpd.read_file("Africa/Cameroon/FPSO_floating_production_storage_and_offloading.kml.shp")
CAM_offWells = gpd.read_file("Africa/Cameroon/WHP_wellhead_platform.kml.shp")

DRC_fpso = gpd.read_file("Africa/DRC/FPSO_floating_production_storage_offloading_DRC.kml.shp")
DRC_offWells = gpd.read_file("Africa/DRC/WHP_wellhead_platform_DRC.kml.shp")

GAN_fpso = gpd.read_file("Africa/Ghana/FPSO_Floating_Production_Storage_and_Offloading_Ghana.kml.shp")

IVC_fpso = gpd.read_file("Africa/Ivory_Coast/FPSO_Floating_Production_Storage_and_Offloading_IvoryCoast.kml.shp")

NIG_offWells = gpd.read_file("Africa/Nigeria/WHP_Wellhead_Platform_Nigeria.kml.shp")
NIG_fpso = gpd.read_file("Africa/Nigeria/FPSO_Floating_Production_Storage_and_Offloading_Nigeria.kml.shp")

SAFR_fpso = gpd.read_file("Africa/South_Africa/FPSO_Floating_Production_Storage_and_Offloading_SouthAfrica.kml.shp")

# Add country info
# ---------------------------------------------------------------------------
for df in [CAM_fpso, CAM_offWells]:
    df['country'] = 'Cameroon'

for df in [DRC_fpso, DRC_offWells]:
    df['country'] = 'Democratic Republic of the Congo'

for df in [NIG_fpso, NIG_offWells]:
    df['country'] = 'Nigeria'

GAN_fpso['country'] = 'Ghana'
IVC_fpso['country'] = 'Ivory Coast'
SAFR_fpso['country'] = 'South Africa'

# Label facility type based on info from original filename
# ---------------------------------------------------------------------------
fpso_df_list = [CAM_fpso, DRC_fpso, GAN_fpso, IVC_fpso, NIG_fpso, SAFR_fpso]
for df in fpso_df_list:
    df['factype'] = 'Floating Production Storage and Offloading'

whp_df_list = [CAM_offWells, DRC_offWells, NIG_offWells]
for df in whp_df_list:
    df['factype'] = 'Wellhead Platform'

# Check that all dfs are same CRS before concatenating into one GDF
# ---------------------------------------------------------------------------
all_dfs = fpso_df_list + whp_df_list
for df in all_dfs:
    print(df.crs)
data = pd.concat(all_dfs)
# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data,
                     target_epsg_code="epsg:4326",
                     appendLatLon=True)

# Insert N/A where needed in Name and Descriptio field
data.loc[data.Name == 'Untitled Placemark', 'Name'] = 'N/A'
data.Name = data.Name.fillna('N/A')

# =============================================================================
# %% AFRICA - Offshore WHPs and FPSOs - Integration + Export
# =============================================================================
data_AF_WHP, errors = integrate_facs(
    data,
    starting_ids=0,
    category='Offshore platforms',
    fac_alias='OTHER',
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='Offshore',
    fac_name='Name',
    # fac_id = None,
    fac_type='factype',
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

save_spatial_data(
    data_AF_WHP,
    file_name="africa_other_offshore_platforms_part_02",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% MIDDLE EAST - Offshore CPFs
# ===========================================================================
os.chdir(pubdata)

iran_cpf = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Facilities\CPF.kml.shp')
qatar_cpf = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Facilities\Processing_Facilities\CPF_Central_Processing_Facility_Qatar.kml.shp')
saudi_cpf = gpd.read_file(r'Middle_East+Caspian\Saudi_Arabia\Oil_Gas_Infra_.com\Facilities\Processing_Facilities\CPF_Central_Processing_Facility_Saudi_Arabia.kml.shp')
uae_cpf = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Facilities\Processing_Facilities\CPF_Central_Processing_Facility_Offshore_UAE.kml.shp')

# Central processing facilities
# ---------------------------------------------------------------------------
cpf = [
    iran_cpf,
    qatar_cpf,
    saudi_cpf,
    uae_cpf
]
# ---------------------------------------------------------------------------
for df in cpf:
    df['type_'] = 'Central processing facility'

uae_cpf['onoff'] = 'Offshore'
# ---------------------------------------------------------------------------
# Add 'country' column to all country-specific gdfs
iran_cpf['country'] = 'Iran'
qatar_cpf['country'] = 'Qatar'
saudi_cpf['country'] = 'Saudi Arabia'
uae_cpf['country'] = 'UAE'

# Offshore processing platforms
all_dfs_final_me_offshore_proc = [
    iran_cpf,
    qatar_cpf,
    saudi_cpf,
    uae_cpf
]

# ---------------------------------------------------------------------------
data_cpf_me = pd.concat(all_dfs_final_me_offshore_proc)
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

# =============================================================================
# %% MIDDLE EAST (Central Processing Platforms) - Integration + Export
# =============================================================================
data_cpf_me2_offshore, errors = integrate_facs(
    data_cpf_me,
    starting_ids=0,
    category='Offshore platforms',
    fac_alias='OTHER',
    country='country',
    # state_prov=None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='onoff',
    fac_name='Name',
    # fac_id=None,
    fac_type='type_',
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
    data_cpf_me2_offshore,
    file_name="middle_east_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% MIDDLE EAST (Wellhead Platforms)
# IRAN, OMAN, QATAR, SAUDI ARABIA,
# ===========================================================================
os.chdir(pubdata)

iran = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Offshore_Platforms\WHP.kml.shp')
oman = gpd.read_file(r'Middle_East+Caspian\Oman\Oil_Gas_Infra_.com\Offshore_Platforms\Offshore_Platform_Oman.kml.shp')
qatar = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Offshore_Platforms\WHP_Wellhead_Platform_Qatar.kml.shp')
saudi = gpd.read_file(r'Middle_East+Caspian\Saudi_Arabia\Oil_Gas_Infra_.com\Offshore_Platforms\WHP_Wellhead_Platform_Saudi_Arabia.kml.shp')
uae = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Offshore_Platforms\WHP_Wellhead_Platform_UAE.kml.shp')

# Assign more specific facility type information, based on what's in filename
# wellhead Platforms
whp_dfs = [
    iran,
    oman,
    qatar,
    saudi,
    uae
]

for df in whp_dfs:
    df['type'] = 'Wellhead Platform'
    df['off'] = 'Offshore'
# ---------------------------------------------------------------------------
# Assign country name to all country-specific gdfs
iran['country'] = 'Iran'
oman['country'] = 'Oman'
qatar['country'] = 'Qatar'
saudi['country'] = 'Saudi Arabia'
uae['country'] = 'UAE'

# Join all dfs into one df
all_dfs = [iran, oman, qatar, saudi, uae]
data = pd.concat(all_dfs)
data = data.reset_index(drop=True)

# Replace all instances of 'Untitled' in Name column
data['Name'] = data.Name.fillna('N/A')
data.loc[data.Name.str.contains('untitled', case=False), 'Name'] = 'N/A'

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# drop duplicate WHPs
data = data.drop_duplicates(subset=['geometry'], keep='first')

# =============================================================================
# %% MIDDLE EAST (Wellhead Platforms) - Integration + Export
# =============================================================================
data_ME_WHP, errors = integrate_facs(
    data,
    starting_ids=0,
    category='Offshore platforms',
    fac_alias='OTHER',
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='off',
    fac_name='Name',
    # fac_id = None,
    fac_type='type',
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

save_spatial_data(
    data_ME_WHP,
    file_name="middle_east_offshore_platforms_part_02",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% NORWAY
# ===========================================================================
os.chdir(pubdata)

fp1 = "Europe\\Norway\\Facilities\\TLP_TensionLegPlatform_Norway.kml.shp"
offshore_platforms1 = read_spatial_data(fp1, table_gradient=True)
offshore_platforms1["FAC_TYPE"] = 'TENSION LEG PLATFORM'
fp2 = "Europe\\Norway\\Facilities\\FPSO_Floating_Production_Storage_and_Offloading_Norway.kml.shp"
offshore_platforms2 = read_spatial_data(fp2, table_gradient=True)
offshore_platforms2["FAC_TYPE"] = 'FLOATING PRODUCTION, STORAGE, AND OFFLOADING'
fp3 = "Europe\\Norway\\Facilities\\CPF_Central_Processing_Facility_Norway.kml.shp"
offshore_platforms3 = read_spatial_data(fp3, table_gradient=True)
offshore_platforms3['FAC_TYPE'] = 'Central Processing Facility'

# ---------------------------------------------------------------------------
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
offshore_platforms11 = transform_CRS(offshore_platforms1, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms22 = transform_CRS(offshore_platforms2, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms33 = transform_CRS(offshore_platforms3, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms_concat = gpd.GeoDataFrame(pd.concat([offshore_platforms11, offshore_platforms22, offshore_platforms33]))
offshore_platforms_concat1 = offshore_platforms_concat[offshore_platforms_concat.latitude_calc != 0]

# ---------------------------------------------------------------------------
# Format Name column
# ---------------------------------------------------------------------------
dict_names = {}
colName = "Name"
dict_names = {
    '?SGARD A': 'ÅSGARD A',
    'NORD?ST FRIGG': 'NORDØST FRIGG',
    'NORD?ST FRIGG A': 'NORDØST FRIGG A',
    'KVITEBJ?RN': 'KVITEBJØRN',
    'OSEBERG ?ST': 'OSEBERG ØST',
    'OSEBERG S?R': 'OSEBERG SØR',
    'VALHALL FLANKE S?R': 'VALHALL FLANKE SØR',
}

# ---------------------------------------------------------------------------
offshore_platforms_concat2 = replace_row_names(offshore_platforms_concat1, colName, dict_names)

offshore_platforms_concat2["FAC_TYPE"] = offshore_platforms_concat2["FAC_TYPE"].replace({"NAN": "N/A", None: "N/A"})

# =============================================================================
# %% NORWAY - Integration + Export
# =============================================================================
offshore_platforms_final_NOR, errors = integrate_facs(
    offshore_platforms_concat2,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Norway",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE",
    # drill_type="FORMATTED_DRILL_TYPE",
    # spud_date = "FORMATTED_SPUD",
    # comp_date="FORMATTED_END",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    offshore_platforms_final_NOR,
    file_name="norway_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% DENMARK
# ===========================================================================
os.chdir(v24data)
# Read data and add source ID for each wells source
fp1 = r"denmark\facilities\OffshoreInstallations_20230322.shp"  # FIXME, schema might have changed
den_platforms = read_spatial_data(fp1,
                                  table_gradient=True,
                                  specify_encoding=True,
                                  data_encoding="utf8")
den_platforms = transform_CRS(den_platforms,
                              target_epsg_code="epsg:4326",
                              appendLatLon=True)


# =============================================================================
# %% DENMARK - Integration + Export
# =============================================================================
den_platforms_final, errors = integrate_facs(
    den_platforms,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Denmark",
    # state_prov="",
    src_ref_id="81",
    src_date="2020-09-07",
    on_offshore="OFFSHORE",
    fac_name="Name",
    fac_id="ID",
    fac_type="Category",
    fac_status="Current_St",
    op_name="Operator",
    # install_date='',
    commodity='Primary_pr',
    # liq_capacity_bpd = "",
    # liq_throughput_bpd="",
    # gas_capacity_mmcfd="",
    # gas_throughput_mmcfd="",
    # num_storage_tanks="",
    # site_hp = "",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    den_platforms_final,
    file_name="denmark_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% NETHERLANDS
# ===========================================================================
# OLD OILANDGASINFRASTRUCTURE.COM DATA
# os.chdir(pubdata)
# fp1 = "Europe\\Netherlands\\Facilities\\CPF_Central_Processing_Facility_Netherlands.kml.shp"
# offshore_platforms = read_spatial_data(fp1, table_gradient=True)
# offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
# offshore_platforms1['FAC_TYPE'] = 'Central Processing Facility'
# # Wellhead platforms
# fp3 = "Europe\\Netherlands\\Wells\\WHP_Wellhead_Platform_Netherlands.kml.shp"
# offshore_platforms2 = read_spatial_data(fp3, table_gradient=True)
# offshore_platforms2['FAC_TYPE'] = 'Wellhead Platform'
# offshore_platforms22 = transform_CRS(offshore_platforms2, target_epsg_code="epsg:4326", appendLatLon=True)
# offshore_platforms_concat = gpd.GeoDataFrame(pd.concat([offshore_platforms1, offshore_platforms22]))


nl_facs = gpd.read_file(r'netherlands/Feb-2025_NLOG_Facilities_ED50UTM31N.shp')
nl_facs = transform_CRS(nl_facs, target_epsg_code="epsg:4326", appendLatLon=True)

# Translate TYPE from Dutch
nl_facs.TYPE = nl_facs.TYPE.replace({
    'Productieplatform': 'Production platform',
    'Productiesatelliet': 'Production satellite',
    'Productielocatie': 'Production location',
    # 'Plant': 'Plant',
    # 'Geothermie installatie': 'Geothermal installation',
    # 'Subsea': 'Subsea',
    # 'Sidetap'
    # 'Tanker mooring and loading system'
    # 'Off-load Terminal'
    'Raffinaderij': 'Refinery'})

# Translate status from Dutch
nl_facs.STATUS = nl_facs.STATUS.replace({
    'In gebruik': 'In use',
    'Verwijderd': 'Removed',
    'Buiten gebruik gesteld': 'Decommissioned',
    'Niet in gebruik': 'Not in use'})

# Only keep production platforms
nl_plats = nl_facs.query("TYPE == 'Production platform'")
# Only keep platforms with a country code of Netherlands (NLD)
nl_plats = nl_plats.query("COUNTRY_CD == 'NLD'")
# Drop any platforms that are marked as "removed"
nl_plats = nl_plats.query("STATUS != 'Removed'").reset_index()

# =============================================================================
# %% NETHERLANDS - Integration + Export
# =============================================================================
nl_plats_integrated, errors = integrate_facs(
    nl_plats,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Netherlands",
    # state_prov="",
    src_ref_id="263",
    src_date="2024-11-01",
    on_offshore="Offshore",
    fac_name="FACILITY_N",
    # fac_id="IDENTIFIER",
    fac_type="TYPE",
    # drill_type="FORMATTED_DRILL_TYPE",
    fac_status="STATUS",
    op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    nl_plats_integrated,
    file_name="netherlands_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% UNITED KINGDOM
# ===========================================================================
os.chdir(v24data)
uk_points = gpd.read_file(r'united_kingdom/Surface_Points_(WGS84).geojson')
uk_points = transform_CRS(uk_points, target_epsg_code="epsg:4326", appendLatLon=True)

# Drop cols I don't need
uk_points = uk_points.filter(['NAME',
                              'INF_TYPE',
                              'REP_GROUP',
                              'DESCRIPTIO',
                              'STATUS',
                              'START_DATE',
                              'latitude_calc',
                              'longitude_calc',
                              'geometry'])
# Make a few columns all-caps
uk_points.NAME = uk_points.NAME.str.upper()
uk_points.DESCRIPTIO = uk_points.DESCRIPTIO.str.upper()

# Only keep points that are offshore platforms (for example, drop flare stacks
# and onshore terminals)
types2keep = ['PLATFORM', 'FPSO', 'FSO']
uk_plats = uk_points[uk_points.INF_TYPE.isin(types2keep)]

# Drop facilities that are not physically in place
status2drop = ['REMOVED', 'PRECOMMISSIONED', 'PROPOSED']
uk_plats = uk_plats[~uk_plats.STATUS.isin(status2drop)]

# Format install date
uk_plats['installdate'] = pd.to_datetime(uk_plats['START_DATE']).dt.strftime("%Y-%m-%d")

# Create a new "factype" field
uk_plats['factypenew'] = uk_plats.DESCRIPTIO
# If a record is a FPSO or FSO, let this be the factype
uk_plats.loc[uk_plats.INF_TYPE.isin(['FPSO', 'FSO']), 'factypenew'] = uk_plats.INF_TYPE
# if factypenew is identical to the name, drop it
uk_plats.loc[uk_plats.factypenew == uk_plats.NAME, 'factypenew'] = 'N/A'
# if factypenew is identical to the name plus the inf_type, drop it
uk_plats['nameplusinftype'] = uk_plats.NAME + ' ' + uk_plats.INF_TYPE
uk_plats.loc[uk_plats.factypenew == uk_plats.nameplusinftype, 'factypenew'] = 'N/A'
# Finally, if there is truly nothing in the factypenew col and its a platform,
# add the word platform there
uk_plats.loc[((uk_plats.factypenew == 'N/A') & (uk_plats.INF_TYPE == 'PLATFORM')), 'factypenew'] = 'PLATFORM'

# =============================================================================
# %% UNITED KINGDOM - Integration + Export
# =============================================================================
uk_plats_integrated, errors = integrate_facs(
    uk_plats.reset_index(),
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="United Kingdom",
    # state_prov="",
    src_ref_id="265",
    src_date="2024-01-24",
    on_offshore="Offshore",
    fac_name="NAME",
    # fac_id="",
    fac_type="factypenew",
    install_date='installdate',
    fac_status="STATUS",
    op_name="REP_GROUP",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    uk_plats_integrated,
    file_name="united_kingdom_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% KAZAKHSTAN
# ===========================================================================
os.chdir(pubdata)
fp4 = r'Middle_East+Caspian\Kazakhstan\Oil_Gas_Infra_.com\Facilities\Offshore_Central_Processing_Facility\CPF_Kazakhstan.kml.shp'
offshore_platforms = read_spatial_data(fp4, table_gradient=False)
offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1["FAC_TYPE_"] = 'Central Processing Facility'

# ---------------------------------------------------------------------------
# % Data integration
# ---------------------------------------------------------------------------
offshore_platforms_final_KZK, errors = integrate_facs(
    offshore_platforms1,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Kazakhstan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE_",
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


save_spatial_data(
    offshore_platforms_final_KZK,
    file_name="kazakhstan_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% AZERBAIJAN
# ===========================================================================
os.chdir(pubdata)
fp4 = "Middle_East+Caspian\\Azerbaijan\\Oil_Gas_Infra_.com\\Facilities\\CPF_Central_Processing_Facility_Azerbajan.kml.shp"
offshore_platforms = read_spatial_data(fp4, table_gradient=False)
offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1["FAC_TYPE"] = 'Central Processing Facility'

fp5 = "Middle_East+Caspian\\Azerbaijan\\Oil_Gas_Infra_.com\\Offshore_Wells\\WHP_Azerbajan.kml.shp"
offshore_platforms2 = read_spatial_data(fp5, table_gradient=False)
offshore_platforms22 = transform_CRS(offshore_platforms2, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms22["FAC_TYPE"] = "Wellhead Platform"

offshore_platforms_concat = gpd.GeoDataFrame(pd.concat([offshore_platforms1, offshore_platforms22]))

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------
offshore_platforms_final_AZER, errors = integrate_facs(
    offshore_platforms_concat,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Azerbaijan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE_",
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

save_spatial_data(
    offshore_platforms_final_AZER,
    file_name="azerbaijan_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% TURKMENISTAN
# ===========================================================================
os.chdir(pubdata)
fp5 = "Middle_East+Caspian\\Turkmenistan\\Oil_Gas_Infra_.com\\Offshore_Platforms\\WHP_Wellhead_Platform_Turkmenistan.kml.shp"
offshore_platforms = read_spatial_data(fp5, table_gradient=True)
offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1["FAC_TYPE"] = "WELLHEAD PLATFORM"
offshore_platforms1["Name"] = offshore_platforms1["Name"].replace({"NAN": "N/A", None: "N/A"})

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------

offshore_plat_final_TURK, errors = integrate_facs(
    offshore_platforms1,
    starting_ids=0,
    category="Offshore platforms",
    fac_alias="OTHER",
    country="Turkmenistan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="OFFSHORE",
    fac_name="Name",
    # fac_id="",
    fac_type="FAC_TYPE",
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
    offshore_plat_final_TURK,
    file_name="turkmenistan_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% BANGLADESH
# ===========================================================================
os.chdir(pubdata)
fp5 = "China+SE_Asia\\Bangladesh\\CPF_Central_Processing_Facility_Bangladesh.kml.shp"
offshore_platforms = read_spatial_data(fp5, table_gradient=True)
offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1['FAC_TYPE'] = 'Central Processing Facility'

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------
offshore_platforms_final_BANG, errors = integrate_facs(
    offshore_platforms1,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE",
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

save_spatial_data(
    offshore_platforms_final_BANG,
    file_name="bangladesh_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% INDIA
# ===========================================================================
os.chdir(pubdata)
fp5 = r"China+SE_Asia\India\CPF_Central_Processing_Facility_India.kml.shp"
offshore_platform = read_spatial_data(fp5, table_gradient=True)
offshore_platform1 = transform_CRS(offshore_platform, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1['FAC_TYPE'] = 'Central Processing Facility'

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------

offshore_platforms_IND, errors = integrate_facs(
    offshore_platforms1,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="India",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    on_offshore="OFFSHORE",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE",
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

save_spatial_data(
    offshore_platforms_IND,
    file_name="india_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% MYANMAR
# ===========================================================================
os.chdir(pubdata)
fp5 = "China+SE_Asia\\Myanmar\\CPF_Central_Processing_Facility_Myanmar.kml.shp"
offshore_platforms = read_spatial_data(fp5, table_gradient=True)
offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1["FAC_TYPE_"] = 'Central Processing Facility'

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------

offshore_platforms_final_MYAN, errors = integrate_facs(
    offshore_platforms1,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Myanmar",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE_",
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


save_spatial_data(
    offshore_platforms_final_MYAN,
    file_name="myanmar_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)

# ===========================================================================
# %% THAILAND
# ===========================================================================
os.chdir(pubdata)
fp1 = "China+SE_Asia\\Thailand\\Offshore_platforms_Thailand.kml.shp"
offshore_platforms = read_spatial_data(fp1, table_gradient=True)
offshore_platforms1 = transform_CRS(offshore_platforms, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms1["FAC_TYPE"] = "N/A"

fp2 = "China+SE_Asia\\Thailand\\FPSO_Floating_Production_Storage_and_Offloading_Thailand.kml.shp"
offshore_platforms2 = read_spatial_data(fp2, table_gradient=True)
offshore_platforms22 = transform_CRS(offshore_platforms2, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms22["FAC_TYPE"] = "FLOATING PRODUCTION STORAGE AND OFFLOADING"

offshore_platforms_concat = gpd.GeoDataFrame(pd.concat([offshore_platforms1, offshore_platforms22]))

# ---------------------------------------------------------------------------
# % Data integration
#  - Apply standard data schema
# ---------------------------------------------------------------------------
offshore_platforms_final_THAI, errors = integrate_facs(
    offshore_platforms_concat,
    starting_ids=0,
    category="Offshore Platforms",
    fac_alias="OTHER",
    country="Thailand",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="OFFSHORE",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    fac_type="FAC_TYPE",
    # drill_type="FORMATTED_DRILL_TYPE",
    # spud_date = "FORMATTED_SPUD",
    # comp_date="FORMATTED_END",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    offshore_platforms_final_THAI,
    file_name="thailand_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)


# ===========================================================================
# %% AUSTRALIA AND NEW ZEALAND
# ===========================================================================
os.chdir(pubdata)
fp4 = r"Australia+NewZealand\Australia\CPF_Central_Processing_Facility_Offshore_Australia.kml.shp"
offshore_platforms1 = read_spatial_data(fp4, table_gradient=True)
offshore_platforms11 = transform_CRS(offshore_platforms1, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms11['FAC_TYPE'] = 'CENTRAL PROCESSING FACILITY'

fp5 = r"Australia+NewZealand\Australia\FPSO_Floating_Production_Storage_and_Offloading_Offshore_Australia.kml.shp"
offshore_platforms2 = read_spatial_data(fp5, table_gradient=True)
offshore_platforms22 = transform_CRS(offshore_platforms2, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms22['FAC_TYPE'] = 'FLOATING PRODUCTION STORAGE AND OFFLOADING'

fp8 = r"Australia+NewZealand\Australia\WHP_Wellhead_Platform_Offshore_Australia.kml.shp"
offshore_platforms3 = read_spatial_data(fp8, table_gradient=True)
offshore_platforms33 = transform_CRS(offshore_platforms3, target_epsg_code="epsg:4326", appendLatLon=True)
offshore_platforms33["FAC_TYPE"] = "WELLHEAD PLATFORM"

offshore_platforms_concat = gpd.GeoDataFrame(pd.concat([offshore_platforms11, offshore_platforms22, offshore_platforms33]))

# Fix names attributes

names2 = []
for idx1_, row1_ in tqdm(offshore_platforms_concat.iterrows(), total=offshore_platforms_concat.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.Name
    if "Untitled" in name:
        names2.append('N/A')

    else:
        names2.append(name)

offshore_platforms_concat['NAME_'] = names2
# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------

offshore_plat_final_AUS, errors = integrate_facs(
    offshore_platforms_concat,
    starting_ids=0,
    category="Offshore platforms",
    fac_alias="OTHER",
    country="Australia",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    on_offshore="OFFSHORE",
    fac_name="Name",
    # fac_id="",
    fac_type="FAC_TYPE",
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

offshore_plat_final_AUS2 = replace_row_names(offshore_plat_final_AUS, colName="FAC_TYPE", dict_names={'NAN': 'N/A'})


save_spatial_data(
    offshore_plat_final_AUS2,
    file_name="australia_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_plats
)
