# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global PETROLEUM TERMINALS.

@author: maobrien, momara, ahimmelberger
"""
import os
# import re
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
from unidecode import unidecode
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, standardize_dates_hifld_us,
                     transform_CRS, integrate_facs, save_spatial_data,
                     schema_LNG_STORAGE, read_spatial_data, dict_us_states)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
save_terms = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% UNITED STATES
# =============================================================================
os.chdir(v24data)
terms_US = read_spatial_data(r"united_states\national\HIFLD\POL_Terminals.geojson")

# Select US
terms_us = terms_US.query("COUNTRY=='USA'")

# Fix state names
terms_us2 = replace_row_names(terms_us, "STATE", dict_names=dict_us_states)

# Fix dates
terms_us3 = standardize_dates_hifld_us(terms_us2,
                                       attrName="SOURCEDATE",
                                       newAttrName="src_dates")

# Transform CRS
terms_us4 = transform_CRS(terms_us3, appendLatLon=True)

# Fix "N/A"
terms_us5 = replace_row_names(terms_us4,
                              "COMMODITY",
                              dict_names={'NOT AVAILABLE': 'N/A'}).reset_index(drop=True)

# =============================================================================
# %% UNITED STATES - Integration + Export
# =============================================================================
terms_us7, terms_errors_3 = integrate_facs(
    terms_us5,
    starting_ids=1,
    category="PETROLEUM TERMINALS",
    fac_alias="LNG_STORAGE",
    country="United States",
    state_prov="STATE",
    src_ref_id="93",
    src_date="2021-07-09",
    on_offshore="Onshore",
    fac_name="NAME",
    fac_id="TERM_ID",
    fac_type="TYPE",
    install_date=None,
    fac_status='STATUS',
    op_name="OPERATOR",
    commodity="COMMODITY",
    liq_capacity_bpd='CAPACITY',
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
    terms_us7,
    file_name="united_states_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% BRAZIL
# ===========================================================================
os.chdir(v24data)
# EPE dataset contains capacity info we want
# Contains a special character and non-accented name field
epe_terms = gpd.read_file(r'brazil\WebMap_EPE\Oil_and_Fuels_Terminals.shp')  # 117 records
# Rename some columns
epe_terms = epe_terms.rename(columns={'nome_ter': 'name'}, errors='raise')
epe_terms['name_decode'] = epe_terms.name.apply(lambda x: unidecode(x))
epe_terms = epe_terms[['name',
                       'name_decode',
                       'munic',
                       'cap_pe',
                       'cap_de_bio',
                       'cap_glp',
                       'num_tanq',
                       'geometry']]

# ANP dataset contains operator info we want
# Name field contains special characters
anp_terms = gpd.read_file(r'brazil\GeoMapsANP\terminais_de_combustiveis_liquidosPoint.shp')  # 135 records
# Rename some columns
anp_terms = anp_terms.rename(columns={'NOME': 'name',
                                      'MUNICIPIO': 'munic',
                                      'RAZAO_SOCI': 'company',
                                      'SITUACAO_A': 'status'}, errors='raise')
anp_terms['name_decode'] = anp_terms.name.apply(lambda x: unidecode(x))
anp_terms = anp_terms[['name',
                       'name_decode',
                       'munic',
                       'company',
                       'status',
                       'geometry']]

# FOR NOW, if there are any EPE records that are a PERFECT MATCH for a ANP name,
# join them, so each terminal has operator and capacity info.
anp_plus = anp_terms.merge(epe_terms,
                           how='left',
                           on='name_decode',
                           suffixes=('_x', '_y'))
anp_plus = anp_plus.set_geometry('geometry_x')
anp_plus = transform_CRS(anp_plus, appendLatLon=True)

values2translate = ['AUTORIZADA OPERACAO', 'AUTORIZADA OPERAÇÃO']
anp_plus.loc[anp_plus.status.isin(values2translate), 'status'] = 'OPERATION AUTHORIZED'

# TODO - Convert capacity units; have to ask Mark how I should convert and
# express the (volumetric) capacity of a terminal in a "per day" rate capacity like BPD

# cap_pe = Oil storage capacity (m^3)
# cap_de_bio = Oil products and biofuels storage capacity (m^3)
# cap_glp = LPG storage capacity (m^3)



# ----------------------------------------------------------------------------
# TODO - join the facilities even if their names are not identical

# from shapely.ops import nearest_points

# # Create a unary union of the point gdf that you want to compare to your first gdf
# anp_terms_union = anp_terms.geometry.unary_union


# def near(point, pts=anp_terms_union):
#     # Find the ANP point nearest to the provided point geometry
#     # Return the corresponding NAME value
#     nearest = anp_terms.geometry == nearest_points(point, pts)[1]
#     return anp_terms[nearest].name.values[0]


# epe_terms['Nearest_ANP_point'] = epe_terms.apply(lambda row: near(row.geometry), axis=1)


# # use the EPE "unicode normalized" name field to associate each ANP
# # record with a "decoded" version of its name
# joined = anp_terms.merge(epe_terms,
#                             how='outer',
#                             on='name_decode',
#                             suffixes=('_x', '_y'))


# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
brz_terms_integrated, terms_err = integrate_facs(
    anp_plus.reset_index(drop=True),
    starting_ids=1,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Brazil",
    # state_prov=None,
    src_ref_id="126, 267",
    src_date="2024-11-04",
    # on_offshore=None,
    fac_name="name_x",
    # fac_id=None,
    # fac_type=None,
    # install_date=None,
    fac_status="status",
    op_name='company',
    # commodity=None,
    liq_capacity_bpd=None,  # TODO
    # liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,  # TODO
    # gas_throughput_mmcfd=None,
    # num_compr_units=None,
    num_storage_tanks='num_tanq',
    # site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    brz_terms_integrated,
    file_name="brazil_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% ARGENTINA
# ===========================================================================
os.chdir(v24data)
fp_terms = r'argentina\facilities\comercializacin-de-hidrocarburos-terminales-de-despacho-de-combustibles-lquidos-segn-res-110204--shp.shp'
terms_ARG = read_spatial_data(fp_terms, specify_encoding=True, data_encoding="utf-8")
terms_ARG = transform_CRS(terms_ARG, appendLatLon=True)

terms_ARG['factype'] = 'Liquid fuel dispatch terminal'

# Remove the trailing zero from the "DIRECCION" field, which we're using
# in lieu of a FAC_NAME
terms_ARG['DIRECCION'] = terms_ARG['DIRECCION'].str.rstrip(' 0')

terms_ARG = terms_ARG.drop_duplicates(subset=['DIRECCION',
                                              'CUIT',
                                              'factype',
                                              'RAZON_SOCI',
                                              'geometry'], keep='first')

# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
terms_ARG_integrated, terms_ArgEr = integrate_facs(
    terms_ARG,
    starting_ids=1,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Argentina",
    state_prov='PROVINCIA',
    src_ref_id="112",
    src_date="2023-01-25",
    on_offshore=None,
    fac_name='DIRECCION',
    fac_id="CUIT",
    fac_type="factype",
    # install_date=None,
    # fac_status=None,
    op_name='RAZON_SOCI',
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
    terms_ARG_integrated,
    file_name="argentina_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
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

terminals_ = [
    'SHIPPING TERMINAL',
    'TANK YARD'
]
vez_terminals = vez_fac[vez_fac['TIPO'].isin(terminals_)].reset_index(drop=True)

# =============================================================================
# %% VENEZUELA - Integration + Export
# =============================================================================
terms_VEZ, _err = integrate_facs(
    vez_terminals,
    starting_ids=1,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
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
    commodity="MAPA",
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
    terms_VEZ,
    file_name="venezuela_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% AFRICA - oginfra.com - REMOVE and use USGS instead
# ===========================================================================
# os.chdir(pubdata)

# drc_terminals = read_spatial_data("Africa/DRC/Terminal_DRC.kml.shp")
# cam_terminals = read_spatial_data("Africa/Cameroon/Terminal.kml.shp")
# gan_terminals = read_spatial_data("Africa/Ghana/Terminals_Ghana.kml.shp")
# ivc_terminals = read_spatial_data("Africa/Ivory_Coast/Terminals_IvoryCoast.kml.shp")
# lib_terminals = read_spatial_data("Africa/Libya/Terminal_Onshore_Libya.kml.shp")
# moz_terminals = read_spatial_data("Africa/Mozambique/Terminals_Mozambique.kml.shp")
# nig_terminals = read_spatial_data("Africa/Nigeria/Terminal_Nigeria.kml.shp")
# sAfric_terminals = read_spatial_data("Africa/South_Africa/Terminals_SouthAfrica.kml.shp")

# # ---------------------------------------------------------------------------
# # % Data manipulation / processing if needed
# # ---------------------------------------------------------------------------
# # Add "Country" value to each gdf
# drc_terminals['country'] = 'Democratic Republic of the Congo'
# cam_terminals['country'] = 'Cameroon'
# gan_terminals['country'] = 'Ghana'
# ivc_terminals['country'] = 'Ivory Coast'
# lib_terminals['country'] = 'Libya'
# moz_terminals['country'] = 'Mozambique'
# nig_terminals['country'] = 'Nigeria'
# sAfric_terminals['country'] = 'South Africa'

# # ---------------------------------------------------------------------------
# # Confirm that all dataframes are the same CRS before appending into one gdf
# all_dfs_final = [
#     drc_terminals,
#     cam_terminals,
#     gan_terminals,
#     ivc_terminals,
#     lib_terminals,
#     moz_terminals,
#     nig_terminals,
#     sAfric_terminals
# ]

# for df in all_dfs_final:
#     print(df.crs)
# # ---------------------------------------------------------------------------
# # Append country-specific gdfs into one gdf
# data = pd.concat(all_dfs_final)
# data = data.reset_index(drop=True)

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)
# # ---------------------------------------------------------------------------
# # Change values in 'Name' column to properly mark N/A values
# data.loc[data.Name == 'Untitled Placemark', 'Name'] = 'N/A'
# data.loc[data.Name.isna(), 'Name'] = 'N/A'

# # ---------------------------------------------------------------------------
# # % Quick map to investigate potential duplicate features
# world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
# base = world[world.continent == 'Africa'].boundary.plot(color='black')
# data.plot(ax=base, color='red', markersize=7)
# # ---------------------------------------------------------------------------
# '''
# Upon inspection of these points in Google Maps, locations with the same
# name DO appear to represent distinct facilities -- the "name" moreso
# describes the CITY the facility is in. I haven't removed any points as being
# duplicates.
# '''

# # =============================================================================
# # %% AFRICA - oginfra.com - Integration + Export
# # =============================================================================
# data_TERMS_AF, errors = integrate_facs(
#     data.reset_index(drop=True),
#     starting_ids=0,
#     category='Petroleum terminals',
#     fac_alias='LNG_STORAGE',
#     country='country',
#     # state_prov = None,
#     src_ref_id='22',
#     src_date='2014-01-01',
#     # on_offshore = None,
#     fac_name='Name',
#     # fac_id = None,
#     # fac_type = None,
#     # spud_date = None,
#     # comp_date = None,
#     # drill_type = None,
#     # install_date = None,
#     # fac_status = None,
#     # op_name = None,
#     # commodity = None,
#     # liq_capacity_bpd = None,
#     # liq_throughput_bpd = None,
#     # gas_capacity_mmcfd = None,
#     # gas_throughput_mmcfd = None,
#     # num_compr_units = None,
#     # num_storage_tanks = None,
#     # site_hp = None,
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )

# save_spatial_data(
#     data_TERMS_AF,
#     file_name="africa_petroleum_terminals",
#     schema=schema_LNG_STORAGE,
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path=save_terms
# )

# ===========================================================================
# %% LIBYA - ArcGIS Online dataset
# ===========================================================================
os.chdir(pubdata)
fp = r"Africa\Libya\Libya_AGO_kmitchell\Libya_Infrastructure_Storage.shp"
data = read_spatial_data(fp, table_gradient=False)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)
# ---------------------------------------------------------------------------
# % Data manipulation / processing if needed
# ---------------------------------------------------------------------------
'''
# According to the field names in the webmap, the FULL variable names are as follows.
HOWEVER, since these capacities are just what the storage terminals CAN HOLD,
and don't provide input about flow / throughput, don't integrate these capacity attributes.

capacity_l = CAPACITY_LIQUID_MMTONS
capacity_1 = CAPACITY_LIQUID_MBBL
capacity_2 = CAPACITY_LIQUID_MSCM
capacity_g = CAPACITY_GAS_MMSCM
capacity_3 = CAPACITY_GAS_MMSCF
capacity_4 = CAPACITY_GAS_BSCM

on_line_da = ON_LINE_DATE_TEXT
number_of_ = NUMBER_OF_TANKS
'''

# Convert single known on-line date to the format we want
data['on_line_da'] = data['on_line_da'].fillna('1900')
data['on_line_da'] = data['on_line_da'] + '-01-01'

# One terminal does have a value for "number of tanks" --
# fill the rest of the cells that have zero with -999 instead
data.loc[data['number_of_'] == 0, 'number_of_'] = -999

# Only keep the terminals that are NOT already represented in the USGS Africa dataset
terms2keep = ['Marsa El Hariga', 'Zueitina']
libya_ago_terms = data[data.storage_na.isin(terms2keep)].reset_index()


# =============================================================================
# %% LIBYA - ArcGIS Online dataset - Integration + Export
# =============================================================================
data_STOR_LIB, errors = integrate_facs(
    data.reset_index(drop=True),
    starting_ids=0,
    category='Petroleum Terminals',
    fac_alias='LNG_STORAGE',
    country='Libya',
    # state_prov = None,
    src_ref_id='166',
    src_date='2017-06-01',
    # on_offshore=None,
    fac_name='storage_na',
    # fac_id=None,
    fac_type='storage_ty',
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    install_date='on_line_da',
    fac_status='status',
    op_name='operator_n',
    commodity='fluid_type',
    # liq_capacity_bpd = None,
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    # num_compr_units = None,
    num_storage_tanks='number_of_',
    # site_hp = None,
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    data_STOR_LIB,
    file_name="libya_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% NIGERIA - OIL SPILLS DATASET
# ===========================================================================
os.chdir(pubdata)
fp = "Africa//Nigeria//AGO_data_source.@3mmanu3l//OilSpillDatabase.shp"
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

data["Type_of_Fa"] = data["Type_of_fa"]
data = data[-data.Type_of_Fa.isin(types2drop)]
# ---------------------------------------------------------------------------
# Fix and re-name FAC_TYPE values I know to be wrong (and RETAIN the ones I don't want to change)
# ---------------------------------------------------------------------------
# First, create dictionary of the old fac_type (key) and the new fac_type (value)
factypedict = {
    'mf': 'Manifold',
    'Flow Storage Tankation': 'Flow Station',
    'Floating or Production or Storage TankoRigae  or Offloading PipelineaTank Farmorms': 'Floating production storage and offloading',
    'Pumping Storage Tankation': 'Pumping Station',
    'Compressor Pipelineant': 'Compressor Station'
}
# ---------------------------------------------------------------------------
data['Type_of_Fa'] = data['Type_of_Fa'].map(factypedict).fillna(data['Type_of_Fa'])

# % Fill in null fac_type values, based on other clues elsewhere in the table
# ---------------------------------------------------------------------------
# Location description
data["Site_Locat"] = data["Site_locat"]

# Spill area
data["Spill_Area"] = data["Spill_area"]

# Create lowercase version of Site_Locat attribute, for easier string comparison
data['Site_Locat_casefold'] = data.Site_Locat.str.lower()

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
# ---------------------------------------------------------------------------
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
# ---------------------------------------------------------------------------
# Create 'on_off' shore category
data['on_off'] = 'Onshore'
data.loc[data.Spill_Area == 'of', 'on_off'] = 'Offshore'
# ---------------------------------------------------------------------------
# Create separate gdfs for each infra category
storage = data[data.category_new == 'Petroleum storage and terminals'].reset_index(drop=True)

# Replace 'Site_locat" values containing '' with " (denotes inches)
storage.Site_locat = storage.Site_locat.str.replace("''", '"')

# TODO - Strip special characters from the 'Site_Locat' field

# Drop records that are duplicated in the fields we care about
storage = storage.drop_duplicates(subset=['Company',
                                          'Type_of_Fa',
                                          'Site_Locat',
                                          'Latitude',
                                          'Longitude'],
                                  keep='first').reset_index()

# =============================================================================
# %% NIGERIA - OIL SPILLS DATASET - Integration + Export
# =============================================================================
storage_NIG2, errors = integrate_facs(
    storage.reset_index(drop=True),
    starting_ids=0,
    category='Petroleum terminals',
    fac_alias='LNG_STORAGE',
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

# Drop any records with NULL geometries
storage_NIG3 = storage_NIG2[~storage_NIG2.LATITUDE.isnull()].reset_index(drop=True)
print("# of records in original dataset versus cleaned dataset = ", storage_NIG2.shape[0], " ", storage_NIG3.shape[0])


save_spatial_data(
    storage_NIG3,
    file_name="nigeria_petroleum_terminals_part_02",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% AFRICA - USGS
# ===========================================================================
os.chdir(pubdata)
fp = r"Africa\Continent\Africa_GIS.gdb\AFR_Infra_Transport_Ports.shp"
data = gpd.read_file(fp)

# ---------------------------------------------------------------------------
# % Data manipulation / processing if needed
# ---------------------------------------------------------------------------
# Based on USGS documentation,
# "DsgAttr04" = Commodity Form
# "DsgAttr05" = Estimated Annual Capacity
# "DsgAttr06" = Capacity Units
# rename columns accordingly
data = data.rename(columns={"DsgAttr04": "CommodityForm",
                            "DsgAttr05": "EstimatedAnnualCapacity",
                            "DsgAttr06": "CapacityUnits"}, errors="raise")

# Select only oil and gas terminals, remove other cargo ports
data = data[data.FeatureTyp == 'Oil and Gas Terminal']

# properly convert missing values to null cells
data.loc[data.OperateNam == '<Null>', 'OperateNam'] = 'N/A'
data.loc[data.CapacityUnits == 'NA', 'CapacityUnits'] = 'N/A'
# ---------------------------------------------------------------------------
# Since many capacities are reported ANNUALLY, convert these annual values to a per day estimate
# First, write an expression to select only rows that aren't NA and aren't already in per-day
expression = ((~data.CapacityUnits.str.contains('per day')) & (data.CapacityUnits != 'N/A'))
data['EstimatedPerDayCapacity'] = data.EstimatedAnnualCapacity
data.loc[expression, 'EstimatedPerDayCapacity'] = data.EstimatedAnnualCapacity / 365
# After this conversion is done, note the new capacity unit in a new column
data['capacity_units_new'] = data.CapacityUnits
data.loc[expression, 'capacity_units_new'] = data.capacity_units_new + ' per day'

# ---------------------------------------------------------------------------
# For each row, create a "string" version of capacity information as new attribute column
# (just for rows where capacity isn't NA or -999 in the original dataset)
data.loc[data.EstimatedAnnualCapacity != -999, 'capacity_string'] = data.EstimatedPerDayCapacity.apply(lambda x: "{0:.5g}".format(x)) + ' ' + data.capacity_units_new
# Fill in my N/A value of choice in my new capacity_string column
data['capacity_string'] = data['capacity_string'].fillna('N/A')
# ---------------------------------------------------------------------------

# % Create a new column that lists all commodities (+ capacities if available)
# associated with each unique terminal (i.e. each unique 'FeatureUID')
# NOTE: All values in my new field 'commodity_details' will be THE SAME for rows
# with the same 'FeatureUID'

# Make list of all IDs that identify rows belonging to a distinct terminal
unique_ids = list(data.FeatureUID.unique())

# ---------------------------------------------------------------------------
# Iterate through the rows belonging to each facility
# ---------------------------------------------------------------------------
for id_ in unique_ids:
    # Subset data to only rows belonging to that facility
    facility_rows = data[data.FeatureUID == id_]

    # zip together the Commodity and capacity_string column I created earlier
    # so each commodity (for the given facility) is paired with its capacity + units
    capacity_dict = dict(zip(facility_rows.CommodityForm, facility_rows.capacity_string))

    # If ALL the capacity_string values are N/A, don't bother writing those to the output
    # Just write what the commodity/commodities is
    if all(x == 'N/A' for x in capacity_dict.values()):
        key_string = ', '.join(list(capacity_dict.keys()))
        data.loc[data.FeatureUID == id_, 'commodity_details'] = key_string
    else:
        # If there is some non-NA capacity info for a commodity,
        # Write the whole commodity-capacity dictionary as a string to my new column
        data.loc[data.FeatureUID == id_, 'commodity_details'] = str(capacity_dict)

    # Do the same thing for Operator names, since it's possible to have different operators at a port, so capture all of them
    operator_list = facility_rows.OperateNam.unique()

    if all(x == 'N/A' for x in operator_list):
        data.loc[data.FeatureUID == id_, 'operator_new'] = 'N/A'
    else:
        # If there is some non-NA capacity info for a commodity,
        # Write the whole commodity-capacity dictionary as a string to my new column
        operator_list_ = [e for e in operator_list if e not in ('N/A')]
        operator_list_string = '; '.join(operator_list_)
        data.loc[data.FeatureUID == id_, 'operator_new'] = operator_list_string

# ---------------------------------------------------------------------------
# % Group my data table by FeatureUID, i.e., make only one row exist for each terminal
# ---------------------------------------------------------------------------
agg_dictionary = {
    'Country': 'first',
    'FeatureNam': 'first',
    # 'DsgAttr04': list,
    # 'capacity_string':list,
    'commodity_details': 'first',
    'OperateNam': 'first',
    'OBJECTID': 'count',
    'Latitude': 'first',
    'Longitude': 'first',
    'operator_new': 'first'
}
data_grouped = data.groupby(by=['FeatureUID']).agg(agg_dictionary)
data_grouped = data_grouped.reset_index(drop=False)

# % Since GroupBy operation turned my gdf into a df, turn it back into spatial data
data_final = gpd.GeoDataFrame(data_grouped,
                              geometry=gpd.points_from_xy(data_grouped.Longitude, data_grouped.Latitude),
                              crs=4326)
# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data_final = transform_CRS(data_final, target_epsg_code="epsg:4326", appendLatLon=True)


# Format commodity to remove ''
# I've commented out some other formatting below that I started that could be done on this to salvabe some throughout numbers, it just got extremely complex trying to do so
data_final.commodity_details = data_final.commodity_details.str.replace("'", "")


commodity = []

for idx1_, row1_ in tqdm(data_final.iterrows(), total=data_final.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
    name = row1_.commodity_details
    if "{" in name:
        name1 = name.split("{")[1]
        name1_ = name1.split("}")[0]
        if "Unspecified" in name1_:
            commodity.append("N/A")

        elif ": " in name1_:
            name2 = name1_.split(": ")[0]
            commodity.append(name2)

        else:
            commodity.append(name1_)

    else:
        commodity.append(name)

data_final['Formatted_commodity'] = commodity


# commodity =[]
# barrels = []

# for idx1_, row1_ in tqdm(data_final.iterrows(), total=data_final.shape[0]):
#     #Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
#     name = row1_.commodity_details
#     if "{" in name:
#         name1 = name.split("{")[1]
#         name1_ = name1.split("}")[0]
#         if "Unspecified" in name1_:
#             commodity.append("N/A")
#             barrels.append("-999")

#         elif ": " in name1_:
#             name2 = name1_.split(": ")[0]
#             commodity.append(name2)

#             name3 = name1_.split(": ")[1]

#             if " Million metric tons per day" in name3:
#                 name__ = name3.split(" Million metric tons per day")[0]
#                 math = float(name__) * 1
#                 barrels.append(math)
#             elif "Metric tons" in name3:
#                 name__ = name3.split(" Metric tons")[0]
#                 math = float(name__) * 1
#                 barrels.append(math)
#             elif "Thouseand barrels" in name3:
#                 name__ = name3.split(" Thousand barrels")[0]
#                 math = float(name__) * 1
#                 barrels.append(math)
#             elif "Thouseand 42-gallon" in name3:
#                 name__ = name3.split(" Thousand 42-gallon")[0]
#                 math = float(name__) * 1
#                 barrels.append(math)
#             elif "42-gallon" in name3:
#                 name__ = name3.split(" 42-gallon")[0]
#                 math = float(name__) * 1
#                 barrels.append(math)


#         else:
#             commodity.append(name1_)

#     else:
#         commodity.append(name)
#         barrels.append("-999")

# data_final['Formatted_commodity'] = commodity

# =============================================================================
# %% AFRICA - USGS - Integration + Export
# =============================================================================
data_AFRICA_terms, errors = integrate_facs(
    data_final.reset_index(drop=True),
    starting_ids=0,
    category='Petroleum terminals',
    fac_alias='LNG_STORAGE',
    country='Country',
    # state_prov = None,
    src_ref_id='158',
    src_date='2021-08-01',
    # on_offshore = None,
    fac_name='FeatureNam',
    fac_id='FeatureUID',
    # fac_type = None,
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    # install_date = None,
    # fac_status = None,
    op_name='operator_new',
    commodity='Formatted_commodity',
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
    data_AFRICA_terms,
    file_name="africa_petroleum_terminals_part_02",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% MIDDLE EAST - oginfra.com
# #### **Afghanistan, Iraq, Iran, Kuwait, Oman, Qatar, Saudi Arabia, UAE, Yemen**
# ===========================================================================
os.chdir(pubdata)
# Read data
# ---------------------------------------------------------------------------
# afghan = gpd.read_file(r'Middle_East+Caspian\Afghanistan\Oil_Gas_Infra_.com\Storage\Terminal.kml.shp')  # already added from source 168
iran_offshore = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Storage\Offshore Terminal.kml.shp')
iran_tanker = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Storage\_iran_tanker_terminals_.shp')
iraq = gpd.read_file(r'Middle_East+Caspian\Iraq\Oil_Gas_Infra_.com\Storage\Terminals_Iraq.kml.shp')
kuwait = gpd.read_file(r'Middle_East+Caspian\Kuwait\Oil_Gas_Infra_.com\Storage\Terminals_Kuwait.kml.shp')
oman = gpd.read_file(r'Middle_East+Caspian\Oman\Oil_Gas_Infra_.com\Storage\Terminal_Oman.kml.shp')
pak = gpd.read_file(r'Middle_East+Caspian\Pakistan\Oil_Gas_Infra_.com\Storage\Terminals_Pakistan.kml.shp')
qatar = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Storage\Terminals_Qatar.kml.shp')
saudi = gpd.read_file(r'Middle_East+Caspian\Saudi_Arabia\Oil_Gas_Infra_.com\Storage\Terminals_Saudi_Arabia.kml.shp')
uae_on = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Storage\Terminal_Onshore_UAE.kml.shp')
uae_off = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Storage\Terminals_Offshore_UAE.kml.shp')
yemen = gpd.read_file(r'Middle_East+Caspian\Yemen\Oil_Gas_Infra_.com\Storage\Terminal_Yemen.kml.shp')

# COMMENT OUT / IGNORE CENTRAL ASIA DATA
# azer = gpd.read_file(r'Azerbaijan\Oil_Gas_Infra_.com\Storage\Terminal_Azerbajan.kml.shp')
# kaz = gpd.read_file(r'Kazakhstan\Oil_Gas_Infra_.com\Storage\Terminal_Kazakhsta.kml.shp')
# turk = gpd.read_file(r'Turkmenistan\Oil_Gas_Infra_.com\Storage\Terminal_Turkmenistan.kml.shp')
# azer_gashandle = gpd.read_file(r'Azerbaijan\Oil_Gas_Infra_.com\Storage\Gas_handling_Azerbajan.kml.shp')
# turk_gashandle = gpd.read_file(r'Turkmenistan\Oil_Gas_Infra_.com\Storage\Gas_handling_Turkmenistan.kml.shp')

# ---------------------------------------------------------------------------
# % Data manipulation / processing if needed
# ---------------------------------------------------------------------------
# Add onshore and offshore attribute (where known) based on the data's filename
iran_offshore['shore'] = 'Offshore'
uae_off['shore'] = 'Offshore'
uae_on['shore'] = 'Onshore'

# Add 'type' field where I know more information about the facility, based on filename
iran_tanker['type'] = 'Tanker terminal'

# Remove excess fields from iran_tanker just for readability
iran_tanker = iran_tanker.filter(['Name', 'shore', 'type', 'geometry'])

# Combine countries with more than one gdf into a single country-specific gdf
iran = iran_tanker.append(iran_offshore)
uae = uae_off.append(uae_on)

# Add country name to each country-specific gdf
iran['country'] = 'Iran'
iraq['country'] = 'Iraq'
kuwait['country'] = 'Kuwait'
oman['country'] = 'Oman'
pak['country'] = 'Pakistan'
qatar['country'] = 'Qatar'
saudi['country'] = 'Saudi Arabia'
uae['country'] = 'UAE'
yemen['country'] = 'Yemen'

# Append all country gdfs into one final gdf
all_dfs_final = [iran, iraq, kuwait, oman, pak, qatar, saudi, uae, yemen]
data = pd.concat(all_dfs_final)
data = data.reset_index(drop=True)

# Replace any 'Name' values of 'untitled placemark' with NOT AVAILABLE
data['type'] = data['type'].fillna('N/A')
data['shore'] = data['shore'].fillna('N/A')

data['Name'] = data['Name'].fillna('N/A')  # Have to fill empty cells for next line to work
data.loc[data.Name.str.contains('Placemark'), 'Name'] = 'N/A'

# Add additional faciilty type details, based on contents in the 'Name' field
data.loc[data.Name.str.contains('oil', case=False), 'type'] = 'Oil terminal'
data.loc[data.Name.str.contains('Export', case=False), 'type'] = 'Export terminal'
data.loc[data.Name.str.contains('Buoy', case=False), 'type'] = 'Loading buoy'
# After capturing facility type info that's in the "Name" field, change "Name" to N/A
data.loc[data.Name.str.contains('Export', case=False), 'Name'] = 'N/A'
data.loc[data.Name.str.contains('Buoy', case=False), 'Name'] = 'N/A'

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

data = data.drop_duplicates(subset=['Name',
                                    'type',
                                    'country',
                                    'geometry'], keep='first')

# =============================================================================
# %% MIDDLE EAST - oginfra.com - Integration + Export
# =============================================================================
data_TERMS_ME, errors = integrate_facs(
    data.reset_index(drop=True),
    starting_ids=0,
    category='Petroleum terminals',
    fac_alias='LNG_STORAGE',
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='shore',
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
    data_TERMS_ME,
    file_name="middle_east_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% ITALY
# ===========================================================================
os.chdir(v24data)
fp = r"italy\centrali-stoccaggio.csv"
italyterms = pd.read_csv(fp, sep=';', encoding='windows-1252', decimal=',')
italyterms.columns = italyterms.columns.str.lstrip()
# If this erroneous column name exists, replace it
italyterms = italyterms.rename(columns={'ÿÿLatitudine': 'Latitudine'})

# Drop any records with no lat-long coordinates
italyterms = italyterms.dropna(subset=['Longitudine', 'Latitudine'])

# TODO - confirm whether coords are in WGS84
italyterms = gpd.GeoDataFrame(italyterms,
                              geometry=gpd.points_from_xy(italyterms.Longitudine,
                                                          italyterms.Latitudine),
                              crs=4326)
italyterms = transform_CRS(italyterms,
                           target_epsg_code="epsg:4326",
                           appendLatLon=True)

# Translate "Minerale" column
italyterms['Minerale'].replace({'Olio/Gas': 'Oil/Gas',
                                'Olio': 'Oil',
                                np.nan: 'N/A'}, inplace=True)

# =============================================================================
# %% ITALY - Integration + Export
# =============================================================================
italyterms_integrated, errors = integrate_facs(
    italyterms,
    starting_ids=0,
    category="Petroleum Terminals",
    fac_alias="LNG_STORAGE",
    country="Italy",
    state_prov="Provincia",
    src_ref_id="84",
    src_date="2023-05-18",
    # on_offshore="",
    fac_name="Nome centrale",
    # fac_id="",
    # fac_type="Minerale",
    commodity="Minerale",
    # drill_type="",
    # spud_date = "",
    # comp_date="",
    # fac_status="",
    op_name="Operatore",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    italyterms_integrated,
    file_name="italy_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% NORWAY
# ===========================================================================
os.chdir(pubdata)
fp4 = "Europe\\Norway\\Facilities\\TERMINAL_Norway.kml.shp"
petroleum_terminals = read_spatial_data(fp4, table_gradient=True)
petroleum_terminals1 = transform_CRS(petroleum_terminals, target_epsg_code="epsg:4326", appendLatLon=True)
petroleum_terminals1 = petroleum_terminals1[petroleum_terminals1.latitude_calc != 0]

# % Data pre-processing before data integration
# ---------------------------------------------------------------------------
# format Name column
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


dict_names = {}
colName = "Name"
dict_names = {
    'K?RST?': 'KÅRSTØ',
    'KALST?': 'KÅLSTØ',
    'MELK?YA': 'MELKØYA',
}

petroleum_terminals2 = replace_row_names(petroleum_terminals1, colName, dict_names)


# =============================================================================
# %% NORWAY - Integration + Export
# =============================================================================
petroleum_terminals_final_NOR, errors = integrate_facs(
    petroleum_terminals2.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Norway",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # drill_type="FORMATTED_DRILL_TYPE",
    # spud_date = "FORMATTED_SPUD",
    # comp_date="FORMATTED_END",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    petroleum_terminals_final_NOR,
    file_name="norway_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% NETHERLANDS
# ===========================================================================
os.chdir(pubdata)
fp2 = "Europe\\Netherlands\\Facilities\\Terminal_Netherlands.kml.shp"
patroleum_terminals = read_spatial_data(fp2, table_gradient=True)
patroleum_terminals1 = transform_CRS(patroleum_terminals, target_epsg_code="epsg:4326", appendLatLon=True)

# =============================================================================
# %% NETHERLANDS - Integration + Export
# =============================================================================
petroleum_terminals_final_NETH, errors = integrate_facs(
    patroleum_terminals1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Netherlands",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-04-01",
    # on_offshore="Offshore",
    fac_name="Name",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # drill_type="FORMATTED_DRILL_TYPE",
    # spud_date = "FORMATTED_SPUD",
    # comp_date="FORMATTED_END",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    petroleum_terminals_final_NETH,
    file_name="netherlands_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% UNITED KINGDOM
# ===========================================================================
os.chdir(v24data)
uk_points = gpd.read_file(r'united_kingdom/Surface_Points_(WGS84).geojson')
uk_points = transform_CRS(uk_points, target_epsg_code="epsg:4326", appendLatLon=True)
uk_terms = uk_points.query("INF_TYPE == 'TERMINAL'").reset_index(drop=True)

# =============================================================================
# %% UNITED KINGDOM - Integration + Export
# =============================================================================
uk_terms_integrated, errors = integrate_facs(
    uk_terms,
    starting_ids=0,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="United Kingdom",
    # state_prov="",
    src_ref_id="265",
    src_date="2024-01-24",
    # on_offshore="",
    fac_name="NAME",
    # fac_id="",
    # fac_type="",
    # drill_type="",
    # spud_date = "",
    # comp_date="",
    fac_status="STATUS",
    # op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    uk_terms_integrated,
    file_name="united_kingdom_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% AUSTRALIA AND NEW ZEALAND
# ===========================================================================
os.chdir(pubdata)
fp7 = "Australia+NewZealand\\Australia\\Terminal_Onshore_Australia.kml.shp"
petroleum_term = read_spatial_data(fp7, table_gradient=True)
petroleum_term1 = transform_CRS(petroleum_term, target_epsg_code="epsg:4326", appendLatLon=True)

# ---------------------------------------------------------------------------
# Petroleum terminals data integration
# ---------------------------------------------------------------------------
petr_term_final_AUS, errors = integrate_facs(
    petroleum_term1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Australia",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    on_offshore="ONSHORE",
    fac_name="Name",
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
    petr_term_final_AUS,
    file_name="australia_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% TURKMENISTAN
# ===========================================================================
os.chdir(pubdata)
fp2 = "Middle_East+Caspian\\Turkmenistan\\Oil_Gas_Infra_.com\\Storage\\Terminal_Turkmenistan.kml.shp"
terminal = read_spatial_data(fp2, table_gradient=True)

# ---------------------------------------------------------------------------
terminal1 = transform_CRS(terminal, target_epsg_code="epsg:4326", appendLatLon=True)

# ---------------------------------------------------------------------------
# % Data integration
#  - Apply standard data schema
# ---------------------------------------------------------------------------
lng_storage_TURK, errors = integrate_facs(
    terminal1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Turkmenistan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
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
    lng_storage_TURK,
    file_name="turkmenistan_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% AZERBAIJAN
# ===========================================================================
os.chdir(pubdata)
fp2 = "Middle_East+Caspian\\Azerbaijan\\Oil_Gas_Infra_.com\\Storage\\Terminal_Azerbajan.kml.shp"
terminal = read_spatial_data(fp2, table_gradient=False)

# ---------------------------------------------------------------------------
terminal1 = transform_CRS(terminal, target_epsg_code="epsg:4326", appendLatLon=True)

# ---------------------------------------------------------------------------

lng_storage_AZER, errors = integrate_facs(
    terminal1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum Terminals",
    fac_alias="LNG_STORAGE",
    country="Azerbaijan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
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
    lng_storage_AZER,
    file_name="azerbaijan_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% KAZAKHSTAN
# ===========================================================================
os.chdir(pubdata)
fp2 = "Middle_East+Caspian\\Kazakhstan\\Oil_Gas_Infra_.com\\Storage\\Terminal_Kazakhsta.kml.shp"
terminal = read_spatial_data(fp2, table_gradient=False)
terminal1 = transform_CRS(terminal, target_epsg_code="epsg:4326", appendLatLon=True)

# There are 5 points near the port of Aktau; most of them are far enough apart,
# but delete one of the two that are basically next to each other
terminal1 = terminal1.drop(index=[0])

# ---------------------------------------------------------------------------
# % Data integration
#  - Apply standard data schema
# ---------------------------------------------------------------------------
lng_storage_KZK, errors = integrate_facs(
    terminal1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum Terminals",
    fac_alias="LNG_STORAGE",
    country="Kazakhstan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
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
    lng_storage_KZK,
    file_name="kazakhstan_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)


# ===========================================================================
# %% AFGHANISTAN
# ===========================================================================
os.chdir(pubdata)
terms = pd.read_csv(r"Middle_East+Caspian\Afghanistan\other_data_\Terminals\_Afghanistan_storage_terminals_.csv")

terms = gpd.GeoDataFrame(terms, geometry=gpd.points_from_xy(terms.longitude, terms.latitude), crs=4326)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
terms = transform_CRS(terms, target_epsg_code="epsg:4326", appendLatLon=True)
# ---------------------------------------------------------------------------
# % Data manipulation / processing if needed
# ---------------------------------------------------------------------------

# TODO: the 'capacity' attribute in the petroleum terminal needs to be
# converted to million cubic feet per day... not only is it unclear which
# commodity the source webpage was listing the capacity for, I'm not convinced
# that the present "100 metric tons per day" is accurate, since If each *tank*
# at the terminal can hold 100 MT, shouldn't those be summed?
# OGIM v2 currently just lists the capacity as 100.
# ---------------------------------------------------------------------------


def convert_metric_tons_to_barrels_crudeoil(value):
    return value * 7.33


# Conversion factors from this resource: https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2022-approximate-conversion-factors.pdf
# ---------------------------------------------------------------------------
# Convert refinery's capacity of 500 metric tons per day into barrels per day
terms['capacity_new'] = convert_metric_tons_to_barrels_crudeoil(int(terms['Capacity']))

# =============================================================================
# %% AFGHANISTAN - Integration + Export
# =============================================================================
terms_AFGH, errors = integrate_facs(
    terms.reset_index(drop=True),
    starting_ids=0,
    category='Petroleum terminals',
    fac_alias='LNG_STORAGE',
    country='Afghanistan',
    # state_prov = None,
    src_ref_id='168',
    src_date='2022-07-01',
    # on_offshore = None,
    fac_name='Name',
    # fac_id = None,
    # fac_type = None,
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    # install_date = None,
    # fac_status = None,
    op_name='Company',
    # commodity = None,
    liq_capacity_bpd='capacity_new',
    # liq_throughput_bpd = None,
    # gas_capacity_mmcfd = None,
    # gas_throughput_mmcfd = None,
    # num_compr_units = None,
    num_storage_tanks='Number of tanks',
    # site_hp = None,
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    terms_AFGH,
    file_name="afghanistan_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% BANGLADESH
# ===========================================================================
os.chdir(pubdata)
fp1 = "China+SE_Asia\\Bangladesh\\Terminals_Bangladesh.kml.shp"
terminal = read_spatial_data(fp1, table_gradient=True)
terminal1 = transform_CRS(terminal, target_epsg_code="epsg:4326", appendLatLon=True)

# remove duplicate facility "Chittagong"
terminal1 = terminal1.drop_duplicates(subset=['Name'], keep='first')

# ---------------------------------------------------------------------------
# % Data integration
#  - Apply standard data schema
# ---------------------------------------------------------------------------
lng_storage_BANG, errors = integrate_facs(
    terminal1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # on_offshore="",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
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
    lng_storage_BANG,
    file_name="bangladesh_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% THAILAND
# ===========================================================================
os.chdir(pubdata)
fp3 = "China+SE_Asia\\Thailand\\Oil_Terminals_Thailand.kml.shp"
terminal = read_spatial_data(fp3, table_gradient=True)
terminal1 = transform_CRS(terminal, target_epsg_code="epsg:4326", appendLatLon=True)

# ---------------------------------------------------------------------------
# Data integration
# ---------------------------------------------------------------------------

terminals_THAI, errors = integrate_facs(
    terminal1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum Terminals",
    fac_alias="LNG_STORAGE",
    country="Thailand",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="ONSHORE",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
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
    terminals_THAI,
    file_name="thailand_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)

# ===========================================================================
# %% INDIA
# ===========================================================================
os.chdir(pubdata)
fp1 = r"China+SE_Asia\India\Terminal_India.kml.shp"
terminal = read_spatial_data(fp1, table_gradient=True)
terminal1 = transform_CRS(terminal, target_epsg_code="epsg:4326", appendLatLon=True)
# ---------------------------------------------------------------------------
# % Data integration
# Apply standard data schema
# ---------------------------------------------------------------------------
terminals_IND, errors = integrate_facs(
    terminal1.reset_index(drop=True),
    starting_ids=0,
    category="Petroleum Terminals",
    fac_alias="LNG_STORAGE",
    country="India",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    on_offshore="ONSHORE",
    fac_name="Name",
    # fac_id="",
    # fac_type="",
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
    terminals_IND,
    file_name="india_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_terms
)
