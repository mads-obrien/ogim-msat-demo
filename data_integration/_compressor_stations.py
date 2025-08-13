# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global NATURAL GAS COMPRESSOR STATIONS.

@author: maobrien, momara, ahimmelberger
"""
import os
# import re
import pandas as pd
import geopandas as gpd
import numpy as np
# import datetime
import shapely.wkt

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, schema_COMPR_PROC, transform_CRS,
                     integrate_facs, save_spatial_data, read_spatial_data,
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

# Folder in which all integrated data will be saved -- must end in slashes!
save_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# ===========================================================================
# %% UNITED STATES - HIFLD
# ===========================================================================
os.chdir(v24data)
# HIFLD data
fp_us = r'united_states\national\HIFLD\Natural_Gas_Compressor_Stations.geojson'
cs_hifld = read_spatial_data(fp_us)
cs_hifld = transform_CRS(cs_hifld, appendLatLon=True)

# Dates
cs_hifld["src_dates"] = pd.to_datetime(cs_hifld["SOURCEDATE"]).dt.strftime("%Y-%m-%d").replace({"1901-01-01": "1900-01-01",
                                                                                                np.nan: "1900-01-01"})

# Replace "NOT AVAILABLE" status with "N/A"
cs_hifld.STATUS = cs_hifld.STATUS.replace({'NOT AVAILABLE': 'N/A'})
cs_hifld.STATE = cs_hifld.STATE.replace({'NOT APPLICABLE': 'N/A'})

# Use only US data
cs_hifld = cs_hifld.query("COUNTRY == 'USA'").reset_index()

# Use full names for U.S. states.
# Where state info is missing, these compressors are offshore. Label them as such.
# (I confirmed they are all in the gulf of mexico).
cs_hifld = replace_row_names(cs_hifld, "STATE", dict_names=dict_us_states)
cs_hifld.STATE.replace({'N/A': 'GULF OF MEXICO'}, inplace=True)
cs_hifld['on_offshore'] = 'ONSHORE'
cs_hifld.loc[cs_hifld.STATE == 'GULF OF MEXICO', 'on_offshore'] = 'OFFSHORE'


# ---------------------------------------------------------------------------
# Integrate HIFLD compressor station data
# ---------------------------------------------------------------------------
cs_hifld_integrated, comps_errors_ab = integrate_facs(
    cs_hifld,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="United States",
    state_prov="STATE",
    src_ref_id="87",
    src_date="2022-12-06",
    on_offshore="on_offshore",
    fac_name="NAME",
    fac_id="GCOMPID",
    fac_type="NAICS_DESC",
    install_date=None,
    fac_status="STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units='NUM_UNITS',
    num_storage_tanks=None,
    site_hp="CERT_HP",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# =============================================================================
# %% UNITED STATES - EPA FLIGHT - REMOVE FOR NOW
# =============================================================================
# os.chdir(v24data)
# comp_epa = pd.read_excel(r"united_states\national\epa_flight_data\\transmission_compression_stations.xls",
#                          header=5)

# # Create geodataframe
# comp_epa_gdf = gpd.GeoDataFrame(comp_epa,
#                                 geometry=gpd.points_from_xy(comp_epa.LONGITUDE,
#                                                             comp_epa.LATITUDE),
#                                 crs="epsg:4326")
# print(comp_epa_gdf.head())

# # U.S. state names
# comps_epa2 = replace_row_names(comp_epa_gdf,
#                                "STATE",
#                                dict_names=dict_us_states)

# # Type: Transmission compressor stations
# comps_epa2['fac_type'] = 'Transmission compressor station'

# # ---------------------------------------------------------------------------
# # Integrate U.S. EPA transmission compressor station data
# # ---------------------------------------------------------------------------
# comps_us2, comps_errors_ab2 = integrate_facs(
#     comp_epa_gdf,
#     starting_ids=1,
#     category="Natural gas compressor stations",
#     fac_alias="COMPR_PROC",
#     country="United States",
#     state_prov="STATE",
#     src_ref_id="89",
#     src_date="2022-04-15",
#     on_offshore="Onshore",
#     fac_name="FACILITY NAME",
#     fac_id="GHGRP ID",
#     fac_type="fac_type",
#     fac_status=None,
#     op_name="PARENT COMPANIES",
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd=None,
#     gas_throughput_mmcfd=None,
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="LATITUDE",
#     fac_longitude="LONGITUDE"
# )

# =============================================================================
# %% UNITED STATES - Marchese et al. (2015)
# https://pubs.acs.org/doi/abs/10.1021/acs.est.5b02275
# Data for several states
# =============================================================================
os.chdir(v24data)
fp = r"united_states\national\marchese_et_al_data\\es5b02275_si_002.xlsx"
data_ = pd.read_excel(fp, sheet_name=[0, 1, 2, 3, 4, 5, 6, 7])
data_ = pd.concat(data_).reset_index()
print("===========================")
print("Total # of rows in dataset = ", data_.shape[0])
print("Unique columns in dataset = ", data_.columns)
print("# of unique facility types = ", data_.IndustrySegment.unique())
print("===========================")

# ---------------------------------------------------------------------------
# Remove incorrect or unknown lat and lon
data_['Longitude'] = data_.Longitude.replace({0: np.NaN, -999: np.NaN})

# Then drop NaNs
data2_ = data_[~data_.Longitude.isnull()]

# Create GeoDataFrame from Pandas object
# ---------------------------------------------------------------------------
data_gdf = gpd.GeoDataFrame(data2_,
                            geometry=gpd.points_from_xy(data2_.Longitude,
                                                        data2_.Latitude),
                            crs='epsg:4326')
data_gdf.plot()

segment_sel = [
    'Gathering',
    'Transmission',
    'Storage',
    'Distribution'
]

data_gdf_comp = data_gdf[data_gdf.IndustrySegment.isin(segment_sel)]
print("===========================")
print("Total # of compressor facilites in dataset = ", data_gdf_comp.shape[0])

# Facility type
data_gdf_comp['fac_type2'] = [data_gdf_comp.IndustrySegment.iloc[x] + " Compressor Station" for x in range(data_gdf_comp.shape[0])]

print(data_gdf_comp.head())

# ---------------------------------------------------------------------------
# Integrate data from Marchese et al.
# ---------------------------------------------------------------------------
comps_us3, comps_errors_3 = integrate_facs(
    data_gdf_comp,
    starting_ids=cs_hifld_integrated.OGIM_ID.iloc[-1] + 1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="United States",
    state_prov="State",
    src_ref_id="90",
    src_date="2015-08-01",
    on_offshore="Onshore",
    fac_name="FacilityName",
    fac_id="SiteIdentifier",
    fac_type="fac_type2",
    fac_status=None,
    op_name="CompanyName",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp="SiteHP",
    fac_latitude="Latitude",
    fac_longitude="Longitude"
)

# =============================================================================
# %% UNITED STATES - Combine all compressors, then export
# =============================================================================
# Concatenate all compressor station data
comp_data_all = pd.concat([cs_hifld_integrated,
                           # comps_us2,
                           comps_us3]).reset_index(drop=True)
comp_data_all.head()


# Remove POTENTIAL duplicates based on FAC_NAME
# ---------------------------------------------------------------------------
unique_fac_names = comp_data_all.FAC_NAME.unique()

# Keep the first entry, which should be the HIFLD dataset if it is part of the duplicate
comp_data_unique = comp_data_all.drop_duplicates(subset='FAC_NAME',
                                                 keep='first')

print("Length of data for all compressor stations and unique dataset = ", [comp_data_all.shape[0], comp_data_unique.shape[0]])

# Fix ordering of OGIM id
# ---------------------------------------------------------------------------
new_ogim_id = np.arange(comp_data_all.OGIM_ID.iloc[0],
                        comp_data_all.OGIM_ID.iloc[0] + comp_data_unique.shape[0])

comp_data_unique['OGIM_ID'] = new_ogim_id

# Keep only attributes in our OGIM schema
comp_data_unique2 = comp_data_unique[
    ['OGIM_ID', 'CATEGORY', 'COUNTRY', 'STATE_PROV', 'SRC_REF_ID',
     'SRC_DATE', 'ON_OFFSHORE', 'FAC_NAME', 'FAC_ID', 'FAC_TYPE',
     'FAC_STATUS', 'OPERATOR', 'INSTALL_DATE', 'COMMODITY',
     'LIQ_CAPACITY_BPD', 'LIQ_THROUGHPUT_BPD', 'GAS_CAPACITY_MMCFD',
     'GAS_THROUGHPUT_MMCFD', 'NUM_COMPR_UNITS', 'NUM_STORAGE_TANKS',
     'SITE_HP', 'LATITUDE', 'LONGITUDE', 'geometry'
     ]
]

# Where number of compressor units == 0, replace with -999
comp_data_unique2["NUM_COMPR_UNITS"] = comp_data_unique2["NUM_COMPR_UNITS"].replace({0: -999, "0": -999})
comp_data_unique2["SITE_HP"] = comp_data_unique2["SITE_HP"].replace({0: -999, "0": -999})

# Some FAC_NAME values end with `(SEE FOOTNOTE)` -- remove this substring
comp_data_unique2.FAC_NAME = comp_data_unique2.FAC_NAME.str.replace("(SEE FOOTNOTE)", "")


# Save data
# ---------------------------------------------------------------------------
save_spatial_data(
    comp_data_unique2,
    "united_states_compressor_stations",
    schema_def=True,
    schema=schema_COMPR_PROC,
    file_type="GeoJSON",
    out_path=save_path
)


# ===========================================================================
# %% VENEZUELA
# Data source: https://services8.arcgis.com/2jmdYNQsteiDSgjD/arcgis/rest/services/Instalaciones_FPO/FeatureServer
# Data owner: @carlosgonzalez_LAEE
# ===========================================================================
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
# Compressor stations
comps_ = [
    'COMPRESSOR PLANT',
    'COMPRESSOR PLANT (FUTURE PROJECT)'
]

vez_cs = vez_fac[vez_fac['TIPO'].isin(comps_)]
print("==============================")
print("Total number of comp stations in dataset = ", vez_cs.shape[0])
vez_cs.head()


# =============================================================================
# %% VENEZUELA - Integration + Export
# =============================================================================
comps_VEZ, comps_err = integrate_facs(
    vez_cs,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Venezuela",
    # state_prov=None,
    src_ref_id="132",
    src_date="2017-01-01",
    # on_offshore=None,
    fac_name="NOMBRE",
    # fac_id=None,
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
    comps_VEZ,
    "venezuela_gas_compressor_stations",
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_path
)


# ===========================================================================
# %% ARGENTINA
# There are two datasets for facilities in Argentina, one that includes mostly
# facilities and another that includes mostly equipment on the site of
# facilities (e.g., batteries, tanks, valves, etc.)
# ===========================================================================
os.chdir(v24data)
# !!! No need to read in the Installations 318 dataset as there are no compressor stations in it

# fp1_ = r"argentina\facilities\instalaciones-hidrocarburos-instalaciones-res-318-shp.shp"
# arg_fac1 = read_spatial_data(fp1_, specify_encoding=True, data_encoding="utf-8")
# arg_fac1 = transform_CRS(arg_fac1, appendLatLon=True)
# arg_fac1['src_ref'] = "109"
# # Only keep facilities with valid geometries
# arg_fac1 = arg_fac1[arg_fac1.geometry.notna()]

# # Set facility name column
# arg_fac1['fac_name'] = arg_fac1['INSTALACIO']

# # Translate contents of "TIPO" field, based on the translations we've previously done
# arg_fac1['new_fac_type'] = translate_argentina_installations_fac_types(arg_fac1,
#                                                                        fac_type_col_spanish='TIPO')

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

# Select records from the "installation characteristics" dataset that claim to
# be compressor plants
comp_plants = arg_fac2.query("new_fac_type == ['COMPRESSOR PLANT']")


# Not all 'COMPRESSOR PLANT' records point to a midstream facility;
# some of the records point to specific equipment on a site (i.e. tanks or
# individual compressor units).
# Use the existing "DESCPC" (description) field to filter the records to just
# those that point to compressor midstream facilities.
stns_desc = [
    'PLANTA COMPRESORA CONFLUENCIA SUR',
    'PLANTA COMPRESORA CENTRO',
    'PLANTA COMPRESORA CORNEJO',
    'PLANTA COMPRESORA Pozo R-1009',
    'PLANTA COMPRESORA NORTE',
    'PLANTA COMPRESORA',
    'Planta compresora de gas',
    'Planta Compresora',
    'Estacion Compresora ADIS',
    'Estacion Compresora 13-S',
    'Estacion Compresora 28-S',
    'PLANTA compresora y acondicionadora de Gas Natural',
    'Planta compresora de Gas',
    'Centro Compresión',
    'Planta Compresora (compresor fuera de servicio)',
    'ESTACION DE COMPRESION DE GAS',
    'Planta compresora',
    'PLANTA COMPRESORA ZONA NORTE Sierra Chata',
    'PLANTA COMPRESORA LOS LEONES',
    'PLANTA COMPRESORA 12\r\n',
    'Planta Compresora de Gas',
]
comp_plants = comp_plants.query("DESCPC == @stns_desc")


# Translate the contents of the 'DESCPC' (description) field, to use as
# additional FAC_TYPE information in the compressor station layer
stns_type = {
    'PLANTA COMPRESORA CONFLUENCIA SUR': 'SOUTH CONFLUENCE COMPRESSOR PLANT',
    'PLANTA COMPRESORA CENTRO': 'CENTRAL COMPRESSOR PLANT',
    'PLANTA COMPRESORA CORNEJO': 'CORNEJO COMPRESSOR PLANT',
    'PLANTA COMPRESORA Pozo R-1009': 'COMPRESSOR PLANT WELL R-1009',
    'PLANTA COMPRESORA NORTE': 'NORTH COMPRESSOR PLANT',
    'PLANTA COMPRESORA': 'COMPRESSOR PLANT',
    'Planta compresora de gas': 'GAS COMPRESSION PLANT',
    'Planta Compresora': 'COMPRESSOR PLANT',
    'Estacion Compresora ADIS': 'ADIS COMPRESSOR STATION',
    'Estacion Compresora 13-S': 'COMPRESSOR STATION 13-S',
    'Estacion Compresora 28-S': 'COMPRESSOR STATION 28-S',
    'PLANTA compresora y acondicionadora de Gas Natural': 'NATURAL GAS COMPRESSOR AND CONDITIONING PLANT',
    'Planta compresora de Gas': 'GAS COMPRESSION PLANT',
    'Centro Compresión': 'COMPRESSION CENTER',
    'Planta Compresora (compresor fuera de servicio)': 'COMPRESSOR PLANT (COMPRESSOR OUT OF SERVICE)',
    'ESTACION DE COMPRESION DE GAS': 'GAS COMPRESSION STATION',
    'Planta compresora': 'COMPRESSOR PLANT',
    'PLANTA COMPRESORA ZONA NORTE Sierra Chata': 'SIERRA CHATA NORTH ZONE COMPRESSOR PLANT',
    'PLANTA COMPRESORA LOS LEONES': 'LOS LEONES COMPRESSOR PLANT',
    'PLANTA COMPRESORA 12\r\n': 'COMPRESSOR PLANT',
    'Planta Compresora de Gas': 'GAS COMPRESSOR PLANT',
}
comp_plants = replace_row_names(comp_plants, "DESCPC", stns_type)
comp_plants = comp_plants.reset_index()

# -----------------------------------------------------------------------------
# Read in transport compressors dataset
os.chdir(v24data)
fp3_ = r"argentina\facilities\plantas-compresoras-de-transporte-de-gas-enargas--shp.shp"
transport_cs = read_spatial_data(fp3_, specify_encoding=True, data_encoding="utf-8")
transport_cs = transform_CRS(transport_cs, appendLatLon=True)
transport_cs['src_ref'] = "237"

# -----------------------------------------------------------------------------
# Read in distribution compressors dataset
os.chdir(v24data)
fp4_ = r"argentina\facilities\plantas-compresoras-de-distribucin-de-gas-shp.shp"
distrib_cs = read_spatial_data(fp4_, specify_encoding=True, data_encoding="utf-8")
# Original shapefile is multi-point type, convert to single point & drop multi-index
distrib_cs = distrib_cs.explode(column='geometry', ignore_index=True).reset_index(drop=True)
distrib_cs = transform_CRS(distrib_cs, appendLatLon=True)
distrib_cs['src_ref'] = "238"

# Translate facility type  # TODO later, get Mark's thoughts on these translations
distrib_cs.TIPO_PLANT.replace({'Motocompesora': 'Motor compressor',
                               'Electrocompresora': 'Electric compressor',
                               'Turbocompresora': 'Turbo-compressor'},
                              inplace=True)

# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
# Integrate "installation characteristics" compressor stations
comp_plants_integrated, err_ = integrate_facs(
    comp_plants.reset_index(),
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2024-04-01",
    # on_offshore=None,
    fac_name="fac_name",
    # fac_id=None,
    fac_type="DESCPC",
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

# Integrate transport compressor stations
transport_cs_integrated, err_ = integrate_facs(
    transport_cs,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Argentina",
    # state_prov=None,
    src_ref_id="src_ref",
    src_date="2023-06-29",
    # on_offshore=None,
    fac_name="NOMBRE",
    # fac_id=None,
    # fac_type=None,
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # install_date=None,
    # fac_status=None,
    op_name='LICENCIATA',
    commodity='GAS',
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

# Integrate distribution compressor stations
distrib_cs_integrated, err_ = integrate_facs(
    distrib_cs,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Argentina",
    state_prov='PROVINCIA',
    src_ref_id="src_ref",
    src_date="2023-06-29",
    # on_offshore=None,
    fac_name="NOMBRE",
    # fac_id=None,
    fac_type="TIPO_PLANT",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # install_date=None,
    # fac_status=None,
    op_name="LICENCIATA",
    commodity='GAS',
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

# Concatenate all CS tables together
comps_arg_final = pd.concat([comp_plants_integrated,
                            transport_cs_integrated,
                            distrib_cs_integrated]).reset_index(drop=True)

save_spatial_data(
    comps_arg_final,
    "argentina_gas_compressor_stations",
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_path
)


# ===========================================================================
# %% BRAZIL  # DONE
# ===========================================================================
os.chdir(v24data)
bz_cs = gpd.read_file(r'brazil\WebMap_EPE\Compression_stations.shp')
bz_cs = transform_CRS(bz_cs, target_epsg_code="epsg:4326", appendLatLon=True)

# Rename a few columns; I've confirmed that "Transporta" means company here
bz_cs = bz_cs.rename(columns={"Transporta": "company"}, errors="raise")

# Remove the compressor stations which don't exist yet
bz_cs = bz_cs.query("Classifica == 'Existente'").reset_index(drop=True)

bz_cs.MMm3d = pd.to_numeric(bz_cs.MMm3d)

bz_cs['gas_cap_mmcfd'] = bz_cs.MMm3d.apply(lambda x: convert_MMm3d_to_mmcfd(x))

# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
bz_cs_integrated, _err = integrate_facs(
    bz_cs,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Brazil",
    # state_prov=None,
    src_ref_id="267",
    src_date="2024-01-01",
    # on_offshore=None,
    fac_name="Nome",
    # fac_id=None,
    fac_type="Tipo",
    # spud_date=None,
    # comp_date=None,
    # install_date=None,
    # fac_status=None,
    op_name="Operadora",
    # commodity=None,
    # liq_capacity_bpd=None,
    # liq_throughput_bpd=None,
    gas_capacity_mmcfd='gas_cap_mmcfd',
    # gas_throughput_mmcfd=None,
    # num_compr_units=None,
    # num_storage_tanks=None,
    # site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    bz_cs_integrated,
    "brazil_gas_compressor_stations",
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_path
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

# Subset to compressor stations only
bol_cs = bol_stations.query("sitetype == 'Compressor Station'")

# =============================================================================
# %% BOLIVIA - Integrate + Export
# =============================================================================
bol_cs_integrated, comps_err = integrate_facs(
    bol_cs,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Bolivia",
    # state_prov=None,
    src_ref_id="123",
    src_date="2013-03-06",
    on_offshore='ONSHORE',
    fac_name="sitename",
    # fac_id="gml_id",
    # fac_type="SiteType",
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
    bol_cs_integrated,
    "bolivia_gas_compressor_stations",
    schema_def=True,
    file_type="GeoJSON",
    out_path=save_path
)


# =============================================================================
# %% SRON - Russia and other countries
# including Belarus, Germany, Kazakhstan, Latvia, Poland, Turkey, Turkmenistan, Ukraine, Uzbekistan

# !!! Exclude from v2.7 (until we get permission from Bram to share it)

# =============================================================================
# # Read CSV from SRON and create a geodataframe from it
# os.chdir(v24data)
# fp = r'russia\_compressor_station_data_ESRON_.csv'
# sron_cs_csv = pd.read_csv(fp)
# sron_cs = gpd.GeoDataFrame(sron_cs_csv,
#                            geometry=gpd.points_from_xy(sron_cs_csv.Lon,
#                                                        sron_cs_csv.Lat),
#                            crs=4326)

# # There are some identical lat-long locations in this dataset, with the only
# # differentiating factor being a "Pipe" attribute and I'm not sure what that
# # means. Drop all duplicates with the same lat-long coords and 'Station' attrib
# sron_cs = sron_cs.drop_duplicates(subset=['Lat', 'Lon', 'Station'],
#                                   keep='first').reset_index()

# # None of these compressor stations have a COUNTRY value, so create one
# # via a spatial join with country polygons
# countries_fp = r"C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\data\International_data_sets\World_Countries_Boundaries\_world_countries_.shp"
# countries = read_spatial_data(countries_fp, table_gradient=False)
# countries = transform_CRS(countries, target_epsg_code="epsg:4326", appendLatLon=False)

# sron_cs_join = gpd.sjoin(sron_cs, countries)
# print(sron_cs_join.Country.value_counts())


# =============================================================================
# %% SRON - Integration + Export
# =============================================================================
# sron_cs_integrated, errors = integrate_facs(
#     sron_cs_join,
#     starting_ids=0,
#     category="Natural gas compressor stations",
#     fac_alias="COMPR_PROC",
#     country="Country",
#     # state_prov=None,
#     src_ref_id="97",
#     src_date="2021-02-01",
#     # on_offshore=None,
#     # fac_name=None,
#     # fac_id=None,
#     # fac_type=None,
#     # drill_type=None,
#     # spud_date=None,
#     # comp_date=None,
#     # fac_status=None,
#     # op_name=None,
#     fac_latitude="Lat",
#     fac_longitude="Lon"
# )


# save_spatial_data(
#     sron_cs_integrated,
#     "europe_and_central_asia_gas_compressor_stations",
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path=save_path
# )

# ===========================================================================
# %% NIGERIA - StationGas data no longer public on ArcGIS Online; remove from OGIM
# ===========================================================================
# fp = r"Africa\Nigeria\Nigeria\StationGas.shp"
# data = read_spatial_data(fp, table_gradient=True)

# # drop all columns except two useful ones
# data = data.filter(['Name', 'geometry'])

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# # ---------------------------------------------------------------------------
# #% Data manipulation / processing if needed
# # ---------------------------------------------------------------------------
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
# terms_meter = ['metering', 'metetring','mertering', 'm/s']
# terms_cs = ['compressor', 'c/s']
# terms_lbv = ['lbv','lvb']

# # Populate 'factype' field with stations that are a KNOWN TYPE, based on what's in their 'Name' field
# # ---------------------------------------------------------------------------
# data.loc[data['Name_casefold'].str.contains('|'.join(terms_lbv)),'factype'] = 'Line break valve (LBV) control pressure'
# data.loc[data['Name_casefold'].str.contains('pigging'),'factype'] = 'Pigging station'
# data.loc[data['Name_casefold'].str.contains('node'),'factype'] = 'Node'
# data.loc[data['Name_casefold'].str.contains('tie-in'),'factype'] = 'Tie-in'
# data.loc[data['Name_casefold'].str.contains('|'.join(terms_cs)),'factype'] = 'Compressor station'
# data.loc[data['Name_casefold'].str.contains('|'.join(terms_meter)),'factype'] = 'Metering station'
# data.loc[data['Name_casefold'].str.contains('flare stack'),'factype'] = 'Flare stack station'
# data.loc[data['Name_casefold'].str.contains('valve station'),'factype'] = 'Valve station'
# data.loc[data['Name_casefold'].str.contains('regulating'),'factype'] = 'Regulating station'
# data.loc[data['Name_casefold'].str.contains('condensate tank'),'factype'] = 'Condensate tank'

# # Add proper OGIM categories for each record
# # First, set default 'category' value to "other" .....most things like metering stations fall in this category
# # ---------------------------------------------------------------------------
# data['category_new'] = 'Stations (other)'
# # Change the record's CATEGORY value for compressor station points only
# data.loc[data.factype=='Compressor station', 'category_new'] = 'Natural gas compressor stations'
# # data.loc[data.factype=='Condensate tank', 'category_new'] = 'Petroleum storage and terminals'

# # Create two separate gdfs for Compressor Stations and Other Stations
# # ---------------------------------------------------------------------------
# nigeria_cs = data[data.category_new=='Natural gas compressor stations']
# # Retain only "Other Stations" where some O&G facility type info is known
# # This removes points for things like helipads, housing complexes, offices
# nigeria_stations = data[(data.category_new == 'Stations (other)') & (data.factype.notnull()) & (~data.factype.isin(['Line break valve (LBV) control pressure', 'Node', 'Tie-in', 'Condensate tank']))]

# # Equipment and components
# # ---------------------------------------------------------------------------
# nigeria_components = data.query("factype == ['Line break valve (LBV) control pressure', 'Node', 'Tie-in', 'Condensate tank']")

# nigeria_cs = nigeria_cs.reset_index(drop=True)
# nigeria_stations = nigeria_stations.reset_index(drop=True)

# # ---------------------------------------------------------------------------
# #% Integrate data
# # ---------------------------------------------------------------------------
# nigeria_cs_, errors = integrate_facs(
#     nigeria_cs,
#     starting_ids=0,
#     category='Natural gas compressor stations',
#     fac_alias='COMPR_PROC',
#     country='Nigeria',
#     # state_prov = None,
#     src_ref_id='159',
#     src_date='2020-11-01',
#     on_offshore='Onshore',
#     fac_name='Name',
#     # fac_id=None,
#     # fac_type=None,
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
#     fac_latitude = 'latitude_calc',
#     fac_longitude = 'longitude_calc'
# )
# # ---------------------------------------------------------------------------
# # Save data
# # ---------------------------------------------------------------------------
# save_spatial_data(
#     nigeria_cs_,
#     "nigeria_gas_compressor_stations",
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path=save_path
# )


# ===========================================================================
# %% NIGERIA - Oil spill monitor dataset - only provides 1 CS, remove from OGIM
# ===========================================================================
# fp = "Africa//Nigeria//AGO_data_source.@3mmanu3l//OilSpillDatabase.shp"
# data = read_spatial_data(fp, table_gradient=False)
# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# print("====================")
# print(data.columns)
# # ---------------------------------------------------------------------------
# #% Data manipulation / processing if needed
# # ---------------------------------------------------------------------------
# # First, inspect the attributes and see what we know about these records
# # data.Condensatn.value_counts()
# '''
# Crude Oil                    6399
# Gas                           604
# Other                         389
# Condensat                     274
# Refined Products              205
# Chemicals or Drilling Mud      18
# '''
# # ---------------------------------------------------------------------------
# # Whoever originally cleaned this data used a reckless find-and-replace, which
# # replaced every instance of the letters "st" with "Storage Tank", and every
# # instance of "pl" with "Pipeline",  even when "st" or "pl" was
# # part of a substring....
# # I confirmed on the Nigeria Oil Spill Monitor website (https://oilspillmonitor.ng/)
# # that these abbreviations are used in their 'type of facility' field
# # data.Type_of_Fa.value_counts()
# # ---------------------------------------------------------------------------
# '''
# Pipeline                                                                              2933
# Flow Line                                                                             1253
# Well Head                                                                              289
# Other                                                                                  186  # drop for now until we learn more about this category
# mf                                                                                      66  # change to manifold
# Flow Storage Tankation                                                                  60  # change to flow station
# Floating or Production or Storage TankoRigae  or Offloading PipelineaTank Farmorms      18  # change to FPSO
# Storage Tank                                                                            17  # KEEP this
# Tanker                                                                                   9  # in this dataset, Tanker means truck, so remove these records
# Rig                                                                                      3
# Tank Farm                                                                                2
# Pumping Storage Tankation                                                                1 # change to Pumping station
# Compressor Pipelineant                                                                   1 # compressor plant.... in this case, this one IS a compressor station.
# Fuel Dispensation Storage Tankation                                                      1 # fuel dispensation station, aka gas station. REMOVE this record
# '''

# # Drop data rows of Facility Types I don't want to include
# # ---------------------------------------------------------------------------
# types2drop = ['Other','Tanker','Fuel Dispensation Storage Tankation']

# data["Type_of_Fa"] = data["Type_of_fa"]
# data = data[-data.Type_of_Fa.isin(types2drop)]

# # Fix and re-name FAC_TYPE values I know to be wrong (and RETAIN the ones I don't want to change)
# # ---------------------------------------------------------------------------
# # First, create dictionary of the old fac_type (key) and the new fac_type (value)
# factypedict = {
#     'mf': 'Manifold',
#     'Flow Storage Tankation': 'Flow Station',
#     'Floating or Production or Storage TankoRigae  or Offloading PipelineaTank Farmorms': 'Floating production storage and offloading',
#     'Pumping Storage Tankation': 'Pumping Station',
#     'Compressor Pipelineant': 'Compressor Station'
# }

# data['Type_of_Fa'] = data['Type_of_Fa'].map(factypedict).fillna(data['Type_of_Fa'])
# # ---------------------------------------------------------------------------
# #% Fill in null fac_type values, based on other clues elsewhere in the table
# # ---------------------------------------------------------------------------
# # Location description
# data["Site_Locat"] = data["Site_locat"]

# # Spill area
# data["Spill_Area"] = data["Spill_area"]

# # Create lowercase version of Site_Locat attribute, for easier string comparison
# data['Site_Locat_casefold'] = data.Site_Locat.str.lower()
# # ---------------------------------------------------------------------------
# # lists of multiple substrings present in the Site_Locat field that indicate facility type
# # include all permutations and mis-spellings
# # ---------------------------------------------------------------------------
# terms_platform = ['pp','platform']
# terms_tank = ['tank']
# terms_well = ['well','wellhead']
# terms_flowline = ['flow line','flowline','f/l', ' fl', 'delievery line', 'delivery line', 'd/l', 't/l', 'trunk line', 'trunkline', ' tl']
# terms_pipe = ['pipeline', 'p/l', ' pl ']
# terms_flowstat = ['flow station','flowstation', 'f/s']
# # terms_manifold = ['maniford','manifold']

# # Drop cells where Site_Locat is null or else the subsequent section won't work
# data = data[-data.Site_Locat.isna()]

# # ---------------------------------------------------------------------------
# # Populate 'factype' field for points where 'Site_Locat' field describes the facility type
# # ---------------------------------------------------------------------------
# data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_platform)),'Type_of_Fa'] = 'Offshore platforms'
# data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_tank)),'Type_of_Fa'] = 'Storage Tank'
# data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_well)),'Type_of_Fa'] = 'Well Head'
# data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_flowline)),'Type_of_Fa'] = 'Flow Line'
# data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_pipe)),'Type_of_Fa'] = 'Pipeline'
# data.loc[data['Site_Locat_casefold'].str.contains('|'.join(terms_flowstat)),'Type_of_Fa'] = 'Flow Station'
# data.loc[data['Site_Locat_casefold'].str.contains('fpso'),'Type_of_Fa'] = 'Floating production storage and offloading'

# print(data.Type_of_Fa.value_counts())
# print("Total number of null facility types:", data.Type_of_Fa.isnull().sum())

# # Drop cells where Type_of_Fa is null -- we simply don't know enough about what these points are to include them in OGIM
# data = data[-data.Type_of_Fa.isna()]
# # Drop records that are simply point locations along a pipeline -- not the data format we need for this infra type
# types2drop = ['Pipeline','Flow Line']
# data = data[-data.Type_of_Fa.isin(types2drop)]

# # ---------------------------------------------------------------------------
# # Create CATEGORY column that corresponds to my OGIM categories,
# # and assign a CATEGORY attribute to records based on their FAC_TYPE values
# # ---------------------------------------------------------------------------

# data['category_new'] = None
# data.loc[data.Type_of_Fa=='Well Head', 'category_new'] = 'Oil and gas wells'
# data.loc[(data.Type_of_Fa=='Floating production storage and offloading') | (data.Type_of_Fa=='Offshore platforms'), 'category_new'] = 'Offshore platforms'
# data.loc[data.Type_of_Fa=='other:compressor plant', 'category_new'] = 'Natural gas compressor stations'
# data.loc[(data.Type_of_Fa=='Storage Tank') | (data.Type_of_Fa=='Tank Farm'), 'category_new'] = 'Petroleum storage and terminals'

# # Create category for "Stations - Other"
# otherfaclist = ['Manifold','Flow Station','Rig','Pumping Station']
# data.loc[data.Type_of_Fa.str.contains('|'.join(otherfaclist)),'category_new'] = 'Stations - other'

# print(data.category_new.value_counts())
# print("Num. of records with null CATEGORY (should be zero):", data.category_new.isnull().sum())

# # Create 'on_off' shore category
# data['on_off'] = 'Onshore'
# data.loc[data.Spill_Area=='of', 'on_off'] = 'Offshore'

# # Create separate gdfs for each infra category
# wells = data[data.category_new=='Oil and gas wells'].reset_index(drop=True)
# platforms = data[data.category_new=='Offshore platforms'].reset_index(drop=True)
# statoth = data[data.category_new=='Stations - other'].reset_index(drop=True)
# storage = data[data.category_new=='Petroleum storage and terminals'].reset_index(drop=True)
# cs = data[data.category_new=='Natural gas compressor stations'].reset_index(drop=True)

# # ---------------------------------------------------------------------------
# # NIGERIA - Oil spill monitor dataset - Integration + Export
# # ---------------------------------------------------------------------------
# cs_NIG2, errors = integrate_facs(
#     cs.reset_index(drop=True),
#     starting_ids=0,
#     category='Natural gas compressor stations',
#     fac_alias='COMPR_PROC',
#     country='Nigeria',
#     # state_prov = None,
#     src_ref_id='165',
#     src_date='2019-11-01',
#     on_offshore='on_off',
#     fac_name='Site_Locat',
#     # fac_id = None,
#     fac_type='Type_of_Fa',
#     # spud_date = None,
#     # comp_date = None,
#     # drill_type = None,
#     # install_date = None,
#     # fac_status = None,
#     op_name='Company',
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
# # ---------------------------------------------------------------------------
# # Save data
# # ---------------------------------------------------------------------------
# save_spatial_data(
#     cs_NIG2,
#     "nigeria_gas_compressor_stations_part_02",
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path=save_path
# )
