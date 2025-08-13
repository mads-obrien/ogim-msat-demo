# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global CRUDE OIL REFINERIES.

# TODO:
[x] standardize import statements and CWD setting
[] standardize spacing between sections
[] alphabetize countries
[] update all file paths

CHECKLIST OF THINGS TO CHANGE
-- check existing "_facilities" scripts for refineries bits - DONE
-- ensure directory / file paths are correct - DONE
-- ensure output location points to "results_folder" for all exports - DONE
-- ensure output file names are all unique - DONE
-- make headers / line breaks consistent  - DONE
-- ensure all datasets have z coordinates stripped away - !! NOT DONE !!

@author: maobrien, momara, ahimmelberger
"""
import os
import re
import pandas as pd
import geopandas as gpd
import numpy as np
from tqdm import tqdm
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     save_spatial_data, schema_REFINERY, read_spatial_data,
                     transform_geom_3d_2d, strip_z_coord)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
results_folder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# -----------------------------------------------------------------------------
# Define custom functions


def convert_metric_tons_to_barrels_crudeoil(value):
    return value * 7.33
# Conversion factors from this resource: https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2022-approximate-conversion-factors.pdf


def convert_m3d_to_bbld(number):
    # 1 cubic meter = 6.2898107704 bbl (US)
    return number * 6.2898107704


# =============================================================================
# %% AFGHANISTAN
# =============================================================================
os.chdir(pubdata)
refine = pd.read_csv(r"Middle_East+Caspian\Afghanistan\other_data_\Refineries\_Afghanistan_refinery_.csv")
refine = gpd.GeoDataFrame(refine,
                          geometry=gpd.points_from_xy(refine.longitude,
                                                      refine.latitude),
                          crs=4326)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
refine = transform_CRS(refine,
                       target_epsg_code="epsg:4326",
                       appendLatLon=True)

# Convert refinery's capacity of 500 metric tons per day into barrels per day
refine['capacity_new'] = convert_metric_tons_to_barrels_crudeoil(int(refine['Capacity']))


# =============================================================================
# %% AFGHANISTAN - Integration + Export
# =============================================================================
refine_, errors = integrate_facs(
    refine,
    starting_ids=0,
    category='Crude oil refineries',
    fac_alias='REFINERY',
    country='Afghanistan',
    # state_prov=None,
    src_ref_id='168',
    src_date='2022-07-01',
    # on_offshore=None,
    fac_name='Name',
    # fac_id=None,
    # fac_type=None,
    # install_date=None,
    # fac_status=None,
    op_name='Company',
    commodity='Products',
    liq_capacity_bpd='capacity_new',
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
    refine_,
    file_name="afghanistan_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AUSTRALIA, NEW ZEALAND
# =============================================================================
os.chdir(pubdata)
fp1 = r"Australia+NewZealand\\Refineries\\Australia_NZ_Refineries.shp"
refineries = read_spatial_data(fp1, table_gradient=True)
refinery1 = transform_CRS(refineries,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []
operator = []
crude = []
crude2 = []
country = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        formatted_name = split[2]
        operator_name = split[1]
        names.append(formatted_name)
        operator.append(operator_name)
    else:
        names.append(name)

    # split popupinfo column since it has the bpd in there
    amount = row1_.PopupInfo
    if pd.isna(amount):
        crude.append('N/A')
    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

refinery1['NAME1'] = names
refinery1['OPERATOR'] = operator
refinery1['SPLIT_PART1'] = crude

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')

    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery1['LIQ_CAPACITY'] = crude2

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # add country column to refinery layer
    countries = row1_.FolderPath
    if pd.isna(countries):
        country.append('N/A')
    elif 'Australia' in countries:
        country.append('Australia')
    elif 'New Zealand' in countries:
        country.append('New Zealand')
    else:
        pass

refinery1['COUNTRY'] = country

# =============================================================================
# %% AUSTRALIA, NEW ZEALAND  - Integration + Export
# =============================================================================
refineries_final, errors = integrate_facs(
    refinery1,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    # on_offshore="",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

refineries_final1 = replace_row_names(refineries_final,
                                      colName="FAC_NAME",
                                      dict_names={'': 'N/A', 'NAN': 'N/A'})


save_spatial_data(
    refineries_final1,
    file_name="australia_new_zealand_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AUSTRALIA (oilandgasinfrastructure.com)
# =============================================================================
os.chdir(pubdata)
fp3 = "Australia+NewZealand\\Australia\\Refinery_Onshore_Australia.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery1 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# =============================================================================
# %% AUSTRALIA (oilandgasinfrastructure.com)  - Integration + Export
# =============================================================================
refinery_final, errors = integrate_facs(
    refinery1,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Australia",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
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
    refinery_final,
    file_name="australia_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AZERBAIJAN
# =============================================================================
os.chdir(pubdata)
fp3 = "Middle_East+Caspian\\Azerbaijan\\Oil_Gas_Infra_.com\\Refineries\\Refinery_Azerbajan.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery1 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Splitting the Description column to be able to extract the liquid capacity as well as the name
crude = []
names = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    amount = row1_.Descriptio
    if pd.isna(amount):
        crude.append('N/A')

    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])
    names1 = row1_.Name
    if pd.isna(amount):
        crude.append('N/A')

    else:
        name_split = names1.split("- ")
        names_formatted = name_split[1] + name_split[2]
        names.append(names_formatted)

refinery1['SPLIT_PART1'] = crude
refinery1['NAMES1'] = names

crude2 = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')

    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery1['LIQ_CAPACITY'] = crude2

# =============================================================================
# %% AZERBAIJAN  - Integration + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery1,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Azerbaijan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="ONSHORE",
    fac_name="NAMES1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    refineries,
    file_name="azerbaijan_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% BANGLADESH
# =============================================================================
os.chdir(pubdata)
fp3 = "China+SE_Asia\\Bangladesh\\Refinery_Bangladesh.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery2 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []
operator = []
crude = []
crude2 = []

for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        formatted_name = split[2]
        operator_name = split[1]
        names.append(formatted_name)
        operator.append(operator_name)
    else:
        names.append(name)

    amount = row1_.Descriptio
    if pd.isna(amount):
        crude.append('N/A')
    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

refinery2['NAME1'] = names
refinery2['OPERATOR'] = operator
refinery2['SPLIT_PART1'] = crude

for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')
    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery2['LIQ_CAPACITY'] = crude2

# =============================================================================
# %% BANGLADESH - Integration + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery2,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    on_offshore="ONSHORE",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    refineries,
    file_name="bangladesh_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% BRAZIL  # DONE
# =============================================================================
os.chdir(v24data)
bz_ref_1 = gpd.read_file(r'brazil\WebMap_EPE\Processing_facilities.shp')
bz_ref_1['src'] = '267'
bz_ref_1 = transform_CRS(bz_ref_1,
                         target_epsg_code="epsg:4326",
                         appendLatLon=True)
# Drop the Braskem facilities, which I think are petrochemical facilities?
# https://en.wikipedia.org/wiki/Braskem
bz_ref_1 = bz_ref_1.query("sigla != 'BRASKEM'")

# There are two add'l refinery locations from ANP dataset
bz_ref_2 = gpd.read_file(r'brazil\GeoMapsANP\refinariasPoint.shp')
bz_ref_2['src'] = '126'
bz_ref_2 = transform_CRS(bz_ref_2,
                         target_epsg_code="epsg:4326",
                         appendLatLon=True)
refs2keep = ['Complexo de Energias Boaventura', 'Univen Refinaria de Petróleo']
bz_ref_2 = bz_ref_2[bz_ref_2.NOME.isin(refs2keep)].reset_index(drop=True)
# Rename columns before join
bz_ref_2 = bz_ref_2.rename(columns={"NOME": "nome_inst"})

# Join !
bz_ref = bz_ref_1.append(bz_ref_2)

# Create type field
bz_ref['factype'] = None
bz_ref.loc[bz_ref.nome_inst == 'UNIDADE DE INDUSTRIALIZAÇÃO DO XISTO', 'factype'] = 'Shale industrialization unit'
bz_ref.loc[bz_ref.nome_inst.str.contains('LUBRIFICANTES E DERIVADOS'), 'factype'] = 'Lubricants and petroleum derivatives'

# Convert "capacity authorized" field into BBL/day
bz_ref['cap_bpd'] = bz_ref.cap_aut.apply(lambda x: convert_m3d_to_bbld(x))

bz_ref.razao_soci = bz_ref.razao_soci.fillna('N/A')

# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
bz_ref_integrated, errors = integrate_facs(
    bz_ref,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="BRAZIL",
    # state_prov="",
    src_ref_id="src",
    src_date="2024-01-01",  # FIXME
    on_offshore="ONSHORE",
    fac_name="nome_inst",
    # fac_id="",
    fac_type="factype",
    # fac_status="",
    op_name="razao_soci",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="cap_bpd",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    bz_ref_integrated,
    file_name="brazil_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% EUROPE
# =============================================================================
os.chdir(pubdata)
fp1 = r"International_data_sets\Refineries.shp"
refines = read_spatial_data(fp1, table_gradient=True)
# only keep needed columns
refines = refines[['Name', 'FolderPath', 'PopupInfo', 'geometry']]
refines = transform_CRS(refines,
                        target_epsg_code="epsg:4326",
                        appendLatLon=True)

# Reduce to just refineries within Europe, includes Turkey (NOT including Russia)
# Look for filepath strings that contain ANY of the following substrings
refines.FolderPath = refines.FolderPath.str.split('Refineries/').str[1]
refines = refines[refines.FolderPath.str.contains('Europe')]
refines = refines[~refines.FolderPath.str.contains('Russia')].reset_index(drop=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Extract capacity information
start_tag = '<table width="300" border="0"><tr><td colspan="2">'
refines['PopupInfo'] = refines.PopupInfo.str.replace(start_tag, '')
refines['capacity'] = refines.PopupInfo.str.split('</td></tr><tr>').str[0]

# Create a numeric-only capacity column
# TODO - do we want to preserve the KIND of capacity info somewhere in the
# output dataset? like "vacuum distillation capacity" or "catalytic
# hydrotreating capacity"
refines['capacity_num'] = refines.capacity.str.split(' bpd').str[0]
refines.capacity_num.replace({'unknown capacity': np.nan}, inplace=True)
refines['capacity_num'] = pd.to_numeric(refines.capacity_num.str.replace(',', '')).astype('Int64')

# Strip and keep just the operator name from Name column (the value in the
# "middle" of the dashes)
refines['op_name'] = refines.Name.str.split(' - ').str[1]
# Strip and keep just the place/facility name from Name column (value at the end)
refines['fac_name'] = refines.Name.str.split(' - ').str[-1]

# Extract Country Name from the FolderPath field
country = []
for idx1_, row1_ in tqdm(refines.iterrows(), total=refines.shape[0]):
    countries = row1_.FolderPath
    if pd.isna(countries):
        country.append('N/A')
    elif 'Belgium' in countries:
        country.append('Belgium')
    elif 'France' in countries:
        country.append('France')
    elif 'Germany' in countries:
        country.append('Germany')
    elif 'Holland' in countries:
        country.append('Netherlands')
    elif 'Portugal' in countries:
        country.append('Portugal')
    elif 'Spain' in countries:
        country.append('Spain')
    elif 'Switzerland' in countries:
        country.append('Switzerland')
    elif 'Denmark' in countries:
        country.append('Denmark')
    elif 'Finland' in countries:
        country.append('Finland')
    elif 'Norway' in countries:
        country.append('Norway')
    elif 'Sweden' in countries:
        country.append('Sweden')
    elif 'United Kingdom' in countries:
        country.append('United Kingdom')
    elif 'Austria' in countries:
        country.append('Austria')
    elif 'Belarus' in countries:
        country.append('Belarus')
    elif 'Czech' in countries:
        country.append('Czechia')
    elif 'Hungary' in countries:
        country.append('Hungary')
    elif 'Lithuania' in countries:
        country.append('Lithuania')
    elif 'Poland' in countries:
        country.append('Poland')
    elif 'Romania' in countries:
        country.append('Romania')
    # elif 'Russia' in countries:
    #     country.append('Russia')
    elif 'Slovakia' in countries:
        country.append('Slovakia')
    elif 'Ukraine' in countries:
        country.append('Ukraine')
    elif 'Albania' in countries:
        country.append('Albania')
    elif 'Bulgaria' in countries:
        country.append('Bulgaria')
    elif 'Croatia' in countries:
        country.append('Croatia')
    elif 'Greece' in countries:
        country.append('Greece')
    elif 'Italy' in countries:
        country.append('Italy')
    elif 'Macedonia' in countries:
        country.append('Macedonia')
    elif 'Serbia and Montenegro' in countries:
        country.append('Serbia')  # Use Serbia instead of Serbia and Montenegro for OGIM consistency
    elif 'Slovenia' in countries:
        country.append('Slovenia')
    elif 'Turkey' in countries:
        country.append('Turkey')
    else:
        pass

refines['COUNTRY'] = country

# =============================================================================
# %% EUROPE - Integration + Export
# =============================================================================
refines_europe_final, errors = integrate_facs(
    refines,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    # on_offshore="",
    fac_name="fac_name",
    # fac_id="",
    # fac_type="",
    # fac_status="",
    op_name="op_name",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="capacity_num",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    refines_europe_final,
    file_name="europe_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% INDIA
# =============================================================================
os.chdir(pubdata)
fp3 = "China+SE_Asia\\India\\Refinery_India.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery1 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# =============================================================================
# %% INDIA - Integration + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery1,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="India",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
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
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    refineries,
    file_name="india_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% KAZAKHSTAN
# =============================================================================
os.chdir(pubdata)
fp3 = "Middle_East+Caspian\\Kazakhstan\\Oil_Gas_Infra_.com\\Refineries\\Refinery_Kazakhstan.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery1 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
crude = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # Split Descripttio column and append to names and operator lists, since
    # the name column includes both of these
    amount = row1_.Descriptio
    if pd.isna(amount):
        crude.append('N/A')

    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

refinery1['SPLIT_PART1'] = crude

crude2 = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')

    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery1['LIQ_CAPACITY'] = crude2

# =============================================================================
# %% KAZAKHSTAN - Integration + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery1,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Kazakhstan",
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
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    refineries,
    file_name="kazakhstan_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% LIBYA
# =============================================================================
os.chdir(pubdata)
fp = r"Africa\Libya\Libya_AGO_kmitchell\Libya_Infrastructure_Refines.shp"
data = read_spatial_data(fp, table_gradient=True)
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# 'Questionable Proj' values in 'status' column don't make sense;
# change only these values to None
data.loc[data.status == 'Questionable Proj', 'status'] = None

# Fix "online year" field to match our desired date format
# Turn all "zero" dates into our NoData value
data.loc[data.on_line_ye == 0, 'on_line_ye'] = '1900'
data['on_line_ye'] = data['on_line_ye'].astype('str') + '-01-01'

# # According to the field names in the webmap, the FULL variable names are as follows:
# [shapefile var. name] = [full var. name]
# cap_crude_ = CAP_CRUDE_UNDER_CONST_BPCD	# BPCD = barrel per calendar day
# cap_crude1 = CAP_CRUDE_PLANNED_BPCD
# cap_crud_1 = CAP_CRUDE_OPERATING_BPCD   # Keep this one

# =============================================================================
# %% LIBYA - Integration + Export
# =============================================================================
data_, errors = integrate_facs(
    data,
    starting_ids=0,
    category='Crude oil refineries',
    fac_alias='REFINERY',
    country='Libya',
    # state_prov = None,
    src_ref_id='166',
    src_date='2017-06-01',
    # on_offshore = None,
    fac_name='refinery_n',
    # fac_id = None,
    # fac_type = None,
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    install_date='on_line_ye',
    fac_status='status',
    op_name='operator_n',
    commodity='refinery_t',
    liq_capacity_bpd='cap_crud_1',
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
    data_,
    file_name="libya_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% MYANMAR
# =============================================================================
os.chdir(pubdata)
fp3 = "China+SE_Asia\\Myanmar\\oil_refineries_Myanmar.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery2 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []
operator = []
crude = []
crude2 = []

for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        formatted_name = split[2]
        operator_name = split[1]
        names.append(formatted_name)
        operator.append(operator_name)
    else:
        names.append(name)

    amount = row1_.Descriptio
    if pd.isna(amount):
        crude.append('N/A')
    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

refinery2['NAME1'] = names
refinery2['OPERATOR'] = operator
refinery2['SPLIT_PART1'] = crude

for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')
    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery2['LIQ_CAPACITY'] = crude2

# =============================================================================
# %% MYANMAR - Integration + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery2,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Myanmar",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="ONSHORE",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    refineries,
    file_name="myanmar_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AFRICA (oilandgasinfrastructure.com)
# =============================================================================
os.chdir(pubdata)

CAM_refin = gpd.read_file(r"Africa/Cameroon/Refinery.kml.shp")
DRC_refin = gpd.read_file(r"Africa/DRC/Refinery_DRC.kml.shp")
GAN_refin = gpd.read_file(r"Africa/Ghana/Refineries_Ghana.kml.shp")
IVC_refin = gpd.read_file(r"Africa/Ivory_Coast/Refinery_IvoryCoast.kml.shp")
NIG_refin = gpd.read_file(r"Africa/Nigeria/Refinery_Nigeria.kml.shp")
SAFR_refin = gpd.read_file(r"Africa/South_Africa/Refineries_SouthAfrica.kml.shp")
# Remove Libya refineries, as they are already represented by the AGO kmitchell source
# LIB_refin = gpd.read_file(r"Africa/Libya/Refinery_Onshore_Libya.kml.shp")


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Extract operators where company name is in the MIDDLE of the 'Name' string
# 'Name' value is made up of three "parts"... a number, an operator name, and
# a facility name, separated by "-".
# Split the original 'Name' string into 3 parts, and keep the latter two
for df in [CAM_refin, DRC_refin, GAN_refin, IVC_refin]:
    df['opername'] = df['Name'].str.split(' - ').str[1]
    df['Name_new'] = df['Name'].str.split(' - ').str[2]

# Extract Name and Operator for certain countries that are formatted differently
NIG_refin['opername'] = NIG_refin['Name'].str.split(' - ').str[0]
NIG_refin['Name_new'] = NIG_refin['Name'].str.split(' - ').str[1]

# LIB_refin['Name_new'] = LIB_refin['Name']

SAFR_refin['Name_new'] = SAFR_refin['Name']
SAFR_refin.loc[SAFR_refin['Name_new'].str.contains('-'), 'opername'] = SAFR_refin['Name'].str.split(' - ').str[1]
SAFR_refin.loc[SAFR_refin['Name_new'].str.contains('-'), 'Name_new'] = SAFR_refin['Name'].str.split(' - ').str[2]


# Add "Country" attribute to each gdf
CAM_refin['country'] = 'Cameroon'
DRC_refin['country'] = 'Democratic Republic of the Congo'
GAN_refin['country'] = 'Ghana'
IVC_refin['country'] = 'Ivory Coast'
# LIB_refin['country'] = 'Libya'
NIG_refin['country'] = 'Nigeria'
SAFR_refin['country'] = 'South Africa'

# Confirm that all dataframes are the same CRS before appending into one gdf
all_dfs_final = [
    CAM_refin,
    DRC_refin,
    GAN_refin,
    IVC_refin,
    # LIB_refin,
    NIG_refin,
    SAFR_refin
]

for df in all_dfs_final:
    print(df.crs)

# Append country-specific gdfs into one gdf
data = pd.concat(all_dfs_final)
data = data.reset_index(drop=True)
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Extract capacity information from between the "markers" I see in my Description string
# https://www.kite.com/python/answers/how-to-get-the-substring-between-two-markers-in-python


def extract_capacity(thestring):
    # Any time a capacity value is mentioned in the 'Descriptio' column, it's
    # preceeded by the text `colspan="2">` and followed by the text `bpd` (a.k.a. barrels per day)
    # Use regular expressions to find and return the numeric capacity
    if 'colspan' in thestring:
        substring = re.search('colspan="2">(.*?)bpd', thestring).group(1)
        return int(re.sub('[^A-Za-z0-9]+', '', substring))
    else:
        # If the 'Descriptio' column doesn't contain capacity information,
        # return the OGIM 'no data' value
        return -999


# Apply my `extract_capacity` function across all the rows in 'Descriptio' column
# Fill any empty 'Descriptio' fields (my function won't work if there are empty cells in the gdf column)
data['Descriptio'] = data['Descriptio'].fillna('NOT AVAILABLE')
# Extract the substring of capacity information present in some 'Descriptio' values
data['capacity'] = data['Descriptio'].apply(extract_capacity)

# Fill all empty or NAN cells
data = data.fillna('N/A')

# =============================================================================
# %% AFRICA (oilandgasinfrastructure.com) - Integration + Export
# =============================================================================
data_, errors = integrate_facs(
    data,
    starting_ids=0,
    category='Crude oil refinery',
    fac_alias='REFINERY',
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    # on_offshore = None,
    fac_name='Name_new',
    # fac_id = None,
    # fac_type = None,
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    # install_date = None,
    # fac_status = None,
    op_name='opername',
    # commodity = None,
    liq_capacity_bpd='capacity',
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
    data_,
    file_name="oginfracom_africa_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% MIDDLE EAST (oilandgasinfrastructure.com)
# =============================================================================
os.chdir(pubdata)

iran = gpd.read_file(r'Middle_East+Caspian\Iran\Oil_Gas_Infra_.com\Refineries\Refinaries.kml.shp')
iraq = gpd.read_file(r'Middle_East+Caspian\Iraq\Oil_Gas_Infra_.com\Refineries\Refineries_Iraq.kml.shp')
kuwait = gpd.read_file(r'Middle_East+Caspian\Kuwait\Oil_Gas_Infra_.com\Refineries\Refineries_Kuwait.kml.shp')
oman = gpd.read_file(r'Middle_East+Caspian\Oman\Oil_Gas_Infra_.com\Refineries\Refinery_Oman.kml.shp')
pak = gpd.read_file(r'Middle_East+Caspian\Pakistan\Oil_Gas_Infra_.com\Refineries\Refineries_Pakistan.kml.shp')
qatar = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Refineries\Refinaries_Qatar.kml.shp')
saudi = gpd.read_file(r'Middle_East+Caspian\Saudi_Arabia\Oil_Gas_Infra_.com\Refineries\Refineries_Saudi_Arabia.kml.shp')
yemen = gpd.read_file(r'Middle_East+Caspian\Yemen\Oil_Gas_Infra_.com\Refineries\Refinaries_Yemen.kml.shp')

# Add country name column to each country-specific gdf
iran['country'] = 'Iran'
iraq['country'] = 'Iraq'
kuwait['country'] = 'Kuwait'
oman['country'] = 'Oman'
pak['country'] = 'Pakistan'
qatar['country'] = 'Qatar'
saudi['country'] = 'Saudi Arabia'
yemen['country'] = 'Yemen'

# -----------------------------------------------------------------------------
# Extract operator information from 'Name' column, in cases where it's present

# Extract operators where company name is in the MIDDLE of the 'Name' string
# 'Name' value is made up of three "parts"... a number, an operator name, and
# a facility name, separated by "-".
# Split the original 'Name' string into 3 parts, and keep the latter two
for df in [iran, iraq, kuwait, qatar, saudi, yemen]:
    df['opername'] = df['Name'].str.split(' - ').str[1]
    df['Name_new'] = df['Name'].str.split(' - ').str[2]


# Special cases
# One record contains just operator name and fac name separated by dash
oman.loc[oman.Name.str.contains(' - '), 'opername'] = oman['Name'].str.split(' - ').str[0]
oman.loc[oman.Name.str.contains(' - '), 'Name_new'] = oman['Name'].str.split(' - ').str[1]
# Another record includes facility name
# I populated the "opername" with the owner listed in the abarrelfull.wikidot.com page in the 'Descriptio' column
oman.loc[oman.Name.str.contains('Sohar'), 'opername'] = 'ORPC'
oman.loc[oman.Name.str.contains('Sohar'), 'Name_new'] = 'Sohar'

# Mix of records with 3 "parts" as before, and records with only one item
# Split the original 'Name' string into 3 parts, and keep the latter two
pak.loc[pak.Name.str.contains(' - '), 'opername'] = pak['Name'].str.split(' - ').str[1]
pak.loc[pak.Name.str.contains(' - '), 'Name_new'] = pak['Name'].str.split(' - ').str[2]
# For records where there is one 'Name' value, and it's unclear if the
# Also correct the mis-spelling of "Refinery"
pak.loc[pak.Name.str.contains('Pakistan Refinary Limited'), 'opername'] = 'Pakistan Refinery Ltd.'
pak.loc[pak.Name.str.contains('Pakistan Refinary Limited'), 'Name_new'] = 'Pakistan Refinery Ltd.'
pak.loc[pak.Name.str.contains('National Refinary'), 'opername'] = 'National Refinery Ltd.'
pak.loc[pak.Name.str.contains('National Refinary'), 'Name_new'] = 'National Refinery Ltd.'
pak.loc[pak.Name.str.contains('Attock Refinary'), 'opername'] = 'Attock Refinery Ltd.'
pak.loc[pak.Name.str.contains('Attock Refinary'), 'Name_new'] = 'Attock Refinery Ltd.'

# -----------------------------------------------------------------------------
# Extract capacity information from between the "markers" I see in my Description string
# https://www.kite.com/python/answers/how-to-get-the-substring-between-two-markers-in-python

all_dfs = [iran, iraq, kuwait, oman, pak, qatar, saudi, yemen]


def extract_capacity(thestring):
    # Any time a capacity value is mentioned in the 'Descriptio' column, it's
    # preceeded by the text `colspan="2">` and followed by the text `bpd` (a.k.a. barrels per day)
    # Use regular expressions to find and return the numeric capacity
    if 'colspan' in thestring:
        substring = re.search('colspan="2">(.*?)bpd', thestring).group(1)
        return int(re.sub('[^A-Za-z0-9]+', '', substring))
    else:
        # If the 'Descriptio' column doesn't contain capacity information, return
        # the OGIM 'no data' value
        return -999


# Iterate over all dataframes,
# and apply my `extract_capacity` function across all the rows in 'Descriptio' column
for df in all_dfs:
    # Fill any empty 'Descriptio' fields (my function won't work if there are empty cells in the gdf column)
    df['Descriptio'] = df['Descriptio'].fillna('NOT AVAILABLE')
    # Extract the substring of capacity information present in some 'Descriptio' values
    df['capacity'] = df['Descriptio'].apply(extract_capacity)

# -----------------------------------------------------------------------------
# Merge country gdfs into one gdf for all refineries
all_dfs = [iran, iraq, kuwait, oman, pak, qatar, saudi, yemen]
data = pd.concat(all_dfs)
data = data.reset_index(drop=True)

# For readibility, drop columns I no longer need
data = data.filter(['Name_new', 'country', 'opername', 'capacity', 'geometry'])

data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)
data = strip_z_coord(data)

# Add a SRC_ID column
data['SRC_ID'] = '22'
data['sourcedate'] = '2014-01-01'

# =============================================================================
# %% IRAN - add facility-specific info to each oilandgasinfrastructure refinery location
# =============================================================================
os.chdir(pubdata)
fp = r"Middle_East+Caspian\Iran\other_data\Refineries\Iran_refineries_.csv"
# csv to gdf
iran_csv = pd.read_csv(fp, sep=",", header=0)
iran_csv = gpd.GeoDataFrame(iran_csv,
                            geometry=gpd.points_from_xy(iran_csv.longitude,
                                                        iran_csv.latitude),
                            crs=4326)

# Rename certain columns for table merging later on
iran_csv = iran_csv.rename(columns={"Company": "opername",
                                    "Country": "country",
                                    'Capacity': 'capacity'}, errors="raise")

# -----------------------------------------------------------------------------
# Read in standalone source table and join each refinery record
# with its SRC_ID, based on the URL as the join field
table_fp = 'OGIM_Data_Catalog.xlsx'
sourcetable = pd.read_excel(table_fp)
# Change the particular columns I'm going to use from this table to strings
for col in ['SRC_ID', 'SRC_YEAR', 'SRC_MNTH']:
    sourcetable[col] = sourcetable[col].astype('Int64').astype('str')

# For simplicity sake, drop unnecessary columns from `sourcetable` before joining
sourcetable = sourcetable.filter(['SRC_ID', 'SRC_URL', 'SRC_YEAR', 'SRC_MNTH'])

# Add a correct "SRC_URL" column to my gdf, since there are 11 unique sources to cite
iran_csv = iran_csv.merge(
    sourcetable,
    how='left',
    left_on='Source',
    right_on='SRC_URL',
    sort=False,
    suffixes=('_x', '_y'),
    copy=True,
    indicator=False,
    validate=None)

# TODO - make the insertion of a zero for the month (to make it have two digits)
# not manual anymore... make it automatic based on SRC_MNTH length

# Concatenate the date of each source to feed into the integration function,
# since there are a few unique ones in the refinery gdf
iran_csv['sourcedate'] = iran_csv['SRC_YEAR'].astype('str') + '-0' + iran_csv['SRC_MNTH'].astype('str') + '-01'

# -----------------------------------------------------------------------------
# Join these iran_csv records to Iranian refineries from oginfra.com

# Prepare iran_csv facility names for joining
iran_csv['Name_new'] = iran_csv.Name.str.replace(' Refinery', '')
iran_csv['Name_new'] = iran_csv['Name_new'].str.replace(' Oil', '')
# Prepare oginfra.com facility names for joining
data.Name_new.replace({'Lavan Island': 'Lavan'}, inplace=True)

# Join tables by the 'Name_new' field. Use an outer join bc there are some
# unique records that won't join, and all of those should be retained.
refines_joined = data.merge(iran_csv,
                            how='outer',
                            on='Name_new',
                            sort=False,
                            suffixes=('_x', '_y'),
                            copy=True,
                            indicator=False,
                            validate=None)

# Fill NA values in certain columns, using the more up-to-date info from the
# Iran csv (columns with "_y" suffix) whenever possible
refines_joined.opername_y = refines_joined.opername_y.fillna(refines_joined.opername_x)
refines_joined.country_y = refines_joined.country_y.fillna(refines_joined.country_x)
refines_joined.capacity_y = refines_joined.capacity_y.fillna(refines_joined.capacity_x)

# Use the oginfra.com geometries (column with "_x" suffix) where possible
# TODO - is this really the best geom to use?
refines_joined.loc[refines_joined.geometry_x.isna(), "geometry_x"] = refines_joined.geometry_y
refines_joined = refines_joined.set_geometry('geometry_x')
refines_joined.latitude_calc = refines_joined.latitude_calc.fillna(refines_joined.latitude)
refines_joined.longitude_calc = refines_joined.longitude_calc.fillna(refines_joined.longitude)

# In rows where two src_ids were used, list both IDs in the SRC_ID_x cell column
refines_joined.loc[refines_joined.SRC_ID_y.notna(), 'SRC_ID_x'] = refines_joined.SRC_ID_x + ',' + refines_joined.SRC_ID_y
refines_joined.SRC_ID_x = refines_joined.SRC_ID_x.fillna(refines_joined.SRC_ID_y)

# Create a sourcedate column that contains the most recent source date affiliated
# with each record
refines_joined['sourcedate_y'] = refines_joined['sourcedate_y'].fillna(refines_joined.sourcedate_x)

# =============================================================================
# %% MIDDLE EAST (oilandgasinfrastructure.com) - Integration + Export
# =============================================================================
refines_joined_integrated, errors = integrate_facs(
    refines_joined,
    starting_ids=0,
    category='Crude oil refineries',
    fac_alias='REFINERY',
    country='country_y',
    # state_prov = None,
    src_ref_id='SRC_ID_x',
    src_date='sourcedate_y',
    on_offshore='Onshore',
    fac_name='Name_new',
    # fac_id = None,
    # fac_type = None,
    # spud_date = None,
    # comp_date = None,
    # drill_type = None,
    # install_date = None,
    # fac_status = None,
    op_name='opername_y',
    # commodity = None,
    liq_capacity_bpd='capacity_y',
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
    refines_joined_integrated,
    file_name="oginfracom_middleeast_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% RUSSIA, CENTRAL ASIA
# =============================================================================
os.chdir(pubdata)
fp1 = r"International_data_sets\Refineries.shp"
ref_eurasia = read_spatial_data(fp1, table_gradient=True)
ref_eurasia = transform_CRS(ref_eurasia,
                            target_epsg_code="epsg:4326",
                            appendLatLon=True)
ref_eurasia.FolderPath = ref_eurasia.FolderPath.str.lstrip('refineries_world_OilGasInfra.com.kml/Refineries/')

# Filter to facilities in Russia, Kyrgyzstan, and Uzbekistan
# ^ these are countries we have not integrated elsewhere in this script
ref_eurasia['country'] = 'N/A'
countries = ['Russia', 'Kyrgyzstan', 'Uzbekistan']
for c in countries:
    ref_eurasia.loc[ref_eurasia.FolderPath.str.contains(c), 'country'] = c
ref_eurasia = ref_eurasia[ref_eurasia.country.isin(countries)].reset_index(drop=True)


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Update PopupInfo columnn
dict_names = {}
colName = "PopupInfo"
dict_names = {
    '104,900 bpd crude capacity': '>>>104,900 bpd crude capacity',
    '105,682 bpd crude capacity': '>>>105,682 bpd crude capacity',
    '135,200 bpd crude capacity': '>>>135,200 bpd crude capacity',
    '27,000 bpd crude capacity': '>>>27,000 bpd crude capacity',
    '343,000 bpd crude capacity': '>>>343,000 bpd crude capacity',
    '450,000': '>>>450,000 bpd crude capacity',
    '50,000 bpd crude capacity': '>>>50,000 bpd crude capacity',
    '55,000 bpd crude capacity': '>>>55,000 bpd crude capacity',
    '80,000 bpd crude capacity': '>>>80,000 bpd crude capacity',
    '90,000 bpd crude capacity': '>>>90,000 bpd crude capacity'
}
ref_eurasia = replace_row_names(ref_eurasia, colName, dict_names)

# Update Name columnn
dict_names = {}
colName = "Name"
dict_names = {
    '513a - Total SA—Milford Haven': '513a - Total SA—Milford Haven - ',
    '421 - Ulyanovskneft': '421 - Ulyanovskneft - '
}
ref_eurasia = replace_row_names(ref_eurasia, colName, dict_names)

names = []
operator = []
crude = []
crude2 = []
country = []


for idx1_, row1_ in tqdm(ref_eurasia.iterrows(), total=ref_eurasia.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark
    # and then reorganizing the order and format
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        formatted_name = split[2]
        operator_name = split[1]
        names.append(formatted_name)
        operator.append(operator_name)
    else:
        names.append(name)

    amount = row1_.PopupInfo
    if pd.isna(amount):
        crude.append('N/A')
    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

ref_eurasia['NAME1'] = names
ref_eurasia['OPERATOR'] = operator
ref_eurasia['SPLIT_PART1'] = crude

for idx1_, row1_ in tqdm(ref_eurasia.iterrows(), total=ref_eurasia.shape[0]):
    # Append to dates list the reformatted date by splitting at each slash mark
    # and then reorganizing the order and format
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')
    elif amount2 == "unknown capacity</td":
        crude2.append('N/A')

    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

ref_eurasia['LIQ_CAPACITY'] = crude2

# =============================================================================
# %% RUSSIA, CENTRAL ASIA - Integration + Export
# =============================================================================
refineries_final, errors = integrate_facs(
    ref_eurasia,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="country",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    # on_offshore="",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

refineries_final1 = replace_row_names(refineries_final,
                                      colName="FAC_NAME",
                                      dict_names={'': 'N/A'})


save_spatial_data(
    refineries_final,
    file_name="Eurasia_Russia_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% SOUTHEAST ASIA, CHINA
# =============================================================================
os.chdir(pubdata)
fp1 = r"China+SE_Asia\\Refineries\\SE_Asia_Refineries.shp"
refineries = read_spatial_data(fp1, table_gradient=True)
refinery1 = transform_CRS(refineries, target_epsg_code="epsg:4326", appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# Update PopupInfo columnn to match the same format as the rest of the rows in the PopupInfo column
dict_names = {}
colName = "PopupInfo"
dict_names = {
    '135,200 bpd crude capacity': '>>>135,200 bpd crude capacity',
    '450,000': '>>>450,000 bpd crude capacity',
}

refinery2 = replace_row_names(refinery1, colName, dict_names)

names = []
operator = []
crude = []
crude2 = []
country = []


for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        formatted_name = split[2]
        operator_name = split[1]
        names.append(formatted_name)
        operator.append(operator_name)
    else:
        names.append(name)

    # split popupinfo column since it has the bpd in there
    amount = row1_.PopupInfo
    if pd.isna(amount):
        crude.append('N/A')
    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

refinery2['NAME1'] = names
refinery2['OPERATOR'] = operator
refinery2['SPLIT_PART1'] = crude

for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')
    elif amount2 == "unknown capacity</td":
        crude2.append('N/A')

    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery2['LIQ_CAPACITY'] = crude2

for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # create a new column to get the country names from the FolderPath column
    countries = row1_.FolderPath
    if pd.isna(countries):
        country.append('N/A')
    elif 'Papua' in countries:
        country.append('Papua New Guinea')
    elif 'Brunei' in countries:
        country.append('Brunei')
    elif 'China' in countries:
        country.append('China')
    elif 'Taiwan' in countries:
        country.append('Taiwan')
    elif 'Sri' in countries:
        country.append('Sri Lanka')
    elif 'South Korea' in countries:
        country.append('South Korea')
    elif 'North Korea' in countries:
        country.append('North Korea')
    elif 'Singapore' in countries:
        country.append('Singapore')
    elif 'Philippines' in countries:
        country.append('Philippines')
    elif 'Malaysia' in countries:
        country.append('Malaysia')
    elif 'Japan' in countries:
        country.append('Japan')
    elif 'Indonesia' in countries:
        country.append('Indonesia')
    elif 'India' in countries:
        country.append('India')
    else:
        pass

refinery2['COUNTRY'] = country

# =============================================================================
# %% SOUTHEAST ASIA, CHINA - Integration + Export
# =============================================================================
refineries_final, errors = integrate_facs(
    refinery2,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    on_offshore="ONSHORE",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

refineries_final1 = replace_row_names(refineries_final,
                                      colName="FAC_NAME",
                                      dict_names={'': 'N/A'})


save_spatial_data(
    refineries_final,
    file_name="se_asia_china_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% THAILAND
# =============================================================================
os.chdir(pubdata)
fp5 = "China+SE_Asia\\Thailand\\Refineries_Thailand.kml.shp"
refinery = read_spatial_data(fp5, table_gradient=True)
refinery2 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
names = []
operator = []
crude = []
crude2 = []


for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        formatted_name = split[2]
        operator_name = split[1]
        names.append(formatted_name)
        operator.append(operator_name)
    else:
        names.append(name)

    amount = row1_.Descriptio
    if pd.isna(amount):
        crude.append('N/A')
    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])

refinery2['NAME1'] = names
refinery2['OPERATOR'] = operator
refinery2['SPLIT_PART1'] = crude


for idx1_, row1_ in tqdm(refinery2.iterrows(), total=refinery2.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')
    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery2['LIQ_CAPACITY'] = crude2


# =============================================================================
# %% THAILAND - Integration + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery2,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Thailand",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="ONSHORE",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    refineries,
    file_name="thailand_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% TURKMENISTAN
# =============================================================================
os.chdir(pubdata)
fp3 = "Middle_East+Caspian\\Turkmenistan\\Oil_Gas_Infra_.com\\Refineries\\Refinery_Turkmenistan.kml.shp"
refinery = read_spatial_data(fp3, table_gradient=True)
refinery1 = transform_CRS(refinery,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
crude = []
name = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # Split Descripttio column and append to names and operator lists, since
    # the name column includes both of these
    amount = row1_.Descriptio
    if pd.isna(amount):
        crude.append('N/A')

    else:
        crude_split = amount.split(">")
        crude.append(crude_split[3])
    names1 = row1_.Name
    if pd.isna(amount):
        crude.append('N/A')

    else:
        name_split = names1.split("- ")
        names_formatted = name_split[1] + name_split[2]
        name.append(names_formatted)


refinery1['SPLIT_PART1'] = crude
refinery1['NAMES1'] = name


refinery1['SPLIT_PART1'] = crude

crude2 = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # split the intial split again, to pull the bpd
    amount2 = row1_.SPLIT_PART1
    if pd.isna(amount2):
        crude2.append('N/A')

    else:
        crude_split2 = amount2.split(" ")
        string_number = crude_split2[0]
        string_number2 = string_number.replace(',', '')
        crude2.append(float(string_number2))

refinery1['LIQ_CAPACITY'] = crude2

# =============================================================================
# %% TURKMENISTAN - Integration  + Export
# =============================================================================
refineries, errors = integrate_facs(
    refinery1,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Turkmenistan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore="ONSHORE",
    fac_name="NAMES1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    # op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="LIQ_CAPACITY",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    refineries,
    file_name="turkmenistan_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% UNITED STATES
# =============================================================================
os.chdir(v24data)
ref_us = read_spatial_data(r"united_states\national\Petroleum_Refineries.geojson")
ref_us = transform_CRS(ref_us, appendLatLon=True)

# Convert capacity units
# "AD_Mbpd" = Atmospheric Crude Oil Distillation Capacity, in thousand barrels/day
# source: https://www.eia.gov/petroleum/refinerycapacity/table3.pdf
ref_us['crude_bpd'] = ref_us.AD_Mbpd * 1000

# This data set comes from a list of "Operable Refineries" in the US, so we know
# all of them are active
ref_us['status'] = 'OPERATING'

# =============================================================================
# %% UNITED STATES - Integration + Export
# =============================================================================
ref_us_integrated, ref_errors_3 = integrate_facs(
    ref_us,
    starting_ids=1,
    category="CRUDE OIL REFINERY",
    fac_alias="REFINERY",
    country="United States of America",
    state_prov="State",
    src_ref_id="269",
    src_date="2024-01-01",
    on_offshore="Onshore",
    fac_name="Site",  # This is the name of the city in most cases, but it'll do
    fac_id="site_id",
    # fac_type="",
    # install_date=None,
    fac_status="status",
    op_name="Company",
    # commodity=None,
    liq_capacity_bpd="crude_bpd",
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
    ref_us_integrated,
    file_name="united_states_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% VENEZUELA
# =============================================================================
os.chdir(v24data)
# Read facilities data
fp_ = r"venezuela\INSTALACIONES_F_CarlosGonzales.shp"
vez_fac = read_spatial_data(fp_)

# Check and transform CRS
vez_fac = transform_CRS(vez_fac, appendLatLon=True)

# Translate commodity type
vez_fac.MAPA.replace({'PETROLERO': 'OIL',
                      'GASIFERO': 'GAS'}, inplace=True)

# Check facility types
vez_fac.TIPO.unique()
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
refinery_ = ['REFINERY']
vez_refinery = vez_fac[vez_fac.TIPO.isin(refinery_)]
print("==============================")
print("Total number of crude oil refinery in dataset = ", vez_refinery.shape[0])
vez_refinery.head()

# =============================================================================
# %% VENEZUELA - Integration  + Export
# =============================================================================
ref_VEZ, _err = integrate_facs(
    vez_refinery,
    starting_ids=1,
    category="Crude oil refinery",
    fac_alias="REFINERY",
    country="Venezuela",
    state_prov=None,
    src_ref_id="132",
    src_date="2017-01-01",
    on_offshore=None,
    fac_name="NOMBRE",
    fac_id="Id",
    fac_type="TIPO",
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity="MAPA",
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

ref_VEZ1 = replace_row_names(ref_VEZ,
                             colName="FAC_TYPE",
                             dict_names={'REFINERY': 'N/A'})


save_spatial_data(
    ref_VEZ1,
    file_name="venezuela_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% ARGENTINA
# =============================================================================
os.chdir(v24data)
fp = r"argentina/facilities/refinacin-hidrocarburos-refineras-shp.shp"
refs_ARG = read_spatial_data(fp)
refs_ARG = transform_CRS(refs_ARG, appendLatLon=True)

# TODO, not urgent - extract the information in column "HAB_RES419" into some kind of standardized "FAC_TYPE"

# =============================================================================
# %% ARGENTINA - Integration  + Export
# =============================================================================
refs_ARG_integrated, ref_ArgEr = integrate_facs(
    refs_ARG,
    starting_ids=1,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Argentina",
    state_prov="PROVINCIA",
    src_ref_id="113",
    src_date="2024-04-01",
    on_offshore=None,
    fac_name='PLANTA',
    fac_id='CUIT',
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name="EMPRESA",
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
    refs_ARG_integrated,
    file_name="argentina_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% SOUTH AMERICA: OTHER COUNTRIES
# =============================================================================
os.chdir(pubdata)
ref_intl_sa = read_spatial_data("South_America/international_refineries_oilgasinfra.com/_south_america_refineries_.shp")

# Check and transform CRS
ref2_intl_sa = transform_CRS(ref_intl_sa, appendLatLon=True)

# Country names are in the `FolderPath` attribute
list_countries_ = list(ref2_intl_sa.FolderPath)
countries_ = [list_countries_[x].split("/")[-1] for x in range(len(list_countries_))]

# Then add attribute to gdf
ref2_intl_sa['Country'] = countries_

# Fix a few country names
dict_ = {
    'Equador': 'Ecuador',
    'Surinam': 'Suriname',
    'US VIrgin Islands': 'US Virgin Islands'
}

ref3_intl_sa = replace_row_names(ref2_intl_sa,
                                 colName="Country",
                                 dict_names=dict_)

# Refinery capacity in bpd can be found in the `PopupInfo`
capacities_ = [float((ref3_intl_sa.PopupInfo[x].split(">")[3].split(" ")[0]).replace(",", "")) for x in range(len(ref3_intl_sa.PopupInfo))]

# Then add attribute to gdf
ref3_intl_sa['capacity_bpd'] = capacities_

# Flatten 3D geometries
ref4 = transform_geom_3d_2d(ref3_intl_sa)

# Fix names, leave out the starting numbers
names_ = [ref4.Name[x][5:].strip() for x in range(len(ref4.Name))]
ref4['Name2'] = names_

# Fac ID - the first two characters in 'Name'
fac_id = [ref4.Name[x][0:2].strip() for x in range(len(ref4.Name))]
ref4['fac_id'] = fac_id

# =============================================================================
# %% SOUTH AMERICA - OTHER COUNTRIES - Integration  + Export
# =============================================================================
ref5, ref_err = integrate_facs(
    ref4,
    starting_ids=1,
    category="Crude Oil Refineries",
    fac_alias="REFINERY",
    country="Country",
    state_prov=None,
    src_ref_id="22",
    src_date="2014-01-01",
    on_offshore=None,
    fac_name="Name2",
    fac_id="fac_id",
    install_date=None,
    fac_status=None,
    op_name=None,
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
    ref5,
    file_name="south_america_international_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% COLOMBIA
# =============================================================================
os.chdir(pubdata)
# Refineries data from Princeton University catalog
ref_ = read_spatial_data("South_America\\Colombia_v1.2\\refineries\\GISPORTAL_GISOWNER01_COLOMBIAREFINERY06.shp")

# Check and transform CRS
ref2 = transform_CRS(ref_, appendLatLon=True)

# =============================================================================
# %% COLOMBIA - Integration + Export
# =============================================================================
col_refs, col_ref_err = integrate_facs(
    ref2,
    starting_ids=1,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="Colombia",
    state_prov=None,
    src_ref_id="130",
    src_date="2006-01-01",
    on_offshore=None,
    fac_name="NOMBRE",
    fac_id="gml_id",
    fac_type=None,
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
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
    col_refs,
    "colombia_crude_oil_refineries",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AFRICA REFINERIES: OTHER
# TODO - incorporate this into general oginfra.com section above
# =============================================================================
os.chdir(pubdata)
# Read additional data from OILGASINFRA.com
fp = r"International_data_sets\refineries_world_OilGasInfra.com.gdb"
fp_refin = read_spatial_data(fp)
refinery1 = transform_CRS(fp_refin, target_epsg_code="epsg:4326", appendLatLon=True)

names = []
operator = []
crude = []
crude2 = []
country = []

for idx1_, row1_ in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    # Split name column and append to names and operator lists, since the name
    # column includes both of these
    name = row1_.Name
    if '-' in name:
        split = name.split(" - ")
        try:
            formatted_name = split[2]
            operator_name = split[1]
            names.append(formatted_name)
            operator.append(operator_name)
        except:
            formatted_name = split[1]
            operator_name = split[1]
            names.append(formatted_name)
            operator.append(operator_name)

    else:
        names.append(name)


def extract_capacity(thestring):
    # Any time a capacity value is mentioned in the 'Descriptio' column, it's
    # preceeded by the text `colspan="2">` and followed by the text `bpd` (a.k.a. barrels per day)
    # Use regular expressions to find and return the numeric capacity
    if 'colspan' in thestring:
        substring = re.search('colspan="2">(.*?) bpd', thestring).group(1)
        return int(re.sub('[^A-Za-z0-9]+', '', substring))
    else:
        # If the 'Descriptio' column doesn't contain capacity information, return
        # the OGIM 'no data' value
        return -999


capacities = []
for idx1, row1 in tqdm(refinery1.iterrows(), total=refinery1.shape[0]):
    cap_info = row1.PopupInfo
    try:
        cap_ = extract_capacity(cap_info)
        capacities.append(cap_)
    except:
        capacities.append(-999)

refinery1['capacity'] = capacities


refinery1['NAME1'] = names
refinery1['OPERATOR'] = operator

# %% Other African countries
algeria_r = refinery1[refinery1["FolderPath"].str.contains("Algeria")]
algeria_r['country'] = "Algeria"

angola_r = refinery1[refinery1["FolderPath"].str.contains("Angola")]
angola_r['country'] = "Angola"

egypt_r = refinery1[refinery1["FolderPath"].str.contains("Egypt")]
egypt_r["country"] = "Egypt"

eritrea_r = refinery1[refinery1["FolderPath"].str.contains("Eritrea")]
eritrea_r["country"] = "Eritrea"

israel_r = refinery1[refinery1["FolderPath"].str.contains("Israel")]
israel_r["country"] = "Israel"

jordan_r = refinery1[refinery1["FolderPath"].str.contains("Jordan")]
jordan_r["country"] = "Jordan"

kenya_r = refinery1[refinery1["FolderPath"].str.contains("Kenya")]
kenya_r["country"] = "Kenya"

morocco_r = refinery1[refinery1["FolderPath"].str.contains("Morocco")]
morocco_r["country"] = "Morocco"

sudan_r = refinery1[refinery1["FolderPath"].str.contains("Sudan")]
sudan_r["country"] = "Sudan"

syria_r = refinery1[refinery1["FolderPath"].str.contains("Syria")]
syria_r["country"] = "Syria"

tanzania_r = refinery1[refinery1["FolderPath"].str.contains("Tanzania")]
tanzania_r["country"] = "Tanzania"

tunisia_r = refinery1[refinery1["FolderPath"].str.contains("Tunisia")]
tunisia_r["country"] = "Tunisia"

# Concatenate
ref_oth = pd.concat([algeria_r,
                     angola_r,
                     egypt_r,
                     eritrea_r,
                     israel_r,
                     jordan_r,
                     kenya_r,
                     morocco_r,
                     sudan_r,
                     syria_r,
                     tanzania_r,
                     tunisia_r])

ref_oth.head()

# =============================================================================
# %% AFRICA OTHER
# =============================================================================
refineries_oth, errors = integrate_facs(
    ref_oth,
    starting_ids=0,
    category="Crude oil refineries",
    fac_alias="REFINERY",
    country="country",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # on_offshore="",
    fac_name="NAME1",
    # fac_id="IDENTIFIER",
    # fac_type="English",
    # fac_status="L_STATUS",
    op_name="OPERATOR",
    # install_date='',
    # commodity = '',
    liq_capacity_bpd="capacity",
    # liq_throughput_bpd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    refineries_oth,
    file_name="other_africa_middle_east_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=results_folder
)
