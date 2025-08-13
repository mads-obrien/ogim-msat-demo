# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global LNG FACILITIES.

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
from tqdm import tqdm
import numpy as np
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (transform_CRS, integrate_facs, save_spatial_data,
                     schema_LNG_STORAGE, create_concatenated_well_name)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
results_folder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% GLOBAL ENERGY MONITOR
# =============================================================================
os.chdir(v24data)
fp_gem = r'international\GEM-GGIT-LNG-Terminals-2024-09.xlsx'
gem = pd.read_excel(fp_gem, sheet_name='LNG Terminals')

# Remove records with no geospatial information, then make df into gdf
gem = gem[~gem.Latitude.isin(['TBD', 'Unknown'])].reset_index(drop=True)
gem = gpd.GeoDataFrame(gem,
                       geometry=gpd.points_from_xy(gem.Longitude,
                                                   gem.Latitude), crs=4326)
gem = transform_CRS(gem, appendLatLon=True)

# Drop some records by status; GEM defines their status meanings here:
# https://www.gem.wiki/Global_Fossil_Infrastructure_Tracker_Methodology#Data_Dictionary
# Only keep records where we know the project has physically been built
status2keep = ['Operating', 'Construction', 'Retired', 'Mothballed', 'Idle']
gem = gem[gem.Status.isin(status2keep)].reset_index(drop=True)

# Create a facility name column that combines 'TerminalName' and 'UnitName',
# if UnitName is available (since UnitName specifies which train at a terminal
# is being referred to, for example)
gem['fac_name_new'] = gem.TerminalName
gem.loc[gem.UnitName.notna(), 'fac_name_new'] = gem.TerminalName + ' - ' + gem.UnitName

# Create my own "install date" column, which uses the provided StartYear UNLESS
# the project is under construction & the start date is in the future -- in that
# case, the install date should be null.
gem['installdate'] = gem.StartYear1
gem.loc[((gem.Status == 'Construction') & (gem.StartYear1 > 2024)), 'installdate'] = np.nan
gem['installdate'] = pd.to_datetime(gem['installdate'], format='%Y').dt.strftime("%Y-%m-%d")

gem['State/Province'] = gem['State/Province'].fillna('N/A')

# Convert MTPA to MMCFD
capacities_mmcfd = []

for idx1_, row1_ in tqdm(gem.iterrows(), total=gem.shape[0]):
    capacity = row1_.Capacity
    units = row1_.CapacityUnits

    if pd.isna(units):
        null_values = "-999"
        capacities_mmcfd.append(null_values)

    # convert bcm/y to mmcfd
    elif units == "bcm/y":
        bcm_conversion = capacity * 35.3147 * 1000 / 365
        capacities_mmcfd.append(bcm_conversion)

    # convert bcf/d to mmcfd
    elif units == 'bcf/d':
        bcfd_conversion = capacity * 1000
        capacities_mmcfd.append(bcfd_conversion)

    # convert mtpa to mmcfd (including units with a typo in it)
    elif units in ['mtpa', 'mpta']:
        mtpa_conversion = capacity * 48.028 * 1000 / 365
        capacities_mmcfd.append(mtpa_conversion)

    elif units in ['gal/day', 'TJ/d', 'MWh/d']:
        mtpa_value = row1_.CapacityInMtpa
        mtpa_conversion = mtpa_value * 48.028 * 1000 / 365
        capacities_mmcfd.append(mtpa_conversion)

gem['GAS_CAPACITY_MMCFD'] = capacities_mmcfd


# DROP COUNTRIES FOR WHICH WE HAVE A COUNTRY-SPECIFIC SOURCE -- skip as of March '24
# countries2drop = ['United States',
#                   'Argentina',
#                   'Brazil']
# gem = gem[-gem.Country.isin(countries2drop)].reset_index()

# =============================================================================
# %% GLOBAL ENERGY MONITOR - Integration + Export
# =============================================================================
gem_integrated, errors = integrate_facs(
    gem,
    starting_ids=0,
    category="LNG Facilities",
    fac_alias="LNG_STORAGE",
    country="Country",
    state_prov='State/Province',
    src_ref_id="239",
    src_date="2024-09-01",
    # on_offshore="ONSHORE",
    fac_name="TerminalName",
    fac_id="ComboID",
    fac_type="FacilityType",
    fac_status="Status",
    op_name="Owner",
    install_date='installdate',
    # commodity = '',
    # liq_capacity_bpd = "",
    # liq_throughput_bpd="",
    gas_capacity_mmcfd='GAS_CAPACITY_MMCFD',
    # gas_throughput_mmcfd="",
    # num_storage_tanks="",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    gem_integrated,
    file_name="gem_lng_facilities",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% UNITED STATES - REMOVE, use GEM instead
# =============================================================================
# os.chdir(v24data)
# ln_01 = read_spatial_data(r"united_states\national\HIFLD\Liquified_Natural_Gas_Import_Exports_and_Terminals.geojson")
# # Also, based on EIA data, the units here are bcfd for storage, storage capacity, current capacity

# lng_us = ln_01.query("COUNTRY=='USA'")

# lng_us2 = replace_row_names(lng_us, "STATE", dict_names=dict_us_states)

# # Fix dates
# lng_us3 = standardize_dates_hifld_us(lng_us2, attrName="SOURCEDATE", newAttrName="src_dates")

# # Check and transform CRS if needed
# lng_us4 = transform_CRS(lng_us3, appendLatLon=True)

# # Replace zeros with -999
# lng_us4['STORCAP'] = lng_us4['STORCAP'].replace({0: -999, "0": -999})
# lng_us4['CURRENTCAP'] = lng_us4['CURRENTCAP'].replace({0: -999, "0": -999})

# # Convert (non-null) capacities [bcfd] to MMcfd
# lng_us4['capacity_mmcfd'] = lng_us4['STORCAP']
# lng_us4['thru_mmcfd'] = lng_us4['CURRENTCAP']
# lng_us4.loc[lng_us4['capacity_mmcfd'] != -999, 'capacity_mmcfd'] = lng_us4['capacity_mmcfd'] * 1000
# lng_us4.loc[lng_us4['thru_mmcfd'] != -999, 'thru_mmcfd'] = lng_us4['thru_mmcfd'] * 1000

# =============================================================================
# UNITED STATES - Integration + Export
# =============================================================================
# lng_us7, lng_errors_3 = integrate_facs(
#     lng_us4,
#     starting_ids=1,
#     category="LNG FACILITY",
#     fac_alias="LNG_STORAGE",
#     country="United States",
#     state_prov="STATE",
#     src_ref_id="92",
#     src_date="src_dates",
#     on_offshore="Onshore",
#     fac_name="NAME",
#     fac_id="TERMID",
#     fac_type="TYPE",
#     install_date=None,
#     fac_status=None,
#     op_name="OWNER",
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd="capacity_mmcfd",
#     gas_throughput_mmcfd="thru_mmcfd",
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     lng_us7,
#     "united_states_lng_facility",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder
# )

# =============================================================================
# %% SOUTH AMERICA (oginfra.com) - REMOVE, use GEM instead
# =============================================================================
# os.chdir(pubdata)
# lng_ = read_spatial_data("South_America/international_LNG_oilgasinfra.com/_LNG_south_america_other_.shp")

# # Check and transform CRS
# lng2 = transform_CRS(lng_, appendLatLon=True)

# # Check and transform CRS
# lng2 = transform_CRS(lng_, appendLatLon=True)

# # Flatten 3D geometries
# lng4 = transform_geom_3d_2d(lng2)

# =============================================================================
# SOUTH AMERICA (oginfra.com) - Integration + Export
# =============================================================================
# lng5, lng_err = integrate_facs(
#     lng4,
#     starting_ids=1,
#     category="LNG FACILITIES",
#     fac_alias="LNG_STORAGE",
#     country="country",
#     state_prov=None,
#     src_ref_id="22",
#     src_date="2014-01-01",
#     on_offshore=None,
#     fac_name="Name",
#     fac_id=None,
#     fac_type=None,
#     install_date=None,
#     fac_status=None,
#     op_name=None,
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd=None,
#     gas_throughput_mmcfd=None,
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     lng5,
#     "south_america_other_LNG",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder
# )

# =============================================================================
# %% ARGENTINA - REMOVE, use GEM instead
# =============================================================================
# os.chdir(v24data)
# fp = r"argentina\facilities\instalaciones-hidrocarburos-puertos-regasificadores-de-gnl-shp.shp"
# arg_lng = read_spatial_data(fp)
# arg_lng = transform_CRS(arg_lng, appendLatLon=True)
# arg_lng['factype'] = 'LNG regasification facility'


# arg_lng_integrated, lng_ArgEr = integrate_facs(
#     arg_lng,
#     starting_ids=1,
#     category="LNG",
#     fac_alias="LNG_STORAGE",
#     country="Argentina",
#     state_prov="PROVINCIA",
#     src_ref_id="114",
#     src_date="2023-01-17",
#     on_offshore=None,
#     fac_name='NOMBRE_PUE',
#     fac_id=None,
#     fac_type='factype',
#     spud_date=None,
#     comp_date=None,
#     drill_type=None,
#     install_date=None,
#     fac_status=None,
#     op_name=None,
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd=None,
#     gas_throughput_mmcfd=None,
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     arg_lng_integrated,
#     "argentina_lng",
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% BRAZIL - REMOVE, use GEM instead
# =============================================================================
# os.chdir(pubdata)
# fp1_ = "South_America\\Brazil_v1.2\\facilities\\Shape_SIM_COI_GNL_Indicadores.shp"
# lng = read_spatial_data(fp1_)

# # Check and transform CRS
# lng2 = transform_CRS(lng, appendLatLon=True)

# lng4 = replace_row_names(lng2, colName="Situacao_A", dict_names={'Construcao': 'Construction', 'Operação': 'Operational', 'Pre-Operacao': 'Pre-Operational', 'Operacao': 'Operational'})

# lng5, lng5_err = integrate_facs(
#     lng4,
#     starting_ids=1,
#     category="LNG",
#     fac_alias="LNG_STORAGE",
#     country="Brazil",
#     state_prov=None,
#     src_ref_id="126",
#     src_date="2022-05-15",
#     on_offshore=None,
#     fac_name="Nome_Insta",
#     fac_id=None,
#     fac_type=None,
#     install_date=None,
#     fac_status="Situacao_A",
#     op_name=None,
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd=None,
#     gas_throughput_mmcfd=None,
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     lng5,
#     "brazil_lng",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% AFRICA (USGS) - REMOVE, use GEM
# =============================================================================
# os.chdir(pubdata)
# fp = r"Africa\Continent\Africa_GIS.gdb\AFR_Infra_OG_LNG_Terminals.shp"
# data = read_spatial_data(fp, table_gradient=True)
# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)


# # Additional information in DsgAttr04 mostly isn't necessary, but designation of
# # floating (offshore) LNG facilities does seem important
# data['factype_new'] = data.DsgAttr01
# data.loc[data.DsgAttr04 == 'Floating (FSRU)', 'factype_new'] = data.DsgAttr01 + ' - ' + data.DsgAttr04

# data['onshore'] = 'Onshore'
# data.loc[data.DsgAttr04 == 'Floating (FSRU)', 'onshore'] = 'Offshore'  # floating units

# # add placeholder month and date to installation date, so that it's the proper format
# data['date_new'] = data['DsgAttr02'].astype(str) + '-01-01'

# # DsgAttr03 is "Installed capacity for liquefaction or regasification of
# # natural gas at the facility, in million metric tons per year (MTPA)."

# # First, divide the annual capacity into daily capacity
# data['capacity_MTPD'] = data['DsgAttr03'] / 365  # unit is still million metric tons


# def convert_million_tons_lng_to_mmcf_natgas(value):
#     '''
#     From 1 million tonnes LNG to billion cubic feet NG, multiply by 48.028
#     https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2022-approximate-conversion-factors.pdf

#     '''
#     return (value * 48.028) * 1000  # Multiply by 1000 because 1 BCF = 1000 MMCF


# data['capacity_MMCFD'] = data['capacity_MTPD'].apply(lambda x: convert_million_tons_lng_to_mmcf_natgas(x))

# # =============================================================================
# # %% AFRICA (USGS) - Integration + Export
# # =============================================================================
# data_, errors = integrate_facs(
#     data,
#     starting_ids=0,
#     category='LNG facilities',
#     fac_alias='LNG_STORAGE',
#     country='Country',
#     # state_prov = None,
#     src_ref_id='158',
#     src_date='2021-08-01',
#     on_offshore='onshore',
#     fac_name='FeatureNam',
#     fac_id='FeatureUID',
#     fac_type='factype_new',
#     # spud_date = None,
#     # comp_date = None,
#     # drill_type = None,
#     install_date='date_new',
#     fac_status='LocOpStat',
#     op_name='OwnerName',
#     commodity='Result',
#     # liq_capacity_bpd = None,
#     # liq_throughput_bpd = None,
#     gas_capacity_mmcfd='capacity_MMCFD',
#     # gas_throughput_mmcfd = None,
#     # num_compr_units = None,
#     # num_storage_tanks = None,
#     # site_hp = None,
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )


# save_spatial_data(
#     data_,
#     file_name="africa_lng",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% AUSTRALIA (oginfra.com) - REMOVE
# dupes of GEM
# =============================================================================
# os.chdir(pubdata)
# fp1 = "Australia+NewZealand\Australia\LNG_onshore_Australia.kml.shp"
# lng = read_spatial_data(fp1, table_gradient=True)
# lng1 = transform_CRS(lng, target_epsg_code="epsg:4326", appendLatLon=True)

# # =============================================================================
# # %% AUSTRALIA (oginfra.com) - Integration + Export
# # =============================================================================
# lng_storage, errors = integrate_facs(
#     lng1,
#     starting_ids=0,
#     category="LNG Facilities",
#     fac_alias="LNG_STORAGE",
#     country="Australia",
#     # state_prov="",
#     src_ref_id="22",
#     src_date="2011-01-01",
#     on_offshore="ONSHORE",
#     fac_name="Name",
#     # fac_id="",
#     # fac_type="",
#     # fac_status="",
#     # operator="",
#     # install_date='',
#     # commodity = '',
#     # liq_capacity_bpd = "",
#     # liq_throughput_bpd="",
#     # gas_capacity_mmcfd="",
#     # gas_throughput_mmcfd="",
#     # num_storage_tanks="",
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     lng_storage,
#     file_name="australia_lng_facilities_oginfra",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder)


# =============================================================================
# %% INDIA (oginfra.com) - REMOVE
# all dupes of GEM
# =============================================================================
# os.chdir(pubdata)
# fp2 = "China+SE_Asia\\India\\LNG_India.kml.shp"
# lng = read_spatial_data(fp2, table_gradient=True)
# lng1 = transform_CRS(lng, target_epsg_code="epsg:4326", appendLatLon=True)

# # =============================================================================
# # %% INDIA (oginfra.com) - Integration + Export
# # =============================================================================
# lng_storage, errors = integrate_facs(
#     lng1,
#     starting_ids=0,
#     category="LNG Facilities",
#     fac_alias="LNG_STORAGE",
#     country="India",
#     # state_prov="",
#     src_ref_id="22",
#     src_date="2013-01-01",
#     on_offshore="ONSHORE",
#     fac_name="Name",
#     # fac_id="",
#     # fac_type="",
#     # fac_status="",
#     # operator="",
#     # install_date='',
#     # commodity = '',
#     # liq_capacity_bpd = "",
#     # liq_throughput_bpd="",
#     # gas_capacity_mmcfd="",
#     # gas_throughput_mmcfd="",
#     # num_storage_tanks="",
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     lng_storage,
#     file_name="india_lng_facilities",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder)


# =============================================================================
# %% LIBYA (ArcGIS Online) -- REMOVE
# provides only 3 LNG facs, one of which is a dupe and 2 others which I can't corroborate in GEM
# =============================================================================
# os.chdir(pubdata)
# fp = r"Africa\Libya\Libya_AGO_kmitchell\Libya_Infrastructure_LNG_Liquefaction.shp"
# data = read_spatial_data(fp, table_gradient=True)
# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# # 'Questionable Proj' values in 'status' column don't make sense; change only these values to None
# data.loc[data.status == 'Questionable Proj', 'status'] = None

# # Fix "online year" field to match our desired date format
# data['on_line_ye'] = data['on_line_ye'].astype('str') + '-01-01'

# # # According to the field names in the webmap, the FULL variable names are as follows:
# # [shapefile var. name] = [full var. name]
# # storage_op = STORAGE_OPER_CAP_LIQ_MMTONS
# # storage__1 = STORAGE_OPER_CAP_LIQ_MBBL
# # storage__2 = STORAGE_OPER_CAP_LIQ_MSCM
# # storage__3 = STORAGE_OPER_CAP_GAS_MMSCM
# # storage__4 = STORAGE_OPER_CAP_GAS_MMSCF
# # storage__5 = STORAGE_OPER_CAP_GAS_BSCM
# # storage_ot = STORAGE_OTHERS_CAP_LIQ_MMTONS
# # storage__6 = STORAGE_OTHERS_CAP_LIQ_MBBL
# # storage__7 = STORAGE_OTHERS_CAP_LIQ_MSCM
# # storage__8 = STORAGE_OTHERS_CAP_GAS_MMSCM
# # storage__9 = STORAGE_OTHERS_CAP_GAS_MMSCF
# # storage_10 = STORAGE_OTHERS_CAP_GAS_BSCM
# # capacity_p = CAPACITY_PLANNED_MMTY
# # capacity_u = CAPACITY_UNDER_CONST_MMTY

# # Since this data source only seems to describe storage volume, and not a throughput,
# # I'm leaving the capacity field N/A

# # =============================================================================
# # %% LIBYA (ArcGIS Online) - Integration + Export
# # =============================================================================
# data_, errors = integrate_facs(
#     data,
#     starting_ids=0,
#     category='LNG facilities',
#     fac_alias='LNG_STORAGE',
#     country='Libya',
#     # state_prov = None,
#     src_ref_id='166',
#     src_date='2017-06-01',
#     # on_offshore = None,
#     fac_name='plant_name',
#     # fac_id = None,
#     # fac_type = None,
#     # spud_date = None,
#     # comp_date = None,
#     # drill_type = None,
#     install_date='on_line_ye',  # This field is called "ON_LINE_YEAR" in the AGO webmap, so I'm assuming this is install year
#     fac_status='status',
#     op_name='operator_n',
#     # commodity = None,
#     # liq_capacity_bpd = None,
#     # liq_throughput_bpd = None,
#     # gas_capacity_mmcfd = None,
#     # # gas_throughput_mmcfd = None,
#     # num_compr_units = None,
#     # num_storage_tanks = None,
#     # site_hp = None,
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )


# save_spatial_data(
#     data_,
#     file_name="libya_lng",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder)


# =============================================================================
# %% AFRICA (oginfra.com) - REMOVE
# all dupes of GEM
# =============================================================================
# os.chdir(pubdata)
# LIB_lng = gpd.read_file("Africa/Libya/LNG_Onshore_Libya.kml.shp")
# NIG_lng = gpd.read_file("Africa/Nigeria/LNG_Nigeria.kml.shp")

# LIB_lng['country'] = "Libya"
# NIG_lng['country'] = "Nigeria"

# data = pd.concat([LIB_lng, NIG_lng]).reset_index(drop=True)

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# # =============================================================================
# # %% AFRICA (oginfra.com) - Integration + Export
# # =============================================================================
# data_, errors = integrate_facs(
#     data,
#     starting_ids=0,
#     category='LNG facilities',
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
#     data_,
#     file_name="oginfra_africa_lng",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% MIDDLE EAST (oginfra.com) - REMOVE
# all duplicates of GEM
# =============================================================================
# os.chdir(pubdata)

# oman = gpd.read_file(r'Middle_East+Caspian\Oman\Oil_Gas_Infra_.com\Facilities\LNG_Oman.kml.shp')
# qatar = gpd.read_file(r'Middle_East+Caspian\Qatar\Oil_Gas_Infra_.com\Facilities\LNG_Qatar.kml.shp')
# uae = gpd.read_file(r'Middle_East+Caspian\UAE\Oil_Gas_Infra_.com\Facilities\LNG_Offshore_UAE.kml.shp')
# yemen = gpd.read_file(r'Middle_East+Caspian\Yemen\Oil_Gas_Infra_.com\Facilities\LNG_Yemen.kml.shp')


# # Add country name
# oman['country'] = 'Oman'
# qatar['country'] = 'Qatar'
# uae['country'] = 'UAE'
# yemen['country'] = 'Yemen'

# # Concatenate all countries into one LNG gdf
# all_dfs_final = [oman, qatar, uae, yemen]
# data = pd.concat(all_dfs_final)
# data = data.reset_index(drop=True)

# # Indicate that UAE is 'offshore' since that's stated in the filename / original website
# # assume others are 'onshore'
# data['onoff'] = 'Onshore'
# data.loc[data.country == 'UAE', 'onoff'] = 'Offshore'

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# # =============================================================================
# # %% MIDDLE EAST (oginfra.com) - Integration + Export
# # =============================================================================
# data_, errors = integrate_facs(
#     data,
#     starting_ids=0,
#     category='LNG facilities',
#     fac_alias='LNG_STORAGE',
#     country='country',
#     # state_prov = None,
#     src_ref_id='22',
#     src_date='2014-01-01',
#     on_offshore='onoff',
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
#     data_,
#     file_name="middleeast_lng_oginfra",
#     schema_def=True,
#     schema=schema_LNG_STORAGE,
#     file_type="GeoJSON",
#     out_path=results_folder
# )
