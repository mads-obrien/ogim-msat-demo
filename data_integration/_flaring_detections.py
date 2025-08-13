# -*- coding: utf-8 -*-
"""
Created on January 16, 2024

Data integration of global natural gas flaring detections.done

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import integrate_flares, save_spatial_data
from assign_countries_to_feature_2 import assign_stateprov_to_feature

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# !!! Record what year of flares you're using here, so that you don't have to
# manually revise all the column names in the code below
yearstring = '2023'

# =============================================================================
# %% Read and pre-process the original XLSX document
# =============================================================================
# 2023 FLARES
if yearstring == '2023':
    fp = r'international\VIIRS_Global_flaring_d.7_slope_0.029353_2023_v20230614_web_IDmatch.xlsx'

# 2022 FLARES
if yearstring == '2022':
    fp = r"international\VIIRS_Global_flaring_d.7_slope_0.029353_2022_v20230526_web.xlsx"

# 2021 FLARES
if yearstring == '2021':
    fp = r"international\VIIRS_Global_flaring_d.7_slope_0.029353_2021_web.xlsx"


upstream = pd.read_excel(fp, sheet_name='flare upstream', header=0)
upstream['segment'] = 'UPSTREAM'
oil_downstream = pd.read_excel(fp, sheet_name='flare oil downstream', header=0)
oil_downstream['segment'] = 'OIL DOWNSTREAM'
gas_downstream = pd.read_excel(fp, sheet_name='flare gas downstream', header=0)
gas_downstream['segment'] = 'GAS DOWNSTREAM'
flares_all = pd.concat([upstream, oil_downstream, gas_downstream]).reset_index(drop=True)
flares = gpd.GeoDataFrame(flares_all,
                          geometry=gpd.points_from_xy(flares_all.Longitude,
                                                      flares_all.Latitude),
                          crs=4326)

print(*flares.columns, sep='\n')


# !!! For OGIM North America editions, include only detections in USA, CAN, and MEX
# -----------------------------------------------------------------------------
# countries = ['United States', 'Canada', 'Mexico']
# flares_na = flares[flares.Country.isin(countries)]
# print("Total flared volumes globally = ", flares['BCM 2022'].sum())
# print("Total flared volumes in North America = ", flares_na['BCM 2022'].sum())


# Calculate flare volumes in MMCF
bcm_column = f'BCM {yearstring}'
flares['flared_mmcf'] = flares[bcm_column] * 35.314666721 * 1000

flares['temp_k'] = flares['Avg temp., K']  # in versions before 2023 it's `Avg. temp., K`
flares['temp_k'] = pd.to_numeric(flares['temp_k'])

# Flare facility type
flares.Type.unique()
flares.Type.replace({'lng': 'LNG FACILITY',
                     'gpp': 'GAS PROCESSING PLANT',
                     'opp': 'OIL PROCESSING PLANT'}, inplace=True)


# Assign a STATE_PROV value to each flare
# -----------------------------------------------------------------------------
id_column = f'ID {yearstring}'
flares['STATE_PROV'] = 'N/A'
path_to_boundary_geoms = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\NaturalEarth\ne_10m_admin_1_states_provinces.shp'
state_geoms = gpd.read_file(path_to_boundary_geoms)
flares = assign_stateprov_to_feature(flares,
                                     gdf_stateprov_colname='STATE_PROV',
                                     gdf_uniqueid_field=id_column,
                                     boundary_geoms=state_geoms,
                                     overwrite_stateprov_field=True)


flares['flare_year'] = yearstring

# =============================================================================
# %% Integrate flares
# =============================================================================
flares_integrated, _ = integrate_flares(
    flares,
    starting_ids=1,
    category="NATURAL GAS FLARING DETECTIONS",
    fac_alias='FLARING',
    country="Country",
    state_prov='STATE_PROV',
    src_ref_id="142",
    src_date="2024-09-19",  # !!! Change the src date in the data catalog
    # on_offshore=None,
    # fac_name=None,
    fac_id=id_column,
    fac_type="Type",
    # fac_status=None,
    # op_name=None,
    gas_flared_mmcf="flared_mmcf",
    avg_temp="temp_k",
    days_clear_observs="Clear Obs.",
    flare_year="flare_year",
    segment_type="segment",
    fac_latitude="Latitude",
    fac_longitude="Longitude"
)

# Save results
save_spatial_data(
    flares_integrated,
    "natural_gas_flaring_detections",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)
