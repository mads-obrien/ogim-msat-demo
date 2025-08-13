# -*- coding: utf-8 -*-
"""
Created on Thu Sep 19 11:59:11 2024

@author: maobrien
"""

import os
# import re
import pandas as pd
import geopandas as gpd
import numpy as np
import glob
from tqdm import tqdm
import matplotlib.pyplot as plt
import datetime

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import (integrate_production, replace_row_names,
                     save_spatial_data, schema_OIL_GAS_PROD, read_msAccess,
                     clean_a_date_field, create_concatenated_well_name)
from internal_review_protocol_Excel import create_internal_review_spreadsheet

# ======================================================
# %% Set file paths
# ======================================================
# Set current working directory to the Public_Production folder
os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Production_v0\data')
# Set destination folder for exported SHP and JSON outputs
# make sure to end the string with double backslash!
results_folder = "C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory\\Public_Production_v0\\integrated_results\\"

# ======================================================
# %% Define custom functions
# ======================================================


def populate_before_after_table_post_integration(i, df, gdf_integrated):

    # populate the columns related to OIL and GAS
    for h, col in zip(['oil', 'gas'], ['OIL_BBL', 'GAS_MCF']):

        # Record the total hydrocarbon produced in 2022 AFTER integration
        df.at[i, f'{h}_geojson'] = gdf_integrated[col].sum()
        # Calculate the percentage
        x = (df.loc[i, f'{h}_geojson'] / df.loc[i, f'{h}_original'])
        x_as_pct = "{:.4%}".format(x)
        df.at[i, f'{h}_pct_in_geojson'] = x_as_pct


# =============================================================================
# %% Create empty "before and after integration comparison" table
# =============================================================================
before_after_table = pd.DataFrame(index=['MEXICO'],
                                  columns=['oil_original',  # Sum of all 2022 production values from raw data
                                           'oil_aggregated',  # Sum of all 2022 prod. values after aggregating months together
                                           'oil_geojson',  # Sum of all 2022 prod. values in the integrated geojson result that gets exported
                                           'oil_pct_in_geojson',  # What percent of the original production volume is still reported in the final geojson
                                           'gas_original',
                                           'gas_aggregated',
                                           'gas_geojson',
                                           'gas_pct_in_geojson',
                                           'units_reporting_production_original',  # Count of how many unique APIs, Leases, etc. report production in the original dataset before any cleaning
                                           'units_reporting_production_geojson'  # Count of how many unique APIs, Leases report production in the integrated dataset
                                           ])

# ======================================================
# %% MEXICO [2022] - Read + aggregate production
# ======================================================
fp = r"mexico\POZOS_COMPILADO.csv"
mx_prod = pd.read_csv(fp, encoding='latin-1', skiprows=10)
# Create date field, and filter to year of interest
mx_prod['datetime'] = pd.to_datetime(mx_prod['Fecha'], format='%d-%m-%Y', errors='coerce')
mx_prod_2022 = mx_prod[mx_prod.datetime.dt.year == 2022].reset_index(drop=True)

# =============================================================================
# %% MEXICO - Read wells
# =============================================================================
# TODO

# =============================================================================
# %% MEXICO - Merge and clean
# =============================================================================
# TODO

# =============================================================================
# %% MEXICO - Integration
# =============================================================================
mx_integrated, mx_errors = integrate_production(
    mx_prod_merge,
    src_date="2024-07-10",  # updated on daily basis
    category="OIL AND NATURAL GAS PRODUCTION",
    src_ref_id="188",
    fac_alias="OIL_GAS_PROD",
    # country="United States of America",
    # state_prov="California",
    # fac_name="FAC_NAME",
    # fac_id="FAC_ID",
    # fac_type="FAC_TYPE",
    # spud_date='SPUD_DATE',
    # comp_date=None,
    # drill_type='DRILL_TYPE',
    # fac_status='FAC_STATUS',
    # op_name="OPERATOR",
    # oil_bbl="OilorCondensateProduced",
    # gas_mcf="GasProduced",
    # water_bbl="WaterProduced",
    # condensate_bbl=None,
    # prod_days="DaysProducing",
    # prod_year="prod_year",
    # entity_type="entity_type",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('MEXICO',
                                             before_after_table,
                                             mx_integrated)

save_spatial_data(
    mx_integrated,
    "mexico_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)
