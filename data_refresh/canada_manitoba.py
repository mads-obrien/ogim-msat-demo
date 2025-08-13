# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 16:55:58 2023

Code that will automatically download OGIM-relevant data sources IN MANITOBA.
Based almost entirely on Mark's previous code:
    `OGIM_Data_Integration_Canada_Manitoba-v1.2.ipynb`

@author: maobrien, momara
"""

# =============================================================================
# Import libraries
# =============================================================================
import os
from tqdm import trange
import pathlib

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import data_auto_download

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# !!! Change these parameters before running script
DATA_REFRESH = True
DATE_DOWNLOAD = "2024-11-05"
region = 'manitoba'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', 'canada', region)
os.chdir(final_path)

# =============================================================================
# %% WELLS
# =============================================================================
url = "https://www.gov.mb.ca/iem/petroleum/gis/wells.zip"
category = "wells"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category)


# =============================================================================
# %% CROWN LANDS
# =============================================================================
url = "https://www.gov.mb.ca/iem/petroleum/gis/crown.zip"
category = "crown_lands"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category)


# =============================================================================
# %% UNIT BOUNDARIES
# =============================================================================
url = "https://www.gov.mb.ca/iem/petroleum/gis/units.zip"
category = "unit_boundaries"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category)


# =============================================================================
# %% OIL FIELD BOUNDARIES
# =============================================================================
url = "https://www.gov.mb.ca/iem/petroleum/gis/Oilfield_boundaries.zip"
category = "oil_field_boundaries"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category)


# =============================================================================
# %% OIL POOL LAYERS
# =============================================================================
url = "https://www.gov.mb.ca/iem/petroleum/gis/pools.zip"
category = "oil_pool_layers"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category)


# =============================================================================
# %% UWI (unique well identifier) KEY LIST
# =============================================================================
url = "https://www.manitoba.ca/iem/petroleum/reports/uwi_weekly.xlsx"
category = "uwi_key_list"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category,
                       fileName="MB_UWI_Key_List.xlsx")

# =============================================================================
# %% WELL STATUS TYPES (PDF)
# =============================================================================
url = "https://www.manitoba.ca/iem/petroleum/reports/well_status.pdf"
category = "well_status_types"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category,
                       fileName="MB_Well_Status_Types.pdf")

# =============================================================================
# %% COMPANY CODES
# =============================================================================
url = "https://www.manitoba.ca/iem/petroleum/reports/company_codes.xlsx"
category = "company_codes"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category,
                       fileName="MB_Company_Codes.xlsx")


# =============================================================================
# %% BATTERY LISTING
# =============================================================================
url = "https://www.manitoba.ca/iem/petroleum/reports/battery_listing.xlsx"
category = "battery_listing"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category,
                       fileName="MB_Battery_Listing.xlsx")


# =============================================================================
# %% PRODUCTION DATA
# Download 2021-2022 production data
# All production reports are .zip files in both Pdf or excel format
# Note: Volumes for fluid type “GAS” and “GSD” are in 1000 m3. For all other fluid types volumes are in m3
# =============================================================================
url = "https://www.manitoba.ca/iem/petroleum/reports/wmp_2021_2022_excel.zip"
category = "production_data"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category,
                       fileName=None)


# =============================================================================
# %% Manitoba southern quarter grid sections
# =============================================================================
#  - Geospatial data for Southern Quarter Sections by Municipality / LGD
#
#  - All files available here: https://mli2.gov.mb.ca/quarter_sec/index_south.html
#  - Description of grid system: https://www.thinktrees.org/wp-content/uploads/2019/03/Land-Survey-Systems.pdf

url = "https://mli2.gov.mb.ca/quarter_sec/shp_zip_files/municipality_all_shp.zip"
category = "manitoba_quarter_sections"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(url,
                       region=region,
                       category=category,
                       createFolder=False,
                       export_path=category)

# TODO - add the code that Mark used to turn the quarter grid sections into the
# LSD grids shapefile that is used during data integration.

# =============================================================================
# %% Manitoba petrinex data
# =============================================================================
# Updated daily
#  - Activity codes report
#  - Bussiness associate
#  - Facilities identifiers report
#  - Facility type and subtype codes report
#  - Horizon pool codes report
#  - Product codes report
# [LinkToData](https://www.petrinex.ca/PD/Pages/MBPD.aspx)

category = "petrinex_data"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

# URLs for .CSV files for 2021 [January to December]
urls = [
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAActivityCodes.htm",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRABAIdentifiers.csv",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAFacilityIds.csv",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAFacilityCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAFieldandPoolCodes.CSV",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAFieldCodes.CSV",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAFormationCodes.htm",
    "https://www.petrinex.gov.ab.ca/bbreportsMB/PRAProductCodes.CSV"
]

data_names = [
    "mb_activity_codes",
    "mb_business_assoc",
    "mb_facilities_ident",
    "mb_facilities_codes",
    "mb_field_pool_codes",
    "mb_field_codes",
    "mb_formation_codes",
    "mb_product_codes"
]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading MB petrinex data::"):
    # Download
    if DATA_REFRESH:
        if idx_ == 0 or idx_ == 6:
            data_auto_download(urls[idx_],
                               region=region,
                               category=category,
                               createFolder=False,
                               export_path=category,
                               fileName="MB_PetrinexData-" + data_names[idx_] + ".htm")

        else:
            data_auto_download(urls[idx_],
                               region=region,
                               category=category,
                               createFolder=False,
                               export_path=category,
                               fileName="MB_PetrinexData-" + data_names[idx_] + ".csv")
