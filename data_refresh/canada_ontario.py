# -*- coding: utf-8 -*-
"""
Created on Fri Sept 1 2023

Code that will automatically download OGIM-relevant data sources in Ontario
Based almost entirely on Mark's previous code.

@author: maobrien, momara
"""
# =============================================================================
# Import libraries
# =============================================================================
import os
# from tqdm import trange
import pathlib

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import data_auto_download

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# !!! Change these parameters before running script
DATA_REFRESH = True
DATE_DOWNLOAD = "2024-11-05"
region = 'ontario'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', 'canada', region)
os.chdir(final_path)


# =============================================================================
# %% Wells - DOWNLOAD MANUALLY FROM NOW ON
# Ontario now publishes wells (updated daily) on their ArcGIS Online page:
# https://geohub.lio.gov.on.ca/datasets/4f14040065764a76b0a26fb4452a626d_22/explore
# =============================================================================
# url = "https://www.gisapplication.lrc.gov.on.ca/fmedatadownload/Packages/fgdb/PETWELL.zip"
# category = "wells"
# # Create subfolder to contain this infra category, if it doesn't already exist
# pathlib.Path(category).mkdir(exist_ok=True)

# if DATA_REFRESH:
#     data_auto_download(
#         url,
#         region=region,
#         category=category,
#         createFolder=False,
#         export_path=category
#     )

# =============================================================================
# %% Historical oil and gas fields
# =============================================================================
url = "https://www.gisapplication.lrc.gov.on.ca/fmedatadownload/Packages/fgdb/PETHOILF.zip"
category = "oil_gas_fields"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )


# =============================================================================
# %% Historical oil and gas tank data
# A tank is an above ground container that holds either petroleum or water.
# !!! This data is no longer being updated. It is best suited for historical
# research and analysis.
# Maintenance and Update Frequency
# Not planned: there are no plans to update the data
# =============================================================================
url = "https://ws.gisetl.lrc.gov.on.ca/fmedatadownload/Packages/TANK.zip"
category = "tanks_historical_data"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# Download data description / metadata
url2 = "https://www.publicdocs.mnr.gov.on.ca/mirb/Tank%20-%20Data%20Description.pdf"
category = "tanks_historical_data"

if DATA_REFRESH:
    data_auto_download(
        url2,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="ON_Tanks_MetaData_.pdf"
    )
