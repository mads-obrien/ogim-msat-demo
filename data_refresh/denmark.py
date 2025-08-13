# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 16:57:32 2024

@author: maobrien
"""
# =============================================================================
# Import libraries
# =============================================================================
import os
import pathlib
# from tqdm import trange

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import data_auto_download

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# !!! Change these parameters before running script
DATA_REFRESH = True
DATE_DOWNLOAD = "2024-11-05"
region = 'denmark'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', region)
os.chdir(final_path)


# =============================================================================
# %% Wells
# =============================================================================
url = 'https://ens.dk/sites/ens.dk/files/OlieGas/deep_wells_oil_gas.xlsx'
url2 = 'https://ens.dk/sites/ens.dk/files/OlieGas/expappwells.xlsx'
category = 'wells'
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="deep_wells_oil_gas.xlsx"
    )

if DATA_REFRESH:
    data_auto_download(
        url2,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="expappwells.xlsx"
    )

# =============================================================================
# %% Fields
# =============================================================================
url = 'https://ens.dk/sites/ens.dk/files/OlieGas/fielddelineations_2024_13_02.zip'
category = 'fields'
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
# %% Blocks
# =============================================================================
url = 'https://ens.dk/sites/ens.dk/files/OlieGas/blocks.zip'
category = 'fields'

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Offshore installations
# =============================================================================
url = 'https://ens.dk/sites/ens.dk/files/OlieGas/offshoreinstallations_20230322.zip'
category = 'facilities'
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
# %% Licenses
# =============================================================================
url = 'https://ens.dk/sites/ens.dk/files/OlieGas/licences_20230428.zip'
category = 'fields'

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )
