# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 15:04:01 2024

@author: maobrien
"""
# =============================================================================
# Import libraries
# =============================================================================
import os
# from tqdm import trange

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import data_auto_download

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# !!! Change these parameters before running script
DATA_REFRESH = True
DATE_DOWNLOAD = "2024-11-05"
region = 'norway'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', region)
os.chdir(final_path)

# =============================================================================
# %% Wells
# =============================================================================
url = "https://factpages.sodir.no/downloads/csv/wlbPoint.zip"
category = "wells"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Facilities
# =============================================================================
url = "https://factpages.sodir.no/downloads/csv/fclPoint.zip"
category = "facilities"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Pipelines
# =============================================================================
url = "https://factpages.sodir.no/downloads/shape/pipLine.zip"
category = "pipelines"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Fields
# =============================================================================
url = "https://factpages.sodir.no/downloads/shape/fldArea.zip"
category = "fields"

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
url = "https://factpages.sodir.no/downloads/shape/blkArea.zip"
category = "blocks"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )
