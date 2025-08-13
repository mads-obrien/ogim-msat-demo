# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 14:39:34 2023

Download CANVEC DATA for ALL Canadian provinces

@author: maobrien
"""

import os
from tqdm import tqdm

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import data_auto_download

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# !!! Change these parameters before running script
DATA_REFRESH = True
DATE_DOWNLOAD = "2024-11-05"

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', 'canada')
os.chdir(final_path)


links_ = [
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_AB_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_BC_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_CA_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_MB_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_NB_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_NL_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_NS_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_NT_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_NU_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_ON_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_PE_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_QC_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_SK_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_50K_YT_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_AB_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_BC_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_CA_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_MB_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_NB_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_NL_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_NS_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_NT_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_NU_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_ON_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_PE_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_QC_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_SK_Res_MGT_shp.zip",
    "https://ftp.maps.canada.ca/pub/nrcan_rncan/vector/canvec/shp/Res_MGT/canvec_250K_YT_Res_MGT_shp.zip"
]

prov_abbrev = {'AB': 'alberta',
               'BC': 'british_columbia',
               'CA': 'canada_other',
               'MB': 'manitoba',
               'NB': 'new_brunswick',
               'NL': 'newfoundland_labrador',
               'NS': 'nova_scotia',
               'NT': 'northwest_territories',
               'NU': 'nunavut',
               'ON': 'ontario',
               'PE': 'pei',
               'QC': 'quebec',
               'SK': 'saskatchewan',
               'YT': 'yukon'}

# ===========================================================================
# Loop through each URL and extract and save data
for url_ in tqdm(links_):
    # print(url_)
    abbrev_in_url = url_[-18:-16]
    # print(abbrev_in_url)
    provname = prov_abbrev[abbrev_in_url]
    data_auto_download(
        url_,
        # region=region,
        category="canvec_data",
        createFolder=False,
        export_path=provname + '//canvec_data//'
    )
