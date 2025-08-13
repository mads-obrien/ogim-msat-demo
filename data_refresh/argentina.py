# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 2024

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
DATE_DOWNLOAD = "2025-01-15"
region = 'argentina'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', region)
os.chdir(final_path)


def remove_special_chars_from_filename(old_filename):
    # define substrings to check for
    pi = 'π'
    dot = '\uefe3'

    # confirm that file exists
    if os.path.isfile(old_filename):
        if pi in old_filename or dot in old_filename:
            print(f'Existing:\n{old_filename}')
            new = old_filename.replace(pi, '')
            new_ = new.replace(dot, '')
            # Only rename the file if the "new" name doesn't already exist
            if not os.path.isfile(new_):
                os.rename(old_filename, new_)
                print('File renamed to:')
                print(new_)
        else:
            print(f'{old_filename} - no special characters')
    else:
        print(f'!!! {old_filename} not found in current directory')
        print(os.getcwd())


# =============================================================================
# %% Wells
# =============================================================================
url = 'http://datos.energia.gob.ar/dataset/c846e79c-026c-4040-897f-1ad3543b407c/resource/3fcda0c5-68aa-4f33-bbe2-0180e6dbeebe/download/shapefile-de-pozos.zip'
category = 'wells'

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
url1 = 'http://datos.energia.gob.ar/dataset/164e7197-3222-4d21-81df-9db30ccd3940/resource/601bccb9-5e24-4ea1-9d90-e4d72da11d1f/download/instalaciones-hidrocarburos-instalaciones-res-318.zip'
category = 'facilities'

url2 = 'http://datos.energia.gob.ar/dataset/e5c525ff-6dbd-4e1e-a52e-14e9d742c4f1/resource/9029e1ae-77f2-4aee-8e40-c30943ac9d1b/download/instalaciones-hidrocarburos-instalaciones-res-319-93-p-caractersticos-.zip'
category = 'facilities'

if DATA_REFRESH:
    data_auto_download(
        url1,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

if DATA_REFRESH:
    data_auto_download(
        url2,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Petroleum Terminals
# SKIP FOR NOW AND DOWNLOAD MANUALLY, filepath is too long and throws a FileNotFoundError
# Manually save the ZIP, extract, and rename by removing the special chars
# =============================================================================
# url = 'http://datos.energia.gob.ar/dataset/99ba34a0-08f2-48e3-8d37-4d92542f740e/resource/d9e6759e-bcae-4321-9317-10ed7b6ceccb/download/comercializacin-de-hidrocarburos-terminales-de-despacho-de-combustibles-lquidos-segn-res-110204-.zip'
# category = 'facilities'

# if DATA_REFRESH:
#     data_auto_download(
#         url,
#         region=region,
#         category=category,
#         createFolder=False,
#         export_path=category
#     )

# =============================================================================
# %% Refineries
# =============================================================================
url = 'http://datos.energia.gob.ar/dataset/a9eed347-78ab-45c0-a489-b227fe42ee1b/resource/4399cb2d-c221-4b9a-9799-8fc008cefd77/download/refinacin-hidrocarburos-refineras.zip'
category = 'facilities'

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% LNG (regasification)
# =============================================================================
url = 'http://datos.energia.gob.ar/dataset/bf7914d6-1174-4766-a632-0e7d62068b25/resource/c34d2b0a-977a-4c30-b86b-8c9b9d5ed662/download/instalaciones-hidrocarburos-puertos-regasificadores-de-gnl.zip'
category = 'facilities'

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
url1 = 'http://datos.energia.gob.ar/dataset/84681f81-dbbb-49eb-be30-e61778736ad9/resource/17fcc6b7-9d9c-4005-a93a-1b2d3a7b1ce5/download/instalaciones-hidrocarburos-ductos-res-319-93.zip'
url2 = 'http://datos.energia.gob.ar/dataset/8758101a-1e0d-413f-8cc5-83e21ece6391/resource/5af07e15-f356-40b9-a369-63dbf38a938a/download/gasoductos-de-transporte-enargas-.zip'

category = 'pipelines'

if DATA_REFRESH:
    data_auto_download(
        url1,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )


if DATA_REFRESH:
    data_auto_download(
        url2,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )


# =============================================================================
# %% Compressor stations
# =============================================================================
# Transport compressor stations
url1 = 'http://datos.energia.gob.ar/dataset/8758101a-1e0d-413f-8cc5-83e21ece6391/resource/741aebfd-2fc5-4668-9293-a5ad81b31330/download/plantas-compresoras-de-transporte-de-gas-enargas-.zip'

# Distribution compressor stations
url2 = 'http://datos.energia.gob.ar/dataset/8758101a-1e0d-413f-8cc5-83e21ece6391/resource/e34ed7b7-848b-4c15-9e86-6e49bae7efc0/download/plantas-compresoras-de-distribucin-de-gas.zip'

category = 'facilities'

if DATA_REFRESH:
    data_auto_download(
        url1,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

if DATA_REFRESH:
    data_auto_download(
        url2,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Pumping stations (Other?)
# =============================================================================
url = 'http://datos.energia.gob.ar/dataset/10c89490-8f41-4825-8704-5956ecfbad4a/resource/18d56632-9990-46cb-a2b9-fd6f5a433965/download/estaciones-de-bombeo.zip'
category = 'facilities'

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% Basins
# =============================================================================
url = 'http://datos.energia.gob.ar/dataset/0d4a18ee-9371-439a-8a94-4f53a9822664/resource/c477f5a7-75cf-4123-ba7b-e298eb52abbd/download/exploracin-hidrocarburos-cuencas-sedimentarias.zip'
category = 'fields_basins'

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
url = 'http://datos.energia.gob.ar/dataset/7378520e-4d10-48a9-92e9-7e20e69a8277/resource/82f83e7e-5e07-4cf7-8015-0e01d2cfa51f/download/produccin-hidrocarburos-yacimientos.zip'
category = 'fields_basins'

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
url = 'http://datos.energia.gob.ar/dataset/81cfad0a-4162-4f85-ad71-837f5a5fae57/resource/48a306a3-d5da-4f28-8ab5-7f80767ffdec/download/produccin-hidrocarburos-concesiones-de-explotacin.zip'
category = 'fields_basins'

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# =============================================================================
# %% rename files with special characters in Argentina's subfolders
# =============================================================================
folders = ['facilities', 'fields_basins', 'pipelines']

for folder in folders:
    os.chdir(os.path.join(final_path, folder))
    files = os.listdir()
    for file in files:
        remove_special_chars_from_filename(file)
