# -*- coding: utf-8 -*-
"""
Created on Thurs Aug 17 17:14:52 2023

Code that will automatically download OGIM-relevant data sources IN SASKATCHEWAN.
Based almost entirely on Mark's previous code:
    `OGIM_Data_Integration_Canada_Saskatchewan-v1.2.ipynb`

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
DATE_DOWNLOAD = "2024-11-15"
region = 'saskatchewan'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', 'canada', region)
os.chdir(final_path)
# =============================================================================
# %% Wells
# !!! NOTE these paths don't download the latest version updated daily
# =============================================================================s
category = "wells"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)


# Download vertical wells
url = "https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/vertical_well.gdb.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# Download non-vertical wells
url = "https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/nonvertical_well.gdb.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )


# =============================================================================
# %% Unit boundaries -- as of 12/7/23, I don't think we're adding this to OGIM
# =============================================================================
# This data set displays the lands contained within active oil and gas units in
# the Province of Saskatchewan. The information is compiled from unit agreements
# on file with Saskatchewan Industry and Resources.
#
# The depiction of the lands within oil and gas units is intended to provide
# the oil and gas industry and the public with some basic information about oil
# and gas units in Saskatchewan. It should be noted that the interests that are
# subject to a particular unit agreement are specified within the unit
# agreement, and the unit agreement should be referenced for any interpretation
# of the unit status of a particular interest.
#
# MetaData: https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/MetaData/unit_boundaries.html

url = "https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/UnitLand_.zip"
category = "unit_boundaries"
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
# %% Oil and gas pools
# =============================================================================
# This data set displays the lands contained within designated oil and gas
# pools in the Province of Saskatchewan. The information is compiled from the
# Minister's Orders that establish the individual pools.
#
# The depiction of the lands within oil and gas pools is intended to provide
# the oil and gas industry and the public with some basic information about oil
# and gas pools in Saskatchewan. It should be noted that the target area and
# well spacing provisions for oil and gas well drilling within the pool boundary
# may be set out in the pool order associated with the designated pool boundary.
#
# Metadata: https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/MetaData/OilAndGasPools.html

url = "https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/PoolLand_.zip"
category = "pool_lands"
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
# %% Pipelines and flowlines
# !!! NOTE these paths don't download the latest version updated daily
# =============================================================================
# Data: https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/PipelinesPublicGeoDb.gdb.zip
# There are two layers in this GeoDataBase, 1 for pipelines and 1 for flowlines

url = "https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/PipelinesPublicGeoDb.gdb.zip"
category = "pipelines"
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
# %% Facility license inventory report
# =============================================================================
# The Facility Licence Inventory Report provides a list of the upstream oil and
# gas facilities licensed in Saskatchewan. The Facility Licence Inventory includes:
#  - Licensee Name
#  - Facility Licence #
#  - Licence Issued Date
#  - Facility Status
#  - Type of Facility
#  - Location of the Facility
#  - Associated Rural Municipality
#  - Ministry Field Office area associated with the facility
#  - Special Conditions on the facility licence
#
# The inventory report is in Excel format and can be downloaded below. It is
# `updated bi-weekly` starting November 5, 2020.
# Data: https://www.saskatchewan.ca/business/agriculture-natural-resources-and-industry/oil-and-gas/oil-and-gas-news-and-bulletins/well-and-facility-bulletins-data-files-and-drilling-activity-report#facility-bulletin

url = "https://publications.saskatchewan.ca/api/v1/products/108487/formats/121953/download"
category = "facilities"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)


if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="Saskatchewan_Facilities_Inventory_.xlsx"
    )


# =============================================================================
# %% New and active facilities
# =============================================================================
# This report contains all Saskatchewan facilities whose operational status is
# new or active.
# Data: https://www.saskatchewan.ca/business/agriculture-natural-resources-and-industry/oil-and-gas/oil-and-gas-news-and-bulletins/well-and-facility-bulletins-data-files-and-drilling-activity-report#facility-reports

url = "https://training.saskatchewan.ca/EnergyAndResources/files/Registry%20Downloads/NewAndActiveFacilitiesReport.csv"
category = "facilities"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="Saskatchewan_NewActiveFacilitiesReport_.csv"
    )


# =============================================================================
# %% Suspended facilities
# =============================================================================
# This report contains all Saskatchewan facilities whose operational status is
# set to suspended.

url = "https://training.saskatchewan.ca/EnergyAndResources/files/Registry%20Downloads/SuspendedFacilitiesReport.csv"
category = "facilities"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="Saskatchewan_SuspendedFacilitiesReport_.csv"
    )


# =============================================================================
# %% Production reporting areas -- as of 12/7/23, I don't think we're adding this to OGIM
# =============================================================================
# This dataset contains the boundaries of the four production reporting areas:
#  - Lloydminster (Area 1),
#  - Kindersley (Area 2),
#  - Swift Current (Area 3), and
#  - Estevan (Area 4), whose boundaries follow the Cadastre township grid.
#
# Production regions are used to organize oil and gas production volume and
# value statistical reports for all oil and gas activities in Saskatchewan.
# The dataset was created as a file geodatabase feature class and output for
# public distribution.
#
# Data: https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/Production_Reporting_Area.gdb.zip
# MetaData: https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/MetaData/ProductionReportingAreas.html

# url = "https://gisappl.saskatchewan.ca/WebDocs/Geo_Atlas/Data/Production_Reporting_Area.gdb.zip"
# category = "production"

# if DATA_REFRESH:
#     data_auto_download(
#         url,
#         region=region,
#         category=category,
#         createFolder=False,
#         export_path=category
#     )

# NOTE: This error will be thrown, but data gets downloaded just fine.
# FileNotFoundError: [Errno 2] No such file or directory: 'production\\Production_Reporting_Area.gdb\\a00000005.CatItemTypesByParentTypeID.atx'

# =============================================================================
# %% Monthly crude oil and gas production by pool (2022)
# =============================================================================
# This report lists production volumes for all products that were produced by
# area, units and pools and provides a count of the number of wells that
# produced for a pool.
# *Note: The Run Date in the top right hand corner is the date the report was
# run. If the date is older than two months, the numbers could have changed.*
# *Note*: Even though some of these webpaged state that these data are in PDF format,
# the download link downloads a properly formatted xlsx.
# 2022 Data: https://publications.saskatchewan.ca/#/categories/5761

# Overview of all of SK's Oil and Gas Statistical Reports:
# https://www.saskatchewan.ca/business/agriculture-natural-resources-and-industry/oil-and-gas/oil-and-gas-news-and-bulletins/oil-and-gas-statistical-reports

category = "monthly_production_by_pool"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)


# URLs for Excel files for 2022 [January to December]
urls = ["https://publications.saskatchewan.ca/api/v1/products/117761/formats/135283/download",
        "https://publications.saskatchewan.ca/api/v1/products/117762/formats/135284/download",
        "https://publications.saskatchewan.ca/api/v1/products/117763/formats/135285/download",
        "https://publications.saskatchewan.ca/api/v1/products/118079/formats/135712/download",
        "https://publications.saskatchewan.ca/api/v1/products/118719/formats/136484/download",
        "https://publications.saskatchewan.ca/api/v1/products/118721/formats/136486/download",
        "https://publications.saskatchewan.ca/api/v1/products/118723/formats/136488/download",
        "https://publications.saskatchewan.ca/api/v1/products/119102/formats/137074/download",
        "https://publications.saskatchewan.ca/api/v1/products/119379/formats/137539/download",
        "https://publications.saskatchewan.ca/api/v1/products/119812/formats/138174/download",
        "https://publications.saskatchewan.ca/api/v1/products/120050/formats/138572/download",
        "https://publications.saskatchewan.ca/api/v1/products/120165/formats/138718/download"
        ]

url_months = ["01-2022",
              "02-2022",
              "03-2022",
              "04-2022",
              "05-2022",
              "06-2022",
              "07-2022",
              "08-2022",
              "09-2022",
              "10-2022",
              "11-2022",
              "12-2022"]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading SK monthly data::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="SK_OilGasProd_Pool-" + url_months[idx_] + ".xlsx"
        )


# =============================================================================
# %% Saskatchewan fuel, flare, and vent report (2022)
# as of 12/7/23, I don't think we're adding this to OGIM
# =============================================================================
# This report provides the reported fuel, flare and vent volumes for
# Saskatchewan by facility identifier. Each report contains data for one calendar year.
#
# Data Source: https://publications.saskatchewan.ca/#/categories/2541
# Data: https://publications.saskatchewan.ca/#/products/120341

url = "https://publications.saskatchewan.ca/api/v1/products/120341/formats/138958/download"
category = "fuel_flare_vent_report"
year_ = "2022"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)


if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="SK_fuel_vent_flare_report_" + year_ + ".xlsx"
    )


# =============================================================================
# %% Saskatchewan gas plant report
# as of 12/7/23, I don't think we're adding this to OGIM
# =============================================================================
# This report lists all the gas plants in Saskatchewan including:
#  - the area they are located,
#  - Facility ID,
#  - Facility Name,
#  - Facility Location and
#  - Facility Active Date.
#
# The report also provides the receipts of natural gas, natural gas liquids
# processed, and deliveries of natural gas from the gas plant by year.
# Data Source: https://publications.saskatchewan.ca/#/products/120342

url = "https://publications.saskatchewan.ca/api/v1/products/120342/formats/138959/download"
category = "gas_plant_report"
year_ = "2022"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)


if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="SK_gas_plant_report_" + year_ + ".xlsx"
    )


# =============================================================================
# %% Petrinex - Infrastructure Data
# Data: https://www.petrinex.ca/PD/Pages/SPD.aspx
# =============================================================================
# Updated daily
#  - Activity codes report
#  - Bussiness associate
#  - Facilities identifiers report
#  - Facility type and subtype codes report
#  - Horizon pool codes report
#  - Product codes report

category = "petrinex_data"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)


# First, download REPORTS that accompany the infrastructure data itself
# -----------------------------------------------------------------------------
urls = ["https://www.petrinex.gov.ab.ca/bbreportsSK/PRAActivityCodes.csv",
        "https://www.petrinex.gov.ab.ca/bbreportsSK/PRABAIdentifiers.csv",
        "https://www.petrinex.gov.ab.ca/bbreportsSK/PRAFacilityIds.csv",
        "https://www.petrinex.gov.ab.ca/bbreportsSK/PRAFacilityCodes.csv",
        "https://www.petrinex.gov.ab.ca/bbreportsSK/PRAFieldandPoolCodes.csv",
        "https://www.petrinex.gov.ab.ca/bbreportsSK/PRAProductCodes.csv"]


data_names = ["sk_activity_codes",
              "sk_business_assoc",
              "sk_facilities_ident",
              "sk_facilities_types",
              "sk_horizon_pool_codes",
              "sk_product_codes"]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading SK petrinex data::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="SK_PetrinexData-" + data_names[idx_] + ".csv"
        )


# Next, download Petrinex's metadata documents and learning aids
# -----------------------------------------------------------------------------
urls = ["https://www.petrinex.ca/PD/Documents/SK_Public_Data_Business_Associate_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Well_Infrastructure_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Well_%20Licence_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Well_to_Facility_Link_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Facility_Operator_History_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Facility_Infrastructure_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Facility_%20Licence_Report.pdf",
        "https://www.petrinex.ca/PD/Documents/SK_Public_Data_Conventional_Volumetrics_Report.pdf"
        ]

data_names = [
    "sk_business_associate_report",
    "sk_well_infra_report",
    "sk_well_licence_report",
    "sk_well_to_facility_link_report",
    "sk_facility_operator_history_report",
    "sk_facility_infra_report",
    "sk_facility_licence_report",
    "sk_conventional_volumetrics_report"
]


# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading SK petrinex learning aids::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="SK_PetrinexData-" + data_names[idx_] + ".pdf"
        )


# Next, download the infrastructure data (these download as a ZIP format)
# -----------------------------------------------------------------------------
urls = [r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Business%20Associate/CSV',
        r'http://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20Infrastructure/CSV',
        r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20Licence/CSV',
        r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Facility%20Infrastructure/CSV',
        r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Facility%20Operator%20History/CSV',
        r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Well%20to%20Facility%20Link/CSV',
        r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Infra/Facility%20Licence/CSV'
        ]

data_names = [
    "business_assoc",
    "well_infrastructure",
    "well_licence",
    "facility_infrastructure",
    "facility_operator_history",
    "well_to_facility_link",
    "facility_licence"
]

category = "petrinex_data"

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading SK petrinex infrastructure data::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="SK_PetrinexData-" + data_names[idx_] + ".zip"
        )


# =============================================================================
# %% Petrinex - Production Data (2022)
#  Available from https://www.petrinex.ca/PD/Pages/SPD.aspx
# =============================================================================
urls = [
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-01/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-02/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-03/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-04/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-05/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-06/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-07/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-08/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-09/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-10/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-11/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/SK/Vol/2022-12/CSV'
]

data_names = [
    "Vol_2022-01",
    "Vol_2022-02",
    "Vol_2022-03",
    "Vol_2022-04",
    "Vol_2022-05",
    "Vol_2022-06",
    "Vol_2022-07",
    "Vol_2022-08",
    "Vol_2022-09",
    "Vol_2022-10",
    "Vol_2022-11",
    "Vol_2022-12",
]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading SK petrinex production data::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="SK_PetrinexData-" + data_names[idx_] + ".zip"
        )

# %% Unzip all zipped Petrinex data files for Saskatchewan
# Afterwards, manually move the unzipped files to the base 'petrinex_data' folder
# -----------------------------------------------------------------------------
# print(os.getcwd())
# dir_name_ = os.getcwd() + '\\petrinex_data'
# unzip_files_in_folder(
#     dir_name=dir_name_,
#     create_save_path=True,
#     save_path=None
# )

# # Then, we have to run the unzip function again,
# # because the files from Petrinex are nested in two zip folders
# dir_name_ = os.getcwd() + '\\petrinex_data\\unzipped_files'
# unzip_files_in_folder(
#     dir_name=dir_name_,
#     create_save_path=True,
#     save_path=None
# )
