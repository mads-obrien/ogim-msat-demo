# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 15:01:14 2023

Code that will automatically download OGIM-relevant data sources IN ALBERTA.
Based almost entirely on Mark's previous code:
    `Refresh_and_Data_Integration_Canada_Alberta.ipynb`

@author: maobrien, momara
"""
# =============================================================================
# Import libraries
# =============================================================================
import os
from tqdm import trange
import pathlib
import shutil

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import data_auto_download, unzip_files_in_folder

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# !!! Change these parameters before running script
DATA_REFRESH = True
DATE_DOWNLOAD = "2025-02-03"
region = 'alberta'

# Set current working directory to the Bottom Up Infra Inventory folder
os.chdir("C://Users//maobrien//Environmental Defense Fund - edf.org//Mark Omara - Infrastructure_Mapping_Project//Bottom-Up-Infra-Inventory//")
final_path = os.path.join(os.getcwd(), f'OGIM_{version_num}', 'data', 'canada', region)
os.chdir(final_path)


# =============================================================================
# %% Wells
# =============================================================================
# Files available at:
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st37
# Well location data are included in the `ST37_SH_GCS_NAD83.shp` file.
# The ST37 Shapefile format product package has been upgraded to include bottom
# holes, surface holes, and well geometry feature classes with the corresponding
# attributes indicated in the XML metadata files.
category = "wells"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

# Alberta Surface holes
url = "https://static.aer.ca/prd/documents/sts/st37/SurfaceHolesShapefile.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# Alberta Bottom holes
url = "https://static.aer.ca/prd/documents/sts/st37/BottomHolesShapefile.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )


# Metadata PDF
url = "https://static.aer.ca/prd/documents/sts/St37-Listofwellslayout.pdf"
if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="St37-Listofwellslayout.pdf"
    )


# =============================================================================
# %% Abandoned well locations - DON'T INTEGRATE
# These features are duplicates of what's already in ST37_SH and ST37_BH
# =============================================================================
# https://www1.aer.ca/ProductCatalogue/510.html
# category = "wells"

# # Download Alberta Abandoned  (not revised)
# url = "https://static.aer.ca/prd/data/wells/ABNDWells_SHP.zip"

# if DATA_REFRESH:
#     data_auto_download(
#         url,
#         region=region,
#         category=category,
#         createFolder=False,
#         export_path=category
#     )

# # Download Alberta Revised Abandoned Wells Data
# url = "https://static.aer.ca/prd/data/wells/Revised_Abandoned_Well_Locations_Shapefile.zip"

# if DATA_REFRESH:
#     data_auto_download(
#         url,
#         region=region,
#         category=category,
#         createFolder=False,
#         export_path=category
#     )


# =============================================================================
# %% Coal mines in Alberta
# =============================================================================
# https://www1.aer.ca/ProductCatalogue/609.html

url = "https://static.aer.ca/prd/data/shapefiles/Coal_Mines_Alberta_SHP.zip"
category = "coal_mines"
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
# %% Pipelines and pipeline installations
# =============================================================================
# https://www1.aer.ca/ProductCatalogue/557.html
# Document describing field names: https://static.aer.ca/prd/data/pipeline/EnhancedPipeline_Layout.pdf
category = "pipelines"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

url = "https://static.aer.ca/prd/data/pipeline/Pipelines_SHP.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# Download Alberta Pipeline installations
url = "https://static.aer.ca/prd/data/pipeline/Pipeline_Installations_SHP.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# PDF with metadata
url = "https://static.aer.ca/prd/data/pipeline/EnhancedPipeline_Layout.pdf"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName='EnhancedPipeline_Layout.pdf'
    )

# =============================================================================
# %% Rural utilities gas distribution pipelines
# =============================================================================
# NOTE this dataset is no longer managed by AER. Current download source:
# https://geodiscover.alberta.ca/geoportal/rest/metadata/item/4f5d7174ebd34999afec825b9bd3302e/html
# Look for 'publication date' on the metadata page
# Old AER catalog page: https://www1.aer.ca/productcatalogue/289.html
category = "pipelines"

url = "https://extranet.gov.ab.ca/srd/geodiscover/srd_pub/utilitiesCommunication/RuralUtilitiesLowPressurePipelines.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )

# These data unzip into a subdirectory called "Data" -- cut and paste them from
# "Data" into the "alberta/pipeline" directory. Delete the empty "Data"
# subdirectory when done
source = 'pipelines\\Data'
dest = 'pipelines'

for file in os.listdir(source):
    shutil.move(os.path.join(source, file), os.path.join(dest, file))
os.rmdir(source)

# =============================================================================
# %% ST102: Facility List shapefiles
# =============================================================================
# Files available at:
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st102
# These reports each contain list of batteries, gas plants, meter stations, and
# other facilities in the province. As well, the list has been upgraded to include
# additional information frequently requested by customers, such as operator name
# and facility sub-type description.
category = "facilities"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

url = "https://static.aer.ca/prd/data/codes/ST102-SHP.zip"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category
    )


# =============================================================================
# %% ST13: Alberta Gas Plant and Gathering System Activities - Annual Statistics
# ===========================================================================
# Files available at:
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st13

# ST13A: Alberta Gas Plant/Gas Gathering System Activities - Annual Statistics
# ST13A displays annual volumetric data for gas plant and gas gathering
# system activities such as receipts, dispositions, and processes. The 10-year
# history will be refreshed in the current year report only. *NOTE: Some
# facilities may not balance due to different methods of reporting in prior years.*

# ST13B: Alberta Gas Plant/Gas Gathering System Activities - Monthly Statistics
# ST13B contains the monthly volumetric data for gas plants and gas
# gathering systems which are reporting liquids. Activities such as receipts,
# dispositions, and processes are reported for gas and liquid products. A zero
# value in the balance column indicates that the activities at these facilities
# are in balance. The exception to this is for gas gathering systems that have
# production quantities – the balance column would be out by the amount of
# production at this facility. The data is extracted once a month from the
# Petroleum Registry. Each month of the current year will be refreshed up to and
# including the December publication. Future enhancements will include a separate
# report that details all gas gathering systems activities.

# ST13C: Alberta Gas Gathering System Activities - Monthly Statistics
# ST13C contains the monthly volumetric data for gas gathering systems.
# Activities such as production, receipts, dispositions, and processes are
# reported for gas and liquid products. A zero value in the balance column
# indicates that the activities at these facilities are in balance. The data is
# extracted once a month from the Petroleum Registry. Each month of the current
# year will be refreshed up to and including the December publication.

category = "facilities"

# ----------------------------------------------------------------------------
# ST13A (detailed) data for 2023. Annual report. PDF format.
# ----------------------------------------------------------------------------
url = "https://static.aer.ca/prd/documents/sts/st13/st13a_2023_details.pdf" 

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="AB_ST13A_GP_2023_Annual_Stats.pdf"
    )


# ----------------------------------------------------------------------------
# ST13B (detailed) data for 2024. CSV format.
# Remember to look at Run Date on top of CSV
# ----------------------------------------------------------------------------
url = "https://static.aer.ca/prd/documents/sts/st13/st13b_2024_detail.csv"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="AB_ST13B_GP_2024_Annual_Stats_Receipts_Disp.csv"
    )

# ----------------------------------------------------------------------------
# ST13C (detailed) data for 2023. CSV format.
# Remember to look at Run Date on top of CSV
# ----------------------------------------------------------------------------
url = "https://static.aer.ca/prd/documents/sts/st13/st13c_2024_detail.csv"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="AB_ST13C_2024_GasGathering_Volumetric_Data.csv"
    )


# =============================================================================
# %% ST50A and ST50B: Gas Plant and Gathering System Activities - Annual Statistics
# =============================================================================
# Files available at:
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st50
#
# ST50A: Gas Processing Plants in Alberta (sorted by gas plant reporting facility ID code)
# List of gas plants in Alberta, identifying location and names
# associated with the gas plant reporting ID code
# plant operator, facility licence and design capacities
# licenced sulphur recovery efficiency and maximum daily emission rates
# Download: [XLS](https://static.aer.ca/prd/documents/sts/ST50A.xls) Files produced monthly
#
# ST50B: Gas Processing Plants in Alberta (sorted by gas plant facility licences)
# List of gas plants in Alberta, identifying location and names associated with
# the gas plant reporting ID code
# plant operator, facility licence and design capacities
# licenced sulphur recovery efficienty and maximum daily emission rates

category = "facilities"

# ----------------------------------------------------------------------------
# ST50A. CSV format.
# ----------------------------------------------------------------------------
url = "https://static.aer.ca/prd/documents/sts/ST50A.csv"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="AB_ST50A_GasPlant_Data.csv"
    )

# ----------------------------------------------------------------------------
# ST50B. CSV format.
# ----------------------------------------------------------------------------
url = "https://static.aer.ca/prd/documents/sts/ST50B.csv"

if DATA_REFRESH:
    data_auto_download(
        url,
        region=region,
        category=category,
        createFolder=False,
        export_path=category,
        fileName="AB_ST50B_GasPlant_Data.csv"
    )


# =============================================================================
# %% Alberta Field Surveillance Incident Inspection Shapefile
# as of 12/7/23, I don't think we're adding this to OGIM
# =============================================================================
# Files available at:
# https://www.aer.ca/providing-information/data-and-reports/activity-and-data/field-surveillance-incident-inspection-list

url = "https://static.aer.ca/prd/documents/data/FIS_EDD_SHP.zip"
category = "field_surveillance_incident_inspection"
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
# %% Geological Play outline shapefile - NO LONGER AVAILABLE
# =============================================================================
# Brief description and ZIP file available at:
# https://www.aer.ca/providing-information/data-and-reports/activity-and-data/play-workbook

# url = "https://static.aer.ca/prd/documents/catalog/PlayWorkbookListArea.zip"
# category = "oil_gas_play_outline"

# if DATA_REFRESH:
#     data_auto_download(
#         url,
#         region=region,
#         category=category,
#         createFolder=False,
#         export_path=category
#     )


# =============================================================================
# %% Scheme approval spatial data shapefile
# as of 12/7/23, I don't think we're adding this to OGIM
# DO THIS ONE MANUALLY, FILE PATH TOO LONG
# =============================================================================
# Brief description at:
# https://www1.aer.ca/ProductCatalogue/671.html

# url = "https://static.aer.ca/prd/data/shapefiles/Scheme_Approval_SHP.zip"
# category = "schemes"
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
# %% ST60: Alberta crude oil and crude bitumen batteries monthly flaring, venting, and production data
# Feb 2024 -- SKIP FOR NOW
# =============================================================================
# Files available at:
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st60

# Updated monthly, this statistical report provides data on crude oil and
# bitumen production, as well as flaring and venting, from batteries. The report
# contains such details as battery location, type, and operator, among others. It
# also indicates where conservation of gas has been economically feasible.
#
# Companies must submit this data to us under Directive 60: Upstream
# Petroleum Industry Flaring, Incinerating, and Venting. The directive sets out
# requirements for flaring, incinerating, and venting in Alberta at all upstream
# petroleum industry wells and facilities.
#
#  Read "Manual 011: How to Submit Volumetric Data to the AER" to understand
# how this data is submitted to us and how to interpret this report.
# https://www.aer.ca/regulating-development/rules-and-directives/manuals

# # Download ST60 2022 DATA
# export_path = None
# category = "crude_production_data"
# region = 'alberta'
# createFolder = True

# urls_ = [
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-01.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-02.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-03.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-04.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-05.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-06.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-07.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-08.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-09.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-10.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-11.xlsx",
#     "https://static.aer.ca/prd/documents/sts/st60/ST60_2022-12.xlsx"
# ]


# # Loop through each URL and extract and save data
# for idx_ in trange(len(urls_), desc="Downloading AB crude oil production data::::"):
#     # Download
#     url_ = urls_[idx_]
#     if DATA_REFRESH:
#         data_auto_download(
#             url_,
#             region=region,
#             category=category,
#             createFolder=True,
#             fileName=url_.split("/")[-1]
#         )


# =============================================================================
# %% Petrinex - Infrastructure data
#  Available from https://www.petrinex.ca/PD/Pages/APD.aspx
# =============================================================================

# First, download REPORTS that accompany the infrastructure data itself
# -----------------------------------------------------------------------------
category = "petrinex_data"
# Create subfolder to contain this infra category, if it doesn't already exist
pathlib.Path(category).mkdir(exist_ok=True)

urls = [
    "https://www.petrinex.gov.ab.ca/bbreports/PRAActivityCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRABAIdentifiers.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAFacilityIds.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAFacilityCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAFieldCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAFieldandPoolCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAFormationCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAOilSandsAreaCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAOilSandsAreaDepositCodes.csv",
    "https://www.petrinex.gov.ab.ca/bbreports/PRAProductCodes.csv"
]

data_names = [
    "ab_activity_codes",
    "ab_business_assoc",
    "ab_facilities_ids",
    "ab_facilities_codes",
    "ab_field_codes",
    "ab_field_and_pool_codes",
    "ab_formation_codes",
    "ab_oil_sands_area_codes",
    "ab_oil_sands_area_deposit_codes",
    "ab_product_codes"
]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading AB petrinex reports::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="AB_PetrinexData-" + data_names[idx_] + ".csv"
        )


# Next, download Petrinex's metadata documents and learning aids
# -----------------------------------------------------------------------------
urls = [
    "https://www.petrinex.ca/PD/Documents/PD_Business_Associate_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Well_Infrastructure_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Well_%20Licence_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Facility_Infrastructure_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Facility_Operator_History_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Well_to_Facility_Link_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Facility_%20Licence_Report.pdf",
    "https://www.petrinex.ca/PD/Documents/PD_Conventional_Volumetrics_Report.pdf"
]

data_names = [
    "ab_business_associate_report",
    "ab_well_infra_report",
    "ab_well_licence_report",
    "ab_facility_infra_report",
    "ab_facility_operator_history_report",
    "ab_well_to_facility_link_report",
    "ab_facility_licence_report",
    "ab_conventional_volumetrics_report"
]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading AB petrinex learning aids::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="AB_PetrinexData-" + data_names[idx_] + ".pdf"
        )


# Next, download the infrastructure data (these download as a ZIP format)
# -----------------------------------------------------------------------------
urls = [
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Business%20Associate/CSV',
    r'http://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20Infrastructure/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20Licence/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Infrastructure/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Operator%20History/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Well%20to%20Facility%20Link/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Infra/Facility%20Licence/CSV'
]

data_names = [
    "business_assoc",
    "well_infrastructure",
    "well_licence",
    "facility_infrastructure",
    "facilitiy_operator_history",
    "well_to_facility_link",
    "facility_licence"
]

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading AB petrinex infrastructure data::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="AB_PetrinexData-" + data_names[idx_] + ".zip"
        )

# =============================================================================
# %% Petrinex - Production Data
#  Available from https://www.petrinex.ca/PD/Pages/APD.aspx
# =============================================================================

urls = [
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-01/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-02/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-03/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-04/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-05/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-06/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-07/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-08/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-09/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-10/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-11/CSV',
    r'https://www.petrinex.gov.ab.ca/publicdata/API/Files/AB/Vol/2022-12/CSV'
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

export_path = None
category = "petrinex_data"
region = 'alberta'
createFolder = True

# Loop through each URL and extract and save data
for idx_ in trange(len(urls), desc="Downloading AB petrinex production data::::"):
    # Download
    if DATA_REFRESH:
        data_auto_download(
            urls[idx_],
            region=region,
            category=category,
            createFolder=False,
            export_path=category,
            fileName="AB_PetrinexData-" + data_names[idx_] + ".zip"
        )
# %%
# Unzip all zipped Petrinex data files for Alberta
# -----------------------------------------------------------------------------
# print(os.getcwd())
# dir_name_ = os.getcwd()
# unzip_files_in_folder(
#     dir_name=dir_name_,
#     create_save_path=True,
#     save_path=None
# )

# # Then, we have to run the unzip function again,
# # because the files from Petrinex are nested in two zip folders
# dir_name_ = os.getcwd() + '\\unzipped_files'
# unzip_files_in_folder(
#     dir_name=dir_name_,
#     create_save_path=True,
#     save_path=None
# )
