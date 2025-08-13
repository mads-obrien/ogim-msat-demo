# -*- coding: utf-8 -*-
"""
Created on Thu Jul 27 16:55:34 2023

Data integration of Alberta, Canada OGIM data -- all categories except wells
Based extensively on Mark's previous code:
    `Refresh_and_Data_Integration_Canada_Alberta.ipynb`

    # TODO - add metering stations from ST102 dataset, export as stations-other

@author: maobrien, momara
"""
import os
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
import glob
import csv

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs, integrate_pipelines,
                     save_spatial_data, read_spatial_data,
                     schema_LNG_STORAGE, schema_COMPR_PROC, schema_REFINERY,
                     schema_OTHER, schema_PIPELINES, calculate_pipeline_length_km)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'canada', 'alberta'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% Custom functions
# =============================================================================


def standardize_DLS_AB(
    df,
    legalSubDivision_col: str,
    section_col: str,
    township_col: str,
    range_col: str,
    meridian_col: str
):
    """
    standardize_DLS_AB generates a standard DLS format
    for the location of the facility based on the information
    for legal sub division (00), section (00), township (000), range (00), meridian (W0)
    XX-XX-XXX-XXWX

    Location data in Alberta are reported usign Dominion Land Survey format.
    """

    lsd_ = []
    sec_ = []
    town_ = []
    range__ = []
    full_lsd = []

    for idx, row in df.iterrows():
        legalSubdivision = str(row[legalSubDivision_col])
        section = str(row[section_col])
        township = str(row[township_col])
        range_ = str(row[range_col])
        meridian = str(row[meridian_col])

        # Legal sub division
        if len(legalSubdivision) != 2:
            # Append a zero before the value
            lsd = '0' + legalSubdivision
            lsd_.append(lsd)
        else:
            lsd = legalSubdivision
            lsd_.append(lsd)

        # Section
        if len(section) == 2:
            sect = section
            sec_.append(sect)
        else:
            sect = '0' + section
            sec_.append(sect)

        # Township
        if len(township) == 3:
            townsh = township
            town_.append(townsh)
        elif len(township) == 2:
            townsh = '0' + township
            town_.append(townsh)
        else:
            townsh = '00' + township
            town_.append(townsh)

        # Range
        if len(range_) == 2:
            Range = range_
            range__.append(Range)
        else:
            Range = '0' + range_
            range__.append(Range)

        # Reconstructed DLS
        reconstruct_dls = lsd + "-" + sect + "-" + townsh + "-" + Range + "W" + meridian
        full_lsd.append(reconstruct_dls)

    # Create dataframe
    df2 = pd.DataFrame(lsd_, columns=['LegalSubDivision'])
    df2['Section'] = sec_
    df2['Township'] = town_
    df2['Range'] = range__
    df2['Meridian'] = meridian
    df2['FacilityLocation'] = full_lsd

    return df2


def clean_AB_ST13_csv(input_file, output_file, substrings, num_footer_lines=8):
    '''Pre-clean a CSV file to remove subheader rows before reading it in as a DataFrame.'''
    with open(input_file, mode='r', newline='', encoding='latin-1') as infile, \
            open(output_file, mode='w', newline='', encoding='latin-1') as outfile:

        reader = csv.reader(infile)
        writer = csv.writer(outfile)

        # Read all rows into memory
        rows = list(reader)

        # Remove the last `num_footer_lines` (blank or non-blank) from the rows
        rows = rows[:-num_footer_lines]

        # Write the filtered rows back to the output file
        for row in rows:
            # Skip completely blank lines or lines starting with the specified substring
            if not row or any(row[0].startswith(sub) for sub in substrings):
                continue
            writer.writerow(row)


# =============================================================================
# %% Read + Clean - Gas Plants + Gathering, Receipts & Dispositions (ST13B, ST13C)
# =============================================================================
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st13
#
# **ST13B** contains the monthly volumetric data for gas plants and gas
# gathering systems which are reporting liquids. Activities such as receipts,
# dispositions, and processes are reported for gas and liquid products. A zero
# value in the balance column indicates that the activities at these facilities
# are in balance. The exception to this is for gas gathering systems that have
# production quantities – the balance column would be out by the amount of
# production at this facility. The data is extracted once a month from the
# Petroleum Registry. Each month of the current year will be refreshed up to and
# including the December publication. Future enhancements will include a separate
# report that details all gas gathering systems activities.
#
# **ST13C** contains the monthly volumetric data for gas gathering systems.
# Activities such as production, receipts, dispositions, and processes are
# reported for gas and liquid products. A zero value in the balance column
# indicates that the activities at these facilities are in balance. The data is
# extracted once a month from the Petroleum Registry. Each month of the current
# year will be refreshed up to and including the December publication.
# =============================================================================

# Clean the ST13B file, save a cleaned copy, and then read in the cleaned CSV
original_st13b = "facilities\\AB_ST13B_GP_2024_Annual_Stats_Receipts_Disp.csv"
cleaned_st13b = "facilities\\AB_ST13B_GP_2024_Annual_Stats_Receipts_Disp_cleaned.csv"
substrings = ['Monthly Gas', 'Gas Plant', 'Gas Gathering System', 'GGS', 'Sub total']
clean_AB_ST13_csv(original_st13b, cleaned_st13b, substrings, num_footer_lines=8)
st13b = pd.read_csv("facilities\\AB_ST13B_GP_2024_Annual_Stats_Receipts_Disp_cleaned.csv",
                    on_bad_lines='skip',
                    index_col=False,
                    encoding='latin-1')


# Clean the ST13C file, save a cleaned copy, and then read in the cleaned CSV
original_st13c = "facilities\\AB_ST13C_2024_GasGathering_Volumetric_Data.csv"
cleaned_st13c = "facilities\\AB_ST13C_2024_GasGathering_Volumetric_Data_cleaned.csv"
substrings = ['Monthly Gas', 'Gas Plant', 'Gas Gathering System', 'GGS', 'Sub total']
clean_AB_ST13_csv(original_st13c, cleaned_st13c, substrings, num_footer_lines=8)
st13c = pd.read_csv("facilities\\AB_ST13C_2024_GasGathering_Volumetric_Data_cleaned.csv",
                    on_bad_lines='skip',
                    index_col=False,
                    encoding='latin-1')

# Preview
st13b.head()
st13c.head()
# Feature counts
print([st13b.shape[0], st13c.shape[0]])


# 13B: Group by facility ID and sum up receipts and dispositions from each
# month to arrive at an annual total
# -----------------------------------------------------------------------------
aggFns_st13b = {
    'AB Receipt (1000 cu.m.)': 'sum',
    'Non AB Receipt (1000 cu.m.)': 'sum',
    'Battery Disposition (1000 cu.m.)': 'sum',
    'Gathering System Disposition (1000 cu.m.)': 'sum',
    'Gas Plant Disposition (1000 cu.m.)': 'sum',
    'Injection Facility Disposition exc CO2(1000 cu.m.)': 'sum',
    'Injection Facility Disposition CO2 (1000 cu.m.)': 'sum',
    'Meter Station Disposition (1000 cu.m.)': 'sum',
    'Other Disposition (1000 cu.m.)': 'sum',
    'AB Commercial Disposition (1000 cu.m.)': 'sum',
    'AB Industry Disposition (1000 cu.m.)': 'sum',
    'AB Residential Disposition (1000 cu.m.)': 'sum',
    'AB Elec Gen Disposition (1000 cu.m.)': 'sum',
    'SHR Acid Gas Disposition (1000 cu.m.)': 'sum',
    'SHR Others Disposition (1000 cu.m.)': 'sum',
    'Fuel Disposition (1000 cu.m.)': 'sum',
    'Flared Disposition exc CO2(1000 cu.m.)': 'sum',
    'Flared Disposition CO2 (1000 cu.m.)': 'sum',
    'Vented Disposition exc CO2 (1000 cu.m.)': 'sum',
    'Vented Disposition CO2 (1000 cu.m.)': 'sum',
    'Inventory Adjustment (1000 cu.m.)': 'sum',
    'Metering Difference Disposition (1000 cu.m.)': 'sum',
    'Facility Sub Type Code': 'count'  # Represents the NUMBER OF MONTHS for which we have records
}

st13b_gp = st13b.groupby(by="Facility ID").agg(aggFns_st13b).reset_index()
st13b_gp['total_receipts (1000 cu.m.)'] = st13b_gp['AB Receipt (1000 cu.m.)'] + st13b_gp['Non AB Receipt (1000 cu.m.)']
st13b_gp['total_disposition (1000 cu.m.)'] = [st13b_gp.iloc[x, 3:-1].sum() for x in range(st13b_gp.shape[0])]


# 13C: Group by facility ID and sum up receipts and dispositions
# -----------------------------------------------------------------------------
aggFns_st13c = {
    'AB Receipt (1000 cu.m.)': 'sum',
    'Non AB Receipt (1000 cu.m.)': 'sum',
    'Facility Sub Type Code': 'count'    # Represents the NUMBER OF MONTHS for which we have records
}

st13c['AB Receipt (1000 cu.m.)'] = [float(st13c['AB Receipt (1000 cu.m.)'].iloc[x]) for x in range(st13c.shape[0])]
st13c['Non AB Receipt (1000 cu.m.)'] = [float(st13c['Non AB Receipt (1000 cu.m.)'].iloc[x]) for x in range(st13c.shape[0])]

st13c_gp = st13c.groupby(by="Facility ID").agg(aggFns_st13c).reset_index()
st13c_gp['total_receipts (1000 cu.m.)'] = st13c_gp['AB Receipt (1000 cu.m.)'] + st13c['Non AB Receipt (1000 cu.m.)']


# Combine the datasets for gathering systems and gas plants
# -----------------------------------------------------------------------------
# Due to the grouping operation above, the 'Facility Sub Type Code'
# attributes represents a count of the # of months in the year for which
# throughput data are reported.

fac_comb_receipts = pd.concat([st13c_gp[['Facility ID',
                                         'total_receipts (1000 cu.m.)',
                                         'Facility Sub Type Code']],
                               st13b_gp[['Facility ID',
                                         'total_receipts (1000 cu.m.)',
                                         'Facility Sub Type Code']]])
# fac_comb_receipts.head()

# This concatenation causes duplicate records to appear in this table! Drop em.
# (Duplicated Facility IDs with the exact same total receipts value)
fac_comb_receipts = fac_comb_receipts.drop_duplicates(keep='first').reset_index(drop=True)

# Rename 'Facility Sub Type Code' variable to 'months'
fac_comb_receipts['months'] = fac_comb_receipts['Facility Sub Type Code']

# Estimate facility's gas throughput in MMCFD, assuming an average of 30.4 days
# per month, and using the number of months that have reported in ST13B and C
# -----------------------------------------------------------------------------
num_days_month = 30.4
m3_to_ft3 = 35.314666721
fac_comb_receipts["gas_thru_mmcfd"] = fac_comb_receipts['total_receipts (1000 cu.m.)'] * m3_to_ft3 / 1000 / fac_comb_receipts['months'] / num_days_month


# =============================================================================
# %% ST102: Facility List, formerly Battery Codes and Facility Codes [ASCII] (updated monthly)
# =============================================================================
# https://www.aer.ca/providing-information/data-and-reports/maps-mapviewers-and-shapefiles
# These reports each contain list of batteries, gas plants, meter stations, and
# other facilities in the province. As well, the list has been upgraded to
# include additional information frequently requested by customers, such as
# operator name and facility sub-type description.
#
# They are supplied in two parts:
# Part A - List of New and Active Facilities [PDF](https://static.aer.ca/prd/data/codes/ActiveFacility.pdf)
# Part B - List of Other Facilities [PDF](https://static.aer.ca/prd/data/codes/InactiveFacility.pdf)
#
# We also have a Facility List Shapefile [ZIP](https://static.aer.ca/prd/data/codes/ST102-SHP.zip)
# which contains both Part A and Part B, from above.
#
# **Definitions**
# EDCT - Energy Development Category Type - identifies the energy development
# type the facility is currently licensed for, as well as the minimum
# consultation and notification requirements.
#
# Facility Subtype - assigned by the operator when requesting a Facility ID in
# Petrinex, a facility subtype is a more detailed classification of a facility
# type. When a facility operator applies for a Facility ID, the facility subtype
# must be specified in the application. Petrinex validates the facility subtype
# selection with the facility license data. An example of a facility subtype for
# the facility type of GP (gas plant) is facility subtype 405, Gas Plant Sulphur Recovery.
#
# EDCT Code / Facility Subtype comparison [PDF](https://static.aer.ca/prd/data/codes/ST102_code.pdf)
#
# Please note, the Petrinex Facility Subtype is typically not changed once
# assigned and may not reflect the current operating facility subtype, EDCT
# Codes reflect current licence type. EDCT codes are assigned at the licence
# level. If there are two or more facilities on the same licence, the EDCT code
# of the highest priority facility will be assigned to all of the facilities on that licence.
# =============================================================================

# Read AER facilities data shapefile
fp_ = "facilities\\ST102_Facility_GCS_NAD83.shp"
ab_fac = read_spatial_data(fp_, table_gradient=True)
ab_fac2 = transform_CRS(ab_fac, appendLatLon=True)
print(*ab_fac2.columns, sep='\n')

# Read Facility Infrastructure data from Petrinex, which includes additional
# attributes (like installation dates, facility type, etc.) that can be combined
# with the ST102 .shp data based on "FacilityID" or "FAC_ID" attributes
# -----------------------------------------------------------------------------
fp_fac_ = "petrinex_data\\Facility Infrastructure-AB.CSV"
ab_fac3_ = pd.read_csv(fp_fac_)
ab_fac3_.iloc[:, 0:10].head()
print(*ab_fac3_.columns, sep='\n')

# Check for similarities in facility IDs (they start with 'AB', contain letters and nums)
# How many unique fac IDs are in the original ST102 shapefile?
# How many records in the Petrinex data can we merge with the ST102 based on facility IDs?
unique_ids_st102 = ab_fac2.FAC_ID.unique()
unique_ids_petrinex = ab_fac3_.FacilityID.unique()
print("Length of unique IDs in AER ST102 and Petrinex datasets = ", [len(unique_ids_st102), len(unique_ids_petrinex)])
common_ids_ = ab_fac2[ab_fac2.FAC_ID.isin(unique_ids_petrinex)]
print("Number of IDs in Petrinex CSV that are in the AER shapefile = ", common_ids_.shape[0])

# Merge the two datasets based on FAC_ID, using a left-join so that all
# attributes in the location-explicit data (ST102) are retained
ab_fac3_['FAC_ID'] = ab_fac3_.FacilityID
ab_fac_merged = pd.merge(ab_fac2, ab_fac3_, on='FAC_ID', how='left')
ab_fac_merged.iloc[:, 0:10].head()
print(*ab_fac_merged.columns, sep='\n')

# Combine facilities gdf [ab_fac] with gas/liquids throughput data [fac_comb_receipts],
# so that some attributes from the receipts and disposition data (STB13B, STB13C)
# are associated with the relevant facility in the ST102 dataset
fac_comb_receipts["FAC_ID"] = fac_comb_receipts["Facility ID"]
ab_fac_merged2 = pd.merge(ab_fac_merged, fac_comb_receipts, on="FAC_ID", how='left')
ab_fac_merged2.head()

# Check data size
# `fac_comb_receipts` will be much smaller, since it only contains records
# of gas processing / handling facilities, while `ab_fac` contains other fac types
print(fac_comb_receipts.shape[0], ab_fac.shape[0], ab_fac_merged2.shape[0])


# Fix abbreviated facility TYPE descriptions
# -----------------------------------------------------------------------------
colName = "FacilityType"
dict_type = {
    'BT': 'Battery',
    np.nan: 'N/A',
    'CS': 'Compressor station',
    'CT': 'Custom treating facility',
    'GP': 'Gas plant',
    'GS': 'Gas gathering system',
    'IF': 'Injection/disposal facility',
    'MS': 'Metering station',
    'OS': 'Oil sands processing plant',
    'PL': 'Pipeline',
    'RF': 'Refinery',
    'WP': 'Waste plant',
    'WS': 'Water source',
    'TM': 'Tank terminal'
}

ab_fac_merged2 = replace_row_names(ab_fac_merged2,
                                   colName=colName,
                                   dict_names=dict_type)

# Inspect the FAC_STATUS values
ab_fac_merged2.FAC_STATUS.unique()

# Inspect the installation date to see its format
ab_fac_merged2.FacilityStartDate.unique()[0:7]

# Remove the leading asterisk in front of some FAC_NAME values.
ab_fac_merged2.FAC_NAME = ab_fac_merged2.FAC_NAME.str.lstrip('* ')

# =============================================================================
# %% TANK BATTERIES - PREPROCESSING
# =============================================================================
# *Batteries*
#  - Includes: multi-well group battery, effluent measurement battery, etc
#
# A system or arrangement of tanks or other surface equipment receiving
# flow-lined production from one or more wells. Batteries must provide for
# measurement and disposition of production and may:
#   • include equipment for separating production into oil, gas, and water,
#   • include storage equipment for produced liquids before disposition, and
#   • receive product from other facilities.
# Crude oil wells include heavy oil wells outside the AER-designated oil sands area.
# Heavy oil wells inside the AER-designated oil sands area are classified as
# crude bitumen and must be linked to and reported as part of a crude bitumen
# battery. See subtype codes 331, 341, 342, or 343.
#
# These are production facilities, and will be co-located with oil and gas
# wellheads, in some cases.
# =============================================================================

# Subset batteries from the catch-all facilities dataset
batteries_id = ['Battery']
ab_tanks_bat = ab_fac_merged2[ab_fac_merged2.FacilityType.isin(batteries_id)].reset_index()
print("Total number of tank batteries in Alberta = ", ab_tanks_bat.shape[0])


# Check if facility ID is in the IDs for the gathering and processing plant receipts data
# -----------------------------------------------------------------------------
ab_tanks_bat[ab_tanks_bat.FAC_ID.isin(list(fac_comb_receipts['Facility ID'].unique()))]


# =============================================================================
# %% TANK BATTERIES - INTEGRATION
# ===============================================================================
ab_tanks_bat3, ab_tanks_errors = integrate_facs(
    ab_tanks_bat,
    starting_ids=1,  # will eventually get over-written anyway
    category="Tank batteries",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="3,4",
    src_date="2024-04-01",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_SUB_TY",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="FacilityStartDate",
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="gas_thru_mmcfd",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_tanks_bat3,
    "canada_alberta_batteries_production_facilities",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% COMPRESSOR STATIONS - PREPROCESSING
# - Includes: gathering and transmission natural gas compressor stations
# =============================================================================
# Subset comp stations from the catch-all facilities dataset
_id = ['Compressor station']
ab_comps = ab_fac_merged2[ab_fac_merged2.FacilityType.isin(_id)].reset_index()
print(f"Total number of compressor stations in Alberta = {len(ab_comps)}")

# Check if facility ID is in the IDs for the gathering and processing plant receipts data
ab_comps[ab_comps.FAC_ID.isin(list(fac_comb_receipts['Facility ID'].unique()))]

# Check contents of other columns
ab_comps.FAC_STATUS.unique()
ab_comps.FacilityType.unique()

# =============================================================================
# %% COMPRESSOR STATIONS - INTEGRATION
# =============================================================================
ab_comps3, ab_comp_errors = integrate_facs(
    ab_comps,
    starting_ids=ab_tanks_bat3.OGIM_ID.iloc[-1] + 1,
    category="Natural gas compressor station",
    fac_alias="COMPR_PROC",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="3,4",
    src_date="2024-04-01",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_SUB_TY",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="FacilityStartDate",
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="gas_thru_mmcfd",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_comps3,
    "canada_alberta_compressor_stations",
    schema_def=True,
    schema=schema_COMPR_PROC,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% GATHERING AND PROCESSING PLANTS - PREPROCESSING
# =============================================================================
# Subset gas plants from the catch-all facilities dataset
_id = ['Gas plant',
       'Oil sands processing plant',
       'Custom treating facility',
       'Gas gathering system']

ab_proc = ab_fac_merged2[ab_fac_merged2.FacilityType.isin(_id)].reset_index()
print(f"Total number of gathering and processing facilities in Alberta = {len(ab_proc)}")


# Check if any gas plant facility IDs are in the IDs from the
# gathering and processing plant receipts dataset
# -----------------------------------------------------------------------------
ab_proc[ab_proc.FAC_ID.isin(list(fac_comb_receipts['Facility ID'].unique()))]


# Load in actual throughput data for gas processing plants as reported in ST50A,
# and give preference to these throughput values in the integrated output.
# https://www.aer.ca/providing-information/data-and-reports/statistical-reports/st50
# ===============================================================================
fpProc = "facilities\\AB_ST50A_GasPlant_Data.csv"
proc_data_ = pd.read_csv(fpProc,
                         skiprows=[0, 1, 2, 3, 4, 6],  # remove header rows
                         skipfooter=28)  # remove footer rows

# try:
#     # Attempt to read the CSV file
#     proc_data_ = pd.read_csv(fpProc).reset_index()
#     print(*proc_data_.columns, sep='\n')
# except pd.errors.ParserError:
#     # If a ParserError occurs, print the custom error message
#     print('Error parsing the input CSV. Did you remember to edit the original CSV to remove the subheader rows?')
#     print(f'File that threw the error: {fpProc}')


# Modifications to ST50A data:
# Replace thousand separators, exclude rows where throughput is blank, and
# add a FAC_ID attribute
# -----------------------------------------------------------------------------
proc_data_['Raw Gas E3m3/d'] = proc_data_['Raw Gas E3m3/d'].str.replace(',', '')
proc_data_ = proc_data_[~proc_data_['Raw Gas E3m3/d'].isin([' '])]
proc_data_['FAC_ID'] = proc_data_['Reporting Facility ID']

# Convert from 1e3 m3/d to mmcfd
proc_data_['throughput_mmcfd'] = [float(proc_data_['Raw Gas E3m3/d'].iloc[x]) * 1000 * m3_to_ft3 / 1e6 if proc_data_['Raw Gas E3m3/d'].iloc[x] != np.nan else -9999 for x in range(proc_data_.shape[0])]

# Check if all FAC_IDs reported in ST50A are present in the gathering & processing
# gdf that we created above (subsetted from the general facilities shapefie)
ids_new = list(proc_data_.FAC_ID.unique())
print(f"Length of FAC_IDs in ST50A proc_data = {len(ids_new)}")
print("# of FAC_IDs in ST50A proc_data that are in our general facilities dataset = ", ab_proc[ab_proc.FAC_ID.isin(ids_new)].shape[0])


# For gas plants with throughput info available in ST50A, replace their
# existing throughput value in the `ab_proc` gdf with the ST50A throughput value.
# Where ST50A doesn't have information, keep the throughput value we estimated
# from the receipts & dispositions dataset.
# -----------------------------------------------------------------------------
data_with_throughput_data = ab_proc[ab_proc.FAC_ID.isin(ids_new)]
data_without_new_throughput_data = ab_proc[~ab_proc.FAC_ID.isin(ids_new)]

data_without_new_throughput_data['throughput_mmcfd_2'] = data_without_new_throughput_data['gas_thru_mmcfd']

merged2_ = pd.merge(data_with_throughput_data,
                    proc_data_[['FAC_ID', 'throughput_mmcfd']],
                    on='FAC_ID',
                    how='left')
merged2_['throughput_mmcfd_2'] = merged2_['throughput_mmcfd']

# Concatenate the two gdfs (records with new throughput info from ST50A, and those without)
ab_proc_merged_2 = pd.concat([merged2_, data_without_new_throughput_data])
print("Total length of data in original merged dataset = {} compared with new merged dataset = {}".format(ab_proc.shape[0], ab_proc_merged_2.shape[0]))

# # What is the distribution of facility througput and does it make sense?
# get_ipython().run_line_magic('matplotlib', 'inline')
# print("Min and max MMcfd = ", [ab_proc_merged_2.throughput_mmcfd_2.min(), ab_proc_merged_2.throughput_mmcfd_2.max()])
# plot_ = plt.hist(ab_proc_merged_2.throughput_mmcfd_2, density=True)

# Check FAC_TYPE
ab_proc_merged_2.FAC_SUB_TY.unique()

# =============================================================================
# %% GATHERING AND PROCESSING PLANTS - INTEGRATION
# =============================================================================
ab_proc3, ab_proc_errors = integrate_facs(
    ab_proc_merged_2,
    starting_ids=ab_comps3.OGIM_ID.iloc[-1] + 1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="3,4,5,6,7,8",
    src_date="2024-04-01",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_SUB_TY",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="FacilityStartDate",
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="throughput_mmcfd_2",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_proc3,
    "canada_alberta_gathering_processing",
    schema_def=True,
    schema=schema_COMPR_PROC,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% TERMINALS - PREPROCESSING
# =============================================================================
# Subset terminals from the catch-all facilities dataset
_id = ['Tank terminal']
ab_term = ab_fac_merged2[ab_fac_merged2.FacilityType.isin(_id)].reset_index()
print(f"Total number of terminals in Alberta = {len(ab_term)}")

# Check if facility ID is in the IDs for the gathering and processing plant receipts data
ab_term[ab_term.FAC_ID.isin(list(fac_comb_receipts['Facility ID'].unique()))]


# =============================================================================
# %% TERMINALS - INTEGRATION
# =============================================================================
ab_term3, ab_term_errors = integrate_facs(
    ab_term,
    starting_ids=ab_proc3.OGIM_ID.iloc[-1] + 1,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="3,4",
    src_date="2024-04-01",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_SUB_TY",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="FacilityStartDate",
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="gas_thru_mmcfd",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_term3,
    "canada_alberta_petroleum_terminals",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% INJECTION AND DISPOSAL - PREPROCESSING
# =============================================================================
# Subset injection and disposal facs from the catch-all facilities dataset
_id = ['Injection/disposal facility']
ab_inj = ab_fac_merged2[ab_fac_merged2.FacilityType.isin(_id)].reset_index()
print("Total number of injection and disposal facilities in Alberta = ", ab_inj.shape[0])

# Check if facility ID is in the IDs for the gathering and processing plant receipts data
ab_inj[ab_inj.FAC_ID.isin(list(fac_comb_receipts['Facility ID'].unique()))]

# Check facility type designations
ab_inj.FAC_SUB_TY.unique()

# Are there production data for these datasets?
ab_inj.gas_thru_mmcfd.unique()

# =============================================================================
# %% INJECTION AND DISPOSAL - INTEGRATION
# =============================================================================
ab_inj3, ab_inj_errors = integrate_facs(
    ab_inj,
    starting_ids=ab_term3.OGIM_ID.iloc[-1] + 1,
    category="Injection and disposal",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="3,4",
    src_date="2024-04-01",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_SUB_TY",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="FacilityStartDate",
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="gas_thru_mmcfd",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_inj3,
    "canada_alberta_injection_disposal",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% REFINERIES - PREPROCESSING
# =============================================================================
# Subset data
_id = ['Refinery']
ab_ref = ab_fac_merged2[ab_fac_merged2.FacilityType.isin(_id)].reset_index()
print("Total number of refineries in Alberta = ", ab_ref.shape[0])

# Check if facility ID is in the IDs for the gathering and processing plant receipts data
ab_ref[ab_ref.FAC_ID.isin(list(fac_comb_receipts['Facility ID'].unique()))]

# throughput data?
ab_ref.gas_thru_mmcfd.unique()

# =============================================================================
# %% REFINERIES - INTEGRATION
# =============================================================================
ab_ref3, ab_ref_errors = integrate_facs(
    ab_ref,
    starting_ids=ab_inj3.OGIM_ID.iloc[-1] + 1,
    category="Crude oil refinery",
    fac_alias="REFINERY",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="3,4",
    src_date="2024-04-01",  # Daily and Monthly
    on_offshore="Onshore",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_SUB_TY",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="FacilityStartDate",
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd="gas_thru_mmcfd",
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    ab_ref3,
    "canada_alberta_crude_oil_refinery",
    schema_def=True,
    schema=schema_REFINERY,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% PIPELINE INSTALLATIONS - PREPROCESSING
#  - Data manual at https://static.aer.ca/prd/data/pipeline/EnhancedPipeline_Layout.pdf
#  - Actual .SHP data https://static.aer.ca/prd/data/pipeline/Pipeline_Installations_SHP.zip
#   - 'Compressor Station'
#   - 'Meter Station'
#   - 'Tank Farm'
#   - 'Pump Station'
#   - 'Meter Regulator Stn.'
#   - 'Regulator Station'
#   - 'Oil Terminal'
#   - 'Line Heater'
# =============================================================================
fp_pipe_instl = "pipelines\\Pipeline_Installations_SHP\\Pipeline_Installations_GCS_NAD83.shp"
ab_pipes_instl = read_spatial_data(fp_pipe_instl)

# Transform CRS to EPSG:4326
print("CRS of data = {}".format(ab_pipes_instl.crs))
ab_pipes_instl3 = transform_CRS(ab_pipes_instl, target_epsg_code='epsg:4326', appendLatLon=True)

# Attributes
print(*ab_pipes_instl3.columns, sep='\n')

# Check facility installation types
print(ab_pipes_instl3.INSTA_TYPE.unique())

# We know there to be a small number of duplicate records in this dataset, drop them
ab_pipes_instl3 = ab_pipes_instl3.drop_duplicates(subset=['LICINSTNO',
                                                          'INSTA_LIC',
                                                          'INSTA_TYPE',
                                                          'BA_ID',
                                                          'BA_NAME',
                                                          'PERM_APPR',
                                                          'PLINSTATUS',
                                                          'longitude_calc',
                                                          'latitude_calc'],
                                                  keep='first').reset_index()

# =============================================================================
# %% FROM INSTALLATIONS - COMP STATIONS
# Additional data for petroluem terminals along major pipelines in Alberta
# =============================================================================
comp2 = ab_pipes_instl3.query("INSTA_TYPE == 'Compressor Station'")
comp2.head()


# Create gdf
# ===============================================================================
ab_comp3r, ab_comp3r_errors = integrate_facs(
    comp2,
    starting_ids=ab_ref3.OGIM_ID.iloc[-1] + 1,
    category="Natural gas compressor station",
    fac_alias="COMPR_PROC",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="9",
    src_date="2024-04-17",  # Daily
    on_offshore="Onshore",
    fac_name=None,
    fac_id="LICINSTNO",
    fac_type="INSTA_TYPE",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status="PLINSTATUS",
    op_name="BA_NAME",
    commodity="SUBSTANCE1",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    ab_comp3r,
    "canada_alberta_compressor_stations_part_02",
    schema_def=True,
    schema=schema_COMPR_PROC,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% FROM INSTALLATIONS - STATIONS OTHER
# Meter station, pumping station, regulatory station, etc
# ===============================================================================
ab_stns_oth = ab_pipes_instl3.query("INSTA_TYPE == ['Meter Station', 'Pump Station', 'Meter Regulator Stn.', 'Regulator Station']")
ab_stns_oth.head()

# Create gdf
# ===============================================================================
ab_stns_oth3r, ab_stns_oth3r_errors = integrate_facs(
    ab_stns_oth,
    starting_ids=ab_comp3r.OGIM_ID.iloc[-1] + 1,
    category="Stations - other",
    fac_alias="OTHER",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="9",
    src_date="2024-04-17",  # Daily
    on_offshore="Onshore",
    fac_name=None,
    fac_id="LICINSTNO",
    fac_type="INSTA_TYPE",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status="PLINSTATUS",
    op_name="BA_NAME",
    commodity="SUBSTANCE1",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_stns_oth3r,
    "canada_alberta_stations_other",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% FROM INSTALLATIONS - TERMINALS
# Additional data for petroluem terminals along major pipelines in Alberta
# =============================================================================
ab_terms_02r = ab_pipes_instl3.query("INSTA_TYPE == ['Tank Farm', 'Oil Terminal']")
ab_terms_02r.head()

# Create gdf
# ===============================================================================
ab_terms3r, ab_terms3r_errors = integrate_facs(
    ab_terms_02r,
    starting_ids=ab_stns_oth3r.OGIM_ID.iloc[-1] + 1,
    category="Petroleum Terminals",
    fac_alias="LNG_STORAGE",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="9",
    src_date="2024-04-17",  # Daily
    on_offshore="Onshore",
    fac_name=None,
    fac_id="LICINSTNO",
    fac_type="INSTA_TYPE",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status="PLINSTATUS",
    op_name="BA_NAME",
    commodity="SUBSTANCE1",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    ab_terms3r,
    "canada_alberta_petroleum_terminals_part_02",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% FROM INSTALLATIONS - EQUIPMENT AND COMPONENTS
# =============================================================================
ab_lineheater = ab_pipes_instl3.query("INSTA_TYPE == 'Line Heater'")
ab_lineheater.head()


# Create gdf
# ===============================================================================
ab_LH3r, ab_LH3r_errors = integrate_facs(
    ab_lineheater.reset_index(),
    starting_ids=ab_terms3r.OGIM_ID.iloc[-1] + 1,
    category="Equipment and Components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="9",
    src_date="2024-04-17",  # Daily
    on_offshore="Onshore",
    fac_name=None,
    fac_id="LICINSTNO",
    fac_type="INSTA_TYPE",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status="PLINSTATUS",
    op_name="BA_NAME",
    commodity="SUBSTANCE1",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    ab_LH3r,
    "canada_alberta_equipment_components",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% EQUIPMENT AND COMPONENTS (CANVEC) - inspect in qgis
# =============================================================================
# Read CANVEC data fOR VALVES
valves_ = read_spatial_data("canvec_data\\canvec_50K_AB_Res_MGT\\valve_0.shp")
valves2_ = transform_CRS(valves_, appendLatLon=True)
# Specify facility type
valves2_['type_'] = "Valves"


# Integrate data
# ===============================================================================
ab_Eq3r, ab_Eq3_errors = integrate_facs(
    valves2_,
    starting_ids=ab_LH3r.OGIM_ID.iloc[-1] + 1,
    category="Equipment and Components",
    fac_alias="OTHER",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="10",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    fac_id="feature_id",
    fac_type="type_",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    ab_Eq3r,
    "canada_alberta_equipmnent_components_part_02",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% PIPELINES - PREPROCESSING
# AER's "Enhanced Pipeline Shapefile" -- includes major transportation pipelines
# =============================================================================
pipes_ = read_spatial_data("pipelines\\Pipelines_SHP\\Pipelines_GCS_NAD83.shp")
pipes_ab = transform_CRS(pipes_)
pipes_ab.columns

# Pipes diamaeter in mm
pipes_ab['out_diam_mm'] = [float(pipes_ab['OUT_DIAMET'].iloc[x]) for x in range(pipes_ab.shape[0])]

# calculate pipeline length automatically
pipes_ab = calculate_pipeline_length_km(pipes_ab, attrName='PIPELINE_LENGTH_KM')

# Drop duplicate pipeline features (because I know there are some)
pipes_ab = pipes_ab.drop_duplicates(subset=['LIC_LI_NO',
                                            'COMP_NAME',
                                            'SEG_STATUS',
                                            'FROM_FAC',
                                            'FROM_LOC',
                                            'TO_FAC',
                                            'TO_LOC',
                                            'OUT_DIAMET',
                                            'PIPE_TYPE',
                                            'PIPE_GRADE',
                                            'PIP_MATERL',
                                            'FLD_CTR_NM',
                                            'SUBSTANCE1',
                                            'SUBSTANCE2',
                                            'SUBSTANCE3',
                                            'geometry'],
                                    keep=False).reset_index(drop=True)

# =============================================================================
# %% PIPELINES - INTEGRATION
# =============================================================================
ab_pipes, errors_pipes = integrate_pipelines(
    pipes_ab,
    starting_ids=ab_Eq3r.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="222",
    src_date="2024-04-17",  # Daily
    on_offshore="Onshore",
    fac_name=None,
    fac_id="LIC_LI_NO",
    fac_type=None,
    install_date="LICAPPDATE",
    fac_status="SEG_STATUS",
    op_name="COMP_NAME",
    commodity="SUBSTANCE1",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm="out_diam_mm",
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material="PIP_MATERL"
)

save_spatial_data(
    ab_pipes,
    "canada_alberta_oil_natural_gas_pipelines",
    schema_def=False,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% DISTRIBUTION PIPELINES (aka rural pipelines)
# =============================================================================
# Add in detailed distribution pipeline data from rural areas, by concatenating
# individual geodatabase contents into a single GDF
fp3 = "pipelines"
files_ = glob.glob(fp3 + "\\*.gdb")
pipes_rural = []

for pipe in tqdm(files_):
    gdf_ = gpd.read_file(pipe)
    pipes_rural.append(gdf_)

# Concatenate
pipes_rural_all = pd.concat(pipes_rural).reset_index()

# Transform CRS to EPSG:4326 -- takes ~4 minutes to run on all these pipelines
# -----------------------------------------------------------------------------
print("CRS of data = {}".format(pipes_rural_all.crs))
ab_pipes_rural = transform_CRS(pipes_rural_all, target_epsg_code='epsg:4326')

# Calculate pipeline length in km
ab_pipes_rural_v2 = calculate_pipeline_length_km(ab_pipes_rural)

# Add info here for pipeline type to distribution pipeline
ab_pipes_rural_v2["type_"] = 'Natural gas distribution'

# Some pipelines (Private Gas Distribution lines) have Year info. Reformat it.
ab_pipes_rural_v2['Year'] = ab_pipes_rural_v2.Year.astype(str)
ab_pipes_rural_v2.loc[ab_pipes_rural_v2.Year == ' ', 'Year'] = np.nan
ab_pipes_rural_v2.loc[ab_pipes_rural_v2.Year == 'None', 'Year'] = np.nan
ab_pipes_rural_v2.loc[ab_pipes_rural_v2.Year == 'nan', 'Year'] = np.nan

# Turn two-digit years (and four-digit years) into full dates
mask2 = (ab_pipes_rural_v2.Year.str.len() == 2)
ab_pipes_rural_v2.loc[mask2, 'Year'] = '19' + ab_pipes_rural_v2['Year'] + '-01-01'
mask4 = (ab_pipes_rural_v2.Year.str.len() == 4)
ab_pipes_rural_v2.loc[mask4, 'Year'] = ab_pipes_rural_v2['Year'] + '-01-01'

# Most pipelines in Alberta have the OPERATOR value "ATCO NATURAL GAS
# DISTRIBUTION CUSTOMER CORRESPONDENCE", which doesn't read like a company name.
# Change these values to simply "ATCO", one of the natural gas distrib companies in Alberta
ab_pipes_rural_v2.Utility_Name.replace(
    {'ATCO Natural Gas Distribution Customer Correspondence': 'ATCO'},
    inplace=True)


# Drop duplicate pipeline features (because I know there are some)
ab_pipes_rural_v2 = ab_pipes_rural_v2.drop_duplicates(subset=['License',
                                                              'Year',
                                                              'Status',
                                                              'Utility_Name',
                                                              'Diameter',
                                                              'Pipe_Material',
                                                              'geometry'],
                                                      keep=False).reset_index(drop=True)


# =============================================================================
# %% RURAL PIPES - INTEGRATION
# Takes 15+ minutes to run
# =============================================================================
ab_pipes_rural, errors_pipes = integrate_pipelines(
    ab_pipes_rural_v2.reset_index(),
    starting_ids=ab_pipes.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="11",
    src_date="2023-12-15",  # based on 'publication date' on metadata website
    on_offshore="Onshore",
    fac_name=None,
    fac_id="License",
    fac_type="type_",
    install_date="Year",
    fac_status="Status",
    op_name='Utility_Name',
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm="Diameter",
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material="Pipe_Material"
)

save_spatial_data(
    ab_pipes_rural,
    "canada_alberta_oil_natural_gas_pipelines_part_02_distribution",
    schema_def=False,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% Geological oil plays - DO NOT INCLUDE
# =============================================================================
# # Alberta play areas
# shp_fields = "oil_gas_play_outline\\AER_Play_Areas.shp"
# fields_ = read_spatial_data(shp_fields, table_gradient=True)
# fields_.columns

# # Calculate play area
# fields3 = calculate_basin_area_km2(fields_, attrName="AREA_KM2")
# fields3.head()
# fields3.columns

# # Integrate
# ab_plays, ab_errors_ = integrate_basins(
#     fields3,
#     starting_ids=ab_pipes_rural.OGIM_ID.iloc[-1] + 1,
#     category="Oil and natural gas plays",
#     fac_alias="OIL_GAS_BASINS",
#     country="Canada",
#     state_prov="Alberta",
#     src_ref_id="221",
#     src_date="2019-08-07",   # based on 'Change_Dat' field in original dataset
#     on_offshore="Onshore",
#     _name="Play_Area",
#     reservoir_type="Res_Type",
#     op_name=None,
#     _area_km2="AREA_KM2"
# )


# save_spatial_data(
#     ab_plays,
#     "canada_alberta_oil_natural_gas_plays",
#     schema_def=False,
#     schema=schema_BASINS,
#     file_type="GeoJSON",
#     out_path=integration_out_path
# )


# =============================================================================
# %% PRODUCTION DATA -  # TODO
# =============================================================================
# # Reading location data from Microsoft Access database
# # This data came from Scott Seymour, and represents the lat/lon centroid of the smallest grid in the Alberta DLS grid system
# # ===================================================
# tableNamesIdx, tableNames, dfs = read_msAccess("latlong_dls_\\LatLong_DLS_Trim.accdb")

# # Assign facility location attribute based on `Alberta_DL` attribute
# # ===================================================
# dfs[0]['ReportingFacilityLocation'] = dfs[0]['Alberta_DL']

# dfs[0].head()


# # Script for looping through each .CSV for 2021 and extracting production data
# # ===================================================
# fpp = r"petrinex_data\manual_downloads\unzipped_files"
# files_ = glob.glob(fpp + "/*2021*.CSV")
# all_prod_ = []

# # Loop through each file, extracting production data for OIL [m3], GAS [1e3 m3], WATER [m3], and CONDENSATE [m3]

# for file in trange(len(files_)):
#     df = pd.read_csv(files_[file])

#     df['Volume'] = [np.float(df.Volume.iloc[x]) if df.Volume.iloc[x] != "***" else 0 for x in range(df.shape[0])]
#     # Group by reporting facility ID
#     # ===================================================
#     aggFns4 = {
#         'ProductionMonth':'first',
#         'OperatorName':'first',
#         'ReportingFacilityProvinceState':'first',
#         'ReportingFacilityType':'first',
#         'ReportingFacilityIdentifier':'first',
#         'ReportingFacilityName':'first',
#         'ReportingFacilitySubType':'first',
#         'ReportingFacilitySubTypeDesc':'first',
#         'ReportingFacilityLocation':'first',
#         'FacilityLegalSubdivision':'first',
#         'FacilitySection':'first',
#         'FacilityTownship':'first',
#         'FacilityRange':'first',
#         'FacilityMeridian':'first',
#         'Volume':'sum'
#         }
#     # ===================================================
#     fac_ = df.groupby(by=['ReportingFacilityID', 'ActivityID', 'ProductID']).agg(aggFns4)

#     # Query for only production data and for water, gas, oil, and condensate production
#     # ===================================================
#     fac_2 = fac_.query("(ActivityID == 'PROD') & (ProductID == ['WATER', 'GAS', 'OIL', 'COND'])")

#     # ===================================================
#     # Pivot table focusing on `Volume` values
#     data_fac2 = fac_2[['Volume']].reset_index()
#     data_fac22 = pd.pivot_table(data_fac2, values='Volume', index='ReportingFacilityID', columns=['ProductID']).reset_index()

#     # Merge with original data based on fac ID
#     # ===================================================
#     data_in = fac_.reset_index()
#     data_in2 = data_in.drop_duplicates(subset=['ReportingFacilityID'])[['ProductionMonth','OperatorName',
#                                                                    'ReportingFacilityID', 'ReportingFacilityProvinceState',
#                                                                    'ReportingFacilityType', 'ReportingFacilityIdentifier',
#                                                                    'ReportingFacilityName', 'ReportingFacilitySubType',
#                                                                    'ReportingFacilitySubTypeDesc', 'ReportingFacilityLocation',
#                                                                    'FacilityLegalSubdivision', 'FacilitySection', 'FacilityTownship',
#                                                                    'FacilityRange', 'FacilityMeridian']]
#     # Production data
#     # ===================================================
#     data_out = pd.merge(data_in2, data_fac22, on='ReportingFacilityID', how='left')

#     # # Standardize LSD coordinates
#     df_listOUT = standardize_DLS_AB(data_out,'FacilityLegalSubdivision','FacilitySection', 'FacilityTownship', 'FacilityRange',
#        'FacilityMeridian' )

#     # Reformat facility location
#     data_out['ReportingFacilityLocation'] = df_listOUT['FacilityLocation']

#     # Then merge the results with the data_out
#     prod_data_ = pd.merge(data_out, dfs[0], on='ReportingFacilityLocation', how='left')

#     # Exclude NULLS
#     prod_data_ = prod_data_[~prod_data_.Longitude.isnull()]

#     # Create a GEODATAFRAME
#     prod_gdf = gpd.GeoDataFrame(prod_data_, geometry=gpd.points_from_xy(prod_data_.Longitude, prod_data_.Latitude), crs="epsg:4326")

#     all_prod_.append(prod_gdf)


# # In[188]:


# # Concatenate files
# # ===========================================================================
# ab_prod_data = pd.concat(all_prod_)

# # Then group by facility ID and sum up annual production
# aggFns5 = {
#     'OperatorName':'first',
#     'ReportingFacilityProvinceState':'first',
#     'ReportingFacilityType':'first',
#     'ReportingFacilityIdentifier':'first',
#     'ReportingFacilityName':'first',
#     'ReportingFacilitySubType':'first',
#     'ReportingFacilitySubTypeDesc':'first',
#     'ReportingFacilityLocation':'first',
#     'FacilityLegalSubdivision':'first',
#     'FacilitySection':'first',
#     'FacilityTownship':'first',
#     'FacilityRange':'first',
#     'FacilityMeridian':'first',
#     'Longitude':'first',
#     'Latitude':'first',
#     'GAS':'sum',
#     'OIL':'sum',
#     'COND':'sum',
#     'WATER':'sum'
#     }

# ab_prod_data_ = ab_prod_data.groupby(by='ReportingFacilityID').agg(aggFns5)
# ab_prod_data_.head()


# # In[189]:


# # Use commonly-used units
# # ===========================================================================

# ab_prod_data_ = ab_prod_data_.reset_index()
# ab_prod_data_['GAS_Mcf'] = ab_prod_data_['GAS']*35.314666721
# ab_prod_data_['OIL_BBL'] = ab_prod_data_['OIL']*6.2898 #https://apps.cer-rec.gc.ca/Conversion/conversion-tables.aspx?GoCTemplateCulture=fr-CA
# ab_prod_data_['WATER_BBL'] = ab_prod_data_['WATER']*6.2898
# ab_prod_data_['CONDENSATE_BBL'] = ab_prod_data_['COND']*6.2898
# ab_prod_data_['Prod_Year'] = 2021

# # ===========================================================================


# # In[190]:


# # Convert to a GeoDataFrame
# # ===========================================================================
# ab_prod_data2_ = gpd.GeoDataFrame(ab_prod_data_, geometry=gpd.points_from_xy(ab_prod_data_.Longitude, ab_prod_data_.Latitude), crs="epsg:4326")


# # In[191]:


# ab_prod_data2_.columns


# # In[192]:


# ab_prod_data2_.head()


# # In[193]:


# ab_prod1, ab_prod_errors = integrate_production(
#     ab_prod_data2_,
#     starting_ids=ab_plays.OGIM_ID.iloc[-1]+1,
#     category="Oil and natural gas production",
#     fac_alias = "OIL_GAS_PROD",
#     country="Canada",
#     state_prov="Alberta",
#     src_ref_id="3",
#     src_date="2022-04-09",
#     on_offshore="Onshore",
#     fac_name="ReportingFacilityName",
#     fac_id="ReportingFacilityID",
#     fac_type="ReportingFacilitySubTypeDesc",
#     spud_date=None,
#     comp_date=None,
#     drill_type=None,
#     fac_status=None,
#     op_name="OperatorName",
#     oil_bbl="OIL_BBL",
#     gas_mcf='GAS_Mcf',
#     water_bbl='WATER_BBL',
#     condensate_bbl='CONDENSATE_BBL',
#     prod_days=None,
#     prod_year="Prod_Year",
#     entity_type='Facility',
#     fac_latitude='Latitude',
#     fac_longitude='Longitude'
#     )


# # In[194]:


# # Save results
# save_spatial_data(
#     ab_prod1,
#     "canada_alberta_oil_natural_gas_production",
#     schema_def=True,
#     schema=schema_OIL_GAS_PROD,
#     file_type="GeoJSON",
#     out_path="results\\"
#     )
