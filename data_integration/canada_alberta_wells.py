# -*- coding: utf-8 -*-
"""
Created on Tue Jul 25 15:56:37 2023

Data integration of Alberta, Canada OGIM data -- WELLS
Based extensively on Mark's previous code:
    `Refresh_and_Data_Integration_Canada_Alberta.ipynb`

@author: maobrien, momara
"""
import os
import pandas as pd
import numpy as np
import geopandas as gpd
# from tqdm import trange

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import (replace_row_names,
                     transform_CRS, integrate_facs, save_spatial_data,
                     schema_WELLS, read_spatial_data, check_invalid_geoms, explode_multi_geoms)

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
# %% Define custom functions
# =============================================================================


# Create temporary new fields to contain coords from the geometry column
def keep_records_with_matching_latlong(gdf, lat_col, long_col):
    """Retain gdf records where geometry point values matches specified lat-long column values.

    Parameters
    ----------
    gdf : geopandas geodataframe
        GeoDataFrame containing point-type geometries;
        must have latitude and longitude values in separate columns.
    lat_col : str
        gdf column containing latitude coordinate of point;
        column must be of numeric type.
    long_col : str
        gdf column containing longitude coordinate of point;
        column must be of numeric type.

    Returns
    -------
    output : geopandas geodataframe
        Subset of records from input `gdf`.

    """
    # Create temporary columns to contain the X and Y coordinate values present
    # in the `geometry` field
    gdf['lat_temp'] = gdf['geometry'].y
    gdf['long_temp'] = gdf['geometry'].x

    # Compare the record's latitude "according to its attribute column" to the
    # record's latitude in the geometry column; do the same with longitude.
    # If the "attribute" coordinate and the "geometry" coordinate are
    # nearly-identical, mark the record with the boolean value 'True'
    gdf['keepme'] = False
    cond_statement = (np.isclose(gdf['long_temp'], gdf[long_col], rtol=1e-07, atol=0) & np.isclose(gdf['lat_temp'], gdf[lat_col], rtol=1e-07, atol=0))
    gdf.loc[cond_statement, 'keepme'] = True

    # Filter gdf to only keep records marked as 'True'; drop temporary columns
    # before returning the function.
    output = gdf[gdf.keepme]
    output = output.drop('lat_temp', axis=1)
    output = output.drop('long_temp', axis=1)
    output = output.drop('keepme', axis=1)

    return output


# =============================================================================
# %% WELLS: Read in data, merge surface and bottom holes
# =============================================================================
# Read Surface Holes Data as well as Bottom Holes Data and merge the two datasets
fp_sh = "wells//ST37_SH//ST37_SH_GCS_NAD83.shp"
fp_bh = "wells//ST37_BH//ST37_BH_GCS_NAD83.shp"

print(":::: reading surface holes dataset :::: ")
wells_sh_ab = read_spatial_data(fp_sh)
print(":::: reading bottom holes dataset :::: ")
wells_bh_ab = read_spatial_data(fp_bh)

# =============================================================================
# Remove duplicate records in the surface hole dataset
# if record has identical Licence, CompName, Latitude, Longitude, and LicStatDat
# =============================================================================
print("Surface: Unique licenses versus total records (before deduplicating)")
print([len(wells_sh_ab.Licence.unique()), wells_sh_ab.shape[0]])

# Display an example of duplicated licenses for surface holes
wells_sh_ab = wells_sh_ab.drop_duplicates(subset=['Licence',
                                                  'CompName',
                                                  'Latitude',
                                                  'Longitude',
                                                  'LicStatDat'],
                                          keep='first')

print("Surface: Unique licenses versus total records (AFTER deduplicating)")
print([len(wells_sh_ab.Licence.unique()), wells_sh_ab.shape[0]])

# =============================================================================
# Remove duplicate License records in the bottom hole dataset,
# and keeping the most recently updated record (i.e., newest status date)
# =============================================================================
print("Bottom Unique licenses versus total records (before deduplicating)")
print([len(wells_bh_ab.License.unique()), wells_bh_ab.shape[0]])

wells_bh_ab = wells_bh_ab.sort_values(by=['License', 'StatDate'],
                                      ascending=[True, False],
                                      na_position='last')

wells_bh_ab = wells_bh_ab.drop_duplicates(subset=['License'], keep='first')

print("Bottom Unique licenses versus total records (AFTER deduplicating)")
print([len(wells_bh_ab.License.unique()), wells_bh_ab.shape[0]])

# =============================================================================
# Transform Multi-part geometries to single-part, and check for invalid geometries
# =============================================================================
wells_sh_ab1 = explode_multi_geoms(wells_sh_ab)
wells_bh_ab1 = explode_multi_geoms(wells_bh_ab)
print("Original feature count and new count: surface holes = ", [wells_sh_ab.shape[0], wells_sh_ab1.shape[0]])
print("Original feature count and new count: bottom holes = ", [wells_bh_ab.shape[0], wells_bh_ab1.shape[0]])

sh_null_geoms, sh_list = check_invalid_geoms(wells_sh_ab1, id_attr="Licence")
bh_null_geoms, bh_list = check_invalid_geoms(wells_bh_ab1, id_attr="License")

# =============================================================================
# Check for duplicate Licence records that were created from "exploding" geometries.
# There are some cases where the POINT geometry doesn't match the reported
# Lat and Long column value; keep the copy of the record that DOES match.
# =============================================================================
surface_dupes = len(wells_sh_ab1[wells_sh_ab1.duplicated(subset=['Licence'], keep=False)])
print(f"Num. of surface records with a newly-added duplicate Licence: {surface_dupes}")
bottom_dupes = len(wells_bh_ab1[wells_bh_ab1.duplicated(subset=['License'], keep=False)])
print(f"Num. of bottom records with a newly-added duplicate License value: {bottom_dupes}")

wells_sh_ab2 = keep_records_with_matching_latlong(wells_sh_ab1,
                                                  'Latitude',
                                                  'Longitude')
print("Surface: Unique licenses versus total records (AFTER deduping)")
print([len(wells_sh_ab2.Licence.unique()), wells_sh_ab2.shape[0]])

wells_bh_ab2 = keep_records_with_matching_latlong(wells_bh_ab1,
                                                  'BH_Lat',
                                                  'BH_Long')
print("Bottom: Unique licenses versus total records (AFTER deduping)")
print([len(wells_bh_ab2.License.unique()), wells_bh_ab2.shape[0]])

# Remove remaining instances of duplicate Licence values
# (while still confirming that the record's other attribs are identical)
wells_sh_ab2 = wells_sh_ab2.drop_duplicates(subset=['Licence',
                                                    'CompName',
                                                    'Latitude',
                                                    'Longitude',
                                                    'LicStatDat'],
                                            keep='first')

wells_bh_ab2 = wells_bh_ab2.drop_duplicates(subset=['License',
                                                    'Operator',
                                                    'BH_Lat',
                                                    'BH_Long',
                                                    'LicDate'],
                                            keep='first')

print("Surface: Unique licenses versus total records (AFTER deduping)")
print([len(wells_sh_ab2.Licence.unique()), wells_sh_ab2.shape[0]])
print("Bottom: Unique licenses versus total records (AFTER deduping)")
print([len(wells_bh_ab2.License.unique()), wells_bh_ab2.shape[0]])


# =============================================================================
# Merge the bottom and surface hole datasets based on `License` field,
# ultimately creating a new geodataframe
# =============================================================================
wells_sh_ab = transform_CRS(wells_sh_ab2, appendLatLon=True)
wells_bh_ab = transform_CRS(wells_bh_ab2, appendLatLon=True)

# Note the different spellings for `Licence` and `License` in the
# bottom hole and surface hole datasets
wells_sh_ab['License'] = wells_sh_ab['Licence']

# Merge surface holes and bottom holes into a single dataframe
ab_merged_wells = pd.merge(wells_sh_ab, wells_bh_ab, on='License', how='left')
print("=========================")
print("Total # of records in merged dataset = {}".format(int(ab_merged_wells.shape[0])))
print("=========================")
print(ab_merged_wells.columns)

# Create GeoDataFrame from DataFrame

ab_merged_wells2 = gpd.GeoDataFrame(ab_merged_wells,
                                    geometry=gpd.points_from_xy(ab_merged_wells.longitude_calc_x,
                                                                ab_merged_wells.latitude_calc_x),
                                    crs="epsg:4326")


# =============================================================================
# %% WELLS: Merge well locations with infrastructure attributes from Petrinex.
# "Well Infrastructure" file from Petrinex contains additional attributes for:
# Drilling trajectory, well status, facility description, etc.
# =============================================================================
fpInfra_ = "petrinex_data\\Well Infrastructure-AB.CSV"
ab_wellData_ = pd.read_csv(fpInfra_)

# Then change the LicenceNumber column to Licence
ab_wellData_['License'] = ab_wellData_['LicenceNumber']
# ===========================================================================
# Only do the unique Licence IDs
ab_wellData_Licenses = ab_wellData_.License.unique()
print("Number of unique licences = ", len(ab_wellData_Licenses), " compared with length of original data =", ab_wellData_.shape[0])
# ===========================================================================

# Drop duplicate Licences
ab_wellData_sel = ab_wellData_.drop_duplicates(subset='License', keep='last')

# Then merge the data based on the `License` attribute, keeping the orginal data with geometry attributes
ab_wells_merged_ = pd.merge(ab_merged_wells2, ab_wellData_sel, on='License', how='left')
# ===========================================================================
print("Total data in ab_wells_merged = ", [ab_merged_wells2.shape[0], ab_wells_merged_.shape[0]])
print(ab_wells_merged_.columns)
ab_wells_merged_.iloc[:, 0:16].head()


# =============================================================================
# %% Fix abbreviations in facility types, facility status, etc., Date formats: *YYYY-MM-DD*
# Codes are described at https://static.aer.ca/prd/documents/sts/St37-Listofwellslayout.pdf
# =============================================================================

# Well types
# -----------------------------------------------------------------------------
ab_wells_merged_.WellStatusType.unique()
dict_status = {
    "OBSERV": "OBSERVATION",
    "FARM": "FARM",
    "INJ": "INJECTION",
    "DISP": "DISPOSAL",
    "STOR": "STORAGE",
    "INDUS": "INDUSTRIAL",
    "SOURCE": "SOURCE",
    "TRAING": "TRAINING",
    "CYCL": "CYCLICAL",
    "CAVERN": "LINKED TO A CAVERN",
    "SAGD": "STEAM ASSISTED GRAVITY DRAINAGE",
    "N/A": "N/A",
    np.nan: "N/A"
}

# Fix abbreviated attributes
ab_wells_merged_ = replace_row_names(
    ab_wells_merged_,
    colName="WellStatusType",
    dict_names=dict_status
)


# Check date format
# -----------------------------------------------------------------------------
ab_wells_merged_.SpudDate.unique()
ab_wells_merged_.FinishedDrillDate.unique()


# Drill types
# -----------------------------------------------------------------------------
ab_wells_merged_.HorizontalDrill.unique()

# Define attributes
dict_drill = {
    "HORIZONTAL": "HORIZONTAL",
    "Horizontal": "HORIZONTAL",
    "VERTICAL": "VERTICAL",
    "NATURAL DRIFT": "NATURAL DRIFT",
    "DIRECTIONAL": "DIRECTIONAL",
    "SLANT": "SLANT",
    "Vertical": "VERTICAL",
    "Directional": "DIRECTIONAL",
    "Slant": "SLANT",
    np.nan: "N/A"
}

# Fix abbreviated drilling configuration
ab_wells_merged_ = replace_row_names(
    ab_wells_merged_,
    colName="HorizontalDrill",
    dict_names=dict_drill
)


# Well status
# -----------------------------------------------------------------------------
print(ab_wells_merged_.WellStatusMode.unique())

# Define attributes
dict_status = {
    "ABAN": "ABANDONED",
    "SUSP": "SUSPENDED",
    "ABZONE": "ABANDONED ZONE",
    "PUMP": "PUMPING",
    "ABRENT": "ABANDONED AND RE-ENTERED",
    "FLOW": "FLOWING",
    "ABDWHP": "ABANDONED AND WHIPSTOCKED",
    "DRL & C": "DRILLED AND CASED",
    "J & A": "JUNKED AND ABANDONED",
    "GASLFT": "GAS LIFT",
    "CLOSED": "CLOSED",
    "TEST": "TESTING",
    "Tstcmp": "TEST COMPLETED",
    "POT": "POTENTIAL",
    "D & COMP": "DRILLING AND COMPLETING",
    "PRESET": "PRESET",
    np.nan: "N/A"
}

# Fix status descriptions
ab_wells_merged_ = replace_row_names(
    ab_wells_merged_,
    colName="WellStatusMode",
    dict_names=dict_status
)


# =============================================================================
# %% WELLS INTEGRATION
# =============================================================================
# Inspect important attributes for integration
ab_wells_merged_[["WellName",
                  "WellID",
                  "WellStatusType",
                  "SpudDate",
                  "FinishedDrillDate",
                  "HorizontalDrill",
                  "WellStatusMode",
                  "CompName",
                  "latitude_calc_x",
                  "longitude_calc_x",
                  "geometry"]].head(10)


# Check unique status and type
print("Unique status: ", ab_wells_merged_.WellStatusMode.unique())
print("============================")
print("Unique type: ", ab_wells_merged_.WellStatusType.unique())


ab_wells, ab_wells_errors = integrate_facs(
    ab_wells_merged_,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="Alberta",
    src_ref_id="1,2,3",
    src_date="2024-04-01",  # Monthly
    on_offshore="Onshore",
    fac_name="WellName",
    fac_id="WellID",
    fac_type="WellStatusType",
    spud_date="SpudDate",
    comp_date="FinishedDrillDate",
    drill_type="HorizontalDrill",
    install_date=None,
    fac_status="WellStatusMode",
    op_name="CompName",
    commodity=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc_x",
    fac_longitude="longitude_calc_x"
)


# Check again for any invalid geometries
# =================================================
sh_null_geoms_gdf, sh_list_gdf = check_invalid_geoms(ab_wells, id_attr="OGIM_ID")


# Save data
# ===========================================================================
save_spatial_data(
    ab_wells,
    "canada_alberta_oil_natural_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% Alberta abandoned wells data - DON'T INTEGRATE, duplicates of what's
# already in ST37_SH and ST37_BH
# =============================================================================

# Read data
# ab_aban = read_spatial_data("wells\\Abandoned Well Map_GCS_NAD83.shp")

# How many of the records in this shapefile are repeats of what's already
# present in Alberta's ST37_SH or ST37_BH dataset?
# If Abandoned Well shapefile seems to largely repeat records found in
# ST37 dataset, don't integrate it.
# print('Licence records in the Abandoned Wells SHP that are already present in dataset above:')
# print(ab_aban.Licence.isin(ab_wells_merged_.Licence).value_counts())
# True     228303
# False         1

# # Transform CRS
# ab_aban2 = transform_CRS(ab_aban, appendLatLon=True)

# # Check status
# ab_aban2.Status.unique()

# # Check for any NULL geometries
# ab_null_geoms, ab_list = check_invalid_geoms(ab_aban2, id_attr="Licence")


# # Integrate well-level data
# # ===========================================================================
# ab_wells_ab, ab_wells_errors_ab = integrate_facs(
#     ab_aban2,
#     starting_ids=ab_wells.OGIM_ID.iloc[-1] + 1,
#     category="Abandoned oil and natural gas wells",
#     fac_alias="WELLS",
#     country="Canada",
#     state_prov="Alberta",
#     src_ref_id="23",
#     src_date="2022-04-09",
#     on_offshore="Onshore",
#     fac_name=None,
#     fac_id="Licence",
#     fac_type=None,
#     spud_date=None,
#     comp_date=None,
#     drill_type=None,
#     install_date=None,
#     fac_status="Status",
#     op_name="Licensee",
#     commodity=None,
#     liq_capacity_bpd=None,
#     liq_throughput_bpd=None,
#     gas_capacity_mmcfd=None,
#     gas_throughput_mmcfd=None,
#     num_compr_units=None,
#     num_storage_tanks=None,
#     site_hp=None,
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )


# # Save data
# # ===========================================================================
# save_spatial_data(
#     ab_wells_ab,
#     "canada_alberta_abandoned_oil_natural_gas_wells",
#     schema_def=True,
#     file_type="GeoJSON",
#     out_path="results\\"
# )
