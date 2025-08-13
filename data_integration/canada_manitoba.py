# -*- coding: utf-8 -*-
"""
Created on Tue Aug  8 17:43:19 2023

Data integration of Manitoba, Canada OGIM data
Based extensively on Mark's previous code:
    `OGIM_Data_Integration_Canada_Manitoba-v1.2.ipynb`

@author: maobrien, momara
"""
import os
import pandas as pd
# import numpy as np
import geopandas as gpd
# from tqdm import trange
import glob

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     read_spatial_data, save_spatial_data, schema_WELLS,
                     schema_LNG_STORAGE, schema_COMPR_PROC, schema_OTHER,
                     calculate_pipeline_length_km,
                     integrate_pipelines, schema_PIPELINES,
                     calculate_basin_area_km2, schema_BASINS, integrate_basins)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'canada', 'manitoba'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% WELLS
# !!! As of Aug 2023, attributes that were previously in this shapefile aren't
# in this file anymore? (DEVIATION, COMPANY). Though these attributes are still
# present in Manitoba's online GIS viewer https://rdmaps.gov.mb.ca/Html5Viewer/index.html?viewer=MapGallery_petroleum.MapGallery
# =============================================================================
fp = "wells//mb_wells.shp"
mb_wells = read_spatial_data(fp, table_gradient=True)

mb_wells.crs
mb_wells.columns

# Check facility status
mb_wells.MAP_STATUS.unique()

# Check well trajectory
# mb_wells.DEVIATION_.unique()

# Map deviation data
# mb_wells2 = replace_row_names(mb_wells, "DEVIATION_", {'V':'Vertical', 'H':'Horizontal', 'D':'Directional'})

# Transform CRS
mb_wells = transform_CRS(mb_wells, appendLatLon=True)


mb_wells2, _ = integrate_facs(
    mb_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="50",
    src_date="2024-02-21",  # from webpage, updated roughly quarterly?
    on_offshore="Onshore",
    # fac_name="NAME",
    fac_id="LICENCE",
    fac_type=None,
    spud_date=None,
    comp_date=None,
    # drill_type="DEVIATION_",
    install_date=None,
    fac_status="MAP_STATUS",
    # op_name="COMPANY",
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


# GeoJSON
save_spatial_data(
    mb_wells2,
    file_name="canada_manitoba_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% Read in the Manitoba LSD grid centroid shapefile created by Mark
# 'LOCATION' attribute of these points is in the format 00-00-000-00W0
# Original easting and northing locations are in EPSG:3158 (a.k.a. NAD83(CSRS) / UTM zone 14N)
# Note that because the lat and lon are approximated based on the grid system [each grid is ~400mx400m]
# =============================================================================
data_grids_gdf = read_spatial_data("manitoba_grids\\manitoba_grids_LSD_.shp")

# TODO - add code that creates centroids from these LSD grid squares,
# rather than import a shapefile that's been manipulated in ways that aren't
# documented.

# =============================================================================
# %% Read in general facilities file (Petrinex)
# Data source: https://www.petrinex.ca/PD/Pages/MBPD.aspx
# =============================================================================
fac_data_mb = pd.read_csv(r"petrinex_data\MB_PetrinexData-mb_facilities_ident.csv")

# Extract the two characters in the FacilityCode attribute that indicate the
# facility's type, and save these characters in a new column.
# Then, based on the code definitions in the Petrinex PRAFacilityCodes_mb.csv
# file, replace the abbreviations with a full facility description.
fac_codes = []
for idx, row in fac_data_mb.iterrows():
    code_ = row.FacilityCode[2:4]
    fac_codes.append(code_)

fac_data_mb['FacilityCodeId'] = fac_codes
print(fac_data_mb.FacilityCodeId.unique())

fac_codes_id = {
    'BT': 'Battery',
    'GS': 'Gas Gathering System',
    'IF': 'Injection/Disposal System',
    'PL': 'Pipeline',
    'TM': 'Tank Terminal',
    'WP': 'Waste Plant',
    'WT': 'Fresh Water Source'
}
fac_data_mb2 = replace_row_names(fac_data_mb, "FacilityCodeId", fac_codes_id)


# Merge the facility records with the LSD centroid location points,
# based on the LOCATION attribute in both dataframes.
locs_with_fac = data_grids_gdf[data_grids_gdf.LOCATION.isin(fac_data_mb2.Location)]
print("Number of LSD centroids that correspond with a facility location in the Petrinex facilities df = ", locs_with_fac.shape[0])

fac_data_mb2['LOCATION'] = fac_data_mb2['Location']  # change attribute name for merging
mb_fac_merged = pd.merge(fac_data_mb2,
                         data_grids_gdf,
                         on='LOCATION',
                         how='left')
print(f'Total number of facilities originally = {len(fac_data_mb)}')
print(f'Total number of rows in merged facility dataset = {len(mb_fac_merged)}')

# REMOVE any infrastructure records which did NOT get matched to a location
mb_fac_merged = mb_fac_merged[mb_fac_merged.EASTING.notna()]

# TODO - for now, drop the last duplicate records randomly
mb_fac_merged = mb_fac_merged.drop_duplicates(subset=['FacilityCode',
                                                      'FacilityName',
                                                      'Location',
                                                      'OperationalStatus',
                                                      'OperatorName',
                                                      'FacilityCode']).reset_index()

# Finally, convert the merged facilities dataframe into a geodataframe,
# retaining only the columns that I need
cols_select = [
    'FacilityCode',
    'FacilityName',
    'Location',
    'LicenseNumber',
    'OperationalStatus',
    'OperationalStatusDate',
    'OperatorId',
    'OperatorName',
    'FacilityCodeId',
    'EASTING',
    'NORTHING'
]
mb_fac_gdf = mb_fac_merged[cols_select]

mb_fac_gdf = gpd.GeoDataFrame(mb_fac_gdf,
                              geometry=gpd.points_from_xy(mb_fac_gdf.EASTING,
                                                          mb_fac_gdf.NORTHING),
                              crs="epsg:3158")
mb_fac_gdf2 = transform_CRS(mb_fac_gdf, appendLatLon=True)


# =============================================================================
# %% TANK BATTERIES
# =============================================================================
batteries_id = ['Battery']
mb_tanks_bat = mb_fac_gdf2[mb_fac_gdf2['FacilityCodeId'].isin(batteries_id)].reset_index()
print("Total number of tank batteries in MB = ", mb_tanks_bat.shape[0])

# Create gdf
mb_tanks_bat3, errors = integrate_facs(
    mb_tanks_bat,
    fac_alias="LNG_STORAGE",
    starting_ids=mb_wells2.OGIM_ID.iloc[-1] + 1,
    category="Tank batteries",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="52",
    src_date="2024-04-18",  # Daily
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="FacilityCode",
    fac_type='FacilityCodeId',
    install_date=None,
    fac_status='OperationalStatus',
    op_name='OperatorName',
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save data
save_spatial_data(
    mb_tanks_bat3,
    file_name="canada_manitoba_batteries",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% GATHERING AND PROCESSING
# =============================================================================
stats_id = ['Gas Gathering System']
mb_gath = mb_fac_gdf2[mb_fac_gdf2['FacilityCodeId'].isin(stats_id)].reset_index()
print("Total number of gas gathering systems in MB = ", mb_gath.shape[0])

# Create gdf
mb_gath3, errors = integrate_facs(
    mb_gath,
    fac_alias="COMPR_PROC",
    starting_ids=mb_tanks_bat3.OGIM_ID.iloc[-1] + 1,
    category="Gathering and processing",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="52",
    src_date="2024-04-18",  # Daily
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="FacilityCode",
    fac_type='FacilityCodeId',
    install_date=None,
    fac_status='OperationalStatus',
    op_name='OperatorName',
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Save data
save_spatial_data(
    mb_gath3,
    file_name="canada_manitoba_gathering_processing",
    schema_def=True,
    schema=schema_COMPR_PROC,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% TERMINALS
# =============================================================================
terminals_ = ['Tank Terminal']
mb_terminals = mb_fac_gdf2[mb_fac_gdf2['FacilityCodeId'].isin(terminals_)]
print("Total number of O&G terminals in dataset = ", mb_terminals.shape[0])

# Create gdf
mb_terms4, errors4 = integrate_facs(
    mb_terminals,
    fac_alias="LNG_STORAGE",
    starting_ids=mb_gath3.OGIM_ID.iloc[-1] + 1,
    category="Petroleum terminals",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="52",
    src_date="2024-04-18",  # Daily
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="FacilityCode",
    fac_type='FacilityCodeId',
    install_date=None,
    fac_status='OperationalStatus',
    op_name='OperatorName',
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save data
save_spatial_data(
    mb_terms4,
    file_name="canada_manitoba_petroleum_terminals",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% INJECTION AND DISPOSAL
# =============================================================================
inject_ = ['Injection/Disposal System']
inject_mb = mb_fac_gdf2[mb_fac_gdf2['FacilityCodeId'].isin(inject_)]
print("Total number of injection/disposal facilities in dataset = ", inject_mb.shape[0])

# Create gdf
mb_inject4, errors4 = integrate_facs(
    inject_mb,
    fac_alias="LNG_STORAGE",
    starting_ids=mb_terms4.OGIM_ID.iloc[-1] + 1,
    category="Injection and disposal",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="52",
    src_date="2024-04-18",  # Daily
    on_offshore="Onshore",
    fac_name='FacilityName',
    fac_id="FacilityCode",
    fac_type='FacilityCodeId',
    install_date=None,
    fac_status='OperationalStatus',
    op_name='OperatorName',
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save data
save_spatial_data(
    mb_inject4,
    file_name="canada_manitoba_injection_disposal",
    schema_def=True,
    schema=schema_LNG_STORAGE,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% EQUIPMENT AND COMPONENTS (Canvec)
# =============================================================================
# Read CanVec Data for Valves
valves_ = read_spatial_data("canvec_data\\canvec_50K_MB_Res_MGT\\valve_0.shp")
mb_valves = transform_CRS(valves_, appendLatLon=True)

# Create gdf
mb_valves4, errors4 = integrate_facs(
    mb_valves,
    fac_alias="OTHER",
    starting_ids=mb_terms4.OGIM_ID.iloc[-1] + 1,
    category="Equipment and Components",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="53",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    fac_id=None,
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Save data
save_spatial_data(
    mb_valves4,
    file_name="canada_manitoba_equipment_components",
    schema_def=True,
    schema=schema_OTHER,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% PIPELINES (CanVec)
# =============================================================================
# CANVEC DATA at two different resolutions
pipes_01 = read_spatial_data("canvec_data\\canvec_50K_MB_Res_MGT\\pipeline_1.shp")
pipes_01['source_id'] = '53'
pipes_02 = read_spatial_data("canvec_data\\canvec_250K_MB_Res_MGT\\pipeline_1.shp")
pipes_02['source_id'] = '54'

results_pipes_ = pd.concat([pipes_01, pipes_02])

# Check if there are duplicates in `feature_id`
print("Total # of unique IDs = ", len(results_pipes_.feature_id.unique()), " COMPARE WITH ", results_pipes_.shape[0])

# Transform CRS
mb_pipes = transform_CRS(results_pipes_, appendLatLon=False)

# Calculate pipeline length
mb_pipes2 = calculate_pipeline_length_km(mb_pipes)

# Replace missing value with N/A
mb_pipes2.loc[mb_pipes2.pippdt_en == 'Not Identified', 'pippdt_en'] = 'N/A'


# Create gdf
mb_pipes3, _ = integrate_pipelines(
    mb_pipes2.reset_index(),
    starting_ids=mb_valves4.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="source_id",
    src_date="2019-07-24",
    on_offshore="Onshore",
    fac_name=None,
    # fac_id="feature_id",
    fac_type="piprgrd_en",
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity="pippdt_en",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)


# Save data
save_spatial_data(
    mb_pipes3,
    file_name="canada_manitoba_oil_gas_pipelines",
    schema_def=True,
    schema=schema_PIPELINES,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% O&G FIELDS - SKIP
# !!! Since these boundaries are way more coarse than the actual oil and gas pools,
# use the pools instead of these boundaries.
# =============================================================================
# mb_fields = read_spatial_data("oil_field_boundaries//Oilfield_boundaries.shp")

# # Transform CRS
# mb_fields2 = transform_CRS(mb_fields)

# # Calculate area in km2
# mb_fields3 = calculate_basin_area_km2(mb_fields2)

# # Strip parenthetical numbers at the end of each field name
# mb_fields3['Field_Name_New'] = mb_fields3.Field_Name.str[:-5]

# # Create gdf
# mb_fields3_, mb_fields_errors = integrate_basins(
#     mb_fields3,
#     starting_ids=mb_pipes3.OGIM_ID.iloc[-1] + 1,
#     category="Oil and natural gas fields",
#     fac_alias= "OIL_GAS_BASINS",
#     country="Canada",
#     state_prov="Manitoba",
#     src_ref_id="55",
#     src_date="2023-07-05",  # from Manitoba download webpage
#     on_offshore="Onshore",
#     _name="Field_Name_New",
#     reservoir_type=None,
#     op_name=None,
#     _area_km2="AREA_KM2"
# )

# mb_fields3.plot()

# =============================================================================
# %% OIL AND NATURAL GAS POOLS
# =============================================================================
# Read in all individual shapefiles containing separate pools, and concatenate
mb_pools_shp = "oil_pool_layers"
all_files = glob.glob(os.getcwd() + "//" + mb_pools_shp + "//*.shp")
data_pools = []

for file in all_files:
    df_ = gpd.read_file(file)
    data_pools.append(df_)

result_pools = pd.concat(data_pools)

result_pools.head()
result_pools.boundary.plot()
result_pools.columns


# Drop `Point` and `LineString` features from dataset, leave only polygons
# !!! As of Aug 2023, there aren't any Point or LineString type geometries in
# this shapefile, so this step isn't necessary.

# result_pools["idx_"] = np.arange(0, result_pools.shape[0])

# IDs = []
# for idx7, row7 in result_pools.iterrows():
#     geom_ = row7.geometry
#     if "POINT" in str(geom_).split(" ") or "LINESTRING" in str(geom_).split(" "):
#         IDs.append(row7.idx_)

# pools7_ = result_pools.query("idx_ != @IDs")
# pools7_.boundary.plot()


mb_pools = transform_CRS(result_pools)
mb_pools3 = calculate_basin_area_km2(mb_pools)


# Create gdf
mb_fields4_, mb_fields_errors = integrate_basins(
    mb_pools3.reset_index(),
    starting_ids=mb_pipes3.OGIM_ID.iloc[-1] + 1,
    category="Oil and natural gas fields",
    fac_alias="OIL_GAS_BASINS",
    country="Canada",
    state_prov="Manitoba",
    src_ref_id="55",
    src_date="2023-07-07",
    on_offshore="Onshore",
    _name="Pool_Name",
    reservoir_type=None,
    op_name=None,
    _area_km2="AREA_KM2"
)


# Save data
save_spatial_data(
    mb_fields4_,
    file_name="canada_manitoba_oil_gas_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% PRODUCTION - WORK IN PROGRESS
# =============================================================================
# # <font color='darkgreen'> *Oil and natural gas production data*<a name="Production"><a>
#  - Production data are reported at "https://www.manitoba.ca/iem/petroleum/reports/wmp_2019_2022_excel.zip"
#  - Production is reported at the well-level. Lat and Lon are not provided, only the UWI which also doubles as locational info
#  - We combine the `Licence` field with `Licence` info in the well-level dataset to derive exact lat and lon for the prod data

# # Read production data
# prod_ = pd.read_excel("production_data\\wmp_2019_2022.xlsx")
# prod_.head()


# # Extract 2021 data
# print("Length of full data = {}".format(prod_.shape[0]))
# prod_2021 = prod_.query("YEAR == 2021")
# print("Length of 2021 data = {}".format(prod_2021.shape[0]))
# display(prod_2021.head())


# # Calculate production days and total production for the year
# # Note: Volumes for fluid type “GAS” and “GSD” are in 1000 m3. For all other fluid types volumes are in m3
# prod_2021['volume_'] = [np.nansum(prod_2021.iloc[:,12:24].iloc[x]) for x in range(prod_2021.shape[0])]


# # Then pivot table on fluid type
# prod_2021_v2 = prod_2021.pivot_table(
#     values=['volume_'],
#     index=['LICENCE'],
#     columns=['FLUID TYPE']
#     )

# # Preview
# prod_2021_v2.head()


# # Add additional attributes based on LICENCE
# prod_2021_v3 = pd.merge(prod_2021_v2, prod_[["LICENCE", "NAME", "WELL LOCATION", "UWI", "FIELD NAME", "DEVIATION", "POOL NAME"]], on="LICENCE", how='left')


# # Merge this data with locational data for wells
# # prod_2021_v3['FAC_ID'] =  str(prod_2021_v3['LICENCE'])

# sel_well_data = mb_wells5[['OGIM_ID', 'FAC_ID', 'FAC_NAME', 'LATITUDE', 'LONGITUDE', 'geometry']]
# sel_well_data['LICENCE'] = [int(sel_well_data['FAC_ID'].iloc[x]) for x in range(sel_well_data.shape[0])]

# prod_wells_merged = pd.merge(prod_2021_v3, sel_well_data, on='LICENCE', how='left')

# # Create a GeoDataFrame
# prod_wells_mb = prod_wells_merged.set_geometry('geometry', crs="epsg:4326")
# prod_wells_mb.head()


# fiG = scatterMaps(
#     prod_wells_mb,
#     lat_lon=False,
#     heatMap=True,
#     colorAttr = ('volume_', 'OIL'),
#     dataScaling=50,
#     dataLabel="Oil production ($m^3$, 2021)",
#     colorMapName="plasma",
#     figHeight=8,
#     figWidth=12
#     )


# # Calculate O&G production in MCF and barrels
# prod_wells_mb['GAS_MCF'] = prod_wells_mb[('volume_', 'GAS')]*35.314666721
# prod_wells_mb['OIL_BBL'] = prod_wells_mb[('volume_', 'OIL')]*6.2898
# prod_wells_mb['WATER_BBL'] = prod_wells_mb[('volume_', 'H2O')]*6.2898


# # Fix Deviation description
# prod_wells_mb.DEVIATION.unique()

# dict_dev = {
#     'V': 'VERTICAL',
#     'H': 'HORIZONTAL',
#     'D': 'DIRECTIONAL'
#     }

# prod_wells_mb2 = replace_row_names(prod_wells_mb, "DEVIATION", dict_names=dict_dev)


# # Check if geometry is None
# nulls_, list_nulls_ = check_invalid_geoms(prod_wells_mb2, id_attr="LICENCE")


# # Check if LATITUDE AND LONGITUDE are NULLS
# data_nulls_ = prod_wells_mb2[prod_wells_mb2.LATITUDE.isnull()]
# data_nulls_

# # Integrate production data

# mb_prod1, mb_prod_errors = integrate_production(
#     prod_wells_mb2,
#     starting_ids=mb_fields3_.OGIM_ID.iloc[-1]+1,
#     category="Oil and natural gas production",
#     fac_alias="OIL_GAS_PROD",
#     country="Canada",
#     state_prov="Manitoba",
#     src_ref_id="57",
#     src_date="2022-03-29",
#     on_offshore="Onshore",
#     fac_name="FAC_NAME",
#     fac_id="LICENCE",
#     fac_type=None,
#     spud_date=None,
#     comp_date=None,
#     drill_type="DEVIATION",
#     fac_status=None,
#     op_name="NAME",
#     oil_bbl="OIL_BBL",
#     gas_mcf='GAS_MCF',
#     water_bbl='WATER_BBL',
#     condensate_bbl=None,
#     prod_days=('volume_', 'DAY'),
#     prod_year=2021,
#     entity_type='Well',
#     fac_latitude='LATITUDE',
#     fac_longitude='LONGITUDE'
#     )


# # Save results
# save_spatial_data(
#     mb_prod1,
#     file_name="canada_manitoba_oil_gas_production",
#     schema_def=True,
#     schema=schema_OIL_GAS_PROD,
#     file_type="GeoJSON",
#     out_path="results\\"
#     )
