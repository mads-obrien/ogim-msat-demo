# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global FIELDS AND BASINS.
Robertson sedimentary basins for the rest of the world, excluding US and
Canada, which have their own data.

# TODO:
[X] standardize import statements and CWD setting
[] standardize spacing between sections
[] alphabetize countries
[] update all file paths

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
import numpy as np
import glob
from tqdm import tqdm
import fiona
from bs4 import BeautifulSoup
# import datetime

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, create_concatenated_well_name,
                     transform_CRS, integrate_basins, save_spatial_data,
                     read_spatial_data, calculate_basin_area_km2,
                     schema_BASINS, strip_z_coord, dict_us_states)
from read_iffy_file import read_iffy_file
from assign_countries_to_feature_2 import assign_countries_to_feature, assign_stateprov_to_feature

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
results_folder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# -----------------------------------------------------------------------------
# Read in country and state boundary shapefiles, for assigning country and
# state names to polygons later
path_to_country_geoms = os.path.join(pubdata,
                                     'International_data_sets',
                                     'National_Maritime_Boundaries',
                                     'marine_and_land_boundaries_seamless.shp')
country_geoms = gpd.read_file(path_to_country_geoms)
path_to_state_geoms = os.path.join(buii_path,
                                   'Public_Data',
                                   'NaturalEarth',
                                   'ne_10m_admin_1_states_provinces.shp')
state_geoms = gpd.read_file(path_to_state_geoms)
state_geoms = state_geoms[['iso_a2',
                           'name',
                           'name_alt',
                           'name_local',
                           'type_en',
                           'admin',
                           'geometry'
                           ]]

# =============================================================================
# %% AFGHANISTAN
# =============================================================================
os.chdir(v24data)
fp = r"afghanistan\fldafg.shp"
data = read_spatial_data(fp, table_gradient=True)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Calculate the area of each polygon, and add the results in a new column
data = calculate_basin_area_km2(data, attrName="AREA_KM2")

# =============================================================================
# %% AFGHANISTAN - Integration + Export
# =============================================================================
data_, errors = integrate_basins(
    data,
    starting_ids=0,
    category='Oil and natural gas fields',
    fac_alias="OIL_GAS_BASINS",
    country='Afghanistan',
    # state_prov = None,
    src_ref_id='163',
    src_date='2006-04-01',
    # on_offshore = None,
    _name='USGS_NAME',
    reservoir_type='COMMODITY',
    # op_name = None,
    _area_km2='AREA_KM2'
)

save_spatial_data(
    data_,
    file_name="afghanistan_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NORTH AFRICA (Libya, Egypt, Algeria)
# =============================================================================
os.chdir(v24data)
fp = r"libya\Libya_AGO_kmitchell\Libya_Egypt_Algeria_Oil_and_Gas_Fields.geojson"
data = read_spatial_data(fp, table_gradient=True)

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Calculate the area of each polygon, and add the results in a new column
data = calculate_basin_area_km2(data, attrName="AREA_KM2")

# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# To be consistent with other features (like pipelines) that are associated with
# more than one nation, change this 'Joint Economic Zone' value to simply
# listing both countries
data.loc[data.countries == 'Tunisia~Libya/Tunisia JEZ', 'countries'] = 'Libya, Tunisia'

# Replace operator names that are a single space with N/A
data.loc[data.opr_curr == ' ', 'opr_curr'] = 'N/A'

# =============================================================================
# %% NORTH AFRICA (Libya, Egypt, Algeria) - Integration + Export
# =============================================================================
data_, errors = integrate_basins(
    data,
    starting_ids=0,
    category='Oil and natural gas fields',
    fac_alias="OIL_GAS_BASINS",
    country='countries',
    # state_prov = None,
    src_ref_id='166',
    src_date='2017-06-01',
    on_offshore='ons_off',
    _name='field_name',
    reservoir_type='gn_hc_type',
    op_name='opr_curr',
    _area_km2='AREA_KM2'
)

save_spatial_data(
    data_,
    file_name="africa_fields",
    schema_def=False,  # Must be false in order to export multipart geoms correctly
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% AUSTRALIA - Fields
# =============================================================================
os.chdir(v24data)
fp1 = r"australia\fields\GAS_fields_Australia.kml.shp"
gas_fields = read_spatial_data(fp1, table_gradient=True)
gas_fields['TYPE'] = 'GAS'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp2 = r"australia\fields\OIL_fields_Australia.kml.shp"
oil_fields = read_spatial_data(fp2, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp3 = r"australia\fields\OIL&GAS_fields_Australia.kml.shp"
oil_gas_fields = read_spatial_data(fp3, table_gradient=True)
oil_gas_fields['TYPE'] = 'OIL & GAS'
oil_gas_fields1 = transform_CRS(oil_gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fields_concat = gpd.GeoDataFrame(pd.concat([gas_fields1, oil_fields1, oil_gas_fields1], ignore_index=True))
fields_concat1 = fields_concat.set_crs(4326)
fields_concat11 = calculate_basin_area_km2(fields_concat1, attrName="AREA_KM2")


names = []

# Format the dates list, as well as the on and offshore fields
for idx1_, row1_ in tqdm(fields_concat11.iterrows(), total=fields_concat11.shape[0]):
    name = str(row1_.Name)
    if 'Untitled' in name:
        names_null = "N/A"
        names.append(names_null)
    elif 'None' in name:
        names_null = "N/A"
        names.append(names_null)
    else:
        names.append(name)

# Append these new attributes to GDF
fields_concat11['NAMES_NEW'] = names


# =============================================================================
# %% AUSTRALIA - License blocks and titles
# =============================================================================
os.chdir(v24data)
fp4 = r"australia\blocks\PetroleumTitles.shp"
titles = read_spatial_data(fp4, table_gradient=True)

titles = transform_CRS(titles, target_epsg_code="epsg:4326", appendLatLon=True)
titles = calculate_basin_area_km2(titles, attrName="AREA_KM2")

# =============================================================================
# %% AUSTRALIA - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields_concat11,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Australia",
    # state_prov="",
    src_ref_id="22",
    src_date="2011-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='NAMES_NEW',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)
# Remove z values from geometry
fields_final_no_z = strip_z_coord(fields_final)


titles_final, errors = integrate_basins(
    titles,
    starting_ids=0,
    category="Oil and natural gas blocks",
    country="Australia",
    # state_prov="",
    src_ref_id="135",
    src_date="2024-04-01",
    on_offshore="OFFSHORE",
    _name='Title',
    # reservoir_type = 'COMMODITY',
    op_name="TitleOprat",
    _area_km2='AREA_KM2'
)

# titles_flattened = flatten_gdf_geometry(titles_final, 'MultiPolygon')


save_spatial_data(
    fields_final_no_z,
    file_name="australia_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


save_spatial_data(
    titles_final,
    file_name="australia_license_blocks",
    # schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% AZERBAIJAN
# =============================================================================
os.chdir(v24data)

fp1 = r"azerbaijan\Gas_Fields_Azerbaijan.kml.shp"
gas_fields = read_spatial_data(fp1, table_gradient=True)
gas_fields['TYPE'] = 'GAS'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp2 = r"azerbaijan\Oil_Fields_Azerbaijan.kml.shp"
oil_fields = read_spatial_data(fp2, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp3 = r"azerbaijan\Condensate_Fields_Azerbaijan.kml.shp"
condensate_fields = read_spatial_data(fp3, table_gradient=True)
condensate_fields['TYPE'] = 'OIL & GAS'
condensate_fields1 = transform_CRS(condensate_fields, target_epsg_code="epsg:4326", appendLatLon=True)


fields_concat = gpd.GeoDataFrame(pd.concat([gas_fields1, oil_fields1, condensate_fields1], ignore_index=True))
fields_concat1 = fields_concat.set_crs(4326)
fields_concat11 = calculate_basin_area_km2(fields_concat1, attrName="AREA_KM2")


# Data manipulation / processing if needed
# -----------------------------------------------------------------------------
# cleaning Name column to get rid of placemark for any of the names
names = []


# Format the dates list, as well as the on and offshore fields
for idx1_, row1_ in tqdm(fields_concat11.iterrows(), total=fields_concat11.shape[0]):
    name = str(row1_.Name)
    if 'Placemark' in name:
        names_null = "N/A"
        names.append(names_null)
    else:
        names.append(name)

# Append these new attributes to GDF
fields_concat11['NAMES_NEW'] = names

# =============================================================================
# %% AZERBAIJAN - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields_concat11,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Azerbaijan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='NAMES_NEW',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

# Remove z values from geometry
fields_final_no_z = strip_z_coord(fields_final)

save_spatial_data(
    fields_final_no_z,
    file_name="azerbaijan_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% BANGLADESH
# =============================================================================
os.chdir(v24data)

fp1 = r"bangladesh\Gas_Fields_Bangladesh.kml.shp"
gas_fields = read_spatial_data(fp1, table_gradient=True)
gas_fields['TYPE'] = 'GAS'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)
gas_fields11 = calculate_basin_area_km2(gas_fields1, attrName="AREA_KM2")

fp2 = r"bangladesh\Bangladesh_LB.shp"
licenseblock = read_spatial_data(fp2, table_gradient=True)
licenseblock1 = transform_CRS(licenseblock, target_epsg_code="epsg:4326", appendLatLon=True)
licenseblock11 = calculate_basin_area_km2(licenseblock1, attrName="AREA_KM2")

# Deduplicate possible duplicate features
licenseblock11 = licenseblock11.drop_duplicates(subset=['Name',
                                                        'geometry'],
                                                keep='first').reset_index()

# =============================================================================
# %% BANGLADESH - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    gas_fields11,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)


block_final, errors = integrate_basins(
    licenseblock11,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="Bangladesh",
    # state_prov="",
    src_ref_id="22",
    src_date="2012-09-18",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    # reservoir_type = 'COMMODITY',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

# Remove z values from geometry
fields_final_no_z = strip_z_coord(fields_final)
blocks_final_no_z = strip_z_coord(block_final)

blocks_final_no_z_ = replace_row_names(blocks_final_no_z, colName="NAME", dict_names={'UNTITLED POLYGON': 'N/A'})


save_spatial_data(
    fields_final_no_z,
    file_name="bangladesh_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


save_spatial_data(
    blocks_final_no_z_,
    file_name="bangladesh_license_blocks",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)

# =============================================================================
# %% BOLIVIA
# =============================================================================
os.chdir(v24data)
fp = r'bolivia\geobolivia\areas_reservadas.shp'
bol_fields = gpd.read_file(fp)
bol_fields.loc[~bol_fields.AREA_ID.str.startswith('Bloque'), 'AREA_ID'] = 'N/A'


bol_fields_integrated, errors = integrate_basins(
    bol_fields,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="Bolivia",
    # state_prov="",
    src_ref_id="125",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="",
    _name='nombre-ar',
    # reservoir_type='TYPE',
    op_name="BA_NAME",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    bol_fields_integrated,
    file_name="bolivia_license_blocks",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% DENMARK
# =============================================================================
os.chdir(v24data)
fp1 = r"denmark\fields\FieldDelineations_2024_13_02.shp"
fields = read_spatial_data(fp1, table_gradient=True)

# fp2 = r"denmark\fields\Blocks.shp"
# blocks = read_spatial_data(fp2, table_gradient=True)

fp3 = r"denmark\fields\Licences_20230428.shp"
licenses = read_spatial_data(fp3, table_gradient=True)

# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
fields = transform_CRS(fields, target_epsg_code="epsg:4326", appendLatLon=True)
fields = calculate_basin_area_km2(fields, attrName="AREA_KM2")

licenses = transform_CRS(licenses, target_epsg_code="epsg:4326", appendLatLon=True)
licenses = calculate_basin_area_km2(licenses, attrName="AREA_KM2")

licenses['POLYGON_NA'] = licenses['POLYGON_NA'].fillna('N/A')
create_concatenated_well_name(licenses,
                              'LICENCE',
                              'POLYGON_NA',
                              'polynamenew')

# =============================================================================
# %% DENMARK  - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Denmark",
    # state_prov="",
    src_ref_id="51",
    src_date="2024-02-13",  # date present in file name
    on_offshore="OFFSHORE",
    _name='Field',
    # reservoir_type = '',
    # op_name="",
    _area_km2='AREA_KM2'
)

licenses_final, errors = integrate_basins(
    licenses,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="Denmark",
    # state_prov="",
    src_ref_id="236",
    src_date="2023-04-28",  # date present in file name
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='polynamenew',
    # reservoir_type = 'COMMODITY',
    op_name="OPERATOR",
    _area_km2='AREA_KM2'
)

# Remove z values from geometry, if there are any
fields_final_no_z = strip_z_coord(fields_final)
licenses_final_no_z = strip_z_coord(licenses_final)


save_spatial_data(
    fields_final_no_z,
    file_name="denmark_fields",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)

save_spatial_data(
    licenses_final_no_z,
    file_name="denmark_license_blocks",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% INDIA
# =============================================================================
os.chdir(v24data)
fp1 = r"india\GAS_fields_offshore_India.kml.shp"
gas_fields = read_spatial_data(fp1, table_gradient=True)
gas_fields['TYPE'] = 'CONDENSATE'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp2 = r"india\OIL_fields_offshore_India.kml.shp"
oil_fields = read_spatial_data(fp2, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)


fields_concat = gpd.GeoDataFrame(pd.concat([gas_fields1, oil_fields1], ignore_index=True))
fields_concat1 = fields_concat.set_crs(4326)
fields_concat11 = calculate_basin_area_km2(fields_concat1, attrName="AREA_KM2")


fp3 = r"india\India_License_Blocks.shp"
licenseblock = read_spatial_data(fp3, table_gradient=True)
licenseblock1 = transform_CRS(licenseblock, target_epsg_code="epsg:4326", appendLatLon=True)
licenseblock11 = calculate_basin_area_km2(licenseblock1, attrName="AREA_KM2")


# =============================================================================
# %% INDIA  - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields_concat11,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="India",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # src_date="1",
    on_offshore="OFFSHORE",
    _name='Name',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

block_final, errors = integrate_basins(
    licenseblock11,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="India",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    # reservoir_type = 'COMMODITY',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)
# Remove z values from geometry
fields_final_no_z = strip_z_coord(fields_final)
blocks_final_no_z = strip_z_coord(block_final)


save_spatial_data(
    fields_final_no_z,
    file_name="india_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)

save_spatial_data(
    blocks_final_no_z,
    file_name="india_license_blocks",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% IRELAND
# =============================================================================
# os.chdir(pubdata)
fp1 = r"ireland\Fields_Ireland.kml.shp"
fields1 = read_spatial_data(fp1, table_gradient=True)


# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
fields11 = transform_CRS(fields1, target_epsg_code="epsg:4326", appendLatLon=True)
fields2 = calculate_basin_area_km2(fields11, attrName="AREA_KM2")

# =============================================================================
# %% IRELAND - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields2,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Ireland",
    # state_prov="",
    src_ref_id="22",
    src_date="2022-04-01",
    on_offshore="OFFSHORE",
    _name='Name',
    # reservoir_type = '',
    # op_name="",
    _area_km2='AREA_KM2'
)


save_spatial_data(
    fields_final,
    file_name="ireland_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% ITALY
# =============================================================================
os.chdir(v24data)
# Turn on GeoPandas's KML support.
gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'

# Read in distinct layers of the license blocks KML
fp_titoli = r'italy\titoli-idrocarburi.kml'
fiona.listlayers(fp_titoli)

perm_on = gpd.read_file(fp_titoli, driver='KML', layer='Permessi di ricerca in terraferma')
perm_off = gpd.read_file(fp_titoli, driver='KML', layer='Permessi di ricerca in mare')
conc_on = gpd.read_file(fp_titoli, driver='KML', layer='Concessioni di coltivazione in terraferma')
conc_off = gpd.read_file(fp_titoli, driver='KML', layer='Concessioni di coltivazione in mare')

italy_fields = pd.concat([perm_on, perm_off, conc_on, conc_off]).reset_index()
italy_fields = calculate_basin_area_km2(italy_fields, attrName="AREA_KM2")

# TODO - Extract more information from the 'Description' column
# TODO - join both of these KMLs, once cleaned up, to 'titoli-idrocarburi.csv' for attribute information?


def extract_operator_from_html(html_string):
    '''Extract operator information from the HTML in the Descsription field.'''
    # Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(html_string, "html.parser")

    # Locate the 'TITOLARI' table in the HTML soup
    titolari_div = soup.find("div", text="TITOLARI")
    titolari_table = titolari_div.find_next("table") if titolari_div else None

    # Extract table data
    if titolari_table:
        rows = titolari_table.find_all("tr")
        table_data = []
        for row in rows:
            cells = row.find_all(["th", "td"])
            table_data.append([cell.get_text(strip=True) for cell in cells])
        # Create a small df from the info you extracted.
        df = pd.DataFrame(table_data[1:], columns=table_data[0])
        # Return the operator name(s) associated with this record as a string
        if len(df.Operatore.unique()) > 1:
            operator_str = '; '.join(df.Operatore.unique())
        else:
            operator_str = df.Operatore.unique()[0]
        return operator_str
    else:
        print("TITOLARI table not found.")


italy_fields['Operator'] = italy_fields.Description.apply(lambda x: extract_operator_from_html(x))

# =============================================================================
# %% ITALY - Integration + Export
# =============================================================================
italy_fields_integrated, errors = integrate_basins(
    italy_fields,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Italy",
    # state_prov="",
    src_ref_id="36",
    src_date="2024-12-31",
    # on_offshore="",
    _name='Name',
    # reservoir_type = '',
    op_name="Operator",
    _area_km2='AREA_KM2'
)

# Remove z values from geometry
italy_fields_integrated_no_z = strip_z_coord(italy_fields_integrated)
# flattened_fields= flatten_gdf_geometry(fields_final_no_z, 'MultiPolygon' )

save_spatial_data(
    italy_fields_integrated_no_z,
    file_name="italy_fields",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% KAZAKHSTAN
# =============================================================================
# os.chdir(pubdata)
fp4 = r"kazakhstan\Kazhakhstan_license_blocks_.shp"
licenseblock = read_spatial_data(fp4, table_gradient=True)


licenseblock1 = transform_CRS(licenseblock, target_epsg_code="epsg:4326", appendLatLon=True)
licenseblock11 = calculate_basin_area_km2(licenseblock1, attrName="AREA_KM2")

# =============================================================================
# %% KAZAKHSTAN - Integration + Export
# =============================================================================
block_final, errors = integrate_basins(
    licenseblock11,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="Kazakhstan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    # reservoir_type = 'COMMODITY',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)


save_spatial_data(
    block_final,
    file_name="kazakhstan_license_blocks",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% MYANMAR
# =============================================================================
# os.chdir(pubdata)
fp1 = r"myanmar\Condensate_Field_Myanmar.kml.shp"
condensate_fields = read_spatial_data(fp1, table_gradient=True)
condensate_fields['TYPE'] = 'CONDENSATE'
condensate_fields1 = transform_CRS(condensate_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp2 = r"myanmar\Gas_Fields_Myanmar.kml.shp"
gas_fields = read_spatial_data(fp2, table_gradient=True)
gas_fields['TYPE'] = 'GAS'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp3 = r"myanmar\Oil_fields_Myanmar.kml.shp"
oil_fields = read_spatial_data(fp3, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)


fields_concat = gpd.GeoDataFrame(pd.concat([gas_fields1, oil_fields1, condensate_fields1], ignore_index=True))
fields_concat1 = fields_concat.set_crs(4326)
fields_concat11 = calculate_basin_area_km2(fields_concat1, attrName="AREA_KM2")


# =============================================================================
# %% MYANMAR - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields_concat11,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Myanmar",
    # state_prov="",
    src_ref_id="22",
    src_date="2013-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

# Remove z values from geometry
fields_final_no_z = strip_z_coord(fields_final)


save_spatial_data(
    fields_final_no_z,
    file_name="myanmar_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NETHERLANDS
# =============================================================================
os.chdir(v24data)
nl_fields = gpd.read_file(r'netherlands\Feb-2025_NLOG_Fields_ED50UTM31N.shp')
nl_fields = transform_CRS(nl_fields,
                          target_epsg_code="epsg:4326",
                          appendLatLon=True)
nl_fields = calculate_basin_area_km2(nl_fields, attrName="AREA_KM2")

# Create onshore/offshore field
nl_fields['onoff'] = None
nl_fields.loc[nl_fields.LANDSEA == 'Sea', 'onoff'] = 'OFFSHORE'
nl_fields.loc[nl_fields.LANDSEA == 'Land', 'onoff'] = 'ONSHORE'

# Translate some values from Dutch
nl_fields.RESULT = nl_fields.RESULT.replace({'Olie': 'Oil',
                                             'Olie en Gas': 'Oil and Gas'})

# =============================================================================
# %% NETHERLANDS - Integration + Export
# =============================================================================
nl_fields_integrated, errors = integrate_basins(
    nl_fields,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Netherlands",
    # state_prov="",
    src_ref_id="34",
    src_date="2024-11-01",
    # src_date="",
    on_offshore="onoff",
    _name='FIELD_NAME',
    reservoir_type='RESULT',
    op_name="OPERATOR",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    nl_fields_integrated,
    file_name="netherlands_fields",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NEW ZEALAND
# =============================================================================
# Mads determined these two data sets didn't add anything to OGIM
# particularly, the Closed Block Offer shapefile seems to be tons of empty
# square regions across NZ in which people can apply for mining permits

# fp1 = "Australia+NewZealand\\New_Zealand\\Updated\\zipfolder\\Block_Offer___Consultation_Blocks.shp"
# licenseblock1 = read_spatial_data(fp1, table_gradient=True)
# licenseblock11 = transform_CRS(licenseblock1, target_epsg_code="epsg:4326", appendLatLon=True)
# licenseblock111 = calculate_basin_area_km2(licenseblock11, attrName="AREA_KM2")
# licenseblock111['STATUS'] = 'BLOCK OFFER'

# fp2 = "Australia+NewZealand\\New_Zealand\\Updated\\zipfolder\\Closed_Block_Offer.shp"
# licenseblock2 = read_spatial_data(fp2, table_gradient=True)
# licenseblock22 = transform_CRS(licenseblock2, target_epsg_code="epsg:4326", appendLatLon=True)
# licenseblock222 = calculate_basin_area_km2(licenseblock22, attrName="AREA_KM2")
# licenseblock222['STATUS'] = 'CLOSED'

# blocks_concat = gpd.GeoDataFrame(pd.concat([licenseblock111, licenseblock222], ignore_index=True))
# blocks_concat1 = blocks_concat.set_crs(4326)
# blocks_concat11 = calculate_basin_area_km2(blocks_concat1, attrName="AREA_KM2")

fp3 = r"new_zealand\blocks\Petroleum_Active_Permits.shp"
activepermits = read_spatial_data(fp3, table_gradient=True)
activepermits = transform_CRS(activepermits, target_epsg_code="epsg:4326", appendLatLon=True)
activepermits = calculate_basin_area_km2(activepermits, attrName="AREA_KM2")
activepermits['STATUS'] = 'ACTIVE'
activepermits['name'] = activepermits.PERMIT_TYP + ' ' + activepermits.PERMIT_NUM
activepermits = calculate_basin_area_km2(activepermits, attrName="AREA_KM2")

# Drop possible duplicate geometries
# activepermits = activepermits.drop_duplicates(subset=['Name', 'geometry'], keep='first')

# =============================================================================
# %% NEW ZEALAND - Integration + Export
# =============================================================================
# block_final1, errors = integrate_basins(
#     blocks_concat11,
#     starting_ids=0,
#     category="Oil and natural gas license blocks",
#     country="New Zealand",
#     # state_prov="",
#     src_ref_id="148",
#     src_date="2022-11-01",
#     # on_offshore="OFFSHORE",
#     _name='BLOCK_NAME',
#     # status = 'STATUS',
#     # reservoir_type = 'COMMODITY',
#     # op_name="Operator",
#     _area_km2='AREA_KM2'
# )

activepermits_integrated, errors = integrate_basins(
    activepermits,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="New Zealand",
    # state_prov="",
    src_ref_id="148",
    src_date="2024-09-08",
    on_offshore="PERMIT_OFF",
    _name='name',
    reservoir_type='MINERALS',
    op_name="OPERATOR",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    activepermits_integrated,
    file_name="new_zealand_license_blocks",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NORWAY
# =============================================================================
# Read Fields and License Blocks from Norway Government Website
os.chdir(v24data)
fp5 = r"norway\fields\fldArea.shp"
fields_fldArea = read_spatial_data(fp5, table_gradient=True)
fp6 = r"norway\blocks\blkArea.shp"
blocks = read_spatial_data(fp6, table_gradient=True)

# Transform CRS to EPSG 4326 and calculate and append lat and lon values
fields_fldArea = transform_CRS(fields_fldArea,
                               target_epsg_code="epsg:4326",
                               appendLatLon=True)
fields_fldArea = calculate_basin_area_km2(fields_fldArea, attrName="AREA_KM2")

blocks = transform_CRS(blocks,
                       target_epsg_code="epsg:4326",
                       appendLatLon=True)
blocks = calculate_basin_area_km2(blocks, attrName="AREA_KM2")

# =============================================================================
# %% NORWAY - Integration + Export
# =============================================================================
# Integrate fields
fields_fldArea_integrated, errors = integrate_basins(
    fields_fldArea,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Norway",
    state_prov="main_area",
    src_ref_id="30",
    src_date="2024-04-18",  # Daily
    on_offshore="OFFSHORE",
    _name='fieldName',
    reservoir_type='Dctype',
    op_name="OpLongName",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    fields_fldArea_integrated,
    file_name="norway_fields",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# integrate license blocks
blocks_final, errors = integrate_basins(
    blocks,
    starting_ids=0,
    category="Oil and natural gas blocks",
    country="Norway",
    state_prov="main_area",
    src_ref_id="88",
    src_date="2024-04-18",  # Daily
    on_offshore="OFFSHORE",
    _name='block_name',
    # reservoir_type='Dctype',
    # op_name="OpLongName",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    blocks_final,
    file_name="norway_license_blocks",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% AFRICA (oilandgasinfrastructure.com)
# =============================================================================
# os.chdir(pubdata)

ghana_cond = gpd.read_file(r'ghana\Condensate_Fields_Ghana Condensate Fields.shp')
ghana_gas = gpd.read_file(r'ghana\Gas_Fields_Ghana Gas Fields.shp')
ghana_oil = gpd.read_file(r'ghana\Oil_Fields_Ghana Oil Fields.shp')

nigeria_cond = gpd.read_file(r'nigeria\Condensate_Fields_Nigeria Condensate Fields.shp')
nigeria_gas = gpd.read_file(r'nigeria\Gas_Fields_Nigeria Gas Fields.shp')
nigeria_oil = gpd.read_file(r'nigeria\Oil_Fields_Nigeria Oil Fields.shp')

libya_new = gpd.read_file(r'libya\oginfrastructure_dot_com\Fields_new_Libya Fields new.shp')
libya_off = gpd.read_file(r'libya\oginfrastructure_dot_com\Field_Offshore_Libya Field.shp')

sudan = gpd.read_file(r'sudan\Fields_Sudan Fields.shp')

cam_gas = gpd.read_file(r'cameroon\Gas Fields Cameroon Gas Fields.shp')

drc_gas = gpd.read_file(r'drc\Gas Fields_DRC Gas Fields.shp')
drc_oil = gpd.read_file(r'drc\Oil Fields_DRC Oil Fields.shp')

ivory_gas = gpd.read_file(r'ivory_coast\Gas_Fields_IvoryCoast Gas Fields.shp')
ivory_oil = gpd.read_file(r'ivory_coast\Oil_Fields_IvoryCoast Oil Fields.shp')

moz_gas = gpd.read_file(r'mozambique\Gas_Fields_Mozambique Gas Fields.shp')

southaf_gas = gpd.read_file(r'south_africa\Gas_Fields_SouthAfrica Gas Fields.shp')
southaf_oil = gpd.read_file(r'south_africa\Oil_Fields_SouthAfrica Oil Fields.shp')

uganda_gas = gpd.read_file(r'uganda\Gas_Fields_Uganda Gas Fields.shp')
uganda_oil = gpd.read_file(r'uganda\Oil_Fields_Uganda Oil Fields.shp')


# -----------------------------------------------------------------------------
# Add a field for on/offshore (Assign on/off shore attribute to polygons based on
# where I can easily see, in QGIS, that all polygons fall onshore or offshore)
for df in [ivory_gas, ivory_oil, ghana_cond, ghana_gas, ghana_oil, libya_off, southaf_gas, southaf_oil]:
    df['onoff'] = 'Offshore'

for df in [sudan, uganda_gas, uganda_oil, libya_new]:
    df['onoff'] = 'Onshore'


# Make lists of all gas fields, oil fields, and condensate fields
cond_df = [ghana_cond,
           nigeria_cond]
gas_df = [ghana_gas,
          nigeria_gas,
          cam_gas,
          drc_gas,
          ivory_gas,
          moz_gas,
          southaf_gas,
          uganda_gas]
oil_df = [ghana_oil,
          nigeria_oil,
          drc_oil,
          ivory_oil,
          southaf_oil,
          uganda_oil]

# Assign the "field type" attribute to each gdf in the lists above
for df in cond_df:
    df['type'] = 'Condensate field'
for df in gas_df:
    df['type'] = 'Gas field'
for df in oil_df:
    df['type'] = 'Oil field'


# For countries with more than one gdf, combine into one unified country gdf

ghana = ghana_cond.append(ghana_gas).append(ghana_oil)
nigeria = nigeria_cond.append(nigeria_gas).append(nigeria_oil)
libya = libya_new.append(libya_off)
drc = drc_gas.append(drc_oil)
ivory = ivory_gas.append(ivory_oil)
southaf = southaf_gas.append(southaf_oil)
uganda = uganda_gas.append(uganda_oil)

# add country attribute to each country-specific gdf
ghana['country'] = 'Ghana'
nigeria['country'] = 'Nigeria'
libya['country'] = 'Libya'
sudan['country'] = 'Sudan'
cam_gas['country'] = 'Cameroon'
drc['country'] = 'DRC'
ivory['country'] = 'Ivory Coast'
moz_gas['country'] = 'Mozambique'
southaf['country'] = 'South Africa'
uganda['country'] = 'Uganda'


# Merge all country gdfs into one gdf
all_dfs_final = [ghana,
                 nigeria,
                 libya,
                 sudan,
                 cam_gas,
                 drc,
                 ivory,
                 moz_gas,
                 southaf,
                 uganda]
data = pd.concat(all_dfs_final)
data = data.reset_index(drop=True)

data['Name'] = data.Name.fillna('N/A')

# Where field name is 'Untitled', replace with NOT AVAILABLE
data.loc[data.Name.str.contains('Untitled', case=False), 'Name'] = 'N/A'
data.loc[data.Name.str.contains('unknown', case=False), 'Name'] = 'N/A'


# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Calculate the area of each polygon, and add the results in a new column
data = calculate_basin_area_km2(data, attrName="AREA_KM2")

# Drop a couple duplicate polygons in the Ivory Coast
data = data.drop_duplicates(subset=['country', 'geometry'],
                            keep='first')

# ========================================================================
# %% AFRICA - Integration + Export
# ========================================================================
data_, errors = integrate_basins(
    data,
    starting_ids=0,
    category='Oil and natural gas fields',
    fac_alias="OIL_GAS_BASINS",
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='onoff',
    _name='Name',
    reservoir_type='type',
    # op_name = None,
    _area_km2='AREA_KM2'
)

save_spatial_data(
    data_,
    file_name="oginfra_africa_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% MIDDLE EAST (oilandgasinfrastructure.com)
# =============================================================================
os.chdir(v24data)

iran_gas = gpd.read_file(r'iran\Gas Fields Gas Fields.shp')
iran_oil = gpd.read_file(r'iran\Oil Field Oil Field.shp')

iraq_cond = gpd.read_file(r'iraq\Condensate_field_Iraq Condensate.shp')
iraq_gas = gpd.read_file(r'iraq\Gas_field_Iraq Gas.shp')
iraq_oil = gpd.read_file(r'iraq\Oil_field_Iraq Oil.shp')

kuwait_gas = gpd.read_file(r'kuwait\Gas_Fields_Kuwait Gas.shp')
kuwait_oil = gpd.read_file(r'kuwait\Oil_Fields_Kuwait Oil.shp')

# Some geometries in Oman shapefiles are still corrupted and will throw an error when read in with gpd.read_file().
# Use my custom function that looks at each geometry one at a time,
# and only includes non-corrupt geometries in the final gdf
oman_cond = read_iffy_file(r'oman\Condensat_Fields_Oman Fields.shp')
oman_gas = read_iffy_file(r'oman\Gas_Fields_Oman Gas Fields.shp')
oman_oil = read_iffy_file(r'oman\Oil_Fields_Oman Oil Fields.shp')

pak_cond = gpd.read_file(r'pakistan\Condensate_fields_Pakistan Condensate fields.shp')
pak_gas = gpd.read_file(r'pakistan\gas_fields_Pakistan gas fields.shp')
pak_oil = gpd.read_file(r'pakistan\oil_fields_Pakistan oil fields.shp')

qatar_gas = gpd.read_file(r'qatar\Gas_Fields_Qatar Gas.shp')
qatar_oil = gpd.read_file(r'qatar\Oil_Fields_Qatar Oil.shp')

saudi_gas = gpd.read_file(r'saudi_arabia\fields\Gas_Fields_Saudi_Arabia Gas.shp')
saudi_oil = gpd.read_file(r'saudi_arabia\fields\Oil_Fields_Saudi_Arabia Oil.shp')
saudi_oil_ghawar = gpd.read_file(r'saudi_arabia\fields\Oil_Fields_Saudi_Arabia Ghawar.shp')

uae_gas_off = gpd.read_file(r'uae\Gas_Fields_Offshore_UAE Gas Fields.shp')
uae_gas_on = gpd.read_file(r'uae\GAS_fields_onshore_UAE GAS.shp')
uae_oil_off = gpd.read_file(r'uae\Oil_fields_offshore_UAE Oil fields.shp')
uae_oil_on = gpd.read_file(r'uae\OIL_fields_onshore_UAE OIL.shp')

yemen_cond = gpd.read_file(r'yemen\Condensate_Fields_Yemen Condensate.shp')
yemen_gas = gpd.read_file(r'yemen\Gas_Fields_Yemen Gas.shp')
yemen_oil = gpd.read_file(r'yemen\Oil_Fields_Yemen Oil.shp')


# -----------------------------------------------------------------------------
# Add field for on/offshore
for df in [uae_gas_off, uae_oil_off]:
    df['onoff'] = 'Offshore'

for df in [uae_gas_on, uae_oil_on]:
    df['onoff'] = 'Onshore'

# merge UAE on/offshore together
uae_gas = uae_gas_off.append(uae_gas_on)
uae_oil = uae_oil_off.append(uae_oil_on)


# add field for oil / gas / condensate
cond_df = [iraq_cond, oman_cond, pak_cond, yemen_cond]
gas_df = [iran_gas, iraq_gas, kuwait_gas, oman_gas, pak_gas, qatar_gas, saudi_gas, uae_gas, yemen_gas]
oil_df = [iran_oil, iraq_oil, kuwait_oil, oman_oil, pak_oil, qatar_oil, saudi_oil, uae_oil, yemen_oil]

for df in cond_df:
    df['type'] = 'Condensate field'
for df in gas_df:
    df['type'] = 'Gas field'
for df in oil_df:
    df['type'] = 'Oil field'

# merge by country, add country field
iran = iran_gas.append(iran_oil)
iraq = iraq_cond.append(iraq_gas).append(iraq_oil)
kuwait = kuwait_gas.append(kuwait_oil)
oman = oman_cond.append(oman_gas).append(oman_oil)
pak = pak_cond.append(pak_gas).append(pak_oil)
qatar = qatar_gas.append(qatar_oil)
saudi = saudi_gas.append(saudi_oil)
uae = uae_gas.append(uae_oil)
yemen = yemen_cond.append(yemen_gas).append(yemen_oil)

iran['country'] = "Iran"
iraq['country'] = "Iraq"
kuwait['country'] = "Kuwait"
oman['country'] = "Oman"
pak['country'] = "Pakistan"
qatar['country'] = "Qatar"
saudi['country'] = "Saudi Arabia"
uae['country'] = "UAE"
yemen['country'] = "Yemen"

# There's at least one "None" geometry in Iran; remove "None" geometries
iran = iran[~iran.geometry.isna()]

# Merge into one dataset
all_dfs_final = [iran, iraq, kuwait, oman, pak, qatar, saudi, uae, yemen]
data = pd.concat(all_dfs_final)
data = data.reset_index(drop=True)

data['Name'] = data.Name.fillna('N/A')
data['onoff'] = data.onoff.fillna('N/A')


# Replace 'Untitled' with NOT AVAILABLE
data.loc[data.Name.str.contains('Untitled'), 'Name'] = 'N/A'
data.loc[data.Name.str.contains('unknown'), 'Name'] = 'N/A'


# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)
# Calculate the area of each polygon, and add the results in a new column
data = calculate_basin_area_km2(data, attrName="AREA_KM2")

# ========================================================================
# %% MIDDLE EAST (oilandgasinfrastructure.com) - Integration + Export
# ========================================================================
data_, errors = integrate_basins(
    data,
    starting_ids=0,
    category='Oil and natural gas fields',
    fac_alias="OIL_GAS_BASINS",
    country='country',
    # state_prov = None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='onoff',
    _name='Name',
    reservoir_type='type',
    # op_name = None,
    _area_km2='AREA_KM2'
)


save_spatial_data(
    data_,
    file_name="middleeast_fields",
    schema_def=False,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% RUSSIA
# =============================================================================
# os.chdir(pubdata)
fp1 = r"russia\Russian_oil_Fields.shp"
oil_fields = read_spatial_data(fp1, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)


# -----------------------------------------------------------------------------
names = []

# Format the names
for idx1_, row1_ in tqdm(oil_fields1.iterrows(), total=oil_fields1.shape[0]):
    name = str(row1_.Name)
    if 'NO DATA' in name:
        names_null = "N/A"
        names.append(names_null)
    else:
        names.append(name)

# Append these new attributes to GDF
oil_fields1['NAMES_NEW'] = names


# =============================================================================
# %% RUSSIA - Integration + Export
# =============================================================================
oil_fields_final, errors = integrate_basins(
    oil_fields1,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Russia",
    # state_prov="",
    src_ref_id="181",
    src_date="2016-08-15",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='NAMES_NEW',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

# Remove z values from geometry
oil_fields_final_no_z = strip_z_coord(oil_fields_final)


save_spatial_data(
    oil_fields_final_no_z,
    file_name="russia_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% THAILAND
# =============================================================================
# os.chdir(pubdata)

# Read in all field data
fp1 = r"thailand\Condensate_Fields_Thailand.kml.shp"
condensate_fields = read_spatial_data(fp1, table_gradient=True)
condensate_fields['TYPE'] = 'CONDENSATE'
condensate_fields1 = transform_CRS(condensate_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp2 = r"thailand\Thailand_oil_Fields.shp"
oil_fields = read_spatial_data(fp2, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp3 = r"thailand\Thailand_gas_Fields.shp"
gas_fields = read_spatial_data(fp3, table_gradient=True)
gas_fields['TYPE'] = 'GAS'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fields_concat = gpd.GeoDataFrame(pd.concat([gas_fields1, oil_fields1, condensate_fields1], ignore_index=True))
fields_concat1 = fields_concat.set_crs(4326)
fields_concat11 = calculate_basin_area_km2(fields_concat1, attrName="AREA_KM2")

# Remove z values from fields geometry
fields_concat11_no_z = strip_z_coord(fields_concat11)


# Read in all block data
fp4 = r"thailand\Thailand_License_Blocks.shp"
licenseblock = read_spatial_data(fp4, table_gradient=True)
licenseblock1 = transform_CRS(licenseblock, target_epsg_code="epsg:4326", appendLatLon=True)
licenseblock11 = calculate_basin_area_km2(licenseblock1, attrName="AREA_KM2")

# Remove z values from block geometry
licenseblock11_no_z = strip_z_coord(licenseblock11)

# =============================================================================
# %% THAILAND - Integration + Export
# =============================================================================
fields_final, errors = integrate_basins(
    fields_concat11_no_z,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Thailand",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

block_final, errors = integrate_basins(
    licenseblock11_no_z,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="Thailand",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    # reservoir_type = 'COMMODITY',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)


save_spatial_data(
    fields_final,
    file_name="thailand_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)

save_spatial_data(
    blocks_final,
    file_name="thailand_license_blocks",
    # schema_def = True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% TURKMENISTAN
# =============================================================================
# os.chdir(pubdata)
fp1 = r"turkmenistan\Gas_Fields_Turkmenistan.kml.shp"
gas_fields = read_spatial_data(fp1, table_gradient=True)
gas_fields['TYPE'] = 'GAS'
gas_fields1 = transform_CRS(gas_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp2 = r"turkmenistan\Oil_Fields_Turkmenistan.kml.shp"
oil_fields = read_spatial_data(fp2, table_gradient=True)
oil_fields['TYPE'] = 'OIL'
oil_fields1 = transform_CRS(oil_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fp3 = r"turkmenistan\Condensate_Fields_Turkmenistan.kml.shp"
condensate_fields = read_spatial_data(fp3, table_gradient=True)
condensate_fields['TYPE'] = 'OIL & GAS'
condensate_fields1 = transform_CRS(condensate_fields, target_epsg_code="epsg:4326", appendLatLon=True)

fields_concat = gpd.GeoDataFrame(pd.concat([gas_fields1, oil_fields1, condensate_fields1], ignore_index=True))
fields_concat1 = fields_concat.set_crs(4326)
fields_concat11 = calculate_basin_area_km2(fields_concat1, attrName="AREA_KM2")
fields_concat11.Name.replace({'Placemark': 'N/A'}, inplace=True)

# Remove z values from fields geometry
fields_concat11_no_z = strip_z_coord(fields_concat11)


fp4 = r"turkmenistan\Turkmenistan_license_blocks.kml.shp"
licenseblock = read_spatial_data(fp4, table_gradient=True)

licenseblock1 = transform_CRS(licenseblock, target_epsg_code="epsg:4326", appendLatLon=True)
licenseblock11 = calculate_basin_area_km2(licenseblock1, attrName="AREA_KM2")

# Remove z values from block geometry
licenseblock11_no_z = strip_z_coord(licenseblock11)

# =============================================================================
# %% TURKMENISTAN - Integration + Export
# =============================================================================
turk_fields_final, errors = integrate_basins(
    fields_concat11_no_z,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="Turkmenistan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    reservoir_type='TYPE',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)

turk_blocks_final, errors = integrate_basins(
    licenseblock11_no_z,
    starting_ids=0,
    category="Oil and natural gas license blocks",
    country="Turkmenistan",
    # state_prov="",
    src_ref_id="22",
    src_date="2014-01-01",
    # src_date="1",
    # on_offshore="OFFSHORE",
    _name='Name',
    # reservoir_type = 'COMMODITY',
    # op_name="Operator",
    _area_km2='AREA_KM2'
)


save_spatial_data(
    turk_fields_final,
    file_name="turkmenistan_fields",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)

save_spatial_data(
    turk_blocks_final,
    file_name="turkmenistan_license_blocks",
    schema_def=True,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% UNITED KINGDOM
# =============================================================================
os.chdir(v24data)

# Use oginfra.com data for ONSHORE fields
fp3 = r"united_kingdom\oginfra_dotcom\GAS_hydrocarbon_fields_UnitedKingdom.kml.shp"
uk_on_gas = read_spatial_data(fp3, table_gradient=True)
uk_on_gas['COMMODITY'] = 'GAS'
uk_on_gas['ON_OFFSHORE'] = 'ONSHORE'
fp5 = r"united_kingdom\oginfra_dotcom\OIL_hydrocarbon_fields_UnitedKingdom.kml.shp"
uk_on_oil = read_spatial_data(fp5, table_gradient=True)
uk_on_oil['COMMODITY'] = 'OIL'
uk_on_oil['ON_OFFSHORE'] = 'ONSHORE'
# concat oginfra fields
uk_onshore = gpd.GeoDataFrame(pd.concat([uk_on_oil, uk_on_gas],
                                        ignore_index=True),
                              geometry='geometry',
                              crs=4326)
uk_onshore['src'] = '22'

# Use North Sea Transition Authority for offshore fields
fp_uk = r'united_kingdom/NSTA_Offshore_Fields_(ETRS89).geojson'
uk_offshore = gpd.read_file(fp_uk)
# Rename a few columns so they append well to the oginfra fields
uk_offshore = uk_offshore.rename(columns={"FIELDNAME": "Name",
                                          "FIELDTYPE": "COMMODITY"}, errors="raise")
uk_offshore.loc[uk_offshore.COMMODITY == 'COND', 'COMMODITY'] = 'CONDENSATE'
uk_offshore['ON_OFFSHORE'] = 'OFFSHORE'
uk_offshore['src'] = '264'
# Join
uk_fields = uk_onshore.append(uk_offshore).reset_index()
uk_fields.CURR_OPER = uk_fields.CURR_OPER.fillna('N/A')

# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
uk_fields = calculate_basin_area_km2(uk_fields, attrName="AREA_KM2")


# =============================================================================
# %% UNITED KINGDOM - Integration + Export
# =============================================================================
uk_fields_final, errors = integrate_basins(
    uk_fields,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="United Kingdom",
    # state_prov="",
    src_ref_id="src",
    src_date="2024-11-16",
    on_offshore="ON_OFFSHORE",
    _name='Name',
    reservoir_type='COMMODITY',
    op_name="CURR_OPER",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    uk_fields_final,
    file_name="uk_fields",
    # schema_def=True,  # there is a single multipart feature in uk_fields
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% NORTH AMERICA - BASINS (EDF)
# Basin layer assembled by Anthony
# =============================================================================
os.chdir(pubdata)
# !!! NOTE: Do NOT make a copy of this shp and put it in the v2.4 folder,
# read it in from Anthony's original location
fields_us = "International_data_sets\\Basins\\Basin_Boundaries_Project\\NA_basins_OGIM.shp"
data_us = read_spatial_data(fields_us)

# Transform CRS
data_us_flds = transform_CRS(data_us)
data_us_flds.BASIN_NAME = data_us_flds.BASIN_NAME.str.replace('_', ' ')

data_us_flds = data_us_flds.reset_index()
data_us_flds = data_us_flds.rename(columns={"index": "New_ID"})
data_us_flds['New_ID'] = data_us_flds.index.astype(str)

# Add a SRC_ID for records that are missing it (specifically, basins that we created)
data_us_flds.loc[(data_us_flds.SRC_ID.isna()) & (data_us_flds.Source == 'OGIM Creation'), 'SRC_ID'] = '300'

# Assign country name(s) to each basin polygon
# Basins are NOT auto-assigned a STATE_PROV value
data_us_flds1 = assign_countries_to_feature(data_us_flds,
                                            gdf_country_colname='BASINS_AND',
                                            gdf_uniqueid_field='New_ID',
                                            boundary_geoms=country_geoms,
                                            overwrite_country_field=True)


data_us_flds2 = calculate_basin_area_km2(data_us_flds1, attrName="AREA_KM2")

# =============================================================================
# %% NORTH AMERICA - BASINS (EDF) - Integration + Export
# =============================================================================
us_fields_final, errors = integrate_basins(
    data_us_flds2,
    starting_ids=0,
    category="Oil and natural gas basins",
    country="COUNTRY",
    # state_prov="",
    src_ref_id="SRC_ID",
    src_date="SRC_DATE_",
    on_offshore=None,
    _name='BASIN_NAME',
    reservoir_type='',
    # op_name="",
    _area_km2='AREA_KM2'
)

save_spatial_data(
    us_fields_final,
    file_name="united_states_oil_gas_basins",
    schema_def=False,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% UNITED STATES - FIELDS
# =============================================================================
os.chdir(v24data)
# Read in all separate field shapefiles and concatenate together
fp_us_fields = r"united_states\national\oil_gas_fields_eia"
files_ = glob.glob(fp_us_fields + "\\*.shp")
all_field_polys = []
for file in files_:
    df = gpd.read_file(file).to_crs("epsg:4326")
    # Capitalize all column names
    df.columns = map(str.upper, df.columns)
    all_field_polys.append(df)
us_fields_all = pd.concat(all_field_polys).reset_index(drop=True)
us_fields_all = us_fields_all.rename(columns={'GEOMETRY': 'geometry'})
us_fields_all = gpd.GeoDataFrame(us_fields_all, geometry='geometry', crs=4326)


# Convert state name abbreviations into full names
us_fields_all = replace_row_names(us_fields_all,
                                  "STATE",
                                  dict_names=dict_us_states)

# Properly set N/A values
us_fields_all["STATE"] = us_fields_all["STATE"].replace({"NAN": "N/A",
                                                         np.nan: "N/A"})
us_fields_all["ON_OFFSHOR"] = us_fields_all["ON_OFFSHOR"].replace({"NAN": "N/A",
                                                                   np.nan: "N/A",
                                                                   "NaN": "N/A"})

# Assign a STATE value(s) to each field polygon
us_fields_all['tempindex'] = us_fields_all.index
us_fields_all['STATE_new'] = 'N/A'
us_fields_all = assign_stateprov_to_feature(us_fields_all,
                                            gdf_stateprov_colname='STATE_new',
                                            gdf_uniqueid_field='tempindex',
                                            boundary_geoms=state_geoms,
                                            overwrite_stateprov_field=True)

us_fields_all = calculate_basin_area_km2(us_fields_all,
                                         attrName="AREA_KM2").reset_index(drop=True)


# =============================================================================
# %% UNITED STATES - FIELDS - Integration + Export
# =============================================================================
us_fields_integrated, errors = integrate_basins(
    us_fields_all,
    starting_ids=0,
    category="Oil and natural gas fields",
    country="UNITED STATES OF AMERICA",
    state_prov="STATE_new",
    src_ref_id="143,144,145",
    src_date="2022-04-01",
    on_offshore="ON_OFFSHOR",
    _name='FIELD',
    reservoir_type=None,
    # op_name="",
    _area_km2='AREA_KM2'
)

# GeoJSON
save_spatial_data(
    us_fields_integrated,
    file_name="united_states_oil_gas_fields",
    schema_def=False,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder)


# =============================================================================
# %% O&G SEDIMENTARY BASINS (Robertson)
# =============================================================================
os.chdir(pubdata)
rob_basins = read_spatial_data(r"International_data_sets\Basins\Robertson_Sedimentary_Basins\Robertson_Basins_SubRegimes_Fixed_Geometries.shp")

# Check adn transform CRS
rob_basins2 = transform_CRS(rob_basins)

# Exclude US,  Canada, and Mexico
names1 = ['North America Alaska',
          'North America Alaska Fore-arc',
          'North America Appalachian',
          'North America East Coast',
          'North America Foreland',
          'North America Interior Platform',
          'North America Intracratonic Sag',
          'North America Onshore Gulf Salt',
          'North America Rockies',
          'North America West Coast',
          'North America Western US Rockies',
          'Gulf of Mexico',
          'Arctic',
          'Central America North',
          'Central America South',
          'American North Atlantic',
          'Circum-North Atlantic']

basins_sel = rob_basins.query("BASINS_AND != @names1")
basins_sel.plot()

basins_sel = basins_sel.reset_index()
basins_sel = basins_sel.rename(columns={"index": "New_ID"})
basins_sel['New_ID'] = basins_sel.index.astype(str)


# Basin area
basins_ = calculate_basin_area_km2(basins_sel)

# Assign country names to basins
basins___ = assign_countries_to_feature(basins_,
                                        gdf_country_colname='BASINS_AND',
                                        gdf_uniqueid_field='New_ID',
                                        boundary_geoms=country_geoms,
                                        overwrite_country_field=True)

# =============================================================================
# %% O&G SEDIMENTARY BASINS (Robertson) - Integration + Export
# =============================================================================
robertson_, _errors = integrate_basins(
    basins___,
    starting_ids=1,
    category="Oil and natural gas basins",
    fac_alias="OIL_GAS_BASINS",
    country="COUNTRY",
    state_prov=None,
    src_ref_id="198",
    src_date="2022-10-22",
    on_offshore="LOCATION",
    _name="BASIN_NAME",
    reservoir_type=None,
    op_name=None,
    _area_km2="AREA_KM2"
)

save_spatial_data(
    robertson_,
    file_name="robertson_oil_gas_basins_no_north_america",
    schema_def=False,
    schema=schema_BASINS,
    file_type="GeoJSON",
    out_path=results_folder
)
