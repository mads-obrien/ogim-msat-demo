# -*- coding: utf-8 -*-
"""
Created on November 30, 2023

Data integration of global OIL AND GAS WELLS for all locations EXCEPT:
    - Canada wells
    - Mexico wells
    - United States wells

# TODO:
[x] standardize import statements and CWD setting
[] standardize spacing between sections
[x] alphabetize countries
[] update all file paths

@author: maobrien, momara, ahimmelberger
"""
import os
import pandas as pd
import geopandas as gpd
import numpy as np
import re
# from datetime import datetime
import shapely.wkt
# from tqdm import tqdm
# import glob

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, create_concatenated_well_name,
                     transform_CRS, integrate_facs, save_spatial_data,
                     schema_WELLS, read_spatial_data, transform_geom_3d_2d,
                     replace_missing_strings_with_na, deduplicate_with_rounded_geoms)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Define paths to current working directories
# pubdata = os.path.join(buii_path, 'Public_Data', 'data')
v24data = os.path.join(buii_path, f'OGIM_{version_num}', 'data')

# Folder in which all integrated data will be saved
results_folder = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'


# =============================================================================
# %% AFGHANISTAN - remove from OGIM
# =============================================================================
# fp = r"Middle_East+Caspian\\Afghanistan\other_data_\Wells\wellsafg_reexport.shp"
# data = read_spatial_data(fp, table_gradient=True)

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)


# # create more accurate well name field by combining name and the "well number" attribute
# data["wellname"] = data.USGS_NAME +' '+ data.WELL_NO.astype(str)


# wells_AFGHANISTAN, errors = integrate_facs(
#     data,
#     starting_ids=0,
#     category='Oil and natural gas wells',
#     fac_alias='WELLS',
#     country='Afghanistan',
#     state_prov = None,
#     src_ref_id='162',
#     src_date='2006-04-01',
#     on_offshore = None,
#     fac_name='wellname',
#     fac_id='USGS_ID',
#     fac_type='WELL_TYPE',
#     spud_date='SPUD_DATE',
#     comp_date='COMP_DATE',
#     drill_type = None,
#     install_date = None,
#     fac_status = None,
#     op_name = None,
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )

# save_spatial_data(
#     wells_AFGHANISTAN,
#     file_name="afghanistan_oil_gas_wells",
#     schema_def=True,
#     schema=schema_WELLS,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% ALGERIA - REMOVE FROM OGIM
# =============================================================================
# os.chdir(pubdata)
# fp = r"Africa\Algeria\Algeria_Development_wells.shp"
# data = read_spatial_data(fp, table_gradient=False)

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# # Reformat dates in SPUDDED and COMPLETED column to match our desired hyphenated format.

# # Fill empty cells in SPUDDED and COMPLETED column with our NA date value
# data['COMPLETED'] = data['COMPLETED'].fillna('1900.01.01')
# data['SPUDDED'] = data['SPUDDED'].fillna('1900.01.01')

# # Reformat dates from YYYY.MM.DD to YYYY-MM-DD, then convert date to a string
# data['COMPLETED_NEW'] = data['COMPLETED'].apply(lambda x: str(datetime.strptime(x, '%Y.%m.%d').strftime('%Y-%m-%d')))
# data['SPUDDED_NEW'] = data['SPUDDED'].apply(lambda x: str(datetime.strptime(x, '%Y.%m.%d').strftime('%Y-%m-%d')))

# # Replace "UNKNOWN" status and COMMODITY with "N/A"
# print(data["RESULT"].unique())
# print(data["STATUS"].unique())
# data["RESULT"] = data["RESULT"].replace({"UNKNOWN": "N/A", "NOT APPLICABLE": "N/A"})
# data["STATUS"] = data["STATUS"].replace({"UNKNOWN": "N/A"})

# =============================================================================
# %% ALGERIA: Integration + Export
# =============================================================================
# wells_ALG, errors = integrate_facs(
#     data,
#     starting_ids=0,
#     category='Oil and natural gas wells',
#     fac_alias='WELLS',
#     country='Algeria',
#     state_prov=None,
#     src_ref_id='160',
#     src_date='2014-05-01',
#     on_offshore=None,
#     fac_name='WELL_NAME',
#     fac_id=None,
#     fac_type='TYPE',
#     spud_date='SPUDDED_NEW',
#     comp_date='COMPLETED_NEW',
#     drill_type=None,
#     install_date=None,
#     fac_status='STATUS',
#     op_name='OPERATOR',
#     commodity='RESULT',
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )

# save_spatial_data(
#     wells_ALG,
#     file_name="algeria_oil_gas_wells",
#     schema_def=True,
#     schema=schema_WELLS,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% ARGENTINA
# Explanation of what some of these fields mean, in Spanish, is here
# https://github.com/datosenergia/produccion-de-petroleo-y-gas-por-pozo
# =============================================================================
os.chdir(v24data)
fp = r"argentina\wells\shapefile-de-pozos-shp.shp"
arg_wells = read_spatial_data(fp, table_gradient=True, specify_encoding=True, data_encoding="utf-8")
arg_wells = transform_CRS(arg_wells, appendLatLon=True)

# One well has incorrect coordinates -- drop this record by dropping any point
# with a longitude higher than -40
arg_wells = arg_wells.cx[:-40, :]

# Translate fac type
type_dict = {
    'Acuífero': 'Aquifer well',
    'Bidireccional de almacenamiento': 'Bidirectional storage',
    'Gasífero': 'Gas',
    'Inyección de Agua': 'Water injection',
    'Inyección de Gas': 'Gas injection',
    'Monitoreo de almacenamiento': 'Storage monitoring',
    'No informado': 'N/A',
    'Otro tipo': 'N/A',
    'Petrolífero': 'Oil',
    'Sumidero': 'Sinkhole'
}
arg_wells = replace_row_names(arg_wells, colName="TIPO_POZO", dict_names=type_dict)

# Translate status
status_dict = {
    'A Abandonar': 'ABANDONED',
    'Abandonado': 'ABANDONED',
    'Abandono Temporario': 'TEMPORARILY ABANDONED',
    'En Espera de Reparación': 'AWAITING REPAIR',
    'En Estudio': 'MONITORING',
    'En Inyección Efectiva': 'INJECTION',
    'En Reparación': 'MAINTENANCE',
    'En Reserva de Gas': 'GAS RESERVE',
    'En Reserva para Recup. Sec./Asist.': 'IN RESERVE',
    'Extracción Efectiva': 'ACTIVE',
    'Mantenimiento de Presión': 'PRESSURE MAINTENANCE',
    'No informado': 'N/A',
    'Otras Situación Activo': 'ACTIVE',
    'Otras Situación Inactivo': 'INACTIVE',
    'Parado Alta Relación Agua/Petróleo': 'SHUT-IN HIGH WATER/OIL RATIO',
    'Parado Alta Relación Gas/Petróleo': 'SHUT-IN HIGH GAS/OIL RATIO',
    'Parado Transitoriamente': 'TEMPORARILY SHUT-IN'
}
arg_wells = replace_row_names(arg_wells, colName="TIPO_ESTAD", dict_names=status_dict)


# Translate resource type field (non-conventional vs conventional), aka drill type
resource_dict = {'CONVENCIONAL': 'CONVENTIONAL',
                 'No informado': 'N/A',
                 'NO CONVENCIONAL': 'NON-CONVENTIONAL',
                 'SIN RESERVORIO': 'N/A',
                 'NO DISCRIMINADO': 'N/A'}
arg_wells = replace_row_names(arg_wells, colName="TIPO_RECUR", dict_names=resource_dict)


# Replace the province value "Estado Nacional" with N/A
arg_wells.PROVINCIA.replace({'Estado Nacional': 'N/A'}, inplace=True)

# Because this is a shapefile, some of the field names got truncated
# FECHA_INIC = Fecha de inicio de perforación del pozo (well drilling start date) *USE THIS
# FECHA_FIN_ = Fecha de fin de perforación del pozo (Well drilling end date)
# ASFECHA_IN = Fecha de Inicio de la terminación de perforación del pozo (Start date of well drilling completion)
# FECHA_F_01 = Fecha del Fin de terminación de perforación del pozo (Well drilling completion date)  *USE THIS


# =============================================================================
# %% ARGENTINA - Integration + Export
# =============================================================================
arg_wells_integrated, arg_wells_err = integrate_facs(
    arg_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Argentina",
    state_prov="PROVINCIA",
    src_ref_id="105",
    src_date="2024-04-19",
    on_offshore=None,
    fac_name="SIGLA",
    fac_id="IDENTIFICA",
    fac_type="TIPO_POZO",
    spud_date='FECHA_INIC',  # well drilling start date
    comp_date='FECHA_F_01',  # well completion end date
    drill_type='TIPO_RECUR',
    fac_status="TIPO_ESTAD",
    op_name="EMPRESA",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    arg_wells_integrated,
    file_name="argentina_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AUSTRALIA - South Australia
# =============================================================================
os.chdir(v24data)
# Read petroleum wells
fp2 = r"australia\south_australia\Petroleum wells.shp"
sa_petwells = read_spatial_data(fp2, table_gradient=False)
sa_petwells['SOURCE_ID'] = '104'
sa_petwells = transform_CRS(sa_petwells,
                            target_epsg_code="epsg:4326",
                            appendLatLon=True)


# Some wells with a NULL status value actually contain status info in the TYPE column.
proposed_well_mask = ((sa_petwells.TYPE == 'Proposed') & (sa_petwells.STATUSRR.isna()))
sa_petwells.loc[proposed_well_mask, 'STATUSRR'] = 'Proposed'
sa_petwells.loc[sa_petwells.TYPE == 'Proposed', 'TYPE'] = np.nan

drilling_mask = ((sa_petwells.TYPE == 'Currently Drilling') & (sa_petwells.STATUSRR.isna()))
sa_petwells.loc[drilling_mask, 'STATUSRR'] = 'Currently Drilling'
sa_petwells.loc[sa_petwells.TYPE == 'Currently Drilling', 'TYPE'] = np.nan

# Simplify the "Deviation" field
sa_petwells.DEVIATION = sa_petwells.DEVIATION.replace({'Low Angle': 'Deviated',
                                                       'High Angle': 'Deviated'})

# EXCLUDE MINERAL DRILLHOLES DATASET FOR NOW; provides little/no info on petroleum wells
# -----------------------------------------------------------------------------
# fp4 = r"australia\south_australia\Mineral drillholes.shp"
# drillholes = read_spatial_data(fp4, table_gradient=False)
# drillholes = transform_CRS(drillholes,
#                             target_epsg_code="epsg:4326",
#                             appendLatLon=True)

# # Select the two drillholes that are actually wells (after manual searching through
# # the 32 data ponts that have either gas or oil in the TARGET column,
# # in this case and examning satellite imagery
# dhnumbers2keep = [137750, 267244]
# drillholes = drillholes[drillholes.DHNUMBER.isin(dhnumbers2keep)].reset_index(drop=True)

# # Data pre-processing before data integration
# drillholes['COMP_FORMATTED'] = pd.to_datetime(drillholes['COMPDATE'])

# comp_dates = []
# for idx1_, row1_ in tqdm(drillholes.iterrows(), total=drillholes.shape[0]):
#     # Append to dates list the reformatted dates
#     comp = row1_.COMP_FORMATTED
#     if pd.isna(comp):
#         dates_null = "1900-01-01"
#         comp_dates.append(dates_null)

#     else:
#         formatted_comp = comp.strftime('%Y-%m-%d')
#         comp_dates.append(formatted_comp)

# drillholes['FORMATTED_COMP'] = comp_dates

# =============================================================================
# %% AUSTRALIA - South Australia - Integration + Export
# =============================================================================
# integrate geothermal and petroleum wells
sa_petwells_integrated, errors = integrate_facs(
    sa_petwells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Australia",
    state_prov="SOUTH AUSTRALIA",
    src_ref_id="SOURCE_ID",
    src_date="2025-02-01",
    # on_offshore="SHORE",
    fac_name="WELLDISPLA",
    fac_id="WELL_ID",
    fac_type="TYPE",
    drill_type="DEVIATION",
    spud_date="SPUDDED",
    comp_date="RIGREL",
    fac_status="STATUSRR",
    op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


# Export
save_spatial_data(
    sa_petwells_integrated,
    file_name="australia_south_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% AUSTRALIA - Queensland
# =============================================================================
os.chdir(v24data)
fp = r'australia\queensland\All_bore_hole_and_well_locations\data.gdb'
queen_wells = gpd.read_file(fp,
                            layer='All_bore_hole_and_well_locations',
                            driver='FileGDB')
queen_wells = transform_CRS(queen_wells,
                            target_epsg_code="epsg:4326",
                            appendLatLon=True)

# Remove stratigraphic test wells, Coal mining, and metals/minerals mining
types2drop = ['STRATIGRAPHIC', 'COAL', 'MINERAL', 'CARBON CAPTURE AND STORAGE']
queen_wells = queen_wells[~queen_wells.bore_type.isin(types2drop)].reset_index()

# Remove water wells with bore_subtype == SUPPLY BORE, because in sat imagery
# these don't appear to be OG& related
queen_wells = queen_wells[queen_wells.bore_subtype != 'SUPPLY BORE']

# Replace no-data markers in date field
queen_wells.spud_date.replace({'0999-12-31T00:00:00': np.nan},
                              inplace=True)
queen_wells.completion_date.replace({'1000-01-01T00:00:00': np.nan},
                                    inplace=True)

# Cut away any HH:MM:SS suffixes present in date fields
queen_wells.spud_date = pd.to_datetime(queen_wells.spud_date).dt.strftime("%Y-%m-%d")
queen_wells.completion_date = pd.to_datetime(queen_wells.completion_date).dt.strftime("%Y-%m-%d")

# Fill in missing string values
missingvals = {np.nan: 'N/A', 'UNKNOWN': 'N/A', None: 'N/A'}
queen_wells.bore_type.replace(missingvals, inplace=True)
queen_wells.bore_subtype.replace(missingvals, inplace=True)
queen_wells.status.replace(missingvals, inplace=True)

# Create a more complete FAC_TYPE field by concatenating TYPE and SUBTYPE
create_concatenated_well_name(queen_wells,
                              'bore_type',
                              'bore_subtype',
                              'typenew')

# Drop duplicated records
queen_wells = queen_wells.drop_duplicates(subset=['bore_no',
                                                  'operator_name',
                                                  'bore_name',
                                                  'bore_type',
                                                  'bore_subtype',
                                                  'result',
                                                  'status',
                                                  'spud_date',
                                                  'completion_date',
                                                  'bore_label',
                                                  'latitude',
                                                  'longitude'],
                                          keep='first').reset_index()

# =============================================================================
# %% AUSTRALIA - Queensland - Integration + Export
# =============================================================================
# integrate geothermal and petroleum wells
queen_wells_integrated, errors = integrate_facs(
    queen_wells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Australia",
    state_prov="QUEENSLAND",
    src_ref_id="240",
    src_date="2024-04-18",  # Daily
    # on_offshore="SHORE",
    fac_name="bore_name",
    fac_id="borehole_pid",
    fac_type="typenew",
    # drill_type="",
    spud_date="spud_date",
    comp_date="completion_date",
    fac_status="status",
    op_name="operator_name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    queen_wells_integrated,
    file_name="australia_queensland_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% AUSTRALIA - Nation-wide, including Offshore
# From National Offshore Petroleum Titles Administrator (NOPTA)
# =============================================================================
os.chdir(v24data)
fp = r"australia\national\PetroleumWells.shp"
nopta = read_spatial_data(fp, table_gradient=False)
nopta = transform_CRS(nopta, target_epsg_code="epsg:4326", appendLatLon=True)

# Delete a few wells whose coordinates are very wrong (positive latitudes)
nopta = nopta.query("Latitude < 0")

# Drop wells that don't seem to be O&G related (for example, Texas A&M
# research wells with the Ocean Drilling Program or ODP)
types2drop = ['Stratigraphic Investigation',
              'Research',
              'Geochemistry',
              'Moundspring Investigation']
nopta = nopta[~nopta.Purpose.isin(types2drop)]
operators2drop = ['Texas A and M University', 'unknown', 'Bureau of Mineral Resources']
nopta = nopta[~nopta.Operator.isin(operators2drop)]
# Exclude certain well types
nopta = nopta[~nopta.Type.isin(['Groundwater', 'Mineral or Coal', 'Greenhouse Gas'])]

# Remove wells for which we already have a good dataset
nopta_regions_to_remove = ['South Australia', 'Queensland', 'Western Australia']
nopta = nopta[~nopta.OffshrArea.isin(nopta_regions_to_remove)]

# TODO !!!
# # For Western Australia, identify onshore Western Australia wells and drop them
# mask_westaus_onshore = ((nopta.OffshrArea == 'Western Australia') & (nopta.IsOffshore == 'No'))
# nopta = nopta[~mask_westaus_onshore].reset_index(drop=True)

# Create onoffshore field
nopta['onoff'] = nopta.IsOffshore
nopta.onoff = nopta.onoff.replace({'Yes': 'OFFSHORE',
                                   'No': 'ONSHORE'})

# Some of these wells are in Papua New Guinea -- properly label them
nopta['countrynew'] = 'Australia'
nopta = nopta.query("WellName != 'Paritutu 1'")  # drop random New Zealand well
nopta.loc[nopta.OffshrArea == 'Outside of Australia', 'countrynew'] = 'Papua New Guinea'

# =============================================================================
# %% AUSTRALIA - Nation-wide Offshore - Integration + Export
# =============================================================================
nopta_integrated, errors = integrate_facs(
    nopta,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="countrynew",
    # state_prov=None,
    src_ref_id="108",
    src_date="2024-04-01",
    on_offshore='onoff',
    fac_name="WellName",
    fac_id="Uwi",  # I doubt whether this is truly the UWI
    fac_type='Purpose',
    # drill_type=None,
    spud_date="KckOffDate",
    comp_date="RigRlsDate",
    # fac_status=None,
    op_name="Operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    nopta_integrated,
    file_name="australia_nopta_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% AUSTRALIA - Western Australia
# NOTE: Since 1 January 2012 Offshore wells have been managed by National
# Offshore Petroleum Titles Administrator (NOPTA). NOPTA should be contacted
# for currency and completeness of offshore wells since this date. The extent
# of wells is both onshore and offshore (prior to 1/1/2012). Onshore wells are
# updated on a regular basis. Offshore is done on an adhoc basis.

# Therefore, for this Western Australia wells dataset, keep all the ONSHORE
# records and remove the offshore ones, instead using data directly from NOPTA
# to fill in the offshore locations.
# =============================================================================
os.chdir(v24data)
fp5 = r'australia\western_australia\Petroleum_Wells.shp'
western_wells = read_spatial_data(fp5, table_gradient=False)
western_wells = transform_CRS(western_wells,
                              target_epsg_code="epsg:4326",
                              appendLatLon=True)

# Update status columnn
dict_names = {
    'A': 'ABANDONED',
    'ABANDONED': 'ABANDONED',
    'COMPLETED': 'COMPLETED',
    'DRILLING': 'DRILLING',
    'DRILLING_SUSPENDED': 'SUSPENDED',
    'IN': 'INJECTION',
    'G': 'GAS',
    'G&C': 'GAS & COAL',
    'O': 'OIL',
    'O&G': 'OIL & GAS',
    'P&A': 'PLUGGED & ABANDONED',
    'P&A G': 'PLUGGED & ABANDONED',
    'P&A GC': 'PLUGGED & ABANDONED',
    'P&A GC': 'PLUGGED & ABANDONED',
    'P&A GS': 'PLUGGED & ABANDONED',
    'P&A O': 'PLUGGED & ABANDONED',
    'P&A OG': 'PLUGGED & ABANDONED',
    'P&A OGS': 'PLUGGED & ABANDONED',
    'P&A OS': 'PLUGGED & ABANDONED',
    'P&A STR': 'PLUGGED & ABANDONED',
    'P&S': 'PLUGGED & SUSPENDED',
    'P&SUSP O': 'PLUGGED & SUSPENDED',
    'SHUT IN': 'PLUGGED & ABANDONED',
    'SUS-OG': 'SUSPENDED',
    'SUSP': 'SUSPENDED',
    'SUSP G': 'SUSPENDED',
    'SUSP GC': 'SUSPENDED',
    'SUSP GS': 'SUSPENDED',
    'SUSP O': 'SUSPENDED',
    'SUSP OG': 'SUSPENDED',
    'SUSP OGS': 'SUSPENDED',
    'SUSP OS': 'SUSPENDED',
    'SUSPENDED': 'SUSPENDED',
    'UNKNOWN': 'N/A',
    'WI': 'WATER INJECTION',
    np.nan: 'N/A',
    None: 'N/A'
}
western_wells = replace_row_names(western_wells, "STATUS", dict_names)

# Update class columnn
# NOTE: I have looked but cannot find a data dictionary for this Class field
dict_names = {
    'DEV': 'DEVELOPMENTAL',
    'CBM': 'COALBED METHANE WELL',
    'DPT': 'DEEPER POOL TEST',
    'EXT': 'EXPERIMENTAL',
    'GEOTHERMAL': 'GEOTHERMAL',
    'INJECT_CO2': 'CO2 INJECTION WELL',
    'MIN': 'N/A',  # TODO - find what this abbrev means
    'NFW': 'NEW FIELD WILDCAT',
    'NPW': 'NEW POOL WILDCAT',
    'O': 'OIL',
    'OBSERVATION': 'OBSERVATION',
    'SERVICE': 'SERVICE',
    'SPT': 'STANDARD PENETRATION TEST',  # TODO - this is a guess, update if a better answer is found
    'STORAGE': 'STORAGE',
    'STR': 'STRATIGRAPHIC TEST',
    'WASTE_DISPOS': 'WASTE DISPOSAL',
    'WDW': 'WATER DISPOSAL',
    'WIW': 'WATER INJECTION',
    'WSW': 'WATER SOURCE WELL',
    '': 'N/A',
    np.nan: 'N/A',
    None: 'N/A'
}
western_wells = replace_row_names(western_wells, "CLASS", dict_names)

# DEDUPLICATION: Remove records that are dupes in every attribute we care about except NAME
western_wells = western_wells.drop_duplicates(subset=['UWI',
                                                      'BASIN',
                                                      'LEASE_NO',
                                                      'LONG',
                                                      'LAT',
                                                      'OPERATOR',
                                                      'CLASS',
                                                      'STATUS',
                                                      'FIELD',
                                                      'SPUD_DATE',
                                                      'RIG_RELEAS'], keep='first')

# For records where everything (including status) are identical, but NAME and
# SPUD_DATE differs, keep the record which has the MOST RECENT spud date.
# If the two records have everything in common including SPUD DATE, keep the
# record with the newest RIG_RELEASE date
western_wells_newestfirst = western_wells.sort_values(by=['SPUD_DATE', 'RIG_RELEAS'],
                                                      ascending=[False, False],
                                                      na_position='last')

western_wells = western_wells_newestfirst.drop_duplicates(subset=['UWI',
                                                                  'BASIN',
                                                                  'LEASE_NO',
                                                                  'LONG',
                                                                  'LAT',
                                                                  'OPERATOR',
                                                                  'CLASS',
                                                                  'STATUS',
                                                                  'FIELD'], keep='first')

# Next, for records whwere everything is identical except NAME, SPUD_DATE, and STATUS,
# keep the record with the most recent spud date. In theory, this means we are
# keeping the "most recent" status that well had.
# western_wells is still sorted with newest spud_Date first
western_wells = western_wells.drop_duplicates(subset=['UWI',
                                                      'BASIN',
                                                      'LEASE_NO',
                                                      'LONG',
                                                      'LAT',
                                                      'OPERATOR',
                                                      'CLASS',
                                                      'FIELD'], keep='first')

# Confirm all the dupes are gone (This should return zero)
dupes = western_wells[western_wells.duplicated(subset=['UWI', 'LONG', 'LAT'], keep=False)]
western_wells = western_wells.reset_index(drop=True)

# TODO !!!
# # Remove any (offshore) wells from this Western Australia dataset for which
# # there's a more up-to-date record for that well provided from NOPTA.
# nopta_wa_offshore = nopta[((nopta.OffshrArea == 'Western Australia') & (nopta.IsOffshore == 'Yes'))]
# nopta_wa_offshore_well_names = list(nopta_wa_offshore.WellName.unique())
# mask_wa_offshore = (western_wells.WELL_NAME.isin(nopta_wa_offshore_well_names))
# western_wells = western_wells[~mask_wa_offshore].reset_index(drop=True)

# =============================================================================
# %% AUSTRALIA - Western Australia - Integration + Export
# =============================================================================
western_wells_integrated, errors = integrate_facs(
    western_wells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="AUSTRALIA",
    state_prov="WESTERN AUSTRALIA",
    src_ref_id="107",
    src_date="2024-01-26",
    # on_offshore="OFFSHORE",
    fac_name="WELL_NAME",
    fac_id="UWI",
    fac_type="CLASS",
    # drill_type="",
    spud_date="SPUD_DATE",
    comp_date="RIG_RELEAS",
    fac_status="STATUS",
    op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

# Export
save_spatial_data(
    western_wells_integrated,
    file_name="australia_western_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# # =============================================================================
# %% BOLIVIA
# # =============================================================================
os.chdir(v24data)
fp = r'bolivia\geobolivia\pozos.csv'
bol_wells_csv = pd.read_csv(fp)
bol_wells = gpd.GeoDataFrame(bol_wells_csv,
                             geometry=bol_wells_csv.geometry.map(shapely.wkt.loads),
                             crs=4326)
bol_wells = transform_CRS(bol_wells, appendLatLon=True)

# Drop duplicate records (same attributes & identical or nearly-identical geometries)
# Note that well_name and ba_name should be made all-caps first, since some
# records are duplicates all except for their capitalization
bol_wells.well_name = bol_wells.well_name.str.upper()
bol_wells.ba_name = bol_wells.ba_name.str.upper()
bol_wells = deduplicate_with_rounded_geoms(bol_wells,
                                           column_subset=['well_name',
                                                          'well_num',
                                                          'ba_name'],
                                           desired_precision=5)

# =============================================================================
# %% BOLIVIA - Integration + Export
# =============================================================================
bol_wells_integrated, bol_wells_err = integrate_facs(
    bol_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Bolivia",
    # state_prov=None,
    src_ref_id="121",
    src_date="2017-09-08",
    on_offshore='ONSHORE',
    fac_name="well_name",
    fac_id="well_num",
    # fac_type=None,
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # install_date=None,
    # fac_status=None,
    op_name="ba_name",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    bol_wells_integrated,
    file_name="bolivia_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% BRAZIL
# =============================================================================
os.chdir(v24data)
fp = r'brazil\GeoMapsANP\pocos_gishub_db.shp'
brz_wells = read_spatial_data(fp, table_gradient=True)
brz_wells = transform_CRS(brz_wells, appendLatLon=True)
# print(*brz_wells.columns, sep='\n')
# Only keep potentially useful columns
brz_wells = brz_wells[[
    'ogr_fid',
    'name',
    'operador',
    'categoria',
    'tipo_grupo',
    'tipo',
    'reclassifi',
    'situacao',
    'dat_inicio',
    'dat_termin',
    'dat_conclu',
    'direcao',
    'latitude_calc',
    'longitude_calc']]

# Reformat date fields
brz_wells.dat_inicio.replace({'00/01/1900': '01/01/1900'}, inplace=True)
brz_wells.dat_conclu.replace({'00/01/1900': '01/01/1900'}, inplace=True)
brz_wells['spud'] = pd.to_datetime(brz_wells.dat_inicio,
                                   format='%d/%m/%Y').dt.strftime("%Y-%m-%d")
brz_wells['comp'] = pd.to_datetime(brz_wells.dat_conclu,
                                   format='%d/%m/%Y',
                                   errors='coerce').dt.strftime("%Y-%m-%d")

# Translate drilling direction (vert and horz are spelled correctly)
brz_wells.direcao.replace({'Direcional': 'Directional'}, inplace=True)

# Translate status ("situacao")
situacao_dict = {
    'ABANDONADO AGUARDANDO ABANDONO DEFINITIVO/ARRASAMENTO': 'ABANDONED WAITING FOR DEFINITIVE ABANDONMENT/DESTROYAL',
    'ABANDONADO PERMANENTEMENTE': 'PERMANENTLY ABANDONED',
    'ABANDONADO POR LOGÍSTICA EXPLORATÓRIA': 'ABANDONED FOR EXPLORATORY LOGISTICS',  # ??? what does this mean
    'ABANDONADO TEMPORARIAMENTE COM MONITORAMENTO': 'ABANDONED TEMPORARILY WITH MONITORING',
    'ABANDONADO TEMPORARIAMENTE SEM MONITORAMENTO': 'ABANDONED TEMPORARILY WITHOUT MONITORING',
    'ABANDONADO/PARADO AGUARDANDO INTERVENÇÃO PARA AVALIAÇÃO, COMPLETAÇÃO OU RESTAURAÇÃO': 'ABANDONED/STOPPED WAITING FOR INTERVENTION FOR EVALUATION, COMPLETION OR RESTORATION',
    'ARRASADO': 'DESTROYED',  # ??? what does this mean
    'CEDIDO PARA A CAPTAÇÃO DE ÁGUA': 'CONVERTED FOR WATER CAPTURE',
    'DEVOLVIDO': 'RETURNED',  # ??? what does this mean
    'EM AVALIAÇÃO': 'UNDER EVALUATION',
    'EM COMPLETAÇÃO': 'COMPLETION IN PROGRESS',
    'EM INTERVENÇÃO': 'UNDER INTERVENTION',
    'EM OBSERVAÇÃO': 'UNDER OBSERVATION',
    'EM PERFURAÇÃO': 'DRILLING',
    'EQUIPADO AGUARDANDO INÍCIO DE OPERAÇÃO': 'EQUIPPED WAITING FOR START OF OPERATION',
    'EQUIPADO AGUARDANDO INÍCIO DE PRODUÇÃO': 'EQUIPPED WAITING FOR PRODUCTION TO START',
    'FECHADO': 'CLOSED',
    'INJETANDO': 'INJECTING',
    # 'N/A': 'N/A',
    'OPERANDO PARA CAPTAÇÃO DE ÁGUA': 'OPERATING - WATER CAPTURE',
    'OPERANDO PARA DESCARTE': 'OPERATING - DISPOSAL',
    'OUTRO': 'OTHER',
    'PRODUZINDO': 'PRODUCING',
    'PRODUZINDO E INJETANDO': 'PRODUCING AND INJECTING'
}
brz_wells.situacao.replace(situacao_dict, inplace=True)

# Translate "reclassifi" values (use as type information)
reclassifi_dict = {
    'ABANDONADO POR ACIDENTE MECÂNICO': 'ABANDONED',  # DUE TO MECHANICAL ACCIDENT
    'ABANDONADO POR ERUPÇÃO': 'ABANDONED',  # BY ERUPTION
    'ABANDONADO POR IMPOSSIBILIDADE DE AVALIAÇÃO': 'ABANDONED',  # DUE TO IMPOSSIBILITY OF EVALUATION
    'ABANDONADO POR OBJETIVO FORA DE PREVISÃO': 'ABANDONED',  # DUE TO OUT OF FORECAST TARGET
    'ABANDONADO POR OBJETIVO/ALVO NÃO ATINGIDO': 'ABANDONED',  # DUE TO UNATTAINED OBJECTIVE/TARGET
    'ABANDONADO POR OUTRAS RAZÕES': 'ABANDONED',  # FOR OTHER REASONS
    'ABANDONADO POR PERDA CIRCULAÇÃO': 'ABANDONED',  # DUE TO LOSS CIRCULATION
    'CONTROLE DE ERUPÇÃO': 'EXPLOSION CONTROL',
    'DESCARTE DE ÁGUA': 'WATER DISPOSAL',
    'DESCOBRIDOR DE CAMPO COM GÁS NATURAL': 'DISCOVERER OF NATURAL GAS FIELD',
    'DESCOBRIDOR DE CAMPO COM GÁS NATURAL E CONDENSADO': 'FIELD DISCOVERER WITH NATURAL GAS AND CONDENSATE',
    'DESCOBRIDOR DE CAMPO COM PETRÓLEO': 'FIELD DISCOVERER WITH OIL',
    'DESCOBRIDOR DE CAMPO COM PETRÓLEO E GÁS NATURAL': 'DISCOVERER OF OIL AND NATURAL GAS FIELD',
    'DESCOBRIDOR DE CAMPO COM PETRÓLEO, GÁS NATURAL E CONDENSADO': 'FIELD DISCOVERER WITH OIL, NATURAL GAS AND CONDENSATE',
    'DESCOBRIDOR DE NOVA JAZIDA GÁS NATURAL': 'DISCOVERER OF NEW NATURAL GAS RESERVE',
    'DESCOBRIDOR DE NOVA JAZIDA GÁS NATURAL E CONDENSADO': 'DISCOVERER OF NEW NATURAL GAS AND CONDENSATE DEPOSIT',
    'DESCOBRIDOR DE NOVA JAZIDA PETRÓLEO': 'DISCOVERER OF NEW OIL RESERVE',
    'DESCOBRIDOR DE NOVA JAZIDA PETRÓLEO E GÁS NATURAL': 'DISCOVERER OF NEW OIL AND NATURAL GAS RESERVE',
    'DESCOBRIDOR DE NOVA JAZIDA PETRÓLEO, GÁS NATURAL E CONDENSADO': 'DISCOVERER OF NEW OIL, NATURAL GAS AND CONDENSATE',
    'EXPERIMENTAL': 'EXPERIMENTAL',
    'EXTENSÃO PARA GÁS NATURAL': 'EXTENSION TO NATURAL GAS',
    'EXTENSÃO PARA GÁS NATURAL E CONDENSADO': 'EXTENSION FOR NATURAL GAS AND CONDENSATE',
    'EXTENSÃO PARA PETRÓLEO': 'EXTENSION TO OIL',
    'EXTENSÃO PARA PETRÓLEO E GÁS NATURAL': 'EXTENSION TO OIL AND NATURAL GAS',
    'EXTENSÃO PARA PETRÓLEO, GÁS NATURAL E CONDENSADO': 'EXTENSION TO OIL, NATURAL GAS AND CONDENSATE',
    'INDEFINIDO': 'N/A',
    'INJEÇÃO DE CO2': 'CO2 INJECTION',
    'INJEÇÃO DE GÁS NATURAL': 'NATURAL GAS INJECTION',
    'INJEÇÃO DE QUALQUER OUTRO FLUIDO': 'INJECTION OF OTHER FLUID',
    'INJEÇÃO DE VAPOR': 'STEAM INJECTION',
    'INJEÇÃO DE ÁGUA': 'WATER INJECTION',
    'INJEÇÃO DE ÁGUA ADITIVADA': 'ADDITIVE WATER INJECTION',
    'OBSERVAÇÃO': 'OBSERVATION',
    'OUTRAS FINALIDADES': 'OTHER PURPOSE',
    'PESQUISA MINERAL': 'MINERAL RESEARCH',
    'PORTADOR DE GÁS NATURAL': 'CARRIER OF NATURAL GAS',
    'PORTADOR DE GÁS NATURAL E CONDENSADO': 'CARRIER OF NATURAL GAS AND CONDENSATE',
    'PORTADOR DE PETRÓLEO': 'OIL CARRIER',
    'PORTADOR DE PETRÓLEO E GÁS NATURAL': 'CARRIER OF OIL AND NATURAL GAS',
    'PORTADOR DE PETRÓLEO, GÁS NATURAL E CONDENSADO': 'CARRIER OF OIL, NATURAL GAS AND CONDENSATE',
    'PRODUTOR COMERCIAL DE GÁS NATURAL': 'COMMERCIAL NATURAL GAS PRODUCER',
    'PRODUTOR COMERCIAL DE GÁS NATURAL E CONDENSADO': 'COMMERCIAL PRODUCER OF NATURAL GAS AND CONDENSATE',
    'PRODUTOR COMERCIAL DE PETRÓLEO': 'COMMERCIAL OIL PRODUCER',
    'PRODUTOR COMERCIAL DE PETRÓLEO E GÁS NATURAL': 'COMMERCIAL OIL AND NATURAL GAS PRODUCER',
    'PRODUTOR COMERCIAL DE PETRÓLEO, GÁS NATURAL E CONDENSADO': 'COMMERCIAL PRODUCER OF OIL, NATURAL GAS AND CONDENSATE',
    'PRODUTOR SUBCOMERCIAL DE GÁS NATURAL': 'SUB-COMMERCIAL PRODUCER OF NATURAL GAS',
    'PRODUTOR SUBCOMERCIAL DE GÁS NATURAL E CONDENSADO': 'SUB-COMMERCIAL PRODUCER OF NATURAL GAS AND CONDENSATE',
    'PRODUTOR SUBCOMERCIAL DE PETRÓLEO': 'SUB-COMMERCIAL OIL PRODUCER ',
    'PRODUTOR SUBCOMERCIAL DE PETRÓLEO E GÁS NATURAL': 'SUB-COMMERCIAL PRODUCER OF OIL AND NATURAL GAS',
    'PRODUTOR SUBCOMERCIAL DE PETRÓLEO, GÁS NATURAL E CONDENSADO': 'SUB-COMMERCIAL PRODUCER OF OIL, NATURAL GAS AND CONDENSATE',
    'PRODUÇÃO DE ÁGUA': 'WATER PRODUCTION',
    'SECO COM INDÍCIOS DE GÁS NATURAL E CONDENSADO': 'DRY WITH SIGNS OF NATURAL GAS AND CONDENSATE',
    'SECO COM INDÍCIOS DE PETRÓLEO': 'DRY WITH SIGNS OF OIL',
    'SECO COM INDÍCIOS DE PETRÓLEO E GÁS NATURAL': 'DRY WITH SIGNS OF OIL AND NATURAL GAS',
    'SECO COM INDÍCIOS GÁS NATURAL': 'DRY WITH SIGNS OF NATURAL GAS',
    'SECO SEM INDÍCIOS': 'DRY WITHOUT SIGNS',
    'TREINAMENTO': 'TRAINING'
}

brz_wells.reclassifi.replace(reclassifi_dict, inplace=True)

# =============================================================================
# %% BRAZIL - Integration + Export
# =============================================================================
brz_wells_integrated, brz_err = integrate_facs(
    brz_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Brazil",
    # state_prov=None,
    src_ref_id="126",
    src_date="2024-11-04",
    # on_offshore=None,
    fac_name="name",
    fac_id="ogr_fid",
    fac_type="reclassifi",
    spud_date='spud',
    comp_date='comp',
    drill_type='direcao',
    fac_status='situacao',
    op_name="operador",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    brz_wells_integrated,
    file_name="brazil_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% COLOMBIA
# =============================================================================
os.chdir(v24data)
fp_co = r'colombia\Banco_de_Informaci%C3%B3n_Petrolera%3A_Pozos.geojson'
col_wells = gpd.read_file(fp_co)
col_wells = transform_CRS(col_wells, appendLatLon=True)
# Transform Point Z geometries to Point geometries
col_wells2 = transform_geom_3d_2d(col_wells)
print("======================")
print("Total # of features in original dataset = ", col_wells.shape[0])
print("Total # of features in transformed dataset = ", col_wells2.shape[0])

# Translate drilling direction
col_wells2['drilltypenew'] = 'N/A'
col_wells2.loc[col_wells2.WELLTYPE.isin(['VERTICAL', 'VERTI CAL']), 'drilltypenew'] = 'VERTICAL'
col_wells2.loc[col_wells2.WELLTYPE.isin(['HORIZONTAL']), 'drilltypenew'] = 'HORIZONTAL'
col_wells2.loc[col_wells2.WELLTYPE.isin(['DIRECCIONAL', 'DIRECCIONAL-']), 'drilltypenew'] = 'DIRECTIONAL'

# # Format dates properly - AS OF FEB 2025 THESE DATE FIELDS AREN'T IN THE DATASET
# col_wells2['spudnew'] = pd.to_datetime(col_wells2['WELL_SPUD_'],
#                                        format='%Y-%m-%d',
#                                        errors='coerce').dt.strftime("%Y-%m-%d")

# col_wells2['compnew'] = pd.to_datetime(col_wells2['WELL_COMPL'],
#                                        format='%Y-%m-%d',
#                                        errors='coerce').dt.strftime("%Y-%m-%d")

# Translate well status
statusdict = {
    'SECO': 'DRY',
    'TAPONADO Y ABANDONADO': 'PLUGGED AND ABANDONED',
    'PRODUCTOR': 'PRODUCER',
    'INYECTOR': 'INJECTOR',
    'ABANDONADO': 'ABANDONED',
    'DISPOSAL': 'DISPOSAL',
    'SUSPENDIDO': 'SUSPENDED',
    'HUECO PERDIDO': 'LOST HOLE',
    'SIN ESTADO': 'N/A',
    None: 'N/A',
    '': 'N/A',
    ' ': 'N/A',
    '0': 'N/A',
    'SUSPENDIDO TEMPORALMENTE': 'TEMPORARILY SUSPENDED',
    'MONITOR': 'MONITORING',
    'DESARROLLO': 'DEVELOPMENT',
    'ESTRATIGRAFICO': 'STRATIGRAPHIC',
    'INYECTOR DE AGUA': 'WATER INJECTOR',
    'PENDIENTE': 'PENDING'
}
col_wells2['statusnew'] = col_wells2.WELL_STA_1.replace(statusdict)

# =============================================================================
# %% COLOMBIA - Integration + Export
# =============================================================================
col_wells_integrated, col_wells_err = integrate_facs(
    col_wells2,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Colombia",
    # state_prov=None,
    src_ref_id="270",
    src_date="2022-10-20",
    # on_offshore=None,
    fac_name="WELL_NAME",
    fac_id="UWI",
    # fac_type=None,  # TODO - fill in well types when I learn what A0, A3, A2a etc. well types mean
    # spud_date='spudnew',
    # comp_date='compnew',
    drill_type='drilltypenew',
    # install_date=None,
    fac_status='statusnew',
    op_name="OPERATOR_W",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    col_wells_integrated,
    file_name="colombia_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% DENMARK
# =============================================================================
os.chdir(v24data)
fp = r"denmark\wells\deep_wells_oil_gas.xlsx"  # FIXME, schema might be different
deepwells = pd.read_excel(fp, sheet_name='WellsDK')
# Capitalize column names so they match the other XLSX
deepwells.columns = map(str.upper, deepwells.columns)
deepwells['SOURCE_ID'] = '20'
deepwells['SRCDATE'] = '2016-05-01'  # This is a GUESS bsed on the latest comp date in dataset


# If the deepwells DF doesn't have a E_Degrees or N_Degrees column, create one
# Convert the degree-minutes-seconds formatted coordinates into decimal degrees


def dms_to_decimal(dms_str):
    # Replace comma with dot for decimal conversion
    dms_str = dms_str.replace(',', '.').strip()

    # Extract degrees, minutes, and seconds using regex (allowing spaces)
    match = re.match(r"(\d+)\D+(\d+)\D+(\d+(?:\.\d+)?)\s*\D*", dms_str)

    if not match:
        raise ValueError(f"Invalid DMS format: {dms_str}")

    degrees, minutes, seconds = map(float, match.groups())
    decimal_degrees = degrees + minutes / 60 + seconds / 3600

    return decimal_degrees


if 'N_DEGREES' not in deepwells.columns:
    deepwells['N_DEGREES'] = deepwells.N_LATITUDE.apply(lambda x: dms_to_decimal(x))
    deepwells['E_DEGREES'] = deepwells.E_LONGITUDE.apply(lambda x: dms_to_decimal(x))


# Read exploration and appraisal wells
fp2 = r"denmark\wells\expappwells.xlsx"
expappwells = pd.read_excel(fp2, sheet_name='Ark1')
# Capitalize column names so they match the other XLSX
expappwells.columns = map(str.upper, expappwells.columns)
expappwells['SOURCE_ID'] = '21'
expappwells['SRCDATE'] = '2019-11-01'  # This is a GUESS bsed on the latest comp date in dataset

# Merge the two well datasets
dane_wells = pd.concat([deepwells, expappwells]).reset_index(drop=True)

# Convert the dataframe into a geodataframe
# NOTE: the provided degree coordinates are in EPSG:4230, according to the src website
# https://ens.dk/en/our-services/oil-and-gas-related-data/shape-files-maps
dane_wells = gpd.GeoDataFrame(dane_wells,
                              geometry=gpd.points_from_xy(dane_wells.E_DEGREES,
                                                          dane_wells.N_DEGREES),
                              crs=4230)
dane_wells = transform_CRS(dane_wells, target_epsg_code="epsg:4326", appendLatLon=True)

# Convert datetimes to strings
dane_wells['SPUD_DATE'] = pd.to_datetime(dane_wells['SPUD_DATE']).dt.strftime("%Y-%m-%d")
dane_wells['COMP_DATE'] = pd.to_datetime(dane_wells['COMP_DATE']).dt.strftime("%Y-%m-%d")

# Translate some "CLASSIFICATION" values that are still in Italian
dane_wells.CLASSIFICATION.replace({'GEOTERMI': 'GEOTHERMAL',
                                   'EXPLORATION AND PRODUCTIO': 'EXPLORATION AND PRODUCTION'},
                                  inplace=True)

# =============================================================================
# %% DENMARK: Integration + Export
# =============================================================================
dane_wells_integrated, errors = integrate_facs(
    dane_wells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="DENMARK",
    src_ref_id="SOURCE_ID",
    src_date="SRCDATE",
    on_offshore="LOCATION",
    fac_name="WELL_NAME",
    fac_id="WELL_NUMB",
    fac_type='CLASSIFICATION',
    spud_date="SPUD_DATE",
    comp_date="COMP_DATE",
    op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    dane_wells_integrated,
    file_name="denmark_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% GERMANY - OLD
# =============================================================================
# os.chdir(pubdata)
# fp = "Europe\\Germany\\Wells\\Bohrungen_B_ART_B_KLASSE2_Translation.shp"
# wells = read_spatial_data(fp, table_gradient=True)

# # Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
# wells2 = transform_CRS(wells, target_epsg_code="epsg:4326", appendLatLon=True)

# # % Data pre-processing before data integration

# # convert fields to datetime for easier manipulation to the desired date format
# df1 = pd.DataFrame(wells2.drop(columns='geometry'))
# df1['BOHRBEGINN'] = pd.to_datetime(df1['BOHRBEGINN'])
# df1['BOHRENDE'] = pd.to_datetime(df1['BOHRENDE'])

# spud_dates = []
# end_dates = []
# well_name = []

# for idx1_, row1_ in tqdm(df1.iterrows(), total=df1.shape[0]):
#     # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
#     spud = row1_.BOHRBEGINN
#     if pd.isna(spud):
#         dates_null = "1900-01-01"
#         spud_dates.append(dates_null)

#     else:
#         formatted_spud = spud.strftime('%Y-%m-%d')
#         spud_dates.append(formatted_spud)

#     end_date = row1_.BOHRENDE
#     if pd.isna(end_date):
#         dates_null = "1900-01-01"
#         end_dates.append(dates_null)

#     else:
#         formatted_rig = end_date.strftime('%Y-%m-%d')
#         end_dates.append(formatted_rig)

#     # Append to well_name list a new name that combines the lease name with the well number for each row
#     lochname = row1_.LOCHNAME
#     ddr_name = row1_.DDR_NAME
#     # Don't add ddr_name if it is "None"
#     if pd.isna(ddr_name):
#         well_name1 = str(lochname)
#         well_name.append(well_name1)
#     else:
#         well_name2 = str(lochname) + " " + str(ddr_name)
#         well_name.append(well_name2)

# df1['FORMATTED_SPUD'] = spud_dates
# df1['FORMATTED_END'] = end_dates
# df1['WELL_NAME'] = well_name

# wells3 = gpd.GeoDataFrame(df1, geometry=gpd.points_from_xy(df1.longitude_calc, df1.latitude_calc), crs=4267)

# # translate status
# dict_names = {}
# colName = "L_STATUS"
# dict_names = {
#     'Verfüllt': 'PLUGGED',
#     'Teilverfüll': 'PARTIAL BACKFILL',
#     'Offen': 'PRODUCING',
#     None: "N/A"
# }

# wells4 = replace_row_names(wells3, colName, dict_names)

# # =============================================================================
# # GERMANY: DATA INTEGRATION
# # =============================================================================

# cleaned_wells_final_GERMANY, errors = integrate_facs(
#     wells4,
#     starting_ids=0,
#     category="Oil and natural gas wells",
#     fac_alias="WELLS",
#     country="Germany",
#     src_ref_id="19",
#     src_date="2017-06-01",
#     fac_name="WELL_NAME",
#     fac_id="IDENTIFIER",
#     fac_type="English",
#     spud_date="FORMATTED_SPUD",
#     comp_date="FORMATTED_END",
#     fac_status="L_STATUS",
#     op_name="OPERATOR",
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     cleaned_wells_final_GERMANY,
#     file_name="germany_oil_gas_wells",
#     schema_def=True,
#     schema=schema_WELLS,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% GERMANY
# =============================================================================
os.chdir(v24data)
fp = r'germany\Boreholes\Tiefbohrungen (KW).gpkg'
gwells = gpd.read_file(fp, layer='tiefbohrungen__kw_')
gwells = transform_CRS(gwells, target_epsg_code="epsg:4326", appendLatLon=True)
# Remove columns that I don't need
cols2keep = ['identifier',
             'lochname',
             'operator',  # 'Auftraggeber' aka client or operator
             'firma',  # 'Eigentümer' aka owner
             'bohrfirma',  # drilling company
             # 'b_klasse',  # class code (numerical/letter)
             'b_klasse2',  # class decoded
             'b_klasse_a',  # another well type field?
             'b_art',  # Bohrungsart aka drilling type
             'bohrbeginn',  # drill start date
             'bohrende',  # drill end date
             'g_ergebnis',  # result
             't_ergebnis',  # result
             'l_status',
             # 'kurzname',
             # 'lbeg_nr',
             # 'zugang',  # means "access", either "blocked" or "Free"
             # 'rechtswert',
             # 'hochwert',
             # 'gk_ellipse',
             # 'laenge',
             # 'breite',
             # 'geo_ellips',
             # 'a_hoehe',
             # 'endtiefe',  # drilling depth?
             # 'e_horizont',  # horizons aka rock formation
             # 'e_horizo_l',
             # 'ats_e_hori',
             # 'ats_e_ho_l',
             # 'abw_tiefe',
             # 'abw_streck',
             # 'abw_azimut',
             # 'tiefenverl',
             # 'wassertief',
             # 'vorh_art',
             # 'logdaten',  # is there log data y/n
             # 'sv_datei',  # "Layers Directory" whatever that means
             # 'gvm_nr',
             # 'temp_anz',  # number of temperature values
             # 'kern_anz',  # number of cores
             # 'kunter_anz',  # number of core examinations
             # 'abwdaten',  # ABW Daten y/n
             # 'profildate',
             # 'gvm',
             # 'einsicht',  # Notes field
             'up_date',
             'longitude_calc',
             'latitude_calc',
             'geometry']

gwells = gwells[cols2keep]

# -----------------------------------------------------------------------------
# Translate the drill type column (also specifies well type / commodity)
german_drill_type_translations = {
    'Aquiferspeicherbohrung': 'Aquifer storage',
    'Asphalt-/Ölschieferbohrung': 'Asphalt/oil shale',
    'Beobachtungsbohrung': 'Observation',
    'Braunkohlebohrung': 'Lignite',
    'Einpressbohrung': 'Injection',
    'Erdölbohrung': 'Oil',
    'Erzbohrung': 'Ore',
    'Forschungsbohrung': 'Research',
    'Gas/Ölbohrung': 'Gas/oil',
    'Gasbohrung': 'Gas',
    'Geothermikbohrung': 'Geothermal',
    'Kalibohrung': 'Potassium',
    'Kavernenbohrung': 'Cavern',
    'Kavernenspeicherbohrung': 'Cavern storage',
    'Salzbohrung (Steinsalz, Kalisalz)': 'Salt (rock salt, potash salt)',
    'Schacht (keine Bohrung!)': 'Shaft, no drilling',
    'Schachtbohrung': 'Shaft drilling',
    'Schwefelbohrung': 'Sulfur',
    'Solebohrung': 'Brine',
    'Speicher-Hilfsbohrung': 'Storage auxiliary',
    'Speicher-Untersuchungsbohrung': 'Storage investigation',
    'Speicherbohrung (ehem. Lagerstätte)': 'Storage (former deposit)',
    'Steinkohlebohrung': 'Hard coal',
    'Thermal- & Solebohrung': 'Thermal and brine',
    'Versenkbohrung': 'Disposal',
    'Wasserbohrung': 'Water',
    'Öl/Gasbohrung': 'Oil/gas',
    'None': 'N/A',
    None: 'N/A'
}

# Translate the result column (t_ergebnis)
german_result_translations = {
    'Fehl/technisch': 'Missing/technical',
    'Gasanzeichen/Test': 'Gas signs/Test',
    'Gasfündig': 'Gas found',
    'Leangasfündig': 'Lean gas found',
    'Nicht fündig': 'Not found',
    'Nicht fündig/KW-Anzeichen': 'Not found/KW signs',
    'Nicht fündig/verwässert': 'Not found/diluted',
    'Noch nicht bekannt': 'Not yet known',
    'Sauer-/süßgasfündig': 'Sour/sweet gas found',
    'Sauergasfündig': 'Sour gas found',
    'Süß-/leangasfündig': 'Sweet/lean gas found',
    'Süß-/sauergasfündig': 'Sweet/sour gas found',
    'Süßgas-/ölfündig': 'Sweet gas/oil found',
    'Süßgasfündig': 'Sweet gas found',
    'Thermal-/mineralwasserfündig': 'Thermal/mineral water found',
    'Ziel erreicht': 'Target reached',
    'Ziel nicht erreicht': 'Target not reached',
    'Öl-/süßgasfündig': 'Oil/sweet gas found',
    'Öl/Gasfündig': 'Oil/gas found',
    'Ölanzeichen/Spülproben': 'Oil signs/flushing samples',
    'Ölanzeichen/Test': 'Oil signs/Test',
    'Ölfündig': 'Oil found'
}

german_status_translations = {
    'Verfüllt': 'PLUGGED',
    'Teilverfüllt': 'PARTIALLY FILLED',
    'Offen': 'PRODUCING',
    None: 'N/A'
}

german_type_translations = {
    'Aufschlussbohrung': 'Exploration well',
    'Basisbohrung': 'Base well',
    'Erweiterungsbohrung': 'Expansion well',
    'Explorationsbohrung': 'Exploration well',  # what's the diff?
    'Hilfsbohrung': 'Auxiliary well',
    'Produktionsbohrung': 'Production well',
    'Sonstige Bohrung': 'Other well',
    'Speicherbohrung': 'Storage well',
    'Teilfeldsuchbohrung': 'Partial field exploration well',
    'Untersuchungsbohrung': 'Exploration well',  # What's the diff?
    'Wiedererschließungsbohrung': 'Redevelopment well',
    'None': 'N/A',
    None: 'N/A'
}

gwells.b_art = gwells.b_art.replace(german_drill_type_translations)
gwells.t_ergebnis = gwells.t_ergebnis.replace(german_result_translations)
gwells.l_status = gwells.l_status.replace(german_status_translations)
gwells.b_klasse2 = gwells.b_klasse2.replace(german_type_translations)

# Create properly formatted date fields, removing the timestamp
gwells['spud_date'] = gwells.bohrbeginn.fillna('1900-01-01').apply(lambda x: x.split('T')[0])
gwells['comp_date'] = gwells.bohrende.fillna('1900-01-01').apply(lambda x: x.split('T')[0])

# IMPORTANT: Only keep well bores releated to oil and gas!
welltypes2keep = ['Oil',
                  'Gas',
                  'Asphalt/oil shale',
                  'Oil/gas',
                  'Gas/oil',
                  'Injection',
                  'Disposal',
                  'Water',
                  'Brine']
gwells_og = gwells[gwells.b_art.isin(welltypes2keep)].reset_index()

# Create a "complete" well type field that combines the b_klasse2 field and
# b_art field.
create_concatenated_well_name(gwells_og, 'b_klasse2', 'b_art', 'factypenew')

# =============================================================================
# %% GERMANY - Integration + Export
# =============================================================================
gwells_integrated, errors = integrate_facs(
    gwells_og,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="GERMANY",
    src_ref_id="268",
    src_date="2024-12-30",  # "up_date" field
    fac_name="lochname",
    fac_id="identifier",
    fac_type="factypenew",
    spud_date="spud_date",
    comp_date="comp_date",
    # drill_type=
    fac_status="l_status",
    op_name="operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    gwells_integrated,
    file_name="germany_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% IRELAND - OLD
# =============================================================================
# os.chdir(pubdata)
# fp = "Europe\\Ireland\\Wells\\_ireland_offshore_wells_.shp"
# ireland_offshore_wells = read_spatial_data(fp, table_gradient=False)
# ireland_offshore_wells["SHORE"] = "OFFSHORE"
# fp2 = "Europe\\Ireland\\Wells\\_ireland_onshore_wells_.shp"
# wells = read_spatial_data(fp2, table_gradient=False)
# wells["SHORE"] = "ONSHORE"

# # -----------------------------------------------------------------------------

# # Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
# ireland_offshore_wells2 = transform_CRS(ireland_offshore_wells, target_epsg_code="epsg:4326", appendLatLon=True)
# wells2 = transform_CRS(wells, target_epsg_code="epsg:4326", appendLatLon=True)

# wells_concat = gpd.GeoDataFrame(pd.concat([ireland_offshore_wells2, wells2]))

# # % Data pre-processing before data integration
# # cleaning out well_spud & rigrelease columns that have their dates slightly messed up/misstyped
# bad_dates = []
# bad_dates2 = []

# for idx1_, row1_ in tqdm(wells_concat.iterrows(), total=wells_concat.shape[0]):
#     # Append to dates list by removing some typos/errors in the dates
#     spud = row1_.WELL_SPUD
#     if "P&A" in spud:
#         dates_split = spud.split(" P&A")
#         bad_dates.append(dates_split[0])

#     else:
#         bad_dates.append(spud)

#     rigdate = row1_.RIGRELEASE
#     if "11979" in rigdate:
#         date_written = "Monday, May 28, 1979"
#         bad_dates2.append(date_written)

#     else:
#         bad_dates2.append(rigdate)

# wells_concat['FORMATTED_SPUD'] = bad_dates
# wells_concat['FORMATTED_RIG'] = bad_dates2

# # reformat to a pd dataframe in order to convert dates to datetime format for easier manipulation
# df1 = pd.DataFrame(wells_concat.drop(columns='geometry'))
# df1['FORMATTED_SPUD'] = pd.to_datetime(df1['FORMATTED_SPUD'])
# df1['FORMATTED_RIG'] = pd.to_datetime(df1['FORMATTED_RIG'])

# wells_concat2 = gpd.GeoDataFrame(df1, geometry=gpd.points_from_xy(df1.longitude_calc, df1.latitude_calc), crs=4267)

# # -----------------------------------------------------------------------------

# # % Data pre-processing before data integration, part 2
# # Now that the dates have been cleaned, we can manipulate the concated, cleaned data set to the desired date format
# spud_dates = []
# rig_dates = []

# for idx1_, row1_ in tqdm(wells_concat2.iterrows(), total=wells_concat2.shape[0]):
#     spud = row1_.FORMATTED_SPUD
#     if spud is None:
#         dates_null = "1900-01-01"
#         spud_dates.append(dates_null)

#     else:
#         formatted_spud = spud.strftime('%Y-%m-%d')
#         spud_dates.append(formatted_spud)

#     rig = row1_.FORMATTED_RIG
#     if rig is None:
#         dates_null = "1900-01-01"
#         rig_dates.append(dates_null)

#     else:
#         formatted_rig = rig.strftime('%Y-%m-%d')
#         rig_dates.append(formatted_rig)

# wells_concat2['FORMATTED_SPUD_FINAL'] = spud_dates
# wells_concat2['FORMATTED_RIGRELEASE_FINAL'] = rig_dates

# # formatting status column
# dict_names = {}
# colName = "STATUS"
# dict_names = {
#     'P&A': 'PLUGGED AND ABANDONED',
#     'P & A': 'PLUGGED AND ABANDONED',
#     'P & TA': 'PLUGGED AND TEMPORARILY ABANDONED',
#     'P&TA': 'PLUGGED AND TEMPORARILY ABANDONED',
#     'NOT APPLICABLE': 'N/A'
# }

# wells_concat3 = replace_row_names(wells_concat2, colName, dict_names)

# # =============================================================================
# # IRELAND: DATA INTEGRATION
# # =============================================================================
# cleaned_wells_final_IRELAND, errors = integrate_facs(
#     wells_concat3,
#     starting_ids=0,
#     category="Oil and natural gas wells",
#     fac_alias="WELLS",
#     country="Ireland",
#     # state_prov="",
#     src_ref_id="157",
#     src_date="2010-01-01",
#     on_offshore="SHORE",
#     fac_name="RIG_NAME",
#     fac_id="WELL_NUM",
#     fac_type="RESULT",
#     # drill_type="",
#     spud_date="FORMATTED_SPUD_FINAL",
#     comp_date="FORMATTED_RIGRELEASE_FINAL",
#     fac_status="STATUS",
#     op_name="OPERATOR",
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     cleaned_wells_final_IRELAND,
#     file_name="ireland_oil_gas_wells",
#     schema_def=True,
#     schema=schema_WELLS,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% IRELAND
# =============================================================================
os.chdir(v24data)
fp_on_ire = r'ireland\Onshore Wells.shp'
ire_onshore = gpd.read_file(fp_on_ire)
ire_onshore = ire_onshore.set_crs(3857)  # for some reason the CRS isn't set, do it manually
ire_onshore = transform_CRS(ire_onshore, target_epsg_code="epsg:4326", appendLatLon=True)
ire_onshore['onoff'] = 'Onshore'

fp_off_ire = r'ireland\Offshore Wells.shp'
ire_offshore = gpd.read_file(fp_off_ire)
ire_offshore = ire_offshore.set_crs(3857)  # for some reason the CRS isn't set, do it manually
ire_offshore = transform_CRS(ire_offshore, target_epsg_code="epsg:4326", appendLatLon=True)
ire_offshore['onoff'] = 'Offshore'

# Revise spud date formats
ire_offshore['spudnew'] = pd.to_datetime(ire_offshore.WELL_SPUD,
                                         infer_datetime_format=True).dt.strftime("%Y-%m-%d")
ire_onshore.WELL_SPUD = ire_onshore.WELL_SPUD.str.replace('\n', '')
ire_onshore['spudnew'] = pd.to_datetime(ire_onshore.WELL_SPUD,
                                        format='%d/%m/%Y',
                                        errors='coerce').dt.strftime("%Y-%m-%d")
ire_wells = pd.concat([ire_offshore, ire_onshore]).reset_index(drop=True)


# Un-abbreviate status values
ire_status_dict = {
    'P&A': 'PLUGGED AND ABANDONED',
    'P & A': 'PLUGGED AND ABANDONED',
    'P & TA': 'PLUGGED AND TEMPORARILY ABANDONED',
    'P&TA': 'PLUGGED AND TEMPORARILY ABANDONED',
    'P & A with/without shows or untested pay': 'PLUGGED AND ABANDONED WITH/WITHOUT SHOWS',
    'P & A Gas Well': 'PLUGGED AND ABANDONED'
}
ire_wells.STATUS = ire_wells.STATUS.replace(ire_status_dict)

# Combine WELL_CLASS and RESULT for a factype
create_concatenated_well_name(ire_wells,
                              'WELL_CLASS',
                              'RESULT',
                              'welltypenew')

# =============================================================================
# %% IRELAND - Integration + Export
# =============================================================================
ire_wells_integrated, errors = integrate_facs(
    ire_wells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Ireland",
    # state_prov="",
    src_ref_id="157",
    src_date="2024-07-22",
    on_offshore="onoff",
    fac_name="WELL_NUM",
    fac_type="welltypenew",
    spud_date="spudnew",
    fac_status="STATUS",
    op_name="OPERATOR",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    ire_wells_integrated,
    file_name="ireland_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% ITALY
# =============================================================================
os.chdir(v24data)
# Read storage wells, converting any "," numeric separator to a decimal
fp1 = r'italy\pozzi-stoccaggio.csv'
storage = pd.read_csv(fp1,
                      sep=';',
                      encoding='windows-1252',
                      decimal=',',
                      dtype={'Latitudine': float})
storage.columns = storage.columns.str.strip()  # strip spaces from column names
storage['SOURCE_ID'] = '16'

# Read production wells, converting any "," numeric separator to a decimal
# `windows-1252` encoding is needed because of a "curly apostrophe" in some
# of the well names
fp2 = r'italy\pozzi-idrocarburi.csv'
prod = pd.read_csv(fp2,
                   sep=';',
                   encoding='windows-1252',
                   decimal=',')
prod.columns = prod.columns.str.strip()  # strip spaces from column names
# There are some trailing "supplementary information" rows at the end of this
# file; drop these rows by dropping rows with no latlong
prod = prod[prod.Longitudine.notna()]

# prod.loc[prod.Latitudine == '4.159.020,00', 'Latitudine'] = ''
# prod.Latitudine = prod.Latitudine.str.replace(',', '.')
prod.Latitudine = prod.Latitudine.astype(float)
prod['SOURCE_ID'] = '17'

# Append production and storage wells together
modern_wells = pd.concat([storage, prod]).reset_index(drop=True)
# Drop any records with no lat-long coordinates
modern_wells = modern_wells.dropna(subset=['Longitudine', 'Latitudine'])

# Turn df of modern wells into geodataframe
# TODO - confirm whether coords are in WGS84
modern_wells = gpd.GeoDataFrame(modern_wells,
                                geometry=gpd.points_from_xy(modern_wells.Longitudine,
                                                            modern_wells.Latitudine),
                                crs=4326)
modern_wells = transform_CRS(modern_wells,
                             target_epsg_code="epsg:4326",
                             appendLatLon=True)

# Un-abbreviate and translate status names
# Meanings of status abbreviations (in Italian) can be found on last page of PDF data:
# https://unmig.mase.gov.it/wp-content/uploads/dati/pozzi/pozzi-idrocarburi.pdf
modern_wells.Stato.replace({'PR': 'Production, Providing',  # "Produttivo erogante"
                            'PP': 'Production, Not Providing',  # "Produttivo non erogante"
                            'pp': 'Production, Not Providing',
                            'MO': 'Monitoring',  # "Monitoraggio"
                            'RE': 'Reinjection',
                            'AL': 'Not Productive',  # "Non produttivo"
                            'PS': 'Potentially Usable for Storage',  # "Potenzialmente utilizzabile per lo stoccaggio"
                            'ST': 'Storage'},  # Stoccaggio
                           inplace=True)

# TODO - cite why and how I know what these columns mean
# Un-abbreviate on/offshore column
modern_wells.Ub.replace({'T': 'ONSHORE',
                         'M': 'OFFSHORE'}, inplace=True)

# Un-abbreviate mineral column
modern_wells.Min.replace({'G': 'GAS',
                          'g': 'GAS',
                          'O': 'OIL'}, inplace=True)

# Un-abbreviate Province column
modern_wells.Pr.replace({
    'AN': 'Alessandria',
    'AP': 'Ascoli Piceno',
    'BO': 'Bologna',
    'BS': 'Brescia',
    'CB': 'Campobasso',
    'CH': 'Chieti',
    'CL': 'Caltanissetta',
    'CR': 'Cremona',
    'CT': 'Catania',
    'EN': 'Enna',
    'FE': 'Ferrara',
    'FG': 'Foggia',
    'FI': 'Florence',
    'FR': 'Frosinone',
    'KR': 'Crotone',
    'LO': 'Lodi',
    'MC': 'Macerata',
    'MI': 'Milan',
    'MO': 'Modena',
    'MT': 'Matera',
    'NO': 'Novara',
    'PC': 'Piacenza',
    'PI': 'Pisa',
    'PR': 'Parma',
    'PV': 'Pavia',
    'PZ': 'Potenza',
    'RA': 'Ravenna',
    'RG': 'Ragusa',
    'RN': 'Rimini',
    'TE': 'Teramo',
    'TP': 'Trapani',
    'TV': 'Treviso',
    'ZA': 'Zara',
    'ZB': 'N/A',
    'ZC': 'N/A',
    'ZD': 'N/A',
    'ZF': 'N/A',
    'ZG': 'N/A'}, inplace=True)


# -----------------------------------------------------------------------------
# Read historical wells
fp3 = r'italy\pozzi-storici.csv'
historic = pd.read_csv(fp3, sep=';', encoding='windows-1252')  # encoding='unicode_escape'
historic.columns = historic.columns.str.strip()  # strip spaces from column names
historic['SOURCE_ID'] = '18'

# TODO - convert historic wells DD-MM-SS coordinates into decimal degrees WGS84
# -----------------------------------------------------------------------------
# Section below can be used to format the spud date for the VIDEPI data in the cell above, if decided to be used
# dates = []
# for idx1_, row1_ in tqdm(wells_historical.iterrows(), total=wells_historical.shape[0]):
#     #Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
#     spud = row1_.BeginTime
#     if spud is None:
#         dates_null = "1900-01-01"
#         dates.append(dates_null)
#     else:
#         formatted_date = spud + '-01-01'
#         dates.append(formatted_date)

# wells_historical['FORMATTED_DATE'] = dates
# wells_historical['SOURCE_ID'] = '18'


# all_italy_wells = pd.concat([modern_wells, historic])

# =============================================================================
# %% ITALY: DATA INTEGRATION
# =============================================================================
italy_integrated, errors = integrate_facs(
    modern_wells,  # FIXME, right now we're not using historic ones
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Italy",
    state_prov="Pr",
    src_ref_id="SOURCE_ID",
    src_date="2023-05-31",
    on_offshore="Ub",
    fac_name="Nome pozzo",
    fac_type="Min",
    # drill_type="",
    # spud_date = "",
    # comp_date="",
    fac_status="Stato",
    op_name="Operatore",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    italy_integrated,
    file_name="italy_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% NETHERLANDS
# =============================================================================
os.chdir(v24data)
fp = r"netherlands\wells\boreholes.xlsx"
nether = pd.read_excel(fp, sheet_name='Sheet0')

# Drop any records that have missing lat-long coordinates
nether = nether.dropna(subset=['Longitude WGS84', 'Latitude WGS84'])

# Create geodataframe with the original datum
nether = gpd.GeoDataFrame(nether,
                          geometry=gpd.points_from_xy(nether['Longitude WGS84'],
                                                      nether['Latitude WGS84'],
                                                      crs=4326))

# Convert dates from dd-mm-YYYY to YYYY-mm-dd
nether.Startdatum = pd.to_datetime(nether.Startdatum,
                                   format='%d-%m-%Y',
                                   errors='coerce').dt.strftime("%Y-%m-%d")

nether.Einddatum = pd.to_datetime(nether.Einddatum,
                                  format='%d-%m-%Y',
                                  errors='coerce').dt.strftime("%Y-%m-%d")

# Replace null-like dates (1800-01-01) with our N/A value
nether.Startdatum.replace({'1800-01-01': '1900-01-01',
                           '1801-01-01': '1900-01-01'}, inplace=True)
nether.Einddatum.replace({'1800-01-01': '1900-01-01',
                          '1801-01-01': '1900-01-01'}, inplace=True)

# Un-abbreviate offshore designation
nether['On offshore'].replace({'ON': 'ONSHORE',
                               'OFF': 'OFFSHORE'}, inplace=True)

# Translate well type code
# well type codes were translated and inferred from the "Resultaat" dropdown menu on the
# Netherland's webpage for this data source: https://www.nlog.nl/datacenter/brh-overview
nether['Boorgatresultaat Code'].replace({'COAL': 'COAL',
                                         'DRY': 'DRY',
                                         'FLR': 'TECHNICAL FAIL',
                                         'GAS': 'GAS',
                                         'GSS': 'GAS SHOWS',
                                         'OAG': 'OIL & GAS',
                                         'OIL': 'OIL',
                                         'OLS': 'OIL SHOWS',
                                         'SALT': 'ROCK SALT',
                                         'WTR': 'WATER',
                                         'UNK': 'N/A',  # Unknown
                                         'WTS': 'SPRING WATER',  # "bronwater" in Dutch
                                         'GWOS': 'GAS WITH OIL SHOWS',
                                         'OWGS': 'OIL WITH GAS SHOWS',
                                         'GOS': 'GAS AND OIL SHOWS'},
                                        inplace=True)

# translate drilling direction
nether['Boorgatvorm'].replace({'Gedevieerd': 'Deviated',
                               'Vertikaal': 'Vertical',
                               'Horizontaal': 'Horizontal'}, inplace=True)

# Replace any empty cells with the string "N/A"
replace_missing_strings_with_na(nether,
                                ['Provincie Naam',
                                 'Boorgatnaam',
                                 'NITG nummer',
                                 'Boorgatresultaat Code',
                                 'Boorgatvorm',
                                 'Boorgatstatus'],
                                limit_acceptable_columns=False)


# =============================================================================
# %% NETHERLANDS: DATA INTEGRATION
# =============================================================================
nether_integrated, errors = integrate_facs(
    nether,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Netherlands",
    state_prov="Provincie Naam",
    src_ref_id="15",
    src_date="2024-01-01",
    on_offshore="On offshore",
    fac_name="Boorgatnaam",
    fac_id="NITG nummer",
    fac_type="Boorgatresultaat Code",
    drill_type="Boorgatvorm",
    spud_date="Startdatum",
    comp_date="Einddatum",
    fac_status="Boorgatstatus",
    op_name="Opdrachtgever",
    fac_latitude="Latitude WGS84",
    fac_longitude="Longitude WGS84"
)

save_spatial_data(
    nether_integrated,
    file_name="netherlands_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% NEW ZEALAND
# =============================================================================
# os.chdir(pubdata)
# fp = "Australia+NewZealand\\New_Zealand\\Updated\\Petroleum_Wells.shp"
# wells = read_spatial_data(fp, table_gradient=False)

# # Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
# wells2 = transform_CRS(wells, target_epsg_code="epsg:4326", appendLatLon=True)

# # % Data pre-processing before data integration
# comp_dates = []

# for idx1_, row1_ in tqdm(wells2.iterrows(), total=wells2.shape[0]):
#     # Append to dates list the reformatted date by splitting at each slash mark and then reorganizing the order and format
#     comp = row1_.ComplnDate
#     if comp == "2100-12-31":
#         dates_null = "1900-01-01"
#         comp_dates.append(dates_null)

#     else:
#         comp_dates.append(comp)

# wells2['FORMATTED_COMP'] = comp_dates

# # Replace UNKNOWN and NO INFO values with "N/A"
# wells3 = replace_row_names(wells2, "Status", {'unkown': 'N/ A'})
# wells4 = replace_row_names(wells3, "Content", {'unknown': 'N/A', 'no info': 'N/A'})

# # =============================================================================
# # Integration
# # =============================================================================

# wells_NZ, errors = integrate_facs(
#     wells4,
#     starting_ids=0,
#     category="Oil and natural gas wells",
#     fac_alias="WELLS",
#     country="New Zealand",
#     state_prov=None,
#     src_ref_id="148",
#     src_date="2022-11-28",
#     on_offshore=None,
#     fac_name="FullName",
#     fac_id="APINumber",
#     fac_type="Content",
#     drill_type=None,
#     spud_date="SpudDate",
#     comp_date="FORMATTED_COMP",
#     fac_status="Status",
#     op_name="Operator",
#     fac_latitude="latitude_calc",
#     fac_longitude="longitude_calc"
# )

# save_spatial_data(
#     wells_NZ,
#     file_name="new_zealand_oil_gas_wells",
#     schema_def=True,
#     schema=schema_WELLS,
#     file_type="GeoJSON",
#     out_path=results_folder
# )

# =============================================================================
# %% NEW ZEALAND - new
# =============================================================================
os.chdir(v24data)
fp = r"new_zealand\wells\DataRecords.csv"
nz_wells = pd.read_csv(fp)
nz_wells = gpd.GeoDataFrame(nz_wells,
                            geometry=gpd.points_from_xy(nz_wells.x,
                                                        nz_wells.y),
                            crs=2193)  # New Zealand Transverse Mercator 2000 (EPSG:2193)
nz_wells = transform_CRS(nz_wells,
                         target_epsg_code="epsg:4326",
                         appendLatLon=True)

# Format date fields
nz_wells['spudnew'] = pd.to_datetime(nz_wells['Start Date']).dt.strftime("%Y-%m-%d")
nz_wells['compnew'] = pd.to_datetime(nz_wells['Start Date']).dt.strftime("%Y-%m-%d")

# Create more complete well type field
nz_wells.loc[nz_wells.Result.isin(['unknown', 'nan', 'no info']), 'Result'] = 'N/A'
create_concatenated_well_name(nz_wells,
                              'Purpose',
                              'Result',
                              'typenew')

# =============================================================================
# %% NEW ZEALAND - NEW - Integration + Export
# =============================================================================
nz_wells_integrated, errors = integrate_facs(
    nz_wells,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="New Zealand",
    state_prov=None,
    src_ref_id="148",
    src_date="2022-11-28",
    # on_offshore=None,
    fac_name="Title",
    fac_id="UWI",
    fac_type="typenew",
    # drill_type=None,
    spud_date="spudnew",
    comp_date="compnew",
    fac_status="Status",
    op_name="Operator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    nz_wells_integrated,
    file_name="new_zealand_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% NIGERIA - REMOVED src 159, no longer online
# =============================================================================
# os.chdir(pubdata)
# fp = r"Africa\Nigeria\Nigeria\WellsNig.shp"
# wells = read_spatial_data(fp, table_gradient=False)

# # Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
# wells = transform_CRS(wells, target_epsg_code="epsg:4326", appendLatLon=True)

# # Rows 4101 thru 4122 are "empty" and/or contain notes on the original data source
# # These rows were probably accidentally turned into geometries when an Excel
# # file was turned into a shapefile (or something similar)

# # Delete the extraneous / erroneous empty records, which all happen to have a null Block_name
# wells = wells[wells.Block_name.notnull()]

# # Create a simplified onshore-offshore field
# print(wells.Shore_stat.unique())
# # ['Shallow water', 'Deepwater', 'Onshore', 'Ultra-deepwater']
# wells['shore_new'] = None
# wells.loc[wells.Shore_stat == 'Onshore', 'shore_new'] = 'Onshore'
# wells.loc[wells.Shore_stat.str.contains('water'), 'shore_new'] = 'Offshore'

# # Create new well type field -- start by populating it with the basic Well Type info
# wells['well_type_new'] = wells.Well_type
# # IF there is any information about the well's "Result" present (i.e. was oil or gas found)
# # then concatenate that information as part of the value in my new well type field
# wells.loc[wells.Result.notnull(), 'well_type_new'] = wells.Well_type.str.cat(wells.Result, sep=' - ')

# print(wells.well_type_new.unique())

# # There are NUMEROUS wells for which we have attribute data, BUT NO LOCATION
# # The wells are instead mapped at lat-long (0,0), which we don't want
# # Drop these records from the final GDF
# wells.query("Latitude == 0").head()
# wells = wells[wells.Latitude != 0]

# # =============================================================================
# # NIGERIA WELLS - Integration + Export
# # =============================================================================
# wells_NIG, errors = integrate_facs(
#     wells,
#     starting_ids=0,
#     category='Oil and natural gas wells',
#     fac_alias='WELLS',
#     country='Nigeria',
#     # state_prov = None,
#     src_ref_id='159',
#     src_date='2020-11-01',
#     on_offshore='shore_new',
#     fac_name='Well',
#     # fac_id = None,
#     fac_type='well_type_new',
#     spud_date='Spud_date',
#     comp_date='Completion',
#     # drill_type = None,
#     # install_date = None,
#     fac_status='Status',
#     op_name='Operator',
#     fac_latitude='latitude_calc',
#     fac_longitude='longitude_calc'
# )

# save_spatial_data(
#     wells_NIG,
#     file_name="nigeria_oil_gas_wells",
#     schema=schema_WELLS,
#     file_type="GeoJSON",
#     out_path=results_folder
# )


# =============================================================================
# %% NORWAY
# =============================================================================
os.chdir(v24data)
fp = r'norway\wells\wlbPoint.csv'
norway = pd.read_csv(fp)
# Drop points with no WKT info
norway = norway.query("wlbPointGeometryWKT != 'POINT EMPTY'")
# TODO - check if this is really epsg:4326
norway = gpd.GeoDataFrame(norway, geometry=norway['wlbPointGeometryWKT'].map(shapely.wkt.loads), crs=4326)
# Transform CRS to EPSG 4326 and calculate and append lat and lon values [latitude_calc, longitude_calc]
norway = transform_CRS(norway, target_epsg_code="epsg:4326", appendLatLon=True)

# Format dates
norway['spudnew'] = pd.to_datetime(norway.wlbEntryDate).dt.strftime("%Y-%m-%d")
norway['compnew'] = pd.to_datetime(norway.wlbCompletionDate).dt.strftime("%Y-%m-%d")

# Create a custom well type field
norway.wlbPurpose.replace({'UNKNOWN': 'N/A',
                           'NOT AVAILABLE': 'N/A',
                           np.nan: 'N/A'}, inplace=True)
norway.wlbContent.replace({'NOT APPLICABLE': 'N/A',
                           'NOT AVAILABLE': 'N/A',
                           np.nan: 'N/A'}, inplace=True)

create_concatenated_well_name(norway,
                              'wlbPurpose',
                              'wlbContent',
                              'welltypenew')

# =============================================================================
# %% NORWAY - Integration & Export
# =============================================================================
norway_integrated, errors = integrate_facs(
    norway,
    starting_ids=0,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Norway",
    state_prov="wlbMainArea",
    src_ref_id="14",
    src_date="2024-04-18",  # Daily
    on_offshore="Offshore",
    fac_name="wlbWellboreName",
    fac_id="wlbNpdidWellbore",
    fac_type='welltypenew',
    # drill_type=None,
    spud_date="spudnew",
    comp_date="compnew",
    fac_status="wlbStatus",
    op_name="wlbDrillingOperator",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    norway_integrated,
    file_name="norway_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% PARAGUAY
# =============================================================================
os.chdir(v24data)
fp = r"paraguay/Pozos.shp"
pgy_wells = read_spatial_data(fp, table_gradient=True)
pgy_wells = transform_CRS(pgy_wells, appendLatLon=True)

# Translate O&G well type
pgy_wells.Caracteris.unique()
pgy_dict = {
    'Pozo sin Datos': 'N/A',
    'Pozo Gas': 'GAS',
    'Pozo Petróleo y Gas': 'OIL AND GAS',
    'Pozo Petróleo': 'OIL'
}
pgy_wells['Caracteris'] = pgy_wells['Caracteris'].replace(pgy_dict)

pgy_wells_integrated, wells_err_ = integrate_facs(
    pgy_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Paraguay",
    # state_prov=None,
    src_ref_id="134",
    src_date="2016-01-01",  # FIXME
    # on_offshore=None,
    # fac_name=None,
    fac_id="Id",
    fac_type="Caracteris",
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # install_date=None,
    # fac_status=None,
    # op_name=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    pgy_wells_integrated,
    file_name="paraguay_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% PERU - NEW
# =============================================================================
os.chdir(v24data)
fp = r'peru\MapaBase\Pozos.shp'
peru_wells = gpd.read_file(fp)
peru_wells = transform_CRS(peru_wells, appendLatLon=True)

# Reformat spud and completion dates
peru_wells['spudnew'] = pd.to_datetime(peru_wells.Inic_Perf,
                                       format='%Y/%m/%d',
                                       errors='coerce').dt.strftime("%Y-%m-%d")
peru_wells['compnew'] = pd.to_datetime(peru_wells.Term_Perf,
                                       format='%Y/%m/%d',
                                       errors='coerce').dt.strftime("%Y-%m-%d")

# Translate Clase_Pozo aka Type
clase_pozo_dict = {
    'DESARROLLO': 'DEVELOPMENT',
    'EXPLORATORIO': 'EXPLORATORY',
    'SIN INFORMACION': 'N/A',
    'CONFIRMATORIO': 'CONFIRMATORY',
    'EXPLORACION': 'EXPLORATION'
}
peru_wells.Clase_Pozo = peru_wells.Clase_Pozo.replace(clase_pozo_dict)

# Translate Estado aka Status - # TODO once I know what they mean
peru_status_translations = {
    'ABAND. EXPL/DESARR.': 'ABANDONED',
    'ABAND. INYECCION': 'ABANDONED',
    'ABAND. SECO EXPLOR.': 'ABANDONED',
    'ABAND.SECO DESARR.': 'ABANDONED',
    'ABAND.SECO EXPLOR.': 'ABANDONED',
    'ABANDONADO': 'ABANDONED',
    'ACTIVOS': 'ACTIVE',
    'APA': 'N/A',  # idk
    'ATA': 'N/A',  # idk
    'BCP': 'N/A',  # idk
    'BM': 'N/A',  # idk
    'COMPLETADO': 'COMPLETED',
    'CSWAB': 'N/A',  # idk
    'DESF': 'N/A',  # idk
    'DPA': 'N/A',  # idk
    'EN COMPLETACION': 'COMPLETION',
    'EVALUACION': 'EVALUATION',
    'GAS': 'GAS PRODUCER',
    'GAS LIFT': 'GAS LIFT',
    'IDLE': 'IDLE',
    'INACTIVO': 'INACTIVE',
    'INYECTOR': 'INJECTOR',
    'INYG': 'N/A',  # idk
    'INYW': 'N/A',  # idk
    'PABANDONADO': 'ABANDONED',
    'PL': 'N/A',  # idk
    'PRODUCT. ABANDONADO': 'ABANDONED',
    'PRODUCTIVO': 'PRODUCTIVE',
    'PRODUCTIVO-CERRADO': 'CLOSED',
    'PRODUCTIVO-PRODUCTOR': 'PRODUCTIVE',
    'PRODUCTOR': 'PRODUCER',
    'PRODUCTOR GAS': 'GAS PRODUCER',
    'PRODUCTOR INACTIVO': 'INACTIVE PRODUCER',
    'PRODUCTOR PETROLEO': 'OIL PRODUCER',
    'REPERFORADO': 'REDRILLED',
    'SD': 'N/A',  # idk
    'SECO,INDICACION DE PETROLEO': 'DRY, OIL INDICATIONS',
    'SIN INFORMACION': 'N/A',  # idk
    'SWAB': 'N/A',  # idk
    'TSWAB': 'N/A',  # idk
    'TUBING SWAB': 'N/A',  # idk
    'pRODUCTIVO-PRODUCTOR': 'PRODUCING'
}

peru_wells.Estado = peru_wells.Estado.replace(peru_status_translations)

# =============================================================================
# PERU - Integration + Export
# =============================================================================
per_wells_integrated, per_wells_err = integrate_facs(
    peru_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Peru",
    # state_prov=None,
    src_ref_id="133",
    src_date="2021-01-01",  # FIXME
    # on_offshore=None,
    fac_name="Nombre",
    fac_id='UWI',
    fac_type='Clase_Pozo',
    spud_date='spudnew',
    comp_date='compnew',
    # fac_status=None,  # FIXME
    op_name="Operadora",
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    per_wells_integrated,
    file_name="peru_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% UNITED KINGDOM
# =============================================================================
os.chdir(v24data)
# Read offshore data
fp_off = r'united_kingdom\wells\NSTA_Wells_(WGS84).geojson'
uk_offshore = gpd.read_file(fp_off)
uk_offshore['Source_ID'] = '12'
uk_offshore['Shore'] = 'OFFSHORE'

# Read onshore wells
fp_on = r'united_kingdom\wells\NSTA_Onshore_Wells_(BNG).geojson'
uk_onshore = gpd.read_file(fp_on)
uk_onshore['Source_ID'] = '13'
uk_onshore['Shore'] = 'ONSHORE'

# Concatenate both gdfs (confirm CRS is the same before merging)
uk_onshore.crs == uk_offshore.crs
uk_wells = pd.concat([uk_offshore, uk_onshore]).reset_index()
uk_wells = transform_CRS(uk_wells, appendLatLon=True)

# Format dates
uk_wells['spudnew'] = pd.to_datetime(uk_wells.SPUDDATE).dt.strftime("%Y-%m-%d")
uk_wells['compnew'] = pd.to_datetime(uk_wells.COMPLEDATE).dt.strftime("%Y-%m-%d")

# Standardize missing values in various columns
uk_wells["MAPSYMDESC"].replace({"Unknown": "N/A",
                                np.nan: "N/A",
                                None: "N/A",
                                "NAN": "N/A"}, inplace=True)

uk_wells["COMPOP"].replace({"No Data Available": "N/A"}, inplace=True)  # COMPOP = "Competent Operator"

uk_wells['NAME'].replace({'.': 'N/A'}, inplace=True)


# =============================================================================
# %% UNITED KINGDOM: Integration + Export
# =============================================================================
uk_wells_integrated, errors = integrate_facs(
    uk_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="United Kingdom",
    # state_prov="N/A",
    src_ref_id="Source_ID",
    src_date="2024-04-19",
    on_offshore="Shore",
    fac_name="NAME",
    fac_id="WELLREGNO",
    fac_type="MAPSYMDESC",
    drill_type="DEVIATTYP",
    spud_date="spudnew",
    comp_date="compnew",
    fac_status="ORIGINSTAT",
    op_name="COMPOP",
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    uk_wells_integrated,
    file_name="united_kingdom_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% VENEZUELA
# `Data source`: https://services6.arcgis.com/lpJCO3ug8HhNiEOV/arcgis/rest/services/Pozos_Petroleros/FeatureServer<br>
# `Data owner`: *ArcGIS Online, @proyecto.ve360* <br>
# `Last accessed`: *May 2022* <br>
# `Data last updated`: *2021-05-21*
# =============================================================================
os.chdir(v24data)
fp = r"venezuela/Venezuela_Wells.shp"
vez_wells = read_spatial_data(fp)
vez_wells = transform_CRS(vez_wells, appendLatLon=True)

# Facility status
stats_dict = {
    'ACTIVO': 'ACTIVE',
    'PRODUCTOR INACTIVO': 'INACTIVE PRODUCER',
    'CEMENTADO Y ABANDONADO': 'PLUGGED AND ABANDONED',
    'PRODUCTOR': 'ACTIVE',
    'SUPLIDOR DE AGUA': 'WATER SOURCE',
    'INYECTOR': 'INJECTOR',
    'INYECTOR DE AGUA CERRADO': 'CLOSED WATER INJECTOR',
    'SUSPENDIDO': 'SUSPENDED',
    'NO ASIGNADO': 'N/A'
}

vez_wells = replace_row_names(vez_wells,
                              colName="ESTADO_ACT",
                              dict_names=stats_dict)

# Remove completion dates -- 97% of the values are n/a, and all the non-null
# values are from Jan 1901 or Jan 1945, not sure if I trust those...
# vez_wells["comp_dates"] = vez_wells["FECHA_COMP"].replace({None: "1900-01-01", "01/01/1901 01:00:00.00": "1900-01-01"})
# vez_wells["comp_dates"] = pd.to_datetime(vez_wells["comp_dates"]).dt.strftime("%Y-%m-%d")

# Well type  # TODO - how do we know for sure what the abbreviation "CRU" means?
print(vez_wells.CODIGO_TIP.unique())
vez_wells['type_'] = 'oil'

# deduplicate wells
vez_wells = vez_wells.drop_duplicates(subset=['NUMERO_IDE',
                                              'type_',
                                              'FECHA_INCI',  # This is either spud or completion date maybe? but since I'm not sure I'm not including
                                              'ESTADO_ACT',
                                              'geometry'],
                                      keep='first')

# =============================================================================
# %% VENEZUELA - Integration + Export
# =============================================================================
vez_wells_integrated, vez_wells_err = integrate_facs(
    vez_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Venezuela",
    # state_prov=None,
    src_ref_id="131",
    src_date="2021-05-21",
    # on_offshore=None,
    fac_name='CODIGO_INF',
    fac_id="NUMERO_IDE",
    fac_type="type_",
    # spud_date=None,
    # comp_date="comp_dates",
    # drill_type=None,
    fac_status="ESTADO_ACT",
    # op_name=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    vez_wells_integrated,
    file_name="venezuela_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% Africa (oginfra.com) - Ethiopia, Libya, Mozambique, Nigeria, Sudan
# =============================================================================
os.chdir(v24data)

ETH_wells = gpd.read_file(r"ethiopia/Gas_Wells_Ethiopia.kml.shp").to_crs("epsg:4326")
ETH_wells2 = gpd.read_file(r"ethiopia/Oil_Wells_Ethiopia.kml.shp").to_crs("epsg:4326")
ETH_wells3 = gpd.read_file(r"ethiopia/Unknown_Wells_Ethiopia.kml.shp").to_crs("epsg:4326")

LIB_wells1 = gpd.read_file(r"libya/oginfrastructure_dot_com/Wells_Offshore_Libya.kml.shp").to_crs("epsg:4326")
LIB_wells2 = gpd.read_file(r"libya/oginfrastructure_dot_com/Wells_Onshore_2_Libya.kml.shp").to_crs("epsg:4326")

MOZ_wells2 = gpd.read_file(r"mozambique/Gas_Wells_Mozambique.kml.shp").to_crs("epsg:4326")

NIG_wells2 = gpd.read_file(r"nigeria/Wells_Nigeria.kml.shp").to_crs("epsg:4326")

SUD_wells2 = gpd.read_file(r"sudan/_wells_sudan_.shp").to_crs("epsg:4326")

# -----------------------------------------------------------------------------
# assign well type where known based on filename
ETH_wells['factype'] = 'Gas Wells'
ETH_wells2['factype'] = "Oil Wells"

# assign offshore type where known based on filename
ETH_wells['OnOffshore'] = 'Onshore'
ETH_wells2['OnOffshore'] = 'Onshore'
ETH_wells3['OnOffshore'] = 'Onshore'
LIB_wells1['OnOffshore'] = 'Offshore'
LIB_wells2['OnOffshore'] = 'Onshore'
MOZ_wells2['OnOffshore'] = 'Onshore'
NIG_wells2['OnOffshore'] = 'Offshore'
SUD_wells2['OnOffshore'] = 'Onshore'

# Assign countries to each gdf
MOZ_wells2['country'] = 'Mozambique'
NIG_wells2['country'] = 'Nigeria'
SUD_wells2['country'] = 'Sudan'

for df in [ETH_wells, ETH_wells2, ETH_wells3]:
    df['country'] = 'Ethiopia'

for df in [LIB_wells1, LIB_wells2]:
    df['country'] = 'Libya'
# -----------------------------------------------------------------------------
# Confirm that all dataframes are the same CRS before appending into one gdf
all_dfs_final = [
    ETH_wells,
    ETH_wells2,
    ETH_wells3,
    LIB_wells1,
    LIB_wells2,
    MOZ_wells2,
    SUD_wells2,
    NIG_wells2
]

# Append country-specific gdfs into one gdf
data = pd.concat(all_dfs_final)
data = data.reset_index(drop=True)
# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)

# Take care of missing data values
data = data.fillna('N/A')
data.loc[data.Name == 'Untitled Placemark', 'Name'] = 'N/A'
data.loc[data.Name == 'Placemark', 'Name'] = 'N/A'

# =============================================================================
# %% Africa (oginfra.com) - Integration + Export
# =============================================================================
wells_AFRICA, errors = integrate_facs(
    data,
    starting_ids=0,
    category='Oil and natural gas wells',
    fac_alias='WELLS',
    country='country',
    state_prov=None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='OnOffshore',
    fac_name=None,
    fac_id=None,
    fac_type=None,
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    wells_AFRICA,
    file_name="africa_oilgasinfra_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% Middle East (oginfra.com) - SAUDI ARABIA, UAE
# =============================================================================
os.chdir(v24data)

# REMOVING BECAUSE OF DUPLICATES WITH USGS WELLS
# afghan = gpd.read_file(r'Afghanistan\Oil_Gas_Infra_.com\Wells\Wells without logs.kml.shp')

# TODO copy wells
saudi_gas1 = gpd.read_file(r'saudi_arabia\wells\Gas_Wells_AinDar_Saudi_Arabia.kml.shp')
saudi_gas2 = gpd.read_file(r'saudi_arabia\wells\Gas_wells_Harad_Saudi_Arabia.kml.shp')
saudi_oil1 = gpd.read_file(r'saudi_arabia\wells\Oil_Wells_AinDar_Saudi_Arabia.kml.shp')
saudi_oil2 = gpd.read_file(r'saudi_arabia\wells\Oil_Wells_Harad_Saudi_Arabia.kml.shp')
saudi_oil3 = gpd.read_file(r'saudi_arabia\wells\Oil_Wells_Harmaliyah_Saudi_Arabia.kml.shp')
saudi_wells = gpd.read_file(r'saudi_arabia\wells\Wells_Hawiyah_Saudi_Arabia.kml.shp')

# Data set was removed from "oginfracom_middleeast_offshoreplatforms.py" and added here instead on 11/22/22
saudi_offshore_wells = gpd.read_file(r'saudi_arabia\wells\offshore_wells_Saudi_Arabia.kml.shp')

# DON'T INCLUDE WATER WELLS
# saudi_water1 = gpd.read_file(r'Saudi_Arabia\Oil_Gas_Infra_.com\Wells\Water_Wells_Abqaiq_Saudi_Arabia.kml.shp')
# saudi_water2 = gpd.read_file(r'Saudi_Arabia\Oil_Gas_Infra_.com\Wells\Water_Wells_AinDar_Saudi_Arabia.kml.shp')
# saudi_water3 = gpd.read_file(r'Saudi_Arabia\Oil_Gas_Infra_.com\Wells\Water_Wells_Harad_Saudi_Arabia.kml.shp')

uae = gpd.read_file(r'uae\Onshore_Wells_UAE.kml.shp')

# -----------------------------------------------------------------------------
# % Data manipulation / processing if needed
# All UAE wells have a 'Name' value that's simply 'Well', replace these values with NOT AVAILABLE instead
uae['Name'] = 'NOT AVAILABLE'

# Add Onshore or Offshore designation, where it's known
uae['on_offshore'] = "Onshore"
saudi_offshore_wells['on_offshore'] = "Offshore"

# Add country column to single-country gdfs
uae['country'] = 'UAE'

# Create facility type value, based on commodity info in the original shapefile name, where commodity is known
for df in [saudi_gas1, saudi_gas2]:
    df['type'] = 'Gas'

for df in [saudi_oil1, saudi_oil2, saudi_oil3]:
    df['type'] = 'Oil'

# Add country column to all Saudi Arabia gdfs
for df in [saudi_gas1, saudi_gas2, saudi_oil1, saudi_oil2, saudi_oil3, saudi_wells, saudi_offshore_wells]:
    df['country'] = 'Saudi Arabia'

# Append all country-specific gdfs into one well gdf
all_dfs_final = [
    saudi_gas1,
    saudi_gas2,
    saudi_oil1,
    saudi_oil2,
    saudi_oil3,
    saudi_wells,
    saudi_offshore_wells,
    uae
]

data = pd.concat(all_dfs_final)
data = data.reset_index(drop=True)

# remove "untitled placemark" from Name column, replace with NOT AVAILABLE
data.loc[data.Name.str.contains('Untitled'), 'Name'] = 'N/A'
# Fill in any empty cells in my newly-created columns with NOT AVAILABLE
data['type'] = data['type'].fillna('N/A')
data['on_offshore'] = data['on_offshore'].fillna('N/A')

# Transform CRS to EPSG 4326; calculate and append lat+long values [latitude_calc, longitude_calc]
data = transform_CRS(data, target_epsg_code="epsg:4326", appendLatLon=True)


data1 = replace_row_names(data, colName="Name", dict_names={'': 'N/A', 'NAN': 'N/A', None: 'N/A', np.nan: 'N/A', 'NOT AVAILABLE': 'N/A'})


# =============================================================================
# %% Middle East (oginfra.com) - Integration + Export
# =============================================================================
wells_SAUDI_UAE, errors = integrate_facs(
    data1,
    starting_ids=0,
    category='Oil and natural gas wells',
    fac_alias='WELLS',
    country='country',
    state_prov=None,
    src_ref_id='22',
    src_date='2014-01-01',
    on_offshore='on_offshore',
    fac_name='Name',
    fac_id=None,
    fac_type='type',
    fac_latitude='latitude_calc',
    fac_longitude='longitude_calc'
)

save_spatial_data(
    wells_SAUDI_UAE,
    file_name="saudi_arabia_uae_oil_gas_wells",
    schema_def=True,
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=results_folder
)
