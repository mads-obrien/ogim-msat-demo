# -*- coding: utf-8 -*-
"""
Created on Fri Aug 25 2023

Data integration of Mexico OGIM data -- all infrastructure categories.
We have no data in this region for the following categories:
    - injection and disposal
    - LNG

# TODO:
[x] standardize import statements and CWD setting
[] standardize spacing between sections
[] update all file paths

@author: maobrien, momara
"""
import os
import pandas as pd
# import numpy as np
import geopandas as gpd
import glob
# from tqdm import trange

# Import custom functions
path_to_github = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\'
os.chdir(path_to_github + 'functions')
from ogimlib import (replace_row_names, transform_CRS, integrate_facs,
                     read_spatial_data, save_spatial_data, schema_WELLS,
                     schema_LNG_STORAGE, schema_COMPR_PROC, schema_OTHER,
                     calculate_pipeline_length_km, explode_multi_geoms,
                     integrate_pipelines, schema_REFINERY,
                     calculate_basin_area_km2, integrate_basins)

# !!! Specify version number, in v style. Must match name of folder on shared drive
version_num = 'v2.7'

# -----------------------------------------------------------------------------
# Define path to Bottom Up Infra Inventory directory
buii_path = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory'

# Set current working directory
os.chdir(os.path.join(buii_path, f'OGIM_{version_num}', 'data', 'mexico'))

# Folder in which all integrated data will be saved
integration_out_path = f'{buii_path}\\OGIM_{version_num}\\integrated_results\\'

# =============================================================================
# %% WELLS
# =============================================================================
"""
`Data source`: https://mapa.hidrocarburos.gob.mx/?lng=en_US
`Data owner`: *Mexico CNIH, National Center for Hydrocarbon Information*
"""

fp = "wells"
files = glob.glob(fp + "\\*.shp")
all_mx_wells = []

for file in files:
    df = gpd.read_file(file, encoding='utf-8')
    all_mx_wells.append(df)

# Concatenate
mx_wells = pd.concat(all_mx_wells)
print("Total number of features in dataset = ", mx_wells.shape[0])
print(*mx_wells.columns, sep='\n')

# Check and transform CRS
mx_wells = transform_CRS(mx_wells, appendLatLon=True)
# There's one well with extremely wacky coordinates, drop it
mx_wells = mx_wells[mx_wells.longitude_calc < 1000].reset_index()
# -----------------------------------------------------------------------------
# Standardize on-offshore attribute
en_locs = {
    'TERRESTRE': 'ONSHORE',
    'AGUAS SOMERAS': 'OFFSHORE',
    'AGUAS ULTRAPROFUNDAS': 'OFFSHORE',
    'AGUAS PROFUNDAS': 'OFFSHORE',
    'AGUAS ULTRA PROFUNDAS': 'OFFSHORE'
}
mx_wells = replace_row_names(mx_wells, "location", dict_names=en_locs)


# Standardize status information
en_status = {
    'CERRADO': 'CLOSED',
    'PRODUCTOR': 'ACTIVE',
    'INACTIVO': 'INACTIVE',
    'ABANDONO PERMANENTE': 'ABANDONED',
    'INYECTOR RECUPERACION SECUNDARIA O MEJORADA': 'ENHANCED RECOVERY INJECTOR',
    'INYECTOR PARA RECUPERACION SECUNDARIA O MEJORADA': 'ENHANCED RECOVERY INJECTOR',
    'ABANDONO TEMPORAL': 'TEMPORARILY ABANDONED',
    'SUSPENDIDO': 'SUSPENDED',
    'CERRADO ESPERANDO INSTALACIONES DE PRODUCCION': 'SHUT-IN',
    'OPERANDO': 'ACTIVE',
    'INYECTOR PARA DISPOSICION': 'INJECTOR FOR DISPOSAL',
    'PERFORADO': 'DRILLING',
    'ESPERANDO ABANDONO': 'WAITING FOR ABANDONMENT',
    'EXPLORATORIO': 'EXPLORATORY',
    'CANCELADO': 'CANCELLED',
    'DESARROLLO': 'DEVELOPMENT',
    'PRODUCTOR INSTALACIONES TEMPORALES': 'TEMPORARY PRODUCER FACILITIES',
    'DELIMITADOR': 'DELIMITER',
    'TAPONAMIENTO PERMANENTE': 'PERMANENTLY PLUGGED',
    'TAPONAMIENTO TEMPORAL': 'TEMPORARILY PLUGGED',
    'PRODUCTOR CON INSTALACIONES TEMPORALES': 'PRODUCER WITH TEMPORARY FACILITIES',
    'EN CONSTRUCCION': 'UNDER CONSTRUCTION',
    'INYECTOR PARA DISPOSICION O DISPOSICION O CONVERTIDO EN DE DISPOSICION DE RESIDUOS O RECORTES': 'INJECTOR FOR DISPOSAL',
    None: 'N/A'
}
mx_wells = replace_row_names(mx_wells, "current_st", dict_names=en_status)


# Standardize well type information
en_types = {
    'DESARROLLO': 'DEVELOPMENT',
    'EXPLORATORIO': 'EXPLORATION',
    'EXPLORATORIO NUEVO CAMPO': 'EXPLORATION',
    'EXPLORATORIO NUEVO YACIMIENTO': 'EXPLORATION',
    'SONDEO ESTRATIGRAFICO': 'STRATIGRAPHIC TEST WELL',
    'DELIMITADOR': 'WELL TEST SEPARATOR',
    'INYECTOR': 'INJECTOR',
    'ALIVIO': 'RELIEF',
    'EVALUADOR YACIMIENTO MENOS PROFUNDO': 'DEVELOPMENT',
    None: 'N/A',
    'WELL TEST': 'WELL TEST SEPARATOR',
    'POZO DE AGUA': 'WATER WELL'
}
mx_wells = replace_row_names(mx_wells, "well_type", dict_names=en_types)


# Standardize drilling configuration
en_drill = {
    'VERTICAL': 'VERTICAL',
    'DIRECCIONAL': 'DIRECTIONAL',
    'HORIZONTAL': 'HORIZONTAL',
    'DIRECCIONAL S': 'DIRECTIONAL',
    'DIRECCIONAL J': 'DIRECTIONAL',
    'MULTILATERAL': 'MULTILATERAL',
    'DESVIADO': 'DEVIATED',
    'Vertical': 'VERTICAL',
    'EN PERFORACION': 'IN PERFORATION',
    None: 'N/A'
}
mx_wells = replace_row_names(mx_wells, "well_path", dict_names=en_drill)


# Fix dates
# -----------------------------------------------------------------------------
# First, create a dictionary of Spanish month names mapped to month numbers
date_dict_ = {
    'ENE': '01',
    'FEB': '02',
    'MAR': '03',
    'ABR': '04',
    'MAY': '05',
    'JUN': '06',
    'JUL': '07',
    'AGO': '08',
    'SEP': '09',
    'OCT': '10',
    'NOV': '11',
    'DIC': '12'
}

# Find and replace substrings of month names with their respective number
mx_wells['spudnew'] = mx_wells.drilling_s.replace(date_dict_, regex=True)
mx_wells['compnew'] = mx_wells.drilling_e.replace(date_dict_, regex=True)

# Convert the dd-mm-YYYY style dates to YYYY-MM-DD style
mx_wells['spudnew'] = pd.to_datetime(mx_wells.spudnew,
                                     format='%d-%m-%Y',
                                     errors='coerce').dt.strftime("%Y-%m-%d")
mx_wells['compnew'] = pd.to_datetime(mx_wells.compnew,
                                     format='%d-%m-%Y',
                                     errors='coerce').dt.strftime("%Y-%m-%d")

# Change one province name that has underscores in it for some reason
mx_wells.state.replace({'VERACRUZ_DE_IGNACIO_DE_LA_LLAVE': 'VERACRUZ DE IGNACIO DE LA LLAVE'},
                       inplace=True)

# Drop one record that's a pure duplicate
mx_wells = mx_wells.drop_duplicates(subset=['well_name',
                                            'latitude_calc',
                                            'longitude_calc'],
                                    keep='last').reset_index()

# =============================================================================
# %% WELLS - Integration + Export - DONE
# =============================================================================
mx_wells_integrated, mx_wells_ = integrate_facs(
    mx_wells,
    starting_ids=1,
    category="Oil and natural gas wells",
    fac_alias="WELLS",
    country="Mexico",
    state_prov="state",
    src_ref_id="223",
    src_date="2024-04-19",  # Daily
    on_offshore="location",
    fac_name="well_name",
    fac_id=None,
    fac_type="well_type",
    spud_date="spudnew",
    comp_date="compnew",
    drill_type="well_path",
    install_date=None,
    fac_status="current_st",
    op_name=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)

save_spatial_data(
    mx_wells_integrated,
    file_name="mexico_oil_gas_wells",
    schema=schema_WELLS,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% REFINERIES
# =============================================================================
ref_mx = read_spatial_data("facilities\\Refineries.shp")
ref1 = transform_CRS(ref_mx, appendLatLon=True)

# Create install date field
ref1['install'] = ref1.opening_da.astype(int).astype(str) + '-01-01'

# =============================================================================
# %% REFINERIES - Integration + Export
# =============================================================================
ref_integrated, ref_errors = integrate_facs(
    ref1,
    starting_ids=1,
    category="Crude Oil Refineries",
    fac_alias="REFINERY",
    country="Mexico",
    state_prov="state",
    src_ref_id="224",  # UPDATED
    src_date="2021-07-29",
    on_offshore="Onshore",
    fac_name="refinery_n",
    fac_id=None,
    fac_type=None,
    drill_type=None,
    install_date='install',
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
    ref_integrated,
    file_name="mexico_refineries",
    schema=schema_REFINERY,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% Read in general facilities shapefile (contains many infra types)
# =============================================================================
# Read in general facilities shapefile
fac_data = read_spatial_data("facilities\\Facilities.shp")
fac_data = transform_CRS(fac_data, appendLatLon=True)

# Translate status values
status_dict = {
    'OPERANDO': 'Operating',
    'FUERA DE OPERACION TEMPORAL': 'Temporarily closed',
    'FUERA DE OPERACION DEFINITIVA': 'Closed',
    'FUERA DE OPERACION': 'Inactive',
    'PARCIALMENTE DESMANTELADA': 'Partially dismantled',
    'DESMANTELADA': 'Dismantled',
    'CERRADO': 'Closed',
    'DESCONOCIDO': 'N/A',
    'OPERANDO (INTERMITENTE)': 'Intermittent operation',
    'EN CONSTRUCCION': 'Under construction',
    'DESMANTELADO': 'Dismantled'
}

fac_data_mx = replace_row_names(fac_data,
                                colName="condition",
                                dict_names=status_dict)

# =============================================================================
# %% GATHERING AND PROCESSING
# =============================================================================
# Subset the Facilities file to just gathering type facilities
gath_list = [
    'COMPLEJO PROCESADOR DE GAS',
    'PLANTA DESHIDRATADORA',
    'ESTACION DE RECOLECCION DE GAS',
    'ESTACION DE RECOLECCION',
    'ESTACION DE RECOLECCION DE GAS Y CONDENSADO',
    'ESTACION DE RECOLECCION DE ACEITE',
    'MODULO DE RECOLECCION',
    'MODULO DE RECOLECCION DE GAS',
    'ESTACION DE RECOLECCION Y SEPARACION',
    'ESTACION DE RECOLECCION MODULAR',
    'ESTACION DE RECOLECCION Y ACONDICIONAMIENTO'
]
gath = fac_data_mx.query("type == @gath_list")


# Translate the FAC_TYPE values from Spanish to English
dict_gath = {
    'COMPLEJO PROCESADOR DE GAS': 'GAS PROCESSING COMPLEX',
    'PLANTA DESHIDRATADORA': 'DEHYDRATION PLANT',
    'ESTACION DE RECOLECCION DE GAS': 'GAS GATHERING STATION',
    'ESTACION DE RECOLECCION': 'GATHERING STATION',
    'ESTACION DE RECOLECCION DE GAS Y CONDENSADO': 'GAS AND CONDENSATE GATHERING STATION',
    'ESTACION DE RECOLECCION DE ACEITE': 'OIL GATHERING STATION',
    'MODULO DE RECOLECCION': 'GATHERING MODULE',
    'MODULO DE RECOLECCION DE GAS': 'GAS GATHERING MODULE',
    'ESTACION DE RECOLECCION Y SEPARACION': 'GATHERING AND SEPARATION STATION',
    'ESTACION DE RECOLECCION MODULAR': 'MODULAR GATHERING STATION',
    'ESTACION DE RECOLECCION Y ACONDICIONAMIENTO': 'GATHERING AND CONDITIONING STATION'
}

gath = replace_row_names(gath, colName="type", dict_names=dict_gath)


# Read in and prepare gas processing complexes shapefile
# ---------------------------------------------------------------------------
compl_mx = read_spatial_data("facilities\\Gas Processing Complex.shp")
# Convert the MULTIPOINT geometries (all containing just one point) to POINT
compl_mx = explode_multi_geoms(compl_mx).reset_index(drop=True)
compl_mx = transform_CRS(compl_mx, appendLatLon=True)

# Create install date field from 'start_year' field
compl_mx['install'] = compl_mx.start_year.astype(int).astype(str) + '-01-01'

# Add facility type attribute
compl_mx['fac_type_'] = "NATURAL GAS PROCESSING COMPLEX"

# =============================================================================
# %% GATHERING AND PROCESSING - Integration + Export
# =============================================================================
gath_integrated, gath_err = integrate_facs(
    gath,
    starting_ids=1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Mexico",
    state_prov=None,
    src_ref_id="225",  # UPDATED
    src_date="2022-07-18",  # UPDATED
    on_offshore=None,
    fac_name="facility_n",
    fac_id=None,
    fac_type="type",
    install_date=None,
    fac_status="condition",
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
    gath_integrated,
    file_name="mexico_gathering_processing",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# Integrate the records from the Gas Processing Complex shapefile
# ---------------------------------------------------------------------------
compl_mx_integrated, compl_mx_err = integrate_facs(
    compl_mx,
    starting_ids=1,
    category="Gathering and processing",
    fac_alias="COMPR_PROC",
    country="Mexico",
    state_prov=None,
    src_ref_id="226",  # UPDATED
    src_date="2021-07-29",  # UPDATED
    on_offshore=None,
    fac_name="complex",
    fac_id=None,
    fac_type="fac_type_",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date="install",
    fac_status=None,
    op_name="licensor",
    commodity=None,
    liq_capacity_bpd="liquid_rec",  # field is named `LIQUID RECOVERY CAPACITY (MMCFD)`
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="gas_sweete",  # field is named `GAS SWEETENING CAPACITY (MMCFD)`
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    compl_mx_integrated,
    file_name="mexico_gathering_processing_part_02",
    schema=schema_COMPR_PROC,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% EQUIPMENT COMPONENTS
# =============================================================================
# Subset the Facilities file to just equipment / components records
eqp_list = [
    'CABEZAL DE RECOLECCION',
    'SEPARADOR',
    'TRAMPA',
    'CABEZAL',
    'TRAMPAS',
    'JUEGO DE VALVULAS',
    'PLATAFORMA DE PERFORACION',
]
eqpm = fac_data_mx.query("type == @eqp_list")

# Translate facility type
dict_eq = {
    'CABEZAL DE RECOLECCION': 'COLLECTION HEAD',
    'SEPARADOR': 'SEPARATOR',
    'TRAMPA': 'TRAP',
    'CABEZAL': 'HEAD',
    'TRAMPAS': 'TRAP',
    'JUEGO DE VALVULAS': 'VALVES',
    'PLATAFORMA DE PERFORACION': 'RIG'
}

eqpm = replace_row_names(eqpm, colName="type", dict_names=dict_eq)

# Correct facility names with typo / misspelled word
eqpm.facility_n = eqpm.facility_n.str.replace('ISNTRUMENTOS', 'INSTRUMENTOS')

# =============================================================================
# %% EQUIPMENT COMPONENTS - Integration + Export
# =============================================================================
eqpm_integrated, mx_eqpm_err = integrate_facs(
    eqpm,
    starting_ids=1,
    category="Equipment and components",
    fac_alias="OTHER",
    country="Mexico",
    state_prov=None,
    src_ref_id="225",  # UPDATED
    src_date="2022-07-18",  # UPDATED
    on_offshore=None,
    fac_name="facility_n",
    fac_id=None,
    fac_type="type",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    install_date=None,
    fac_status="condition",
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
    eqpm_integrated,
    "mexico_equipment_components_valves_separators",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% COMPRESSOR STATIONS
# =============================================================================
# Subset the Facilities file to just compressor station records
comp_stat_names_ = [
    'ESTACION DE COMPRESION',
    'BATERIA Y ESTACION DE COMPRESION',
    'ESTACION DE RECOLECCION Y COMPRESION',
    'ESTACION DE COMPRESION DE GAS',
    'ESTACION DE RECOLECCION, SEPARACION Y COMPRESION',
    'BATERIA DE SEPARACION Y ESTACION DE COMPRESION',
    'ESTACION DE RECOLECCION Y COMPRESION DE GAS',
    'ESTACION DE COMPRESION Y RECOLECCION DE GAS',
]
comp_mx = fac_data_mx.query("type == @comp_stat_names_")


# Translate compressor facility type
dict_names_comp = {
    'ESTACION DE COMPRESION': 'Gas compressor station',
    'BATERIA Y ESTACION DE COMPRESION': 'Battery and compressor station',
    'ESTACION DE RECOLECCION Y COMPRESION': 'Collection and compressor station',
    'ESTACION DE COMPRESION DE GAS': 'Gas compressor station',
    'ESTACION DE RECOLECCION, SEPARACION Y COMPRESION': 'Collection, separation and compressor station',
    'BATERIA DE SEPARACION Y ESTACION DE COMPRESION': 'Battery and compressor station',
    'ESTACION DE RECOLECCION Y COMPRESION DE GAS': 'Gas Compressor Station',
    'ESTACION DE COMPRESION Y RECOLECCION DE GAS': 'Gas Compressor and Collection Station'
}

comp_mx = replace_row_names(comp_mx,
                            colName='type',
                            dict_names=dict_names_comp)

# =============================================================================
# %% COMPRESSOR STATIONS - Integration + Export
# =============================================================================
comp_mx_integrated, comp_mx_err = integrate_facs(
    comp_mx,
    starting_ids=1,
    category="Natural gas compressor stations",
    fac_alias="COMPR_PROC",
    country="Mexico",
    state_prov=None,
    src_ref_id="225",  # UPDATED
    src_date="2022-07-18",  # UPDATED
    on_offshore=None,
    fac_name="facility_n",
    fac_id=None,
    fac_type="type",
    fac_status="condition",
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
    comp_mx_integrated,
    "mexico_gas_compressor_stations",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% OFFSHORE PLATFORMS
# =============================================================================
# Subset the Facilities file to just Offshore Platform records
platform_list = [
    'PLATAFORMA MAR DE PRODUCCION',
    'PLATAFORMA DE PERFORACION (TETRAPODO)',
    'PLATAFORMA DE PERFORACION (TRIPODE)',
    'PLATAFORMA DE PERFORACION (OCTAPODO)',
    'PLATAFORMA DE PERFORACION (SEA HORSE)',
    'PLATAFORMA DE PERFORACION (SEA PONY)',
    'PLATAFORMA HABITACIONAL (TETRAPODO)',
    'PLATAFORMA MAR DE PERFORACION',
    'PLATAFORMA MAR HABITACIONAL',
    'PLATAFORMA DE PRODUCCION',
    'PLATAFORMA DE SEPARACION',
    'PLATAFORMA DE COMPRESION',
    'PLATAFORMA HABITACIONAL',
    'PLATAFORMA MAR TRAT Y BOMBEO',
    'PLATAFORMA DE SEPARACION',
    'PLATAFORMA DE COMPRESION'
]
platforms = fac_data_mx.query("type == @platform_list")

# Translate facility type
dict_plat = {
    'PLATAFORMA MAR DE PRODUCCION': 'Production Platform',
    'PLATAFORMA DE PERFORACION (TETRAPODO)': 'DRILLING PLATFORM (TETRAPODE)',
    'PLATAFORMA DE PERFORACION (TRIPODE)': 'Drilling platform (tripod)',
    'PLATAFORMA DE PERFORACION (OCTAPODO)': 'Drilling platform (octapode)',
    'PLATAFORMA DE PERFORACION (SEA HORSE)': 'Drilling platform (sea horse)',
    'PLATAFORMA DE PERFORACION (SEA PONY)': 'Drilling platform (sea pony)',
    'PLATAFORMA HABITACIONAL (TETRAPODO)': 'Housing platform (tetrapode)',
    'PLATAFORMA MAR DE PERFORACION': 'OFFSHORE DRILLING PLATFORM',
    'PLATAFORMA MAR HABITACIONAL': 'OFFSHORE HOUSING PLATFORM',
    'PLATAFORMA DE PRODUCCION': 'Production Platform',
    'PLATAFORMA HABITACIONAL': 'HOUSING PLATFORM',
    'PLATAFORMA MAR TRAT Y BOMBEO': 'OFFSHORE TREATMENT AND PUMPING STATION',
    'PLATAFORMA DE SEPARACION': 'SEPARATION PLATFORM',
    'PLATAFORMA DE COMPRESION': 'COMPRESSION PLATFORM'
}
platforms = replace_row_names(platforms, colName="type", dict_names=dict_plat)

# =============================================================================
# %% OFFSHORE PLATFORMS - Integration + Export
# =============================================================================
platforms_integrated, platforms_err = integrate_facs(
    platforms,
    starting_ids=1,
    category="Offshore platforms",
    fac_alias="OTHER",
    country="Mexico",
    state_prov=None,
    src_ref_id="225",  # UPDATED
    src_date="2022-07-18",  # UPDATED
    on_offshore=None,
    fac_name="facility_n",
    fac_id=None,
    fac_type="type",
    install_date=None,
    fac_status="condition",
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
    platforms_integrated,
    file_name="mexico_offshore_platforms",
    schema=schema_OTHER,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% PETROLEUM TERMINALS
# =============================================================================
terms_mx = read_spatial_data("facilities\\Terminal Storage.shp")
# Convert the MULTIPOINT geometries (all containing just one point) to POINT
terms_mx = explode_multi_geoms(terms_mx).reset_index(drop=True)
terms_mx = transform_CRS(terms_mx, appendLatLon=True)

# Convert capacity and throughput to floats
terms_mx['nominal_ca2'] = terms_mx['nominal_ca'].str.replace(',', '').astype(float)
terms_mx['operative_ca2'] = terms_mx['operative_'].str.replace(',', '').astype(float)

# Nominal capacity & operative capacity (a.k.a. throughput) are in barrels per year
# Convert to barrels per day.
terms_mx['capacity_bpd'] = terms_mx['nominal_ca2'] / 365
terms_mx['throughput_bpd'] = terms_mx['operative_ca2'] / 365


# =============================================================================
# %% PETROLEUM TERMINALS - Integration + Export
# =============================================================================
terms_mx_integrated, terms_mx_err = integrate_facs(
    terms_mx.reset_index(drop=True),
    starting_ids=1,
    category="Petroleum terminals",
    fac_alias="LNG_STORAGE",
    country="Mexico",
    state_prov=None,
    src_ref_id="227",  # UPDATED
    src_date="2018-09-28",   # UPDATED
    on_offshore=None,
    fac_name="name",
    fac_id=None,
    fac_type=None,
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity=None,
    liq_capacity_bpd="capacity_bpd",
    liq_throughput_bpd=None,
    gas_capacity_mmcfd="throughput_bpd",
    gas_throughput_mmcfd=None,
    num_compr_units=None,
    num_storage_tanks=None,
    site_hp=None,
    fac_latitude="latitude_calc",
    fac_longitude="longitude_calc"
)


save_spatial_data(
    terms_mx_integrated,
    file_name="mexico_petroleum_terminals",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% STATIONS, OTHER
# =============================================================================
# Subset the Facilities file to just other-type station records
stations_list = [
    'ESTACION DE CALENTAMIENTO',
    'ESTACION SATELITE',
    'ESTACION DE MEDICION',
]

stations_other = fac_data_mx.query("type == @stations_list")

# Translate facility type
dict_stations_other = {
    'ESTACION DE CALENTAMIENTO': 'HEATING STATION',
    'ESTACION SATELITE': 'SATELLITE STATION',
    'ESTACION DE MEDICION': 'METERING STATION',
}

stations_other = replace_row_names(stations_other,
                                   colName="type",
                                   dict_names=dict_stations_other)


# =============================================================================
# %%  STATIONS, OTHER - Integration + Export
# =============================================================================
stations_other_integrated, stations_other_err = integrate_facs(
    stations_other,
    starting_ids=1,
    category="Stations - Other",
    fac_alias="OTHER",
    country="Mexico",
    state_prov=None,
    src_ref_id="225",  # UPDATED
    src_date="2022-07-18",  # UPDATED
    on_offshore=None,
    fac_name="facility_n",
    fac_id=None,
    fac_type="type",
    install_date=None,
    fac_status="condition",
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
    stations_other_integrated,
    "mexico_stations_other",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% TANK BATTERIES
# =============================================================================
# Subset the Facilities file to just tank battery records
bat_ = [
    'BATERIA DE SEPARACION',
    'BATERIA',
    'BATERIA DE SEPARACION DE ACEITE',
    'TANQUE DE ALMACENAMIENTO'
]
batts = fac_data_mx.query("type == @bat_")

# Translate battery type
dict_batt = {
    'BATERIA DE SEPARACION': 'Separation battery',
    'BATERIA': 'Battery',
    'BATERIA DE SEPARACION DE ACEITE': 'Oil separation battery',
    'TANQUE DE ALMACENAMIENTO': 'STORAGE TANK'
}

batts = replace_row_names(batts, colName='type', dict_names=dict_batt)

# =============================================================================
# %% TANK BATTERIES - Integration + Export
# =============================================================================
batts_integrated, batts_err = integrate_facs(
    batts,
    starting_ids=1,
    category="Tank batteries",
    fac_alias="LNG_STORAGE",
    country="Mexico",
    state_prov=None,
    src_ref_id="225",  # UPDATED
    src_date="2022-07-18",  # UPDATED
    on_offshore=None,
    fac_name="facility_n",
    fac_id=None,
    fac_type="facility_t",
    install_date=None,
    fac_status="condition",
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
    batts_integrated,
    file_name="mexico_battery",
    schema=schema_LNG_STORAGE,
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% PIPELINES (E&P)
# =============================================================================
pipes = read_spatial_data("pipelines//Pipelines.shp")
pipes = calculate_pipeline_length_km(pipes, attrName='PIPELINE_LENGTH_KM')

# translate pipeline type (variable is named "service")
dict_type = {
    'OLEOGASODUCTO': 'OIL AND GAS PIPELINE',
    'OLEODUCTO': 'OIL PIPELINE',
    'GASODUCTO': 'GAS PIPELINE',
    'LINEA DE DESCARGA': 'DISCHARGE LINE',
    'LINEA DE BOMBEO NEUMATICO': 'PNEUMATIC PUMPING LINE',
    'NITROGENODUCTO': 'NITROGEN PIPELINE',
    'GASOLINODUCTO': 'GASOLINE PIPELINE',
    'SALODUCTO': 'SALT WATER PIPELINE',
    'LINEA DE INYECCION DE AGUA': 'WATER INJECTION LINE',
    'ACUMULADOR DE LIQUIDOS': 'LIQUID GATHERING',
    'ACUEDUCTO': 'AQUADUCT'  # TODO - should this be water pipeline?
}
pipes = replace_row_names(pipes, colName="service", dict_names=dict_type)


# Translate status (status variable is named "pipeline_s")
dict_st = {
    'OPERANDO': 'Operating',
    'DESCONOCIDO': 'N/A',
    'UNKNOWN': 'N/A',
    'FUERA DE OPERACION TEMPORAL': 'TEMPORARILY CLOSED',
    'FUERA DE OPERACION': 'INACTIVE',
    'FUERA DE OPERACION DEFINITIVA': 'NON-OPERATIONAL'
}
pipes = replace_row_names(pipes, colName="pipeline_s", dict_names=dict_st)


# Pipe material (variable is named "pipe_type")
dict_mat = {
    'POLIPROPILENO': 'POLYPROPYLENE',
    'ACERO AL CARBON': 'Carbon steel',
    'ACERO AL CARBONO': 'Carbon steel',   # TODO - is this right?
    'SIN DATO': 'N/A',
    'FLEXIBLE CON REFUERZO DE FIBRA': 'Flexible with fiber reinforcement',
    'FLEXIBLE CON REFUERZO METALICO': 'Flexible with metallic reinforcement',
    'POLIETILENO ALTA DENSIDAD': 'High density polyethylene',
    'FIBRA DE VIDRIO': 'FIBERGLASS'
}
pipes = replace_row_names(pipes, colName="pipe_type", dict_names=dict_mat)


# Translate the hydrocarbon commodity in the pipeline
# NOTE that some of the original Spanish terms are specific grades of crude oil
# See https://sie.energia.gob.mx/docs/glosario_hc_en.pdf for examples
dict_comm = {
    'MULTIFASICO': 'MULTIPHASE',
    'GAS': 'GAS',
    'DESCONOCIDO': 'N/A',
    'ACEITE Y GAS': 'Oil and gas',
    'ACEITE Y AGUA': 'Oil and water',
    'ACEITE': 'OIL',
    'ACEITE, GAS Y AGUA': 'Oil, gas and water',
    'ACEITE PESADO': 'Heavy oil',
    'DULCE HUMEDO': 'Sweet and wet',
    'GAS DULCE': 'Sweet gas',
    'HUMEDO DULCE': 'Wet and sweet',
    'GAS HUMEDO DULCE': 'Wet and sweet gas',
    'DULCE SECO': 'Dry and sweet',
    'GAS SECO': 'Dry gas',
    'GAS SECO DULCE': 'Dry and sweet gas',
    'GAS AMARGO': 'Sour gas',
    'GAS HUMEDO AMARGO': 'Wet and sour gas',
    'NO APLICA': 'N/A',
    'POZOLEO': 'Pozoleo crude oil',  # specific grade of crude oil
    'SECO': 'DRIED',
    'CONDENSADO': 'CONDENSATE',
    'GAS COMBUSTIBLE': 'FUEL GAS',
    'AGUA CONGENITA': 'CONGENITAL WATER',
    'HUMEDO': 'WET',
    'MAYA': 'Maya crude oil',  # specific grade of crude oil
    'AGUA DE SERVICIO': 'Service water',
    'MARFO': 'Marfo',  # Is this right?
    'BOMBEO NEUMATICO': 'Pneumatic pumping',
    'ISTMO': 'ISTHMUS CRUDE OIL',  # specific grade of crude oil
    'ACEITE LIGERO': 'Light oil',
    'OLMECA': 'Olmeca crude oil',  # specific grade of crude oil
    'NITROGENO': 'NITROGEN',  # Is this right?
    'TERCIARIO': 'TERTIARY',   # Is this right?
    'GASOLINA': 'GASOLINE',
    'ALAMO': 'ALAMO CRUDE OIL',  # crude oil from Alamo San Isidro field
    'MURO': 'MURO CRUDE OIL',     # crude oil from the Muro field
    'VAPORES AMARGOS': 'Sour vapors',
    None: 'N/A',
    'SIN DATO': 'N/A'
}
pipes = replace_row_names(pipes, colName="hydrocarbo", dict_names=dict_comm)


# Replace nodata values in the facility ID field
vals2replace = ['SIN DATO', 'ND', 'NO APLICA']
pipes.loc[pipes.code.isin(vals2replace), 'code'] = 'N/A'


# Convert external diameter from inches to mm
inches_to_mm = 25.4
pipes['external_diam_mm'] = pipes['external_d'] * inches_to_mm


# =============================================================================
# %% PIPELINES (E&P) - Integration + Export
# =============================================================================
pipes_integrated, err_pipes = integrate_pipelines(
    pipes,
    starting_ids=1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Mexico",
    state_prov=None,
    src_ref_id="228",
    src_date="2024-04-19",  # Updated irregularly
    on_offshore=None,
    fac_name="name",
    fac_id='code',
    fac_type='service',
    install_date=None,
    fac_status="pipeline_s",
    op_name=None,
    commodity="hydrocarbo",
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm="external_diam_mm",
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material="pipe_type"
)

save_spatial_data(
    pipes_integrated,
    "mexico_oil_gas_pipelines_upstream",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)


# =============================================================================
# %% PIPELINES (Fuel pipeline)
# =============================================================================
# !!! NOTE: On the English translated version of CNIH's website, they call this
# layer "Fuel pipeline." The name in Spanish is Ductor Petroliferos, which
# actually means oil pipelines?
# -----------------------------------------------------------------------------
oil_pipe = read_spatial_data("pipelines//Fuel pipeline.shp")
oil_pipe = calculate_pipeline_length_km(oil_pipe,
                                        attrName='PIPELINE_LENGTH_KM')

# Calculate capacity in bpd
oil_pipe['capacity_bpd'] = oil_pipe['nominal_ca'] / 365
oil_pipe['throughput_bpd'] = oil_pipe['operative_'] / 365


# translate pipeline type (variable is named "service")
dict_type = {
    'Poliducto': 'MULTI PURPOSE PIPELINE',
    'Combustóleo-ducto': 'FUEL OIL PIPELINE',
    'Turbosino-ducto': 'JET FUEL PIPELINE',
    'Magna-ducto': 'MAGNA GASOLINE (87 OCTANE) PIPELINE',
    'Premium-ducto': 'PREMIUM GASOLINE (92 OCTANE) PIPELINE',
    'Diésel-ducto': 'DIESEL PIPELINE',
}
oil_pipe = replace_row_names(oil_pipe, colName="service", dict_names=dict_type)


# Populate a "commodity" column based on info in the "service" column
oil_pipe['commodity'] = 'N/A'
oil_pipe.loc[oil_pipe.service.str.contains('FUEL OIL'), 'commodity'] = 'FUEL OIL'
oil_pipe.loc[oil_pipe.service.str.contains('JET FUEL'), 'commodity'] = 'JET FUEL'
oil_pipe.loc[oil_pipe.service.str.contains('MAGNA'), 'commodity'] = 'MAGNA GASOLINE (87 OCTANE)'
oil_pipe.loc[oil_pipe.service.str.contains('PREMIUM'), 'commodity'] = 'PREMIUM GASOLINE (92 OCTANE)'
oil_pipe.loc[oil_pipe.service.str.contains('DIESEL'), 'commodity'] = 'DIESEL'

# oil_pipe['type_'] = "Oil transportation pipeline"


# =============================================================================
# %% PIPELINES (Fuel pipeline) - Integration + Export
# =============================================================================
oil_pipe_integrated, err_oil_pipe = integrate_pipelines(
    oil_pipe,
    starting_ids=int(pipes_integrated.OGIM_ID.iloc[-1]) + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Mexico",
    state_prov=None,
    src_ref_id="229",  # UPDATED
    src_date="2021-07-29",  # UPDATED
    on_offshore=None,
    fac_name="name",
    fac_id=None,
    fac_type="service",
    install_date=None,
    fac_status=None,
    op_name=None,
    commodity='commodity',
    liq_capacity_bpd='capacity_bpd',
    liq_throughput_bpd='throughput_bpd',
    gas_capacity_mmcfd=None,
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

save_spatial_data(
    oil_pipe_integrated,
    "mexico_oil_natural_gas_pipelines_oil_transport",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% PIPELINES (SISTRANGAS; gas transportation pipelines)
# =============================================================================
sis_int = read_spatial_data('pipelines//SISTRANGAS Integrated.shp')
sis_noint = read_spatial_data('pipelines//SISTRANGAS Not Integrated.shp')
# Change some column names so they are properly concatenated
sis_int = sis_int.rename(columns={"starting_o": "start_year",
                                  "cre_licens": "license_cr"},
                         errors="raise")

# Add SRC_REF_ID for each source
sis_int['src'] = '230'
sis_noint['src'] = '231'

# Concatenate both shapefiles together
sistrangas = pd.concat([sis_int, sis_noint]).reset_index()

# Rename these fields for convenience, since "type" and "project" are
# python/pandas functions and it's confusing otherwise.
# I confirmed the proper capacity units on Mexico's website
sistrangas = sistrangas.rename(columns={"type": "type_",
                                        "project": "project_",
                                        'capacity_m': 'capacity_mmcfd'},
                               errors="raise")

# Fill in missing operator information
sistrangas['operator'] = sistrangas['operator'].fillna('N/A')
sistrangas.loc[sistrangas.type_ == 'Gasoductos existentes operados por CENAGAS', 'operator'] = 'CENAGAS'

# Create a status field, based on information in other attribute columns
sistrangas['status'] = 'N/A'
sistrangas.loc[sistrangas.type_ == 'Gasoductos existentes operados por CENAGAS', 'status'] = 'OPERATING'
sistrangas.loc[sistrangas.type_ == 'Gasoductos en construcción', 'status'] = 'UNDER CONSTRUCTION'
sistrangas.loc[sistrangas.type_ == 'Proyecto de nuevos gasoductos', 'status'] = 'PROPOSED'

# Create more detailed `name` field that includes `project` information if it's available
sistrangas['namenew'] = sistrangas.name
sistrangas.loc[sistrangas.project_.notna(), 'namenew'] = sistrangas.name + ' (' + sistrangas.project_ + ')'


# Populate other columns we want in the final product
sistrangas['factype'] = 'Gas pipeline'
sistrangas['commodity_'] = 'Gas'
sistrangas = calculate_pipeline_length_km(sistrangas, attrName='PIPELINE_LENGTH_KM')


# Create an INSTALL_DATE column based on "start_year" info
# -----------------------------------------------------------------------------
sistrangas.start_year = sistrangas.start_year.astype(str)

# replace '2° Semestre de 1999' with '1999'
sistrangas.loc[sistrangas.start_year == '2° Semestre de 1999', 'start_year'] = '1999'

# If `start_year` contains a range of years, just keep the last four characters
# aka, the year listed last
sistrangas.loc[sistrangas.start_year.str.contains('-'), 'start_year'] = sistrangas.start_year.str[-4:]

# Fill in no-data years
sistrangas.start_year.replace({None: '1900-01-01',
                               'None': '1900-01-01'},
                              inplace=True)

# if it's four characters in length, then make the new date the existing year
# information with "-01-01" appended
sistrangas.loc[sistrangas.start_year.str.len() == 4, 'start_year'] = sistrangas.start_year + '-01-01'

# Dictionary for switching Spanish months to English months
monthSpanishToEnglish = {'Enero': 'January',
                         'Febrero': 'February',
                         'Marzo': 'March',
                         'Abril': 'April',
                         'Mayo': 'May',
                         'Junio': 'June',
                         'Julio': 'July',
                         'Agosto': 'August',
                         'Septiembre': 'September',
                         'Octubre': 'October',
                         'Noviembre': 'November',
                         'Diciembre': 'December'
                         }
# Replace occurences of Spanish abbrevs within the strings with English abbrevs
for old, new in monthSpanishToEnglish.items():
    sistrangas.start_year = sistrangas.start_year.str.replace(old, new)

# FIXME
sistrangas['install_date_datetime'] = pd.to_datetime(sistrangas['start_year'],
                                                     infer_datetime_format=True,
                                                     errors='coerce')
sistrangas['install_date_str'] = sistrangas['install_date_datetime'].dt.strftime("%Y-%m-%d")
# =============================================================================
# %% PIPELINES (SISTRANGAS) - Integration + Export
# =============================================================================
sistrangas_integrated, err_sistrangas = integrate_pipelines(
    sistrangas,
    starting_ids=int(oil_pipe_integrated.OGIM_ID.iloc[-1]) + 1,
    category="Oil and natural gas pipelines",
    fac_alias="PIPELINES",
    country="Mexico",
    state_prov=None,
    src_ref_id="src",  # UPDATED
    src_date="2021-07-29",
    on_offshore=None,
    fac_name="namenew",
    fac_id='license_cr',
    fac_type="factype",
    install_date='install_date_str',
    fac_status='status',
    op_name='operator',
    commodity='commodity_',
    liq_capacity_bpd=None,
    liq_throughput_bpd=None,
    gas_capacity_mmcfd='capacity_mmcfd',
    gas_throughput_mmcfd=None,
    pipe_diameter_mm=None,
    pipe_length_km="PIPELINE_LENGTH_KM",
    pipe_material=None
)

save_spatial_data(
    sistrangas_integrated,
    "mexico_oil_natural_gas_pipelines_gas_transport",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)

# =============================================================================
# %% FIELDS AND BASINS
# =============================================================================
# Read in Areas With Resources
rec_areas = read_spatial_data("fields//Areas with Resources.shp")

# Translate `location` values to mean offshore or onshore
rec_areas = replace_row_names(rec_areas,
                              colName='location',
                              dict_names={'Terrestre': 'Onshore',
                                          'Marino': 'Offshore'})

rec_areas = calculate_basin_area_km2(rec_areas, attrName="area_km2_new")

rec_areas['type_'] = "Areas with resources"


# -----------------------------------------------------------------------------
# Read in Reserves fields
reserves = read_spatial_data("fields//Reserves Fields (01-01-2024).shp")

# Define onshore and offshore
reserves = replace_row_names(reserves,
                             colName='location',
                             dict_names={'Terrestre': 'Onshore',
                                         'Marino': 'Offshore'})

reserves = calculate_basin_area_km2(reserves, attrName="area_km2_new")

reserves['type_'] = "Reserves fields"


# =============================================================================
# %% FIELDS AND BASINS - Integration + Export
# =============================================================================
# Integrate Areas With Resources
rec_areas_integrated, rec_err = integrate_basins(
    rec_areas,
    starting_ids=1,
    category="Oil and gas fields",
    fac_alias='OIL_GAS_BASINS',
    country="Mexico",
    state_prov=None,
    src_ref_id="232",
    src_date="2023-05-18",  # Irregularly updated
    on_offshore="location",
    _name="name",
    reservoir_type="type_",
    op_name=None,
    _area_km2="area_km2_new"
)

# Integrate Reserves Fields
reserves_integrated, res_err = integrate_basins(
    reserves,
    starting_ids=int(rec_areas_integrated.OGIM_ID.iloc[-1]) + 1,
    category="Oil and gas fields",
    fac_alias='OIL_GAS_BASINS',
    country="Mexico",
    state_prov=None,
    src_ref_id="233",
    src_date="2024-01-15",  # Annually
    on_offshore="location",
    _name="name",
    reservoir_type='type_',
    op_name=None,
    _area_km2="area_km2_new"
)

# Combine the two datasets
fields_mx = pd.concat([rec_areas_integrated,
                       reserves_integrated]).reset_index(drop=True)

save_spatial_data(
    fields_mx,
    "mexico_oil_gas_fields",
    schema_def=True,
    file_type="GeoJSON",
    out_path=integration_out_path
)
