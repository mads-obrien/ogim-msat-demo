# -*- coding: utf-8 -*-
"""
Created on Sun Sep  4 18:57:47 2022

@author: maobrien
"""

import numpy as np
import pandas as pd
from tqdm import tqdm
import pprint
from sigfig import round

def check_invalid_geoms(
    gdf, 
    id_attr=None
    ):
    
    """Checks for any `None` or `inf` geometries in gdf
    
    Inputs:
    ---
        gdf: GeoDataFrame with geometry attributes
        id_attr: is the attribute name--OBJECT_ID or OGIM_ID that would be used to index the null features
        
    Returns:
    ---
        nulls_list: A list of the IDs where geometries are invalid, based on `id_attr`
        gdf_nulls: A geodataframe of all the invalid geometries
        
    Dependencies:
    ---
        geopandas
        tqdm
        shapely
    """
    
    nulls_list = []
    for idx_, row_ in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        if row_.geometry is None:
            nulls_list.append(row_[id_attr])
        elif "inf" in row_.geometry.wkt:
            nulls_list.append(row_[id_attr])
        else:
            pass
    print ("=====================")
    print ("Number of features with INVALID geometries = ", len(nulls_list))
    print ("=====================")
    
    # Return IDs and gdf of nulls
    gdf_nulls = gdf[gdf[id_attr].isin(nulls_list)]
    
    # display(gdf_nulls.head())

    return nulls_list, gdf_nulls



def data_quality_checks(gdf, starting_ogim_id=1, check_attributes=True):
    """Check for consistency in:
        data types [OGIM_ID, SPUD_DATE, INSTALL_DATE, COMP_DATE]
        latitude, longitude significant figure digits
    """
    # Check OGIM_ID to make sure format is int
    gdf['OGIM_ID'] = gdf['OGIM_ID'].astype(int)
    
    # Facility ID type
    # =====================================================================
    try:
        gdf['FAC_ID'] = gdf['FAC_ID'].astype(str)
        unique_ids = gdf['FAC_ID'].unique()
    
        if 'UNKNOWN' in unique_ids or 'NOT AVAILABLE' in unique_ids or None in unique_ids:
            gdf = gdf.replace({'FAC_ID': {'UNKNOWN':'N/A', 'NOT AVAILABLE': 'N/A', None: 'N/A'}})
    except:
        pass
        
    # =====================================================================
    # FAC STATUS
    try:
        unique_status = gdf.FAC_STATUS.unique()
    
        if 'UNKNOWN' in unique_status or 'NOT AVAILABLE' in unique_status or "NA" in unique_status or None in unique_status or "NAN" in unique_status:
            gdf = gdf.replace({'FAC_STATUS': {'UNKNOWN':'N/A', 'NOT AVAILABLE': 'N/A', "NA": "N/A", None: 'N/A', 'NAN': 'N/A'}})
    except:
        pass
        
    # =====================================================================
    # OPERATOR
    try:
        unique_operator = gdf.OPERATOR.unique()
    
        if 'UNKNOWN' in unique_operator or 'NOT AVAILABLE' in unique_operator or None in unique_operator:
            gdf = gdf.replace({'OPERATOR': {'UNKNOWN':'N/A', 'NOT AVAILABLE': 'N/A', None: 'N/A'}})
    except:
        pass
    
    # =====================================================================
    # COMMODITY
    try:
        unique_commodity = gdf.COMMODITY.unique()
    
        if 'UNKNOWN' in unique_commodity or 'NOT AVAILABLE' in unique_commodity or None in unique_commodity or "NAN" in unique_commodity:
            gdf = gdf.replace({'COMMODITY': {'UNKNOWN':'N/A', 'NOT AVAILABLE': 'N/A', None: 'N/A', 'NAN': 'N/A'}})
    except:
        pass
        
    # SPUD DATE
    # =====================================================================
    try:
        unique_spud = gdf.SPUD_DATE.unique()
        if 'UNKNOWN' in unique_spud or 'NOT AVAILABLE' in unique_spud or "1800-01-01" in unique_spud or "NA" in unique_spud or "1901-01-01" in unique_spud or None in unique_spud:
            gdf = gdf.replace({'SPUD_DATE': {'UNKNOWN':'1900-01-01', 'NOT AVAILABLE': '1900-01-01', "1800-01-01": "1900-01-01", "NA": "1900-01-01", None: '1900-01-01'}})
    except:
        pass
    
    # COMPLETION DATE
    # =====================================================================
    try:
        unique_comp = gdf.COMP_DATE.unique()
        if 'UNKNOWN' in unique_comp or 'NOT AVAILABLE' in unique_comp or "1800-01-01" in unique_comp or "NA" in unique_comp or "1901-01-01" in unique_comp or None in unique_comp:
            gdf = gdf.replace({'COMP_DATE': {'UNKNOWN':'1900-01-01', 'NOT AVAILABLE': '1900-01-01', "1800-01-01": "1900-01-01", "NA": "1900-01-01", None: '1900-01-01'}})
    except:
        pass
    
    # INSTALLATION DATE
    # =====================================================================
    try:
        unique_instl = gdf.INSTALL_DATE.unique()
        if 'UNKNOWN' in unique_instl or 'NOT AVAILABLE' in unique_instl or "1800-01-01" in unique_instl or "NA" in unique_instl or "1901-01-01" in unique_instl or None in unique_instl:
            gdf = gdf.replace({'COMP_DATE': {'UNKNOWN':'1900-01-01', 'NOT AVAILABLE': '1900-01-01', "1800-01-01": "1900-01-01", "NA": "1900-01-01", None: '1900-01-01'}})
    except:
        pass

    # LIQ_CAPACITY_BPD
    # =====================================================================
    try:
        unique_liq_capacity = gdf.LIQ_CAPACITY_BPD.unique()
        if 9999 in unique_liq_capacity or 999 in unique_liq_capacity or "9999" in unique_liq_capacity or "999" in unique_liq_capacity or "-999" in unique_liq_capacity or None in unique_liq_capacity:
            gdf = gdf.replace({'LIQ_CAPACITY_BPD': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass
    
    # LIQ_THROUGHPUT_BPD
    # =====================================================================
    try:
        unique_liq_thru = gdf.LIQ_THROUGHPUT_BPD.unique()
        if 9999 in unique_liq_thru or 999 in unique_liq_thru or "9999" in unique_liq_thru or "999" in unique_liq_thru or "-999" in unique_liq_thru or None in unique_liq_thru:
            gdf = gdf.replace({'LIQ_THROUGHPUT_BPD': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass
    
    # GAS_CAPACITY_MMCFD
    # =====================================================================
    try:
        unique_gas_cap = gdf.GAS_CAPACITY_MMCFD.unique()
        if 9999 in unique_gas_cap or 999 in unique_gas_cap or "9999" in unique_gas_cap or "999" in unique_gas_cap or "-999" in unique_gas_cap or None in unique_gas_cap:
            gdf = gdf.replace({'GAS_CAPACITY_MMCFD': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass
    
    # GAS_THROUGHPUT_MMCFD
    # =====================================================================
    try:
        unique_gas_thru = gdf.GAS_THROUGHPUT_MMCFD.unique()
        if 9999 in unique_gas_thru or 999 in unique_gas_thru or "9999" in unique_gas_thru or "999" in unique_gas_thru or "-999" in unique_gas_thru or None in unique_gas_thru:
            gdf = gdf.replace({'GAS_THROUGHPUT_MMCFD': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass
    
    # NUM_COMPR_UNITS
    # =====================================================================
    try:
        unique_num_compr = gdf.NUM_COMPR_UNITS.unique()
        if 9999 in unique_num_compr or 999 in unique_num_compr or "9999" in unique_num_compr or "999" in unique_num_compr or "-999" in unique_num_compr or None in unique_num_compr:
            gdf = gdf.replace({'NUM_COMPR_UNITS': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass
    
    # SITE_HP
    # =====================================================================
    try:
        unique_site_hp = gdf.SITE_HP.unique()
        if 9999 in unique_site_hp or 999 in unique_site_hp or "9999" in unique_site_hp or "999" in unique_site_hp or "-999" in unique_site_hp or None in unique_site_hp:
            gdf = gdf.replace({'SITE_HP': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass
    
    # NUM_STORAGE_TANKS
    # =====================================================================
    try:
        unique_num_stor = gdf.NUM_STORAGE_TANKS.unique()
        if 9999 in unique_num_stor or 999 in unique_num_stor or "9999" in unique_num_stor or "999" in unique_num_stor or "-999" in unique_num_stor or None in unique_num_stor:
            gdf = gdf.replace({'NUM_STORAGE_TANKS': {9999:-999, '9999':-999, "999":-999, "-999":-999, 999:-999, None: -999}})
    except:
        pass

    # Check if there are NULL geometries in dataset
    gdf_list_null_ids, _ = check_invalid_geoms(gdf, id_attr='OGIM_ID')
    
    if len(gdf_list_null_ids) >= 1:
        print("!!!There are INVALID geometries in dataset!!! \n ===>Removing INVALID records <===")
        gdf2 = gdf[~gdf.OGIM_ID.isin(gdf_list_null_ids)]
    else:
        gdf2 = gdf
    
    # FIX LATITUDE AND LONGITUDE
    
    if "LATITUDE" in gdf2.columns:
        print("===================================")
        # Ensure that LATITUDE and LONGITUDE columns are of type FLOAT
        gdf2['LATITUDE'] = gdf2['LATITUDE'].astype(float)
        gdf2['LONGITUDE'] = gdf2['LONGITUDE'].astype(float)
        print("Now, standardizing `LATITUDE` and `LONGITUDE` columns")
        gdf2['LATITUDE'] = np.around(gdf2['LATITUDE'], decimals=5)
        gdf2['LONGITUDE'] = np.around(gdf2['LONGITUDE'], decimals=5)

    # Standardize pipe length and pipe diameter
    if "PIPE_LENGTH_KM" in gdf2.columns:
        print("===================================")
        print("Now, standardizing PIPELINE attributes (length, diameter)")
        gdf2['PIPE_LENGTH_KM'] = gdf2['PIPE_LENGTH_KM'].map(lambda x: round(x, sigfigs=3))
        gdf2['PIPE_DIAMETER_MM'] = gdf2['PIPE_DIAMETER_MM'].map(lambda x: round(x, sigfigs=3))

    # Standardize basin area
    if "AREA_KM2" in gdf2.columns:
        gdf2['AREA_KM2'] = gdf2['AREA_KM2'].map(lambda x: round(x, sigfigs=3))

    # Standardize PRODUCTION metrics
    if "OIL_BBL" in gdf2.columns:
        print("===================================")
        print("Now, standardizing PRODUCTION metrics")
        if gdf2.OIL_BBL.dtype == object or gdf2.WATER_BBL.dtype == object or gdf2.CONDENSATE_BBL.dtype == object or gdf2.GAS_MCF.dtype == object or gdf2.PROD_DAYS.dtype == object:

            gdf2 = gdf2.astype({"OIL_BBL": float,
                                "WATER_BBL": float,
                                "CONDENSATE_BBL": float,
                                "GAS_MCF": float,
                                'PROD_DAYS': float})

        gdf2['OIL_BBL'] = gdf2['OIL_BBL'].map(lambda x: round(x, sigfigs=3))
        gdf2['WATER_BBL'] = gdf2['WATER_BBL'].map(lambda x: round(x, sigfigs=3))
        gdf2['CONDENSATE_BBL'] = gdf2['CONDENSATE_BBL'].map(lambda x: round(x, sigfigs=3))
        gdf2['GAS_MCF'] = gdf2['GAS_MCF'].map(lambda x: round(x, sigfigs=3))
        gdf2['PROD_DAYS'] = gdf2['PROD_DAYS'].map(lambda x: round(x, sigfigs=3))

    # ======================================================================
    # Fix OGIM ID
    gdf2['OGIM_ID'] = np.arange(starting_ogim_id, starting_ogim_id+gdf2.shape[0])
    
    # Standardize data types
    # ======================================================================
    try:
        gdf2['LIQ_CAPACITY_BPD'] = pd.to_numeric(gdf2['LIQ_CAPACITY_BPD'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['LIQ_THROUGHPUT_BPD'] = pd.to_numeric(gdf2['LIQ_THROUGHPUT_BPD'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['GAS_CAPACITY_MMCFD'] = pd.to_numeric(gdf2['GAS_CAPACITY_MMCFD'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['GAS_THROUGHPUT_MMCFD'] = pd.to_numeric(gdf2['GAS_THROUGHPUT_MMCFD'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['NUM_COMPR_UNITS'] = pd.to_numeric(gdf2['NUM_COMPR_UNITS'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['SITE_HP'] = pd.to_numeric(gdf2['SITE_HP'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['NUM_STORAGE_TANKS'] = pd.to_numeric(gdf2['NUM_STORAGE_TANKS'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['PIPE_LENGTH_KM'] = pd.to_numeric(gdf2['PIPE_LENGTH_KM'])
    except:
        pass
    
    # ======================================================================
    try:
        gdf2['PIPE_DIAMETER_MM'] = pd.to_numeric(gdf2['PIPE_DIAMETER_MM'])
    except:
        pass
    
    # ======================================================================
    try:
        gdf2['AREA_KM2'] = pd.to_numeric(gdf2['AREA_KM2'])
    except:
        pass
    # ======================================================================
    try:
        gdf2['PROD_YEAR'] = pd.to_numeric(gdf2['PROD_YEAR'])
    except:
        pass
    # ======================================================================
    # Check unique attributes
    if check_attributes == True:
        for idx, attribute in enumerate(list(gdf2.columns)):
            if attribute != 'geometry':
                print("==================================")
                print("{}.---> {} <----".format(idx, attribute))
                pprint.pprint(gdf2[attribute].unique())
            
    return gdf2