# -*- coding: utf-8 -*-
"""
Created on Mon Aug  1 14:46:07 2022

@author: maobrien
"""
from tqdm import tqdm
import pandas as pd
from datetime import date

def attribute_score_wells(gdf, analysis_mode=False):
    """Assign data quality score to each record 
    
    Assign data quality score to each record in the wells database.
        For each well record, we assign a score based on whether the following 
        attributes have data available:
        FAC_NAME, OPERATOR, FAC_STATUS, (SPUD_DATE, COMP_DATE), FAC_TYPE, DRILL_TYPE
        Highest possible ATTRIBUTE_SCORE is 6.
        
    Inputs:
    ---
        gdf: GeoDataFrame of wells, from OGIM database
        analysis_mode: boolean
            If this flag is set to True, additional columns are created in
            the returned `attribute_score_gped` table. These columns, ending in
            '_pct', report the percent of the records in a given state/country 
            that contain a non-null value for the associated attribute.
        
    Returns:
    ---
        gdf: Same GeoDataFrame with a new attribute ["ATTRIBUTE_SCORE"] for each record 
        attribute_score_gped: A DataFrame that lists the mean ATTRIBUTE_SCORE 
            for each COUNTRY and STATE_PROVINCE in the input gdf
        
    Dependecies:
    ---
        tqdm
        
    Example Usage:
    ---
        data_wells_scored, attribute_score_gped_wells = attribute_score_wells(data_wells, analysis_mode=True)
        
    """
    
    all_scores = []
    
    if analysis_mode == True:
        # Add columns to contain 'presence/absence' of that attribute for that record
        gdf['FAC_NAME_'] = 0
        gdf['OPERATOR_'] = 0
        gdf['FAC_STATUS_'] = 0
        gdf['SPUD_COMP_'] = 0
        gdf['FAC_TYPE_'] = 0
        gdf['DRILL_TYPE_'] = 0

    for idx1, row1 in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        fac_name = row1.FAC_NAME # Well or Facility name
        oper = row1.OPERATOR # Operator info
        status = row1.FAC_STATUS # Facility status
        install1 = row1.SPUD_DATE # Spud date
        install2 = row1.COMP_DATE # Completion date
        fac_type = row1.FAC_TYPE # Facility type
        drill_type = row1.DRILL_TYPE # Drill type
    
        # Fac names
        if fac_name == "N/A" or fac_name == "UNKNOWN" or fac_name == "NOT AVAILABLE" or fac_name == " " or fac_name == "":
            score1 = 0
        else:
            score1 = 1
            if analysis_mode == True:
                gdf.at[idx1,'FAC_NAME_'] = 1
    
        # Operator
        if oper == "N/A" or oper == "UNKNOWN" or oper == "NOT AVAILABLE" or oper == " " or oper == "":
            score2 = 0
        else:
            score2 = 1
            if analysis_mode == True:
                gdf.at[idx1,'OPERATOR_'] = 1
        
        # Spud dates and comp dates
        if install1 == "1900-01-01" and install2 == "1900-01-01":
            score3 = 0
        elif install1 == "1900-01-01" and install2 != "1900-01-01":
            score3 = 1
        elif install1 != "1900-01-01" and install2 == "1900-01-01":
            score3 = 1
        elif install1 != "1900-01-01" and install2 != "1900-01-01":
            score3 = 1
        elif install1 != "1900-01-01" or install2 != "1900-01-01":
            score3 = 1
        if analysis_mode == True and score3 == 1:
            gdf.at[idx1,'SPUD_COMP_'] = 1
        
        # Fac types
        if fac_type == "N/A" or fac_type == "UNKNOWN" or fac_type == "NOT AVAILABLE" or fac_type == " " or fac_type == "":
            score4 = 0
        else:
            score4 = 1
            if analysis_mode == True:
                gdf.at[idx1,'FAC_TYPE_'] = 1
        
        # Status
        if status == "N/A" or status == "UNKNOWN" or status == "NOT AVAILABLE" or status == " " or status == "":
            score5 = 0
        else:
            score5 = 1
            if analysis_mode == True:
                gdf.at[idx1,'FAC_STATUS_'] = 1
        
        # Drill type
        if drill_type == "N/A" or drill_type == "UNKNOWN" or drill_type == "NOT AVAILABLE" or drill_type == " " or drill_type == "":
            score6 = 0
        else:
            score6 = 1
            if analysis_mode == True:
                gdf.at[idx1,'DRILL_TYPE_'] = 1
        
        # Total
        total_score = score1 + score2 + score3 + score4 + score5 + score6
    
        all_scores.append(total_score)
    
    # Append to data
    gdf["ATTRIBUTE_SCORE"] = all_scores

    if analysis_mode == False:
        # Group attribute richness score by country AND STATE/PROVINCE
        attribute_score_gped = gdf.groupby(by=["COUNTRY", "STATE_PROV"]).agg({"OGIM_ID":"count", 
                                                                              "ATTRIBUTE_SCORE":"mean"})

    else:
        attribute_score_gped = gdf.groupby(by=["COUNTRY", "STATE_PROV"]).agg({"OGIM_ID":"count", 
                                                                              "ATTRIBUTE_SCORE":"mean",
                                                                              "FAC_NAME_":"sum",
                                                                              "OPERATOR_":"sum",
                                                                              "FAC_TYPE_":"sum",
                                                                              "FAC_STATUS_":"sum",
                                                                              "DRILL_TYPE_":"sum",
                                                                              "SPUD_COMP_":"sum"})
        
        # Create percentage fields
        attribute_score_gped['FAC_NAME_pct'] = attribute_score_gped['FAC_NAME_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['OPERATOR_pct'] = attribute_score_gped['OPERATOR_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['FAC_STATUS_pct'] = attribute_score_gped['FAC_STATUS_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['SPUD_COMP_pct'] = attribute_score_gped['SPUD_COMP_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['FAC_TYPE_pct'] = attribute_score_gped['FAC_TYPE_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['DRILL_TYPE_pct'] = attribute_score_gped['DRILL_TYPE_'] / attribute_score_gped['OGIM_ID']

        # Drop specific (temporary) columns by name
        attribute_score_gped = attribute_score_gped.drop(['FAC_NAME_','OPERATOR_','FAC_TYPE_','FAC_STATUS_','DRILL_TYPE_','SPUD_COMP_'], axis=1)


    print(attribute_score_gped)
    return gdf, attribute_score_gped


def attribute_score_midstream(gdf, analysis_mode=False):
    """Assign data quality score to each record
    
    Assign data quality score to each record in the following OGIM midstream layers:
        Compressor Stations, Processing Plants, Petroleum Terminals, Refineries, or LNG Facilities.
        For each midstream record, we assign a score based on whether the following 
        attributes have data available:
        FAC_NAME, OPERATOR, FAC_STATUS, (INSTALL_DATE), THROUGHPUT, FAC_TYPE
        Highest possible ATTRIBUTE_SCORE is 6.
        
    Inputs:
    ---
        gdf: GeoDataFrame of facilities, from OGIM database. 
        
    Returns:
    ---
        gdf: Same GeoDataFrame with a new attribute ["ATTRIBUTE_SCORE"] for each record 
        attribute_score_gped: A DataFrame that lists the mean ATTRIBUTE_SCORE 
            for each COUNTRY and STATE_PROVINCE in the input gdf
        
    Dependecies:
    ---
        tqdm
        
    Example Usage:
    ---
        data_midstream_scored, attribute_score_gped_midstream = attribute_score_midstream(data_midstream)
        
        The user can either loop over each infrastructure category with this 
        function to calculate scores, or concatenate all infrastructure 
        categories into one geodataframe before running this function once.
        
    """
    
    
    all_scores = []
    
    if analysis_mode == True:
        # Add columns to contain 'presence/absence' of that attribute for that record
        gdf['FAC_NAME_'] = 0
        gdf['OPERATOR_'] = 0
        gdf['FAC_STATUS_'] = 0
        gdf['INSTALL_DATE_'] = 0
        gdf['FAC_TYPE_'] = 0
        gdf['CAPACITY_'] = 0

    for idx1, row1 in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        fac_name = row1.FAC_NAME # Well or Facility name
        oper = row1.OPERATOR # Operator info
        status = row1.FAC_STATUS # Facility status
        install1 = row1.INSTALL_DATE # Installation date
        cap1 = row1.LIQ_CAPACITY_BPD # Capacity or throughput information
        cap2 = row1.GAS_CAPACITY_MMCFD
        cap3 = row1.GAS_THROUGHPUT_MMCFD
        cap4 = row1.LIQ_THROUGHPUT_BPD 
        fac_type = row1.FAC_TYPE # Facility Type
    
        # Fac names
        if fac_name == "N/A" or fac_name == "UNKNOWN" or fac_name == "NOT AVAILABLE" or fac_name == " " or fac_name == "":
            score1 = 0
        else:
            score1 = 1
            if analysis_mode == True:
                gdf.at[idx1,'FAC_NAME_'] = 1
    
        # Operator
        if oper == "N/A" or oper == "UNKNOWN" or oper == "NOT AVAILABLE" or oper == " " or oper == "":
            score2 = 0
        else:
            score2 = 1
            if analysis_mode == True:
                gdf.at[idx1,'OPERATOR_'] = 1
        
        # Installation dates
        if install1 == "1900-01-01":
            score3 = 0
        else:
            score3 = 1
            if analysis_mode == True:
                gdf.at[idx1,'INSTALL_DATE_'] = 1

        # Status
        if status == "N/A" or status == "UNKNOWN" or status == "NOT AVAILABLE" or status == " " or status == "":
            score4 = 0
        else:
            score4 = 1
            if analysis_mode == True:
                gdf.at[idx1,'FAC_STATUS_'] = 1
            
        # Some information on throughput
        # If either of liq_capacity or gas_capacity or gas_throughput is available, assign a score of 1
        # IF throughput values stored as strings
        if isinstance(cap1, str):
            if any(x != '-999' for x in [cap1, cap2, cap3, cap4]):
                 score5 = 1
            elif cap1 == '-999' and cap2 == '-999' and cap3 == '-999' and cap4 == '-999':
                 score5 = 0   
       # IF throughput values stored as integers     
        if isinstance(cap1, int) or isinstance(cap1, float):
            if any(x != -999 for x in [cap1, cap2, cap3, cap4]):
                score5 = 1
            elif cap1 == -999 and cap2 == -999 and cap3 == -999 and cap4 == -999:
                score5 = 0
        if analysis_mode == True and score5 == 1:
            gdf.at[idx1,'CAPACITY_'] = 1
        
        # Facility Type
        if fac_type == "N/A" or fac_type == "UNKNOWN" or fac_type == "NOT AVAILABLE" or fac_type == " " or fac_type == "":
            score6 = 0
        else:
            score6 = 1
            if analysis_mode == True:
                gdf.at[idx1,'FAC_TYPE_'] = 1
        
        # Total - highest possible is 6
        total_score = score1 + score2 + score3 + score4 + score5 + score6
    
        all_scores.append(total_score)
    
    ## Append to data
    gdf["ATTRIBUTE_SCORE"] = all_scores

    if analysis_mode == False:
        # Group attribute richness score by country AND STATE/PROVINCE
        attribute_score_gped = gdf.groupby(by=["COUNTRY", "STATE_PROV"]).agg({"OGIM_ID":"count", 
                                                                              "ATTRIBUTE_SCORE":"mean"})

    else:
        attribute_score_gped = gdf.groupby(by=["COUNTRY", "STATE_PROV"]).agg({"OGIM_ID":"count", 
                                                                              "ATTRIBUTE_SCORE":"mean",
                                                                              "FAC_NAME_":"sum",
                                                                              "OPERATOR_":"sum",
                                                                              "FAC_TYPE_":"sum",
                                                                              "FAC_STATUS_":"sum",
                                                                              "INSTALL_DATE_":"sum",
                                                                              "CAPACITY_":"sum"})
        
        # Create percentage fields
        attribute_score_gped['FAC_NAME_pct'] = attribute_score_gped['FAC_NAME_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['OPERATOR_pct'] = attribute_score_gped['OPERATOR_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['FAC_STATUS_pct'] = attribute_score_gped['FAC_STATUS_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['INSTALL_DATE_pct'] = attribute_score_gped['INSTALL_DATE_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['FAC_TYPE_pct'] = attribute_score_gped['FAC_TYPE_'] / attribute_score_gped['OGIM_ID']
        attribute_score_gped['CAPACITY_pct'] = attribute_score_gped['CAPACITY_'] / attribute_score_gped['OGIM_ID']

        # Drop specific (temporary) columns by name
        attribute_score_gped = attribute_score_gped.drop(['FAC_NAME_','OPERATOR_','FAC_TYPE_','FAC_STATUS_','CAPACITY_','INSTALL_DATE_'], axis=1)

    print(attribute_score_gped)
    
    return gdf, attribute_score_gped


def refresh_score(data_catalog):
    '''Assessing data update frequencies (data refresh score)
    
    How frequently is the data updated? Use the information in the OGIM data 
    catalog to determine frequency of updates for each unique data source and 
    for each facility category.
    
    Assign the following score: 
        Daily to monthly updates (5)
        Quarterly to annually (4)
        Irregularly updated, but last updated within the last year (4)
        Irregularly updated, but last updated within the last two to three years (3)
        Irregularly updated, but last updated within the last three to five years (2)
        Last updated more than five years ago (1)
    
    Run this function on the catalog before joining it to OGIM data records, 
    to ensure each OGIM record has a proper REFRESH_SCORE.
    
    Parameters
    ----------
    catalog : Pandas dataframe
        OGIM data catalog, a.k.a. the standalone source table. Note that if a 
        REFRESH_SCORE column already exists in this dataframe, it will be 
        over-written by this function.

    Returns
    -------
    catalog_ : Pandas dataframe
        same as input `catalog` paramater with 'REFRESH_SCORE' column added

    Example
    --------
        catalog = pd.read_excel('ogim_standalone_source_table.xlsx')
        catalog = refresh_score(catalog)
    
    '''
    
    catalog = data_catalog.copy()   
  
    # ========================================================================
    # Create a series of datetime objects based on each row's 
    # SRC_YEAR and SRC_MONTH columns. (Cols must be re-named for `to_datetime()` to work)
    catalog['year'] = catalog['SRC_YEAR']
    catalog['month'] = catalog['SRC_MNTH']
    catalog['day'] = 1 # add dummy day field
    datetime_array = pd.to_datetime(catalog[["year", "month", "day"]])
    # Keep only the date part (not the time part) of the datetime objects
    pubdate_array = datetime_array.dt.date
    
    # Calculate number of days between today and source publication
    catalog['DATEDIFF_days'] = pubdate_array.apply(lambda x: (date.today() - x).days)
    catalog['DATEDIFF_years'] = catalog['DATEDIFF_days'] / 365
    
    # ========================================================================
    # Assign data refresh scores to each record
    catalog['REFRESH_SCORE'] = 0  
    
    # Iterate through each row / source
    for idx, row in tqdm(catalog.iterrows(), total=catalog.shape[0]):
        update_freq_ = row.UPDATE_FREQ 
        datediff = row.DATEDIFF_years

        if (update_freq_ == 'Irregularly') | (update_freq_ == 'Other'):
            
            if datediff > 5:
                catalog.at[idx,'REFRESH_SCORE'] = 1
                
            if 3 < datediff <= 5: 
                catalog.at[idx,'REFRESH_SCORE'] = 2
            
            if 1 < datediff <= 3: 
                catalog.at[idx,'REFRESH_SCORE'] = 3    
        
            if datediff <= 1: 
                catalog.at[idx,'REFRESH_SCORE'] = 4    
    
        else: 
        
            if update_freq_ == 'Daily':
                catalog.at[idx,'REFRESH_SCORE'] = 5
                
            if update_freq_ == 'Weekly':
                catalog.at[idx,'REFRESH_SCORE'] = 5   
                
            if update_freq_ == 'Monthly':
                catalog.at[idx,'REFRESH_SCORE'] = 5    
                
            if update_freq_ == 'Quarterly':
                catalog.at[idx,'REFRESH_SCORE'] = 4    
        
            if update_freq_ == 'Annually':
                catalog.at[idx,'REFRESH_SCORE'] = 4 
                
                
    # Drop temporary used within this function 
    catalog = catalog.drop(['year','month','day', 'DATEDIFF_days','DATEDIFF_years'], axis=1)
    
    return catalog

