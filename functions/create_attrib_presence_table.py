# -*- coding: utf-8 -*-
"""
Created on Mon Apr  3 11:11:47 2023

@author: maobrien
"""
import pandas as pd
from tqdm import tqdm


def create_attrib_presence_table(gdf, subregion_col, attrib_list):
    '''Create a table stating what attributes are present or absent, by region.

    For each state and/or country in a layer of the OGIM, check if a particular
    attribute contains ALL N/A values. If the number of non-NA values is
    greater than zero, then indicate that YES, the region contains details on
    that attribute.

    Parameters
    ----------
    gdf : GeoDataFrame
        OGIM geopackage layer that adheres to the OGIM schema.
    subregion_col : str
        name of the column in `gdf` that specifies the state or country in
        which the data record resides.
    attrib_list : list
        list of attribute columns (written as string values)

    Returns
    -------
    pres_abs_table : DataFrame
        Presence/absence table, where index is each unique state or country
        specified in the `subregion_col` column, and each column is one of the
        attributes named in `attrib_list`. Possible cell values are "Y" if the
        subregion contains non-N/A values for that attribute, and "N" if the
        subregion only contains N/A values for that attribute.

    Example
    -------
    attribute_list = ['FAC_STATUS', 'OPERATOR', 'SPUD_DATE']
    available_attribs = create_attrib_presence_table(ogim_wells,
                                                     'STATE_PROV',
                                                     attrib_list)
    '''

    print('Generating presence/absence table......')
    # create an empty dataframe to contain my presence/absence values
    pres_abs_table = pd.DataFrame()

    possible_na_values = ['N/A', '1900-01-01', -999]

    # Iterate through all state or country values
    for state in tqdm(gdf[subregion_col].unique()):

        # Subset the dataset to only records present in that state/country
        gdf_ = gdf[gdf[subregion_col] == state]

        for attrib in attrib_list:
            # check if a particular attribute has ALL N/A values.
            if attrib in gdf_:
                non_na_values = len(gdf_[~gdf_[attrib].isin(possible_na_values)])
    
                # If the number of non-NA values is greater than zero,
                # then YES, the state has details on that attribute.
                if non_na_values > 0:
                    pres_abs_table.at[state, attrib] = 'Y'
                else:
                    pres_abs_table.at[state, attrib] = 'N'
            
            # If the attribute name isn't even present in the original gdf,
            # indicate that there's no details on that attribute
            else: 
                pres_abs_table.at[state, attrib] = 'N'
                
    return pres_abs_table
