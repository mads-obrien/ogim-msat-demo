# -*- coding: utf-8 -*-
"""
Created on Wed Dec  7 11:15:11 2022

@author: maobrien
"""

import geopandas as gpd

def assign_countries_to_features(infra_gdf, 
                                  country_gdf, 
                                  country_gdf_colname, 
                                  maritime_gdf,
                                  maritime_gdf_colname,
                                  new_col_name):
    
    """Add an attribute to a geodataframe identifying the country(s) to which each feature belongs.
    
    This function uses a spatial join to identify what national boundary 
    (or boundaries) each feature in a gdf intersects, using both 
    country land masses and Exclusive Economic Zones (i.e. maritime boundaries) 
    to attribute country names.

    Parameters
    ----------
    infra_gdf : GeoDataframe
        Point or Linestring representations of O&G infrastructure assets
    country_gdf : GeoDataframe
        Polygon representations of world countries.
        *NOTE* that `country_gdf` must use the same CRS as `infra_gdf` for the 
        spatial join to work properly.
    country_gdf_colname : str
        The column in `country_gdf` that contains the name of each country.
    maritime_gdf : GeoDataframe
        Polygon representations of Exclusive Economic Zones 
        (i.e. maritime boundaries)
        *NOTE* that `maritime_gdf` must use the same CRS as `infra_gdf` for the 
        spatial join to work properly.
    maritime_gdf_colname : str
        The column in `maritime_gdf` that contains the name of each country who
        holds claim to the EEZ.
    new_col_name : str
        The name of the column you want added to `infra_gdf`, which will contain
        the country or countries that each infrastructure feature intersects.

    Returns
    -------
    GeoDataframe
        Identical to `infra_gdf`, but with an additional column specified by 
        the user (`new_col_name`)
        
    Example
    -------
    worldcountries_fp = 'C:\\..\\International_data_sets\\World_Countries_Boundaries\\_world_countries_.shp'
    maritimebounds_fp = 'C:\\..\\International_data_sets\\National_Maritime_Boundaries\\eez_v11.shp'
    worldcountries = gpd.read_file(worldcountries_fp)
    maritimebounds = gpd.read_file(maritimebounds_fp)
    
    data_with_countries = assign_countries_to_pipelines(data, 
                                                        worldcountries, 
                                                        'Country', 
                                                        maritimebounds,
                                                        'TERRITORY1',
                                                        'COUNTRY')

    """

    # First, create a dictionary that defines how your spatially-joined table will later be grouped.
    # Ensure all attributes present in the infrastructure data are retained, 
    # and if any rows get duplicated during the join, only the "first" value is kept.
    data_colnames = infra_gdf.columns
    agg_dictionary = dict.fromkeys(data_colnames, "first")
    # Add another key-value pair to your dictionary that ensures the names of countries
    # each asset intersects get aggregated into a list.
    agg_dictionary[country_gdf_colname] = list

    # Then, spatially join assets with terrestrial countries.
    # The resulting GDF has one row per unique asset + country PAIR.
    intersection_table = gpd.sjoin(infra_gdf, country_gdf, how='left', op='intersects')
    
    # If there are any assets that didn't intersect a terrestrial country, 
    # change its country name value to 'NONE'.
    intersection_table[country_gdf_colname] = intersection_table[country_gdf_colname].fillna('NONE')
    
    # TODO - better documentation of this step
    infra_with_country =  intersection_table.groupby(by=intersection_table.index).agg(agg_dictionary)
   
    # Sort the list of countries associated with each asset alphabetically
    infra_with_country[country_gdf_colname] = infra_with_country[country_gdf_colname].apply(lambda x: sorted(x))
    
    # Create new column casting the list objects to strings.
    # Each value in the column now contains a string-type, rather than a list-type object.
    infra_with_country[new_col_name] = infra_with_country[country_gdf_colname].apply(lambda x: ', '.join(map(str, x)))

    # At this point, for rows where new_col_name contains ['NONE'], that means 
    # the asset didn't intersect with any country landmasses in country_gdf.
    # Create a GDF containing only these assets that haven't yet been 
    # attributed to a country.
    infra_without_country = gpd.GeoDataFrame(infra_with_country[infra_with_country[new_col_name]=='NONE'], geometry='geometry')
    
    # Spatially join these assets with maritime boundaries.
    # The resulting GDF has one row per unique asset + country PAIR.
    intersection_table_2 = gpd.sjoin(infra_without_country, maritime_gdf, how='left', op='intersects')
    
    # If there are any assets that still didn't intersect a country, 
    # change its country name value to 'NONE'.
    intersection_table_2[maritime_gdf_colname] = intersection_table_2[maritime_gdf_colname].fillna('NONE')  
    
    # TODO - better documentation of this step
    agg_dictionary[maritime_gdf_colname] = agg_dictionary.pop(country_gdf_colname)
    infra_with_country_marine =  intersection_table_2.groupby(by=intersection_table_2.index).agg(agg_dictionary)
    
    # Sort the list of countries associated with each asset alphabetically.
    infra_with_country_marine[maritime_gdf_colname] = infra_with_country_marine[maritime_gdf_colname].apply(lambda x: sorted(x))
    
    # Create new column casting the list objects to strings.
    infra_with_country_marine[new_col_name] = infra_with_country_marine[maritime_gdf_colname].apply(lambda x: ', '.join(map(str, x)))
    
    # Create one master 'COUNTRY' column by combining the two gdfs you created.
    # Using the table from the first spatial join, drop any assets that
    # didn't overlap with a terrestrial country. 
    # Then, append these assets that were successfully joined to a maritime 
    # zone.
    infra_with_country = infra_with_country[infra_with_country[new_col_name] !='NONE']
    infra_with_country_final = infra_with_country.append(infra_with_country_marine).sort_index()
    
    # Prior to exporting, drop columns that contain countries as list-type 
    # objects, keeping the column of string-type objects.
    infra_with_country_final = infra_with_country_final.drop([country_gdf_colname,maritime_gdf_colname], axis=1)
    
    return gpd.GeoDataFrame(infra_with_country_final, geometry='geometry')

