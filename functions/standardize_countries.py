# -*- coding: utf-8 -*-
"""
Created on Mon Apr 18 11:49:09 2022

@author: maobrien
"""
import pandas as pd
from collections import Counter


def standardize_countries(df,
                          country_col_name,
                          new_country_col_name,
                          path_to_country_csv=None):
    """Standardize certain country names and place the result in a new df column.

    Country names are edited to remove all abbreviations, convert symbols for
    the word "and" like + and &, remove special characters, and ensure country
    names are in English.

    *NOTE* that the replacements and standardizations in this function are
    dependent on the completeness of the "replacement map", whcih is based on the CSV
    `ogim-msat/docs/UN_countries_IEA_regions.csv` in the ogim-msat repository.


    Parameters
    ----------
    df : dataframe object
        The dataframe containing the column you want to edit.
    country_col_name : str
        Dataframe column containing country names. Case doesn't matter.
    new_country_col_name : str
        The name you want your new column to have (which will contain the
        standardized country name)
    path_to_country_csv : str
        Filepath to UN_countries_IEA_regions.csv housed in the ogim-msat repo

    Returns
    -------
    DataFrame (identical to input `df` with added column `new_country_col_name`)

    Example
    -------
    fp = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\docs\\UN_countries_IEA_regions.csv'
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = standardize_countries(world,
                                 'name',
                                 'name_new',
                                 path_to_country_csv = fp)

    """

    # TODO - remove this hard-coded path
    if path_to_country_csv == None:
        my_csv = pd.read_csv(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\docs\UN_countries_IEA_regions.csv', encoding='cp1252')
    else:
        my_csv = pd.read_csv(path_to_country_csv, encoding='cp1252')

    # =========================================================================
    # Replace country names where alternates are known
    # =========================================================================
    # Reduce reference country list to only those with known alternate names
    my_csv_ = my_csv[my_csv.alternate_names.notnull()].reset_index(drop=True)

    # Add a column to my reference CSV that contains a list version of all
    # the alternate names
    my_csv_['alternate_names_list'] = my_csv_['alternate_names'].apply(lambda x: list(x.split(";")))

    # Make df of just place names to change
    name_pairs = my_csv_.filter(['name', 'alternate_names_list'])
    name_pairs = name_pairs.assign(alternate_names_list=name_pairs.alternate_names_list).explode('alternate_names_list')
    name_pairs['alternate_names_list'] = name_pairs['alternate_names_list'].str.strip()
    name_pairs['name'] = name_pairs['name'].str.upper()
    name_pairs['alternate_names_list'] = name_pairs['alternate_names_list'].str.upper()

    # Create a dictionary from the reference CSV of country names
    # key = strinfg to replace ... value(s) = string to keep
    old_to_new_names = dict(zip(name_pairs.alternate_names_list, name_pairs.name))

    # Create new column to contain standardized country name
    # Start off by populating it with the original country names
    # Change all country names to upper case and replace symbols for "and"
    df[new_country_col_name] = df[country_col_name].str.upper()

    # deal with features that have more than one country
    multi_country_features = df[df[country_col_name].str.contains(',')]

    if multi_country_features.empty:
        # if multi_country_features is empty proceed as normal.
        print('No features with multiple countries')

    else:
        # If multi_country_features has items in it...
        print('Handling features that are assigned to more than one country...')
        multi_country_features_aslist = multi_country_features[country_col_name].str.split(',')

        for feature in multi_country_features_aslist:
            # Iterate through each name in the list of country names
            for index, country_name in enumerate(feature):
                # remove trailing and leading spaces from country name
                country_name_ = country_name.strip()
                for key, value in old_to_new_names.items():
                    if key == country_name_:
                        # print(key)
                        feature[index] = country_name.replace(key, old_to_new_names[key])

        df[new_country_col_name] = multi_country_features_aslist.apply(lambda x: ','.join(map(str, x)))
        df[new_country_col_name] = df[new_country_col_name].fillna(df[country_col_name])

    # Use dictionary to "map" (find and replace) multiple values in a column at once
    df[new_country_col_name] = df[new_country_col_name].map(old_to_new_names).fillna(df[new_country_col_name])

    # =========================================================================
    # TEMPORARILY SUPPRESSING THIS PRINT STATEMENT # FIXME  later
    # For convenience, print to the window all original place names that DON'T match
    # our standard list, even after the standardization function was applied
    # =========================================================================
    # standard_country_name_list = list(my_csv.name.unique())
    # standard_country_name_list = [x.upper() for x in standard_country_name_list]

    # placenames_in_df = df[new_country_col_name].str.upper()
    # not_standard_countries = placenames_in_df[~placenames_in_df.isin(standard_country_name_list)]

    # print('The following place names in the "'+country_col_name+'" column do NOT match the standardized list of countries:')
    # print(*sorted(not_standard_countries.unique()), sep='\n')

    return df


def add_region_column(df,
                      country_col_name,
                      path_to_country_csv_=None):
    ''' Add column indicating the IEA region of each record's country

    *NOTE* that the assignments of region in this function are
    dependent on the completeness of the "region_IEA" field in the CSV
    `ogim-msat/docs/UN_countries_IEA_regions.csv` in the ogim-msat repository.

    Parameters
    ----------
    df : dataframe object
        The dataframe containing the column you want to edit.
    country_col_name : str
        Dataframe column containing country names. Case doesn't matter.
    path_to_country_csv_ : str
        Filepath to UN_countries_IEA_regions.csv housed in the ogim-msat repo

    Returns
    -------
    DataFrame (identical to input `df` with added column `REGION`)

    Example
    -------
    fp = 'C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\docs\\UN_countries_IEA_regions.csv'
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    world = add_region_column(world,
                              'name',
                              path_to_country_csv_ = fp)

    '''
    # =========================================================================
    # Load and prepare the countries-to-regions csv
    # TODO - remove this hard-coded path
    # =========================================================================
    if path_to_country_csv_ == None:
        my_csv = pd.read_csv(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\docs\UN_countries_IEA_regions.csv', encoding='cp1252')
    else:
        my_csv = pd.read_csv(path_to_country_csv_, encoding='cp1252')

    # If there are any empty region or country name cells, fill in these missing values
    my_csv = my_csv.fillna('N/A')

    # Create a dictionary from the reference CSV of country names
    # key = country name... value(s) = IEA region
    countries_to_regions = dict(zip(my_csv.name, my_csv.region_IEA))
    # change all values in the dictionary to uppercase
    countries_to_regions = {k.upper(): v.upper() for k, v in countries_to_regions.items()}

    # =========================================================================
    # Use the `countries_to_regions` dictionary to "map" countries to their
    # respective region, and record the result in the REGION column.
    # =========================================================================
    df['REGION'] = df[country_col_name].map(countries_to_regions).fillna('N/A')

    # Deal with records that have more than one country in the COUNTRY field
    multi_country_features = df[df[country_col_name].str.contains(',')]

    if multi_country_features.empty:
        # if multi_country_features is empty proceed as normal.
        print('No features with multiple countries')

    else:
        # For each record, note what REGION corresponds with each country
        # mentioned in its COUNTRY column, and save each result in the
        # `region_list` object. Find the one "most common" REGION in
        # `region_list`, and assign that value to the record's REGION attribute
        print('Handling features that are assigned to more than one country...')
        for i, row in multi_country_features.iterrows():
            country_list = row.COUNTRY.split(',')
            country_list = [s.strip() for s in country_list]
            region_list = []
            for country in country_list:
                region_list.append(countries_to_regions.get(country))

            most_common_region = Counter(region_list).most_common(1)[0][0]
            df.at[i, 'REGION'] = most_common_region

    # Move REGION column position right next to COUNTRY position
    # by getting the column index of COUNTRY column
    loc_of_country_col = df.columns.get_loc(country_col_name)
    df.insert(loc_of_country_col, 'REGION', df.pop('REGION'))

    # =========================================================================
    # For convenience, print to the window all country names that were NOT
    # assigned a proper region, if there are any
    # =========================================================================
    list_of_countries_without_region = list(df[df.REGION == 'N/A'][country_col_name].unique())

    if list_of_countries_without_region:
        print(f'The following place names in the {country_col_name} column received a REGION assignment of N/A:')
        print(*sorted(list_of_countries_without_region), sep='\n')
    if not list_of_countries_without_region:
        print(f'All names in the {country_col_name} column received a REGION assignment.')

    return df
