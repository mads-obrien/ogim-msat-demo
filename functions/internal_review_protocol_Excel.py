# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 13:31:06 2022

@author: maobrien
"""

# import os
# import geopandas as gpd
import pandas as pd
# import fiona
# from tqdm import tqdm
from datetime import datetime
import numpy as np


def merge_excel_cells(writer, sheetname, exported_df, col2merge):
    '''Merge adjacent cells in an Excel report with the same value'''
    workbook = writer.book
    worksheet = writer.sheets[sheetname]
    merge_format = workbook.add_format({'align': 'left',
                                        'valign': 'vcenter',
                                        'border': 1})

    for cellvalue in exported_df[col2merge].unique():
        # find row indices and add one to account for header
        u = exported_df.loc[exported_df[col2merge] == cellvalue].index.values + 1
        # get column index
        i_c = exported_df.columns.get_loc(col2merge)

        if len(u) < 2:
            pass  # do not merge cells if there is only one unique name
        else:
            # merge cells using the first and last indices
            worksheet.merge_range(u[0],  # first row
                                  i_c,  # first column
                                  u[-1],  # last row
                                  i_c,  # last column
                                  exported_df.loc[u[0], col2merge],  # data
                                  merge_format)  # format keywords


def auto_adjust_excel_column_width(writer, sheetname, exported_df):
    '''Auto-adjust column width in Excel report'''
    for column in exported_df:
        column_width = max(exported_df[column].astype(str).map(len).max(), len(column))
        col_idx = exported_df.columns.get_loc(column)
        writer.sheets[sheetname].set_column(col_idx, col_idx, column_width)


def create_uniquevals_dataframe(df, varname):
    '''Placeholder docstring'''
    # uniquevals = list(df[varname].unique())
    return pd.DataFrame({'All Unique ' + varname + ' Values': sorted(list(df[varname].unique()))})


def unique_vals_by_country(df, country_field, field_to_print):
    '''Placeholder docstring'''
    result_fieldname = 'All Unique ' + field_to_print + ' Value(s)'
    df_out = pd.DataFrame({'Country': [],
                           result_fieldname: [],
                           'num. of occurences': []})
    # df_out.columns = ['Country', field_to_print+' Value', 'count']

    countries = sorted(list(df[country_field].unique()))
    countries_with_nodata = []  # create empty list

    for country in countries:
        # subset dataframe to only records from chosen country
        df_country = df[df[country_field] == country]
        # subset df to records with N/A in my field of interest
        df_country_ = df_country[df_country[field_to_print] != 'N/A']

        if len(df_country_) == 0:
            # add country to list of those with nodata
            countries_with_nodata.append(country)
            continue

        # create dataframe with value counts for this country
        dftemp = pd.DataFrame(df_country_[field_to_print].value_counts()).reset_index(drop=False)
        dftemp.columns = [result_fieldname, 'num. of occurences']
        dftemp['Country'] = country

        df_out = pd.concat([df_out, dftemp], ignore_index=True)

    if len(countries_with_nodata) > 0:
        for country in countries_with_nodata:
            new_row = pd.DataFrame({'Country': [country],
                                    result_fieldname: ["All values for this country are N/A"],
                                    'num. of occurences': [0]})
            df_out = pd.concat([df_out, new_row], ignore_index=True)

    df_out = df_out.reset_index(drop=True)

    return df_out


def random_num_by_country(df, country_field, field_to_print, number):
    '''Placeholder docstring'''
    df_out = pd.DataFrame()

    countries = sorted(list(df[country_field].unique()))
    countries_with_nodata = []  # create empty list

    for country in countries:
        # subset dataframe to only records from chosen country
        df_country = df[df.COUNTRY == country]
        # subset df to records with N/A in my field of interest
        df_country_ = df_country[df_country[field_to_print] != 'N/A']

        if len(df_country_) == 0:
            # add country to list of those with nodata
            countries_with_nodata.append(country)
            continue

        df_out.loc[country, 'Number of non-null value(s)'] = len(df_country_)

        # if the country has enough records to sample the amount I want, do so
        if len(df_country_) >= number:
            df_sample = df_country_.sample(n=number)
        # if the country doesn't have enough non-null records to sample,
        # just print them all
        if len(df_country_) < number:
            df_sample = df_country_.sample(n=len(df_country_))

        # values = df_sample[field_to_print].apply(lambda x: ','.join(map(str, x)))
        values_list = list(df_sample[field_to_print])
        # Join all the list objects into one string that can be later split
        values_string = "|".join(values_list)
        # .apply(lambda x: ','.join(map(str, x)))

        df_out.loc[country, 'Sample value(s)'] = values_string

    if 'Sample value(s)' not in df_out.columns:
        df_out_ = df_out
    else:
        # # Break comma-separated strings into individual rows -- all other column values are repeated
        kwargs = {'Sample value(s)': df_out['Sample value(s)'].str.split('|')}
        df_out_ = df_out.assign(**kwargs).explode('Sample value(s)')

    if len(countries_with_nodata) > 0:
        for country in countries_with_nodata:
            df_out_.loc[country, 'Number of non-null value(s)'] = 0

    df_out_ = df_out_.reset_index(drop=False)
    df_out_ = df_out_.rename(columns={"index": "Country"}, errors="raise")
    # change Number field to string
    # df_out_['Number of non-null value(s)'] = df_out_['Number of non-null value(s)'].astype(str)

    return df_out_


def create_most_common_vals_dataframe(df, varname, number):
    '''Placeholder docstring'''
    dfout = pd.DataFrame(df[varname].value_counts().nlargest(number)).reset_index(drop=False)
    dfout.columns = ['Most Common ' + varname + ' Value(s)', 'num. of occurences']
    return dfout


def most_common_values_by_country(df, country_field, field_to_print, number):
    '''Placeholder docstring'''
    result_fieldname = 'Most Common ' + field_to_print + ' Value(s)'
    df_out = pd.DataFrame({'Country': [],
                           result_fieldname: [],
                           'num. of occurences': []})
    # df_out.columns = ['Country', field_to_print+' Value', 'count']

    countries = sorted(list(df[country_field].unique()))
    countries_with_nodata = []  # create empty list

    for country in countries:
        # subset dataframe to only records from chosen country
        df_country = df[df[country_field] == country]
        # subset df to records with N/A in my field of interest
        df_country_ = df_country[df_country[field_to_print] != 'N/A']

        if len(df_country_) == 0:
            # add country to list of those with nodata
            countries_with_nodata.append(country)
            continue

        # create dataframe with value counts for this country
        dftemp = pd.DataFrame(df_country_[field_to_print].value_counts().nlargest(number)).reset_index(drop=False)
        dftemp.columns = [result_fieldname, 'num. of occurences']
        dftemp['Country'] = country

        df_out = pd.concat([df_out, dftemp], ignore_index=True)

    if len(countries_with_nodata) > 0:
        for country in countries_with_nodata:
            new_row = {'Country': [country],
                       result_fieldname: ["All values for this country are N/A"],
                       'num. of occurences': [0]}
            df_out = pd.concat([df_out, pd.DataFrame(new_row)], ignore_index=True)

    df_out = df_out.reset_index(drop=True)

    return df_out


def date_stats_by_country(df, country_field, date_col):
    '''Placeholder docstring'''
    # First, calculate the OVERALL statistics
    # ---------------------------------------
    valid_dates = []
    invalid_dates = []
    nonnulldates = df[date_col][df[date_col] != '1900-01-01']
    # check to find all values that are in YYYY-MM-DD format
    for string in nonnulldates:
        try:
            date_formatted = datetime.strptime(string, '%Y-%m-%d')
            valid_dates.append(date_formatted)
        except ValueError:
            invalid_dates.append(string)

    if len(valid_dates) > 0:
        df_out = pd.DataFrame([['Dates in YYYY-MM-DD format:', str(len(valid_dates))],
                               ['Earliest date:', str(min(valid_dates))],
                               ['Median date (50th percentile):', str(np.quantile(valid_dates, .50))],
                               ['Latest date:', str(max(valid_dates))],
                               ['Date values NOT in YYYY-MM-DD format', str(len(invalid_dates))]],
                              columns=['Description', 'Value'])
    else:
        df_out = pd.DataFrame([['Dates in YYYY-MM-DD format:', str(0)],
                               # ['Earliest date:', str(min(valid_dates))],
                               # ['Median date (50th percentile):', str(np.quantile(valid_dates, .50))],
                               # ['Latest date:', str(max(valid_dates))],
                               ['Date values NOT in YYYY-MM-DD format', str(len(invalid_dates))]],
                              columns=['Description', 'Value'])

    # df_out = pd.DataFrame([['Dates in YYYY-MM-DD format:', str(len(valid_dates))],
    #                   ['Earliest date:', str(min(valid_dates))],
    #                   ['Median date (50th percentile):', str(np.quantile(valid_dates, .50))],
    #                   ['Latest date:', str(max(valid_dates))],
    #                   ['Date values NOT in YYYY-MM-DD format', str(len(invalid_dates))]],
    #                  columns=['Description','Value'])

    # If there ARE invalid dates, print all of the values
    if len(invalid_dates) > 0:
        invalid_dates_string = ",".join(invalid_dates)
        new_row = pd.DataFrame([['Invalid date values:', invalid_dates_string]],
                               columns=['Description', 'Value'])
        df_out = pd.concat([df_out, new_row], ignore_index=True)

    df_out['Country'] = 'OVERALL'
    # Re-order columns for output
    df_out = df_out[['Country', 'Description', 'Value']]

    # Then calculate country by country
    # ---------------------------------------
    countries = sorted(list(df[country_field].unique()))
    countries_with_nodata = []  # create empty list

    for country in countries:
        valid_dates = []
        invalid_dates = []
        # subset dataframe to only records from chosen country
        df_country = df[df[country_field] == country]
        # subset df to records with no NA in my date field of interest
        nonnulldates = df_country[date_col][df_country[date_col] != '1900-01-01']

        # if this country only has null dates
        if len(nonnulldates) == 0:
            # add country to list of those with nodata
            countries_with_nodata.append(country)
            continue

        # check to find all values that are in YYYY-MM-DD format
        for string in nonnulldates:
            try:
                date_formatted = datetime.strptime(string, '%Y-%m-%d')
                valid_dates.append(date_formatted)
            except ValueError:
                invalid_dates.append(string)

        if len(valid_dates) > 0:
            dftemp = pd.DataFrame([['Dates in YYYY-MM-DD format:', str(len(valid_dates))],
                                   ['Earliest date:', str(min(valid_dates))],
                                   ['Median date (50th percentile):', str(np.quantile(valid_dates, .50))],
                                   ['Latest date:', str(max(valid_dates))],
                                   ['Date values NOT in YYYY-MM-DD format', str(len(invalid_dates))]],
                                  columns=['Description', 'Value'])
        else:
            dftemp = pd.DataFrame([['Dates in YYYY-MM-DD format:', str(0)],
                                   # ['Earliest date:', str(min(valid_dates))],
                                   # ['Median date (50th percentile):', str(np.quantile(valid_dates, .50))],
                                   # ['Latest date:', str(max(valid_dates))],
                                   ['Date values NOT in YYYY-MM-DD format', str(len(invalid_dates))]],
                                  columns=['Description', 'Value'])

        # If there ARE invalid dates, print all of the values
        if len(invalid_dates) > 0:
            invalid_dates_string = ",".join(invalid_dates)
            new_row = pd.DataFrame([['Invalid date values:', invalid_dates_string]],
                                   columns=['Description', 'Value'])
            dftemp = pd.concat([dftemp, new_row], ignore_index=True)

        dftemp['Country'] = country
        df_out = pd.concat([df_out, dftemp])

    if len(countries_with_nodata) > 0:
        for country in countries_with_nodata:
            new_row = pd.DataFrame({'Country': [country],
                                    'Description': ['All values for this country are N/A'],
                                    'Value': ['n/a']})
            df_out = pd.concat([df_out, new_row])

    df_out = df_out.reset_index(drop=True)
    return df_out


def numericfields_stats_by_country(df, col, country_field):
    '''Placeholder docstring'''
    # First, calculate the OVERALL statistics
    # ---------------------------------------
    notnullvals = df[col][df[col] != -999]

    df_out = pd.DataFrame(notnullvals.describe()).reset_index(drop=False)

    df_out['Country'] = 'OVERALL'
    df_out = df_out.rename(columns={"index": "Statistic", }, errors="raise")
    # Re-order columns for output
    df_out = df_out[['Country', 'Statistic', col]]

    # Then calculate country by country
    # ---------------------------------------
    countries = sorted(list(df[country_field].unique()))
    countries_with_nodata = []  # create empty list

    for country in countries:
        # subset dataframe to only records from chosen country
        df_country = df[df[country_field] == country]

        if (df_country[col] == -999).all():
            # add country to list of those with nodata
            countries_with_nodata.append(country)
            continue

        else:
            notnullvals = df_country[col][df_country[col] != -999]
            df_temp = pd.DataFrame(notnullvals.describe()).reset_index(drop=False)

        df_temp['Country'] = country
        df_temp = df_temp.rename(columns={"index": "Statistic", }, errors="raise")
        df_out = pd.concat([df_out, df_temp])

    if len(countries_with_nodata) > 0:
        for country in countries_with_nodata:
            df_temp = pd.DataFrame([['All values for this country are -999', 'n/a']],
                                   columns=['index', col])
            df_temp['Country'] = country
            df_temp = df_temp.rename(columns={"index": "Statistic", }, errors="raise")
            df_out = pd.concat([df_out, df_temp])

    df_out = df_out.reset_index(drop=True)
    return df_out


def create_string_length_dataframe_by_country(df, country_field, field_to_print):

    df_out = pd.DataFrame({'Country': [],
                           'Length of string values': [],
                           'Freq. of occurences': [],
                           'Sample value(s)': []})

    countries = sorted(list(df[country_field].unique()))
    countries_with_nodata = []  # create empty list

    # Iterate through each country, and generate its respective row(s) for the output table
    for country in countries:
        # subset input dataframe to only records from the chosen country
        df_country = df[df[country_field] == country]
        # subset df to records without N/A in my field of interest
        df_country_ = df_country[df_country[field_to_print] != 'N/A']

        # Make a list of countries that have only No Data in my field of interest
        if len(df_country_) == 0:
            countries_with_nodata.append(country)
            continue

        # Create an intermediate df column that counts the string lengths present
        # in the field of interest, and how frequent that string length is
        str_len_col = field_to_print + '_strlength'
        df_country_[str_len_col] = df_country[field_to_print].str.len()
        dftemp = pd.DataFrame(df_country_[str_len_col].value_counts()).reset_index(drop=False)
        dftemp.columns = ['Length of string values', 'Freq. of occurences']
        dftemp['Country'] = country

        # Grab an example value from the original df, representing each string-length type
        lengths = df_country_[str_len_col].unique()
        for length in lengths:
            subset = df_country_[df_country_[str_len_col] == length]
            sampled_val = subset[field_to_print].sample(n=1).item()

            dftemp.loc[dftemp['Length of string values'] == length, 'Sample value(s)'] = sampled_val

        df_out = pd.concat([df_out, dftemp])

    if len(countries_with_nodata) > 0:
        for country in countries_with_nodata:
            new_row = pd.DataFrame({'Country': [country],
                                    'Length of string values': ['All values for this country are N/A'],
                                    'Freq. of occurences': [0],
                                    'Sample value(s)': [0]})
            df_out = pd.concat([df_out, new_row], ignore_index=True)

    df_out = df_out.reset_index(drop=True)

    return df_out


def check_for_allowed_nans_excel_report(df):
    '''Placeholder docstring'''
    allow_nulls_dict = {
        'AREA_KM2': False,
        'AVERAGE_FLARE_TEMP_K': False,
        'CATEGORY': False,
        'COMMODITY': True,
        'COMP_DATE': True,
        'CONDENSATE_BBL': True,
        'COUNTRY': False,  # should not be null in infra layers, ensure catalog has values
        'DAYS_CLEAR_OBSERVATIONS': False,
        'DOWNLOAD_INSTRUCTIONS': True,
        'DRILL_TYPE': True,
        'ENTITY_TYPE': True,  # TODO confirm this
        'FAC_ID': True,
        'FAC_NAME': True,
        'FAC_STATUS': True,
        'FAC_TYPE': True,
        'FILE_NAME': True,
        'FLARE_YEAR': False,
        'GAS_CAPACITY_MMCFD': True,
        'GAS_FLARED_MMCF': False,
        'GAS_MCF': True,
        'GAS_THROUGHPUT_MMCFD': True,
        'geometry': False,
        'HAS_BASINS': False,
        'HAS_BLOCKS': False,
        'HAS_COMPRESSORS': False,
        'HAS_EQUIP_COMP': False,
        'HAS_FIELDS': False,
        'HAS_FLARES': False,
        'HAS_INJ_DISP': False,
        'HAS_LNG': False,
        'HAS_OTHER': False,
        'HAS_PIPELINES': False,
        'HAS_PLATFORMS': False,
        'HAS_PROCESSING': False,
        'HAS_PRODUCTION': False,
        'HAS_REFINERIES': False,
        'HAS_TANKS': False,
        'HAS_TERMINALS': False,
        'HAS_WELLS': False,
        'INSTALL_DATE': True,
        'LASTVISIT': False,
        'LATITUDE': False,
        'LIQ_CAPACITY_BPD': True,
        'LIQ_THROUGHPUT_BPD': True,
        'LONGITUDE': False,
        'NAME': True,
        'NOTES': True,
        'NUM_COMPR_UNITS': True,
        'NUM_STORAGE_TANKS': True,
        'OGIM_ID': False,
        'OGIM_STATUS': True,
        'OIL_BBL': True,
        'ON_OFFSHORE': False,
        'OPERATOR': True,
        'ORIGINAL_CRS': True,
        'PIPE_DIAMETER_MM': True,
        'PIPE_LENGTH_KM': False,
        'PIPE_MATERIAL': True,
        'PROD_DATA': False,
        'PROD_DAYS': True,
        'PROD_YEAR': False,  # TODO confirm this
        'PUB_PRIV': False,
        'REFRESH_SCORE': True,
        'REGION': True,  # region allowed to be N/A for multi-country features
        'RESERVOIR_TYPE': True,
        'SEGMENT_TYPE': True,
        'SITE_HP': True,
        'SOURCE_SCORE': True,
        'SPUD_DATE': True,
        'SRC_ALIAS': True,
        'SRC_DATE': False,
        'SRC_ID': False,
        'SRC_MNTH': False,
        'SRC_NAME': False,
        'SRC_REF_ID': False,
        'SRC_TYPE': False,
        'SRC_URL': False,
        'SRC_YEAR': False,
        'STATE_PROV': True,
        'UPDATE_FREQ': False,
        'WATER_BBL': True
    }

    # Create empty df that will be printed to the resulting Excel tab
    df_out = pd.DataFrame()
    attrs_with_noissues = []  # create empty list

    for attr in df.columns:
        # Confirm that the df column is present in our "allow nulls or not" dictionary
        # If it is, then proceed
        if attr not in allow_nulls_dict:
            print(f'{attr} -- error, column name not in dictionary')
            continue

        # If the specified column does NOT allow null values, then check for them.
        # If any null values are present, print this as a warning to the user
        if not allow_nulls_dict.get(attr):
            # subset df to records with N/A in my field of interest
            records_with_nans = df[df[attr].isin([np.nan, 'N/A'])]

            if len(records_with_nans) == 0:
                # add attr to list of those with no issues
                attrs_with_noissues.append(attr)
                continue

            # create dataframe with the offending records
            records_with_nans.insert(0, 'Attribute that should NOT contain nulls', attr)
            records_with_nans = records_with_nans.drop('geometry', axis=1)
            df_out = pd.concat([df_out, records_with_nans])

    if len(attrs_with_noissues) > 0:
        for attr in attrs_with_noissues:
            to_append = pd.DataFrame({'Attribute that should NOT contain nulls': [f'{attr} - no issues found']})
            df_out = pd.concat([df_out, to_append])

    df_out = df_out.reset_index(drop=True)

    return df_out


def check_for_duplicate_records(df):
    '''Placeholder docstring'''
    df_cols = list(df.columns)  # This list includes the `geometry` column
    df_cols.remove('OGIM_ID')
    dupe_records = df[df.duplicated(subset=df_cols, keep=False)]

    # Sort the rows so that duplicate records appear next to each other in df_out
    if 'FAC_NAME' in dupe_records.columns:
        dupe_records = dupe_records.sort_values(by=['COUNTRY', 'STATE_PROV', 'FAC_NAME'],
                                                ascending=[True, True, True],
                                                na_position='last')
    if 'NAME' in dupe_records.columns:
        dupe_records = dupe_records.sort_values(by=['COUNTRY', 'STATE_PROV', 'NAME'],
                                                ascending=[True, True, True],
                                                na_position='last')

    # Move COUNTRY field to first position (for later merging of Excel cells)
    dupe_records.insert(0, 'COUNTRY', dupe_records.pop('COUNTRY'))
    dupe_records = dupe_records.reset_index(drop=True)

    if dupe_records.empty:
        df_out = pd.DataFrame({'COUNTRY': ['No duplicate records detected.']})
    else:
        df_out = dupe_records

    return df_out


def create_internal_review_spreadsheet(infra_df, out_file_name):
    '''Placeholder docstring'''
    # Define how particular fields are going to be treated
    unique_cols = ['COUNTRY',
                   'STATE_PROV',
                   'ON_OFFSHORE',
                   'FAC_TYPE',
                   'FAC_STATUS',
                   'OGIM_STATUS',
                   'DRILL_TYPE',
                   'COMMODITY',
                   'CATEGORY',
                   'SRC_REF_ID']

    random_sample_cols = ['FAC_NAME']

    most_common_cols = ['FAC_TYPE',
                        'OPERATOR',
                        'COMMODITY']

    string_length_cols = ['FAC_ID']

    num_cols_to_exclude = ['OGIM_ID',
                           'LATITUDE',
                           'LONGITUDE',
                           'ATTRIBUTE_SCORE',
                           'DATA_SOURCE_SCORE',
                           'UPDATE_FREQUENCY_SCORE',
                           'AGGREGATE_QUALITY_SCORE']

    date_cols = ['INSTALL_DATE', 'SPUD_DATE', 'COMP_DATE']

    with pd.ExcelWriter(out_file_name) as writer:

        # First, create the dummy / placeholder sheet that will eventually hold
        # the list of all the sheets in the Excel document
        dummy = pd.DataFrame()
        sheetname_ = 'ALL_SHEET_NAMES'
        dummy.to_excel(writer, sheet_name=sheetname_, index=False)

        # Iterate thru each column you want to print unique values of
        # Sheets on which ALL unique values get listed
        for col in unique_cols:
            print('Attempting ' + col)

            # Confirm that column exists before continuing
            if col in infra_df.columns:

                df_out = create_uniquevals_dataframe(infra_df, col)
                sheetname_ = col + '_Unique'
                df_out.to_excel(writer, sheet_name=sheetname_, index=False)
                auto_adjust_excel_column_width(writer, sheetname_, df_out)
                print(sheetname_ + ' exported successfully')

                if col != 'COUNTRY':
                    df_out2 = unique_vals_by_country(infra_df, 'COUNTRY', col)
                    sheetname_2 = col + '_Unique_byCountry'
                    df_out3 = df_out2.set_index(df_out2.columns[0:2].tolist())
                    df_out3.to_excel(writer, sheet_name=sheetname_2, index=True)
                    auto_adjust_excel_column_width(writer, sheetname_2, df_out2)
                    # merge_excel_cells(writer, sheetname_2, df_out2, 'Country')
                    print(sheetname_2 + ' exported successfully')

            else:
                print(col + ' not present, skipping...')

        # Iterate thru each column you want to print top 10 of
        for col in most_common_cols:
            print('Attempting ' + col)

            # Confirm that column exists before continuing
            if col in infra_df.columns:

                df_out = create_most_common_vals_dataframe(infra_df, col, 15)
                sheetname_ = col + '_Top15'
                df_out.to_excel(writer, sheet_name=sheetname_, index=False)
                auto_adjust_excel_column_width(writer, sheetname_, df_out)
                print(sheetname_ + ' exported successfully')

                df_out2 = most_common_values_by_country(infra_df, 'COUNTRY', col, 15)
                sheetname_2 = col + '_Top15_byCountry'
                df_out3 = df_out2.set_index(df_out2.columns[0:2].tolist())
                df_out3.to_excel(writer, sheet_name=sheetname_2, index=True)
                auto_adjust_excel_column_width(writer, sheetname_2, df_out2)
                # merge_excel_cells(writer, sheetname_2, df_out2, 'Country')
                print(sheetname_2 + ' exported successfully')

            else:
                print(col + ' not present, skipping...')

        # Iterate thru each column you want to print the string length of
        for col in string_length_cols:
            print('Attempting ' + col)

            # Confirm that column exists before continuing
            if col in infra_df.columns:

                df_out = create_string_length_dataframe_by_country(infra_df, 'COUNTRY', col)
                sheetname_ = col + '_StrLength'
                df_out2 = df_out.set_index(df_out.columns[0:2].tolist())
                df_out2.to_excel(writer, sheet_name=sheetname_, index=True)
                auto_adjust_excel_column_width(writer, sheetname_, df_out)
                # merge_excel_cells(writer, sheetname_, df_out, 'Country')
                print(sheetname_ + ' exported successfully')

            else:
                print(col + ' not present, skipping...')

        # Iterate thru each column you want to print random sample of
        for col in random_sample_cols:
            print('Attempting ' + col)

            # Confirm that column exists before continuing
            if col in infra_df.columns:

                df_out = random_num_by_country(infra_df, 'COUNTRY', col, 15)
                sheetname_ = col + '_Random15'
                df_out2 = df_out.set_index(df_out.columns[0:2].tolist())
                df_out2.to_excel(writer, sheet_name=sheetname_, index=True)
                auto_adjust_excel_column_width(writer, sheetname_, df_out)
                # merge_excel_cells(writer, sheetname_, df_out, 'Country')
                # merge_excel_cells(writer, sheetname_, df_out, 'Number of non-null value(s)')
                print(sheetname_ + ' exported successfully')

            else:
                print(col + ' not present, skipping...')

        # Iterate thru each column you want date info on
        for col in date_cols:
            print('Attempting ' + col)

            # Confirm that column exists before continuing
            if col in infra_df.columns:

                df_out2 = date_stats_by_country(infra_df, 'COUNTRY', col)
                sheetname_2 = col + '_Stats_byCountry'
                df_out3 = df_out2.set_index(df_out2.columns[0:2].tolist())
                df_out3.to_excel(writer, sheet_name=sheetname_2, index=True)
                auto_adjust_excel_column_width(writer, sheetname_2, df_out2)
                # merge_excel_cells(writer, sheetname_2, df_out2, 'Country')
                # merge_excel_cells(writer, sheetname_, df_out, 'Number of non-null value(s)')
                print(sheetname_2 + ' exported successfully')

            else:
                print(col + ' not present, skipping...')

        numeric_cols = list(infra_df.select_dtypes(include=['number']).columns)
        numeric_cols = [e for e in numeric_cols if e not in num_cols_to_exclude]

        for col in numeric_cols:
            print('Attempting ' + col)
            df_out = numericfields_stats_by_country(infra_df, col, 'COUNTRY')
            sheetname_ = col + '_Stats'
            df_out2 = df_out.set_index(df_out.columns[0:2].tolist())
            df_out2.to_excel(writer, sheet_name=sheetname_, index=True)
            auto_adjust_excel_column_width(writer, sheetname_, df_out)
            # merge_excel_cells(writer, sheetname_, df_out, 'Country')
            # merge_excel_cells(writer, sheetname_, df_out, 'Number of non-null value(s)')
            print(sheetname_ + ' exported successfully')

        # Create tab dedicated to checking for "allowed vs not-allowed" nulls
        df_out = check_for_allowed_nans_excel_report(infra_df)
        sheetname_ = 'Forbidden_Nulls'
        df_out.to_excel(writer, sheet_name=sheetname_, index=False)
        auto_adjust_excel_column_width(writer, sheetname_, df_out)
        # merge_excel_cells(writer, sheetname_, df_out, 'Attribute that should NOT contain nulls')
        print(sheetname_ + ' exported successfully')

        # Create tab dedicated to finding and reporting duplicate records
        df_out = check_for_duplicate_records(infra_df)
        sheetname_ = 'Duplicate_Records'
        df_out.to_excel(writer, sheet_name=sheetname_, index=False)
        auto_adjust_excel_column_width(writer, sheetname_, df_out)
        # merge_excel_cells(writer, sheetname_, df_out, 'COUNTRY')
        print(sheetname_ + ' exported successfully')

        # Finally, create a "directory" list of all sheets in the Excel doc
        mysheetlist = list(writer.sheets.keys())
        df_out = pd.DataFrame({'Sheet Names, in order': mysheetlist})
        sheetname_ = 'ALL_SHEET_NAMES'
        df_out.to_excel(writer, sheet_name=sheetname_, index=False)
        auto_adjust_excel_column_width(writer, sheetname_, df_out)
        print(sheetname_ + ' exported successfully')
