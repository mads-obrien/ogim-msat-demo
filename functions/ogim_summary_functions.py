# -*- coding: utf-8 -*-
"""
Created on Mon Feb 14 15:53:07 2022

functions to help summarize the OGIM database, for internal or external review presentations

TODO as of June 23: Add WAY more documentation to each of these functions. -MAO

@author: maobrien
"""
import pandas as pd


def get_uniques(geopackage, column_name, which_layers=None):
    '''
    If 'which_layers'==None, then ALL layers are examined
    If 'which_layers' is provided a list of layers, then only those layers are examined
    '''
    if which_layers==None:
        which_layers = geopackage.keys()
    uniques_list = ['to_drop']
    for lyr in which_layers:

        uniques_list.extend(geopackage[lyr][column_name].unique().tolist())
        uniques_list_reduced = list(set(uniques_list))
        uniques_df = pd.DataFrame({column_name: uniques_list_reduced})
        uniques_df = uniques_df[uniques_df[column_name] != 'to_drop'].reset_index(drop=True)
    
    uniques_df_list = sorted(uniques_df[column_name].tolist())
    return uniques_df_list

#  EXAMPLE
# test_outdf = get_uniques(gpkg,'SRC_URL',test_lyrlist)


def countbycountry(data, counts_field, countrycounts):
    countries = data.COUNTRY.unique()
    countries = [x for x in countries if x is not None]
    for country in countries:   # Per country, calculate total number of features
        value = len(data[data.COUNTRY == country])
        countrycounts.at[country, counts_field] = value
        print(country + " has " + str(value) + " " + counts_field)
    countrycounts.at['TOTAL', counts_field] = int(countrycounts[counts_field].sum())


def countbystateprov(data, counts_field, provcounts):
    provs = data.STATE_PROV.unique()
    provs = [x for x in provs if x is not None]
    for prov in provs:   # Per prov, calculate total number of features
        value = len(data[data.STATE_PROV == prov])
        provcounts.at[prov, counts_field] = value
        print(prov + " has " + str(value) + " " + counts_field)
    provcounts.at['TOTAL', counts_field] = int(provcounts[counts_field].sum())


def countbyregion(data, counts_field, regcounts):
    regs = data.REGION.unique()
    regs = [x for x in regs if x is not None]
    for reg in regs:   # Per prov, calculate total number of features
        value = len(data[data.REGION == reg])
        regcounts.at[reg, counts_field] = value
        print(reg + " has " + str(value) + " " + counts_field)
    regcounts.at['TOTAL', counts_field] = int(regcounts[counts_field].sum())


def howmanyoperator(data, counts_field, countrycounts, nodatavals):
    '''
    How many records have a non-null 'OPER_NAME' field, by country and by infra category
    
    NOTE: this function needs work, doesn't run as expected right now
    '''
    countries = data.COUNTRY.unique()
    countries = [x for x in countries if x is not None]
    for country in countries:
        try:
            numops = len(data[(-data.OPER_NAME.isin(nodatavals)) & (data.COUNTRY==country)])
            countrycounts.at[country, counts_field+'_op'] = numops
            print(country+" has "+str(numops)+" "+counts_field+" records with operator info")
            
            value = len(data[data.COUNTRY==country])
            pctops = numops / value
            countrycounts.at[country, counts_field+'_op_pct'] = pctops
            
        except AttributeError as e:
            print(e)
            print(country+" "+counts_field+" skipped")
            continue
        
    countrycounts.at['TOTAL', counts_field+'_op'] = int(countrycounts[counts_field+'_op'].sum())
    countrycounts.at['TOTAL', counts_field+'_op_pct'] = int(countrycounts[counts_field+'_op_pct'].sum())



def howmanystatus(data, counts_field, countrycounts, nodatavals):
    '''
    How many records have a non-null 'FAC_STATUS' field, by country and by infra category
    
    NOTE: this function needs work, doesn't run as expected right now
    '''
    countries = data.COUNTRY.unique()
    countries = [x for x in countries if x is not None]
    for country in countries:
        try:
            numstat = len(data[(-data.FAC_STATUS.isin(nodatavals)) & (data.COUNTRY==country)])
            countrycounts.at[country, counts_field+'_status'] = numstat
            print(country+" has "+str(numstat)+" "+counts_field+" records with status info")
            
            value = len(data[data.COUNTRY==country])
            pctstat = numstat / value
            countrycounts.at[country, counts_field+'_st_pct'] = pctstat
            
        except AttributeError as e:
            print(e)
            print(country+" "+counts_field+" skipped")
            continue
    
    countrycounts.at['TOTAL', counts_field+'_status'] = int(countrycounts[counts_field+'_status'].sum())
    countrycounts.at['TOTAL', counts_field+'_st_pct'] = int(countrycounts[counts_field+'_st_pct'].sum())
       
        
        
def howmuchpipe(pipes, borders, country_field, countrycounts):
    '''
    Measures how many KM of pipeline exists within the geospatial borders of different countries
    '''
    print("CRS of input pipelines: ")
    print(pipes.crs)
    print("CRS of input country borders: ")
    print(borders.crs)
    # get list of what countries have pipes in them
    countries = pipes.COUNTRY.unique().tolist()
    # Use that list of countries as iterator in next step
    for country in countries:
        try:
            print(country+" pipeline count started at "+str(datetime.now().time()))
            pipeclip = gpd.clip(pipes, borders[borders[country_field]==country])
            sumlengthkm = np.sum(pipeclip.geometry.length) / 1000
            countrycounts.at[country, "pipeline_km"] = sumlengthkm
            print(country+" has "+str(sumlengthkm)+" kilometers of pipeline")
        except KeyError:
            print(country+" - Region skipped")
            pass