# -*- coding: utf-8 -*-
"""
Created on Tue Apr  5 13:09:40 2022

Functions to assist in creating a hybridized version of the OGIM database

@author: maobrien
"""
import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
# Github repo to import functions
os.chdir("C:\\Users\\maobrien\\Documents\\GitHub\\ogim-msat\\functions")
from gridify import percentage_dif
from tqdm import tqdm

# Dictionary of infra layer names in the geopackage,
# and an abbreviated version to use in their variable name in create_prehybrid_table
infra_abbrev = {'Wells':'well',
                'Oil_Natural_Gas_Wells':'well',
                
                'Offshore Platforms':'plat',
                'Offshore_Platforms':'plat',
                
                'Compressor Stations':'cs',
                'Compressor_Stations':'cs',
                'CompressorStations':'cs',
                'Natural_Gas_Compressor_Stations':'cs',
                
                'Stations - Other':'stat',
                'Stations_Other':'stat',
                
                'Processing Plants':'proc',
                'Processing_Plants':'proc',
                'Processing_Facilities':'proc',
                'ProcessingFacilities':'proc',
                'Natural_Gas_Processing_Facilities':'proc',
                
                'Refineries':'ref',
                'Crude_Oil_Refineries':'ref',
                
                'LNG':'lng',
                'LNG_Facilities':'lng',
                
                'Tank Batteries':'tank',
                'Tank_Batteries':'tank',
                
                'Equipment_Components':'equip',
                
                'Petroleum Terminals':'term',
                'Petroleum_Terminals':'term',
                'Terminals':'term',
                
                'Injection and Disposal':'injd',
                'Injection_Disposal':'injd',
                
                'Pipelines':'pipe',
                'Oil_Natural_Gas_Pipelines':'pipe',
                
                'Oil and Gas License Blocks':'blk',
                'Oil and Gas Fields':'fld',
                'Oil_Natural_Gas_Fields':'fld',
                'Oil and Gas Basins':'bsn'
                }

def get_uniques(geopackage, column_name, which_layers=None):
    # print(which_layers)
    if which_layers==None:
        which_layers = geopackage.keys()
        # print('which_layers is none')
    uniques_list = ['to_drop']
    # print('uniques_list created')
    for lyr in which_layers:
        # print(lyr)
        # print(geopackage[lyr][column_name].unique())
        uniques_list.extend(geopackage[lyr][column_name].unique().tolist())
        uniques_list_reduced = list(set(uniques_list))
        uniques_df = pd.DataFrame({column_name: uniques_list_reduced})
        uniques_df = uniques_df[uniques_df[column_name] != 'to_drop'].reset_index(drop=True)

    # print('Number of unique values:')
    # print(len(uniques_df[column_name]))
    # print(uniques_df[column_name])
    
    uniques_df_list = sorted(uniques_df[column_name].tolist())
    return uniques_df_list

def create_prehybrid_table(ogimdata, envdata, infraname, subregion_col):
    '''
    First step in creating a hybridized infrastructure database for one infrastructure category. 
    
    For each specified subregion (e.g., country, state, county), determine which dataaset 
    (public or proprietary) will be retained via a decision tree.
    
    Parameters
    ----------
    ogimdata : geodataframe
        gdf containing infrastructure locations for one infrastructure category from public data sources
    envdata : geodataframe
        gdf containing infrastructure locations for one infrastructure category from Enverus
    infraname : string
        Short string that will be used as part of the column names in the returned table. 
        Ideally, string provided is an abbreviation of the infrastructure category being processsed
    subregion_col : string
        name of column present in both ogimdata and envdata geodataframes that 
        specifies which sub-region each record resides in. This column 
        designates the 'sub-region' used in the analysis. 

    Returns
    -------
    pandas dataframe
    
    Example usage
    -------
    winnertable = create_prehybrid_table(refineries_ogim, refineries_env, 'ref','COUNTRY')

    '''
    
    # Create columns that report total number of features in each subregion, for OGIM and Enverus data. 
    # Add columns that report the raw difference and percentage difference in feature counts between OGIM and ENV datasets.
    ogimcol = infraname+'_ogim'
    envcol = infraname+'_enverus'
    ogimdata_count = ogimdata.groupby(subregion_col).size().to_frame(ogimcol)
    envdata_count = envdata.groupby(subregion_col).size().to_frame(envcol)
    prehyb_table = pd.concat([ogimdata_count, envdata_count], axis=1).fillna(0)
    prehyb_table[infraname+'_diff'] = prehyb_table[ogimcol] - prehyb_table[envcol]
    prehyb_table[infraname+'_pctdiff'] = prehyb_table[[ogimcol,envcol]].apply(lambda x: percentage_dif(x[ogimcol], x[envcol]), axis=1)
    
    # Create column indicating whether OGIM or ENV data have ANY records ('coverage') in a particular subregion
    covcol = infraname+'_coverage'
    prehyb_table[covcol] = 'No coverage'
    prehyb_table.loc[(prehyb_table[ogimcol] != 0) & (prehyb_table[envcol] == 0),covcol] = 'OGIM only'
    prehyb_table.loc[(prehyb_table[ogimcol] == 0) & (prehyb_table[envcol] != 0),covcol] = 'Enverus only'
    prehyb_table.loc[(prehyb_table[ogimcol] != 0) & (prehyb_table[envcol] != 0),covcol] = 'Both'
    prehyb_table = prehyb_table.rename_axis('subregion').reset_index(drop=False)


    # Create empty column to contain results of decision tree (which dataset to keep)
    prehyb_table['winner'] = None
    
    # Iterate over each sub-region (row) in the output table
    for i in prehyb_table.index:  
        # If a subregion only has coverage from one data source, that data source is kept
        # skip row right away if no coverage from either dataset
        if prehyb_table[covcol][i]=='No coverage':
            prehyb_table.at[i,'winner'] = 'No coverage'
            continue
        if prehyb_table[covcol][i]=='OGIM only':
            prehyb_table.at[i,'winner'] = 'OGIM only'
            continue
        if prehyb_table[covcol][i]=='Enverus only':
            prehyb_table.at[i,'winner'] = 'Enverus only'
            continue          
        
        # If one dataset is "significantly larger" than the other, retain the larger dataset
        # If there is a significant difference, use the sign of the % difference to determine which dataset is larger 
        # A positive % difference = OGIM bigger; A negative % difference = ENV bigger
        cutoff = 5 # use five percent as the threshold for "significantly different"
        pctdiff = prehyb_table[infraname+'_pctdiff'][i]
        if abs(pctdiff) >= cutoff: 
            if pctdiff > 0:
                prehyb_table.at[i,'winner'] = 'OGIM bigger'
            if pctdiff < 0:
                prehyb_table.at[i,'winner'] = 'Enverus bigger'
       
        #if count difference isn't significant... 
        else:
            # keep data published more recently -- Enverus 2021 > 2020 (most contemporary OGIM)
            prehyb_table.at[i,'winner'] = 'Enverus date'      
        # TODO: In case of same year, keep dataset that reports status
        
    # For ease fo analysis, make the 'winner' column binary
    prehyb_table.loc[prehyb_table['winner'].str.contains('Enverus'),'winner_new'] = 'Enverus'
    prehyb_table.loc[prehyb_table['winner'].str.contains('OGIM'),'winner_new'] = 'OGIM'
        
    return prehyb_table

        
def hybrid_plot(gridmerge, basin_gdf, 
                basin_name, infra_name, map_annotation, 
                myPalette, figuresize):
    # Create canvas
    fig, ax = plt.subplots(1, 1, figsize=figuresize)
    
    # Write title and annotations
    ax.set_title(infra_name+' in '+basin_name+' basin')
    plt.text(0, -.08, map_annotation, transform = ax.transAxes) # text at bottom left of figure
    
    # Crop map to basin of interest
    xlim = ([basin_gdf.total_bounds[0],  basin_gdf.total_bounds[2]])
    ylim = ([basin_gdf.total_bounds[1],  basin_gdf.total_bounds[3]])
    ax.set_xlim(xlim)
    ax.set_ylim(ylim)
    
    # Plot data layers
    gridmerge.plot(color=gridmerge['winner'].map(myPalette), ax=ax, legend=True)
    # conus.boundary.plot(color='black', linewidth=0.5, ax=ax)
    basin_gdf.boundary.plot(color='red', linewidth=0.5, ax=ax)
    # targets_.boundary.plot(color='pink',ax=ax)
    
    # add a legend
    handles = [Patch(facecolor=v, edgecolor='w', label=k,) for k, v in myPalette.items()]
    plt.legend(handles=handles, ncol=2, bbox_to_anchor=(1.05, 0))
# loc=legendloc,
    # Remove easting-northing labels
    plt.setp(ax, xticks=[], yticks=[])
    
    

def create_hybrid_layer(ogimdata, envdata, output_winners_table, sr_name):
    '''
    Uses the results of create_prehybrid_table() to combine OGIM and Enverus data into a hybridized geodataframe

    Parameters
    ----------
    ogimdata : TYPE
        DESCRIPTION.
    envdata : TYPE
        DESCRIPTION.
    output_winners_table : TYPE
        DESCRIPTION.
    sr_name : TYPE
        DESCRIPTION.

    Returns
    -------
    outdf : TYPE
        DESCRIPTION.

    '''
    
    # Create list of subregions in which OGIM and ENV should be kept, respectively
    envWinnerList = list(output_winners_table.subregion[output_winners_table.winner_new=='Enverus'])
    ogimWinnerList = list(output_winners_table.subregion[output_winners_table.winner_new=='OGIM'])
    
    # Add column to specify if record comes from public or proprietary source
    ogimdata['PUB_PRIV'] = 'PUBLIC'
    envdata['PUB_PRIV'] = 'PROPRIETARY'
    
    # Add a column to both original infrastructure geodataframes, and based on the subregion the record resides in,
    # flag if that particular record should be retained for the hybrid output 
    ogimdata['keep4hybrid'] = 'No'
    ogimdata.loc[ogimdata[sr_name].isin(ogimWinnerList),'keep4hybrid'] = 'Yes'
    envdata['keep4hybrid'] = 'No'
    envdata.loc[envdata[sr_name].isin(envWinnerList),'keep4hybrid'] = 'Yes'

    
    # TODO -- Might need to make some additional records read "keep4hybrid = Yes"
    # Based on the buffer analysis
    
    #Create new column to indicate if the record was added via spatial gap fill method
    ogimdata['spatialgapfilling'] = 'No'
    envdata['spatialgapfilling'] = 'No'
    
    print('How many records kept for hybrid (OGIM) before spatial gap fill:')
    print(ogimdata.keep4hybrid.value_counts())
    print('------------')
    print('How many records kept for hybrid (ENV) before spatial gap fill:')
    print(envdata.keep4hybrid.value_counts())
    print('------------')

    
    # Reproject ogim and enverus data to a distance-preserving projection for drawing radii later
    # print("reprojecting")
    print('starting re-project...')
    ogim_proj = ogimdata.to_crs(crs='ESRI:54032') # Azimuthal Equidistant
    env_proj = envdata.to_crs(crs='ESRI:54032') 
    print('reproject completed.')

    # Iterate over each sub-region (row) in the winners table / pre-hybrid table
    # to determine if any additional infrastructure points should be retained
    # based on spatial location
    
    pctdiffcol = [col for col in output_winners_table.columns if col.endswith('_pctdiff')][0]
    
    print('Iterating over sub-regions and filling gaps...')
    for i in tqdm(output_winners_table.index):
        subregion = output_winners_table.subregion[i]
        value = output_winners_table[pctdiffcol][i]

        # If pctdif is greater than 5%, and there's not just one datasource in the region...
        if abs(value) != 999 and abs(value) > 5:
            
            # draw buffers around OGIM data
            if subregion in ogimWinnerList:
                data_A = ogim_proj[ogim_proj[sr_name]==subregion]
                data_B = env_proj[env_proj[sr_name]==subregion]
            
            # draw buffers around Enverus data
            if subregion in envWinnerList:  
                data_A = env_proj[env_proj[sr_name]==subregion]
                data_B = ogim_proj[ogim_proj[sr_name]==subregion]
                 
            #Add X-meter buffer around points in Dataset A 
            buffs = data_A.buffer(500) # returns geometry Series
            buffs_df = gpd.GeoDataFrame(geometry = buffs) # convert series to gdf
            buffs_dissolve = buffs_df.dissolve()
            
            # intersect points from "not chosen" Dataset B
            data_B_inside = gpd.sjoin(data_B, buffs_dissolve, how='left', op='within')
            data_B_outside = data_B_inside.loc[data_B_inside['index_right'].isna()]
            # numwithinbuff = str(len(data_B) - len(data_B_outside))
            # print(subregion+' has '+numwithinbuff+' refineries WITHIN buffer')
            
            # If there ARE points in data_B outside the buffers, they should be hybridized
            if not data_B_outside.empty:
                print(subregion+' has '+str(len(data_B_outside))+' OUTSIDE of the buffer')
                
                # if buffers were drawn around OGIM data, then designate the Enverus 'outsidepoints' as keepers
                # keep track of which records were added in this way
                if subregion in ogimWinnerList:
                    envdata.loc[envdata.OGIM_ID.isin(list(data_B_outside.OGIM_ID)),'keep4hybrid'] = 'Yes'
                    envdata.loc[envdata.OGIM_ID.isin(list(data_B_outside.OGIM_ID)),'spatialgapfilling'] = 'Yes'
                    
                # if buffers were drawn around ENV data, then designate the OGIM 'outsidepoints' as keepers
                # keep track of which records were added in this way
                if subregion in envWinnerList:     
                    ogimdata.loc[ogimdata.OGIM_ID.isin(list(data_B_outside.OGIM_ID)),'keep4hybrid'] = 'Yes'  
                    ogimdata.loc[ogimdata.OGIM_ID.isin(list(data_B_outside.OGIM_ID)),'spatialgapfilling'] = 'Yes'

    # Remove all records from ENV and OGIM data that are not to be hybridized
    ogim2hybridize = ogimdata[ogimdata.keep4hybrid=='Yes']
    env2hybridize = envdata[envdata.keep4hybrid=='Yes']
    
    # Combine all 'to-keep' records into a single geodataframe
    outdf = ogim2hybridize.append(env2hybridize).reset_index(drop=True)
    
    # Remove temporary layer used by this function
    outdf = outdf.drop('keep4hybrid', axis=1)
    
    print('How many records kept for hybrid (OGIM) AFTER spatial gap fill:')
    print(ogimdata.keep4hybrid.value_counts())
    print('------------')
    print('How many records kept for hybrid (ENV) AFTER spatial gap fill')
    print(envdata.keep4hybrid.value_counts())
    print('------------') 
    
    buffs_export = buffs_df.to_crs(epsg = 4326)
    
    return outdf, buffs_export


def create_subregion_field(gdf):
    ''' Create `SUBREGION` field for use in hybridization '''
    if not 'SUBREGION' in gdf.columns:
        gdf['SUBREGION'] = gdf['COUNTRY']
        
        gdf.loc[(gdf.COUNTRY.str.upper() == 'CANADA'),'SUBREGION'] = gdf['STATE_PROV']
        gdf.loc[(gdf.COUNTRY.str.upper() == 'UNITED STATES'),'SUBREGION'] = gdf['STATE_PROV']
    else:
        print('SUBREGION field already exists in table')
    
    