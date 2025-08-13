# -*- coding: utf-8 -*-
"""
Data integration of oil and natural gas production data, 21 US states total.

WELL LEVEL:
    Alaska, California, Colorado, Kentucky, Mississippi, Montana, New Mexico,
    New York, North Dakota, Ohio, Pennsylvania, Utah, West Virginia, Wyoming
LEASE LEVEL:
    Kansas, Louisiana, Missouri, Texas
FIELD LEVEL:
    Alabama
COUNTY LEVEL:
    Arkansas
PRODUCTION REPORTING UNIT (PRU):
    Michigan

@authors: momara, maobrien, ahimmelberger, ghauser

"""
import os
# import re
import pandas as pd
import geopandas as gpd
import numpy as np
import glob
from tqdm import tqdm
import matplotlib.pyplot as plt
import datetime

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import (integrate_production, replace_row_names,
                     save_spatial_data, schema_OIL_GAS_PROD, read_msAccess,
                     clean_a_date_field, create_concatenated_well_name,
                     get_msAccess_table_names)
from internal_review_protocol_Excel import create_internal_review_spreadsheet

import cartopy.crs as ccrs
from matplotlib.axes import Axes
from cartopy.mpl.geoaxes import GeoAxes
GeoAxes._pcolormesh_patched = Axes.pcolormesh
import cartopy.feature as cfeature
# import matplotlib.ticker as mticker
# from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
# from mpl_toolkits.axes_grid1 import make_axes_locatable
# from matplotlib.colors import BoundaryNorm
# from matplotlib.ticker import MaxNLocator
# from cartopy.io.shapereader import Reader
# import cartopy.io.shapereader as shpreader
# import matplotlib.ticker as ticker
# from cartopy.io.img_tiles import Stamen

# ======================================================
# %% Set file paths
# ======================================================
# Set current working directory to the Public_Production folder
os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Production_v0\data')
# Set destination folder for exported SHP and JSON outputs
# make sure to end the string with double backslash!
results_folder = "C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory\\Public_Production_v0\\integrated_results\\"

states2keep = ['ALASKA',
               'ARKANSAS',
               'CALIFORNIA',
               'COLORADO',
               'KANSAS',
               'KENTUCKY',
               'LOUISIANA',
               'MICHIGAN',
               'MISSISSIPPI',
               'MONTANA',
               'NEW MEXICO',
               'NEW YORK',
               'NORTH DAKOTA',
               'OHIO',
               'PENNSYLVANIA',
               'UTAH',
               'WEST VIRGINIA',
               'WYOMING']

# ======================================================
# %% Define custom functions
# ======================================================


def populate_before_after_table_post_integration(i, df, gdf_integrated):

    # populate the columns related to OIL and GAS
    for h, col in zip(['oil', 'gas', 'cond'], ['OIL_BBL', 'GAS_MCF', 'CONDENSATE_BBL']):

        # First, cast any missing values as nan instead of -999, so they don't
        # throw off the sum
        gdf_integrated[col] = gdf_integrated[col].replace({-999: np.nan,
                                                           '-999': np.nan})
        # Record the total hydrocarbon produced in 2022 AFTER integration
        df.at[i, f'{h}_geojson'] = gdf_integrated[col].sum()
        # Calculate the percentage
        x = (df.loc[i, f'{h}_geojson'] / df.loc[i, f'{h}_original'])
        x_as_pct = "{:.4%}".format(x)
        df.at[i, f'{h}_pct_in_geojson'] = x_as_pct


def utm_from_lon(lon):
    """
    utm_from_lon - UTM zone for a longitude

    Not right for some polar regions (Norway, Svalbard, Antartica)

    :param float lon: longitude
    :return: UTM zone number
    :rtype: int
    """
    return floor((lon + 180) / 6) + 1

# -----------------------------------------------------------------------------
# Scale bar
# -----------------------------------------------------------------------------


def scale_bar(
    ax,
    proj,
    length,
    location=(0.5, 0.05),
    linewidth=3,
    units='km',
    m_per_unit=1000
):
    """
    ax is the axes to draw the scalebar on.
    proj is the projection the axes are in
    location is center of the scalebar in axis coordinates ie. 0.5 is the middle of the plot
    length is the length of the scalebar in km.
    linewidth is the thickness of the scalebar.
    units is the name of the unit
    m_per_unit is the number of meters in a unit
    """
    # find lat/lon center to find best UTM zone
    x0, x1, y0, y1 = ax.get_extent(proj.as_geodetic())
    # Projection in metres
    utm = ccrs.UTM(utm_from_lon((x0 + x1) / 2))
    # Get the extent of the plotted area in coordinates in metres
    x0, x1, y0, y1 = ax.get_extent(utm)
    # Turn the specified scalebar location into coordinates in metres
    sbcx, sbcy = x0 + (x1 - x0) * location[0], y0 + (y1 - y0) * location[1]
    # Generate the x coordinate for the ends of the scalebar
    bar_xs = [sbcx - length * m_per_unit / 2, sbcx + length * m_per_unit / 2]
    # buffer for scalebar
    buffer = [patheffects.withStroke(linewidth=5, foreground="w")]
    # Plot the scalebar with buffer
    ax.plot(bar_xs, [sbcy, sbcy], transform=utm, color='k',
            linewidth=linewidth, path_effects=buffer)
    # buffer for text
    buffer = [patheffects.withStroke(linewidth=3, foreground="w")]
    # Plot the scalebar label
    t0 = ax.text(sbcx, sbcy, str(length) + ' ' + units, transform=utm,
                 horizontalalignment='center', verticalalignment='bottom',
                 path_effects=buffer, zorder=2)
    left = x0 + (x1 - x0) * 0.05
    # Plot the N arrow
    t1 = ax.text(left, sbcy, u'\u25B2\nN', transform=utm,
                 horizontalalignment='center', verticalalignment='bottom',
                 path_effects=buffer, zorder=2)
    # Plot the scalebar without buffer, in case covered by text buffer
    ax.plot(bar_xs, [sbcy, sbcy], transform=utm, color='k',
            linewidth=linewidth, zorder=3)

# -----------------------------------------------------------------------------
# Sactter maps
# -----------------------------------------------------------------------------


def scatterMaps(
    gdf,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    showPipelines=False,
    pipe_len_attrName='PIPELEN_KM',
    fpShp=None,
    figWidth=8,
    figHeight=8,
    markerSize=3,
    markerColor='darkblue',
    NA_SA_extent=True,
    figTitle=None,
    dataLabel="Wells",
    showLegend=True,
    saveFigPath=None,
    showArea=False,
    showScale=False,
    heatMap=False,
    colorAttr=None,
    dataScaling=1,
    colorMapName="gist_rainbow_r",
    axisLabels=True,
    showStates=True,
    showBorders=True,
    showCoast=True,
    showLand=False,
    showOcean=False,
    showLakes=False,
    showRivers=False
):
    '''
    Inputs:
    ---
        gdf: dataframe or geodataframe.
        lat_lon: bool, if True, no need to calculate these values ('latitude', 'longitude') based on the dataset geometries.
        fpShp: Path to shapefile if it is desired to overlay a polygon boundary on the scatter map
        figHeight: figure height
        figWidth: figure width
        showPipelines: bool, if True, plots pipeline data instead of scatter map
        pipe_len_attrName: Attribute name for data for length of pipeline for each feature in gdf
        markerSize: for simple scatter plots without scaling or heatmaps, specify marker size
        markerColor: marker color based on MatplotLib standard color names
        NA_SA_extent: if True, forces figure extent to the North America/South America extent
        figTitle: show figure title
        dataLabel: displays legend or colorbar axis label if heatmap is plotted
        showLegend: if True, display legend
        showArea: if True, displays the polygon in fpShp
        showScale: if True, displays a 100-km scale on the map
        heatMap: if True, plots the points and colors them by colorAttr
        colorAttr: attribute for sizing and coloring the scattered points
        dataScaling: allows for modification of scatter point marker size
        colorMapName: name of MatplotLib's color ramp to use in plotting heatmap
        axisLabels: if True, label x and y axis of figure object as Longitude and Latitude
        showStates: if True, show state boundaries
        showBorders: if True, show country borders
        showCoast: if True, show coast line
        showLand: if True, show land in light yellow
        showOcean: if True, show ocean in light blue
        showLakes: if True, show lakes
        showRivers: if True, show rivers

    Returns:
    ---
        fig: scatter map or line map

    '''
    import matplotlib.ticker as ticker
    import matplotlib.pyplot as plt
    import geopandas as gpd
    import cartopy.crs as ccrs

    # scatter plot
    # ======================================
    # For setting extents
    # Check that data is in EPSG:4326
    if gdf.crs == "EPSG:4326" and showPipelines == False:
        if lat_lon == True:
            dataLon, dataLat = gdf[lon_attribute], gdf[lat_attribute]  # If latitude and longitude labels are included as attributes in the data
        else:
            dataLon, dataLat = gdf.geometry.x, gdf.geometry.y  # For geodataframe in EPSG:4326 with no latitude/longitude attributes
            gdf['longitude'] = dataLon
            gdf['latitude'] = dataLat
        # Creates the map
        fig = plt.figure(figsize=(figWidth, figHeight))

        # Fig. axis decorations
        ca_map = fig.add_subplot(111, projection=ccrs.PlateCarree())

        if showLand == True:
            ca_map.add_feature(cfeature.LAND)
        if showOcean == True:
            ca_map.add_feature(cfeature.OCEAN)
        if showBorders == True:
            ca_map.add_feature(cfeature.BORDERS, linestyle='-', lw=1, edgecolor='gray')
        if showCoast == True:
            ca_map.add_feature(cfeature.COASTLINE, linestyle='-', lw=1, edgecolor='gray')
        if showLakes == True:
            ca_map.add_feature(cfeature.LAKES, alpha=0.5)
        if showRivers == True:
            ca_map.add_feature(cfeature.RIVERS)
        if showStates == True:
            ca_map.add_feature(cfeature.STATES.with_scale('10m'), lw=0.2, edgecolor='gray')

        ca_map.tick_params(direction='out', length=6, width=1.6, labelsize=16)

        # If desired to aoverlay polygon boundary (.shp) of specific region on the map
        if fpShp is not None:
            reader = shpreader.Reader(fpShp)
            area = list(reader.geometries())
            AREA = cfeature.ShapelyFeature(area, ccrs.PlateCarree())

        if showArea == True:
            ca_map.add_feature(AREA, facecolor='none', edgecolor='darkred', lw=3)

        # Set approximate map extent
        if NA_SA_extent == True:
            ca_map.set_extent([-160, -30, -60, 75], ccrs.PlateCarree())
        elif NA_SA_extent == False:
            try:
                ca_map.set_extent([dataLon.min() - 0.5, dataLon.max() + 0.5, dataLat.min() - 0.5, dataLat.max() + 0.5], ccrs.PlateCarree())
            except:
                print("Not setting map extent")
                pass

        ca_map.xaxis.set_visible(True)
        ca_map.yaxis.set_visible(True)

        # Plots the data onto map
        if heatMap == False:
            scatter_ = ca_map.scatter(
                np.array(gdf[lon_attribute]),
                np.array(gdf[lat_attribute]),
                s=markerSize,
                label=dataLabel,
                c=markerColor,
                transform=ccrs.PlateCarree()
            )
        elif heatMap == True:
            # Sort values first
            gdf = gdf.sort_values(by=colorAttr, ascending=True).reset_index(drop=True)

            scatter_ = ca_map.scatter(
                np.array(gdf[lon_attribute]),
                np.array(gdf[lat_attribute]),
                s=np.array(gdf[colorAttr]) / dataScaling,
                label=dataLabel,
                c=np.array(gdf[colorAttr]),
                cmap=plt.get_cmap(colorMapName),
                transform=ccrs.PlateCarree()
            )
            # Colorbar
            cbar = plt.colorbar(scatter_, ax=ca_map)
            cbar.set_label(dataLabel, fontsize=16)
            cbar.ax.tick_params(labelsize=14)

        # Axes labels
        if axisLabels == True:
            ca_map.set_ylabel("Latitude", fontsize=20)
            ca_map.set_xlabel("Longitude", fontsize=20)

        # Add scale bar
        if showScale == True:
            scale_bar(ca_map, ccrs.PlateCarree(), 100, location=(0.25, 0.05))  # 100 km scale bar

        # Figure title
        if figTitle is not None:
            ca_map.set_title(figTitle + " $n = $" + str(gdf.shape[0]), fontsize=18, fontweight='bold', loc='left')

        # Show legend
        if showLegend == True:
            ca_map.legend(fontsize=16, fancybox=True, shadow=True, loc='lower left', markerscale=4.)

        # Show degree symbols in X and Y axes
        ca_map.xaxis.set_major_formatter(ticker.FormatStrFormatter("%.f\N{DEGREE SIGN}"))
        ca_map.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.f\N{DEGREE SIGN}"))

        ca_map.tick_params(bottom=True, left=True)

    elif showPipelines == True:
        # Creates the map
        fig = plt.figure(figsize=(figWidth, figHeight))

        # Fig. axis decorations
        ca_map = fig.add_subplot(111, projection=ccrs.PlateCarree())

        if showLand == True:
            ca_map.add_feature(cfeature.LAND)
        if showOcean == True:
            ca_map.add_feature(cfeature.OCEAN)
        if showBorders == True:
            ca_map.add_feature(cfeature.BORDERS, linestyle='-', lw=1, edgecolor='gray')
        if showCoast == True:
            ca_map.add_feature(cfeature.COASTLINE, linestyle='-', lw=1, edgecolor='gray')
        if showLakes == True:
            ca_map.add_feature(cfeature.LAKES, alpha=0.5)
        if showRivers == True:
            ca_map.add_feature(cfeature.RIVERS)
        if showStates == True:
            ca_map.add_feature(cfeature.STATES.with_scale('10m'), lw=0.1, edgecolor='gray')

        ca_map.tick_params(direction='out', length=6, labelsize=14)

        # If desired to aoverlay polygon boundary (.shp) of specific region on the map
        if fpShp is not None:
            reader = shpreader.Reader(fpShp)
            area = list(reader.geometries())
            AREA = cfeature.ShapelyFeature(area, ccrs.PlateCarree())

        if showArea == True:
            ca_map.add_feature(AREA, facecolor='none', edgecolor='darkred', lw=3)

        # Set approximate map extent
        if NA_SA_extent == True:
            ca_map.set_extent([-160, -30, -60, 75], ccrs.PlateCarree())
        elif NA_SA_extent == False:
            try:
                ca_map.set_extent([dataLon.min() - 0.5, dataLon.max() + 0.5, dataLat.min() - 0.5, dataLat.max() + 0.5], ccrs.PlateCarree())
            except:
                print("Not setting map extent")
                pass

        ca_map.xaxis.set_visible(True)
        ca_map.yaxis.set_visible(True)

        # Plot
        pipes_ = gdf.plot(ax=ca_map, ls='-', color='blue', lw=2, transform=ccrs.PlateCarree())

        # Axes labels
        if axisLabels == True:
            ca_map.set_ylabel("Latitude", fontsize=18)
            ca_map.set_xlabel("Longitude", fontsize=18)

        # Add scale bar
        if showScale == True:
            scale_bar(ca_map, ccrs.PlateCarree(), 100, location=(0.25, 0.05))  # 100 km scale bar

        # Figure title
        if figTitle is not None:
            ca_map.set_title(figTitle + " total length = " + str(int(gdf[pipe_len_attrName].sum())) + " km", fontsize=18, fontweight='bold', loc='left')

        # Show legend
        if showLegend == True:
            ca_map.legend(fontsize=16, fancybox=True, shadow=True, loc='lower left', markerscale=4.)

        # Show degree symbols in X and Y axes
        ca_map.xaxis.set_major_formatter(ticker.FormatStrFormatter("%.f\N{DEGREE SIGN}"))
        ca_map.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.f\N{DEGREE SIGN}"))

        print("!! CRS is not set or not in EPSG:4326!!")

    if saveFigPath is not None:
        plt.savefig(saveFigPath)

    return fig
# -----------------------------------------------------------------------------
# Production summary stats
# -----------------------------------------------------------------------------


def check_production_stats(
    gdf_integrated: 'GeoDataFrame' = None,
    gdf_original: 'GeoDataFrame' = None,
    gdf_original_oil_col: str = "None",
    gdf_original_gas_col: str = "None"
):
    """
    Run summary statistics for all of the integrated production data

    Parameters:
    ---
        gdf_integrated: Integrated production dataset, integrated using the `integrate_production` function
        gdf_original: original production dataset from state or country, which may not have been reported with lat/lon coordinates
        gdf_original_oil_col: column name for gas production in barrels in the original dataset
        gdf_original_gas_col: column name for oil production in Mcf in the original dataset

    """
    # -----------------------------------------------------------------------------
    # Select columns of interest
    # -----------------------------------------------------------------------------
    cols_ = [
        "OGIM_ID",
        "OIL_BBL",
        "GAS_MCF",
        "CONDENSATE_BBL",
        "PROD_DAYS",
        "PROD_YEAR",
        "LATITUDE",
        "LONGITUDE"
    ]
    # -----------------------------------------------------------------------------
    # Production summary stats
    # -----------------------------------------------------------------------------
    gdf_sel = gdf_integrated[cols_]
    gdf_sel["GAS_MCF"] = gdf_sel["GAS_MCF"].replace({-999: 0, "-999": 0, np.nan: 0})
    gdf_sel["OIL_BBL"] = gdf_sel["OIL_BBL"].replace({-999: 0, "-999": 0, np.nan: 0})
    gdf_sel["CONDENSATE_BBL"] = gdf_sel["CONDENSATE_BBL"].replace({-999: 0, "-999": 0, np.nan: 0})

    print("==============================")
    print("Total # of wells/features in dataset = ", gdf_sel.shape[0])
    print("Total gas production in {} = {} BCF".format(gdf_sel.PROD_YEAR.unique(), round(gdf_sel.GAS_MCF.sum() / 1e6, 3)))
    print("Total oil production in {} = {} Million BBL".format(gdf_sel.PROD_YEAR.unique(), round(gdf_sel.OIL_BBL.sum() / 1e6, 3)))
    print("Total condensate production in {} = {} Million BBL".format(gdf_sel.PROD_YEAR.unique(), round(gdf_sel.CONDENSATE_BBL.sum() / 1e6, 3)))
    print("Total oil and condensate production in {} = {} Million BBL".format(gdf_sel.PROD_YEAR.unique(), round(gdf_sel.CONDENSATE_BBL.sum() / 1e6, 3) + round(gdf_sel.OIL_BBL.sum() / 1e6, 3)))

    print("==============================")
    print("Basic stats for integrated dataset")
    print(gdf_sel.describe())

    # -----------------------------------------------------------------------------
    # COMPARING PRODUCTION DATA IN ORIGINAL DATASET with PRODUCTION DATA in INTEGRATED DATASET
    # -----------------------------------------------------------------------------
    oil_integrated = gdf_sel.OIL_BBL.sum() / 1e6  # Million barrels
    gas_integrated = gdf_sel.GAS_MCF.sum() / 1e6  # MCF/1E6 ==> billion cubic feet

    print("==============================")
    print("Basic stats for original dataset, production")
    print(gdf_integrated.describe())
    # -----------------------------------------------------------------------------
    oil_original = gdf_original[gdf_original_oil_col].sum() / 1e6
    gas_original = gdf_original[gdf_original_gas_col].sum() / 1e6
    # -----------------------------------------------------------------------------
    print("Total oil production (MM Barrels) in original dataset **VERSUS* integrated dataset = \n ", round(oil_original, 4), " *VS* ", round(oil_integrated, 4))
    print("Total gas production (BCF) in original dataset **VERSUS* integrated dataset = \n ", round(gas_original, 4), " *VS* ", round(gas_integrated, 4))

    # Check production days in integrated dataset
    # -----------------------------------------------------------------------------
    if len(gdf_sel.PROD_DAYS.unique()) > 10:  # Make sure production days are not NULLs in the entire dataset before running checks

        prod_days = np.array(gdf_sel[gdf_sel["PROD_DAYS"] >= 0]["PROD_DAYS"])
        print("Minimum and maximum production days = \n ", np.min(prod_days), " \n ** ", np.max(prod_days))

        # Histogram of production days
        fig, ax = plt.subplots(figsize=(5, 4))
        ax.tick_params(direction="out", length=6, width=1.5, labelsize=15)

        ax.hist(prod_days, density=True)
        ax.set_xlabel("Production days per well/facility", fontsize=15)
        ax.set_ylabel("Density", fontsize=14)

    else:
        print("!!There are not enough non-null values for production days in the dataset")


# ======================================================
# % Function for checking facility type, status and drill trajectory
# ======================================================


def check_fac_type_status_drill(df, type_=None, status_=None, drill_=None):
    """
    Quick check of unique fac types and status and drill type
    """
    print("========================")
    print("Unique facility types")
    if type_ is not None:
        print(df[type_].unique())

    print("========================")
    print("Unique facility status")
    if status_ is not None:
        print(df[status_].unique())

    print("========================")
    print("Unique drilling configuration")
    if drill_ is not None:
        print(df[drill_].unique())


# =============================================================================
# %% Create empty "before and after integration comparison" table
# =============================================================================
before_after_table = pd.DataFrame(index=states2keep,
                                  columns=['oil_original',  # Sum of all 2022 production values from raw data
                                           'oil_agg',  # Sum of all 2022 prod. values after aggregating months together
                                           'oil_geojson',  # Sum of all 2022 prod. values in the integrated geojson result that gets exported
                                           'oil_pct_in_geojson',  # What percent of the original production volume is still reported in the final geojson
                                           'gas_original',
                                           'gas_agg',
                                           'gas_geojson',
                                           'gas_pct_in_geojson',
                                           'cond_original',
                                           'cond_agg',
                                           'cond_geojson',
                                           'cond_pct_in_geojson',
                                           'units_reporting_production_original',  # Count of how many unique APIs, Leases, etc. report production in the original dataset before any cleaning
                                           'units_reporting_production_geojson'  # Count of how many unique APIs, Leases report production in the integrated dataset
                                           ])

# =============================================================================
# %% Read in OGIM well data - takes a WHILE
# =============================================================================
print(datetime.datetime.now())
v251_fp = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\OGIM_v2.6\OGIM_v2.6.gpkg'
ogim_wells = gpd.read_file(v251_fp, layer='Oil_and_Natural_Gas_Wells')
# Drop wells in states you don't need to avoid holding a ton in memory
ogim_wells_usa = ogim_wells[ogim_wells.STATE_PROV.isin(states2keep)].reset_index(drop=True)
del ogim_wells
print(datetime.datetime.now())

# ======================================================
# %% ALASKA [2022] - Read + aggregate production
# DATA downloaded from: https://www.commerce.alaska.gov/web/aogcc/Data.aspx
# ======================================================
# Read AK production well data (takes a WHILE to read)
ak_prod = pd.read_excel(r'alaska\wellproductionpost2000.xlsx')
print(ak_prod.columns)
print(ak_prod.head())

# Select only for 2022 production
ak_prod["report_date"] = pd.to_datetime(ak_prod["ReportDate"])
ak_prod["report_year"] = pd.to_datetime(ak_prod["ReportDate"]).dt.year
ak_prod_2022 = ak_prod.query("report_year == 2022").reset_index(drop=True)
print(ak_prod_2022.head())

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['ALASKA', 'oil_original'] = ak_prod_2022.OilProduced.sum()
before_after_table.at['ALASKA', 'gas_original'] = ak_prod_2022.GasProduced.sum()
# -----------------------------------------------------------------------------

# Convert API-14 into API-10
ak_prod_2022['API10'] = ak_prod_2022.Api.astype(str).str[:-4]

# There are a small number of APIs that report more than one row of production
# in a single month, and that's because each row reports the production from a
# particular formation/pool that API has access to.
dupes = ak_prod_2022[ak_prod_2022.duplicated(subset=['Api', 'ReportDate'], keep=False)]

# FIRST, group the API-14 rows so that there's only one API-14 record per month.
# This step prevents DaysProduced for wells being double-counted and above 365 in a year
agg_funcs_1 = {
    'API10': 'last',
    'WellName': 'last',
    'OperatorName': 'last',
    'WellStatus': 'last',
    'AreaName': 'last',
    'FieldName': 'last',
    'PadName': 'last',
    'PoolName': 'last',
    'ProductionType': 'last',
    'ProductionMethod': 'last',
    'OilProduced': 'sum',
    'GasProduced': 'sum',
    'WaterProduced': 'sum',
    'DaysProduced': 'max'  # NOTE that handling of this field differs
}
ak_prod_2022 = ak_prod_2022.groupby(by=['Api',
                                        'ReportDate'],
                                    as_index=False).agg(agg_funcs_1)

# THEN, group by API-10s so that you get one record for that well's 2022 annual production
agg_funcs_2 = {
    'WellName': 'last',
    'OperatorName': 'last',
    'WellStatus': 'last',
    'AreaName': 'last',
    'FieldName': 'last',
    'PadName': 'last',
    'PoolName': 'last',
    'ProductionType': 'last',
    'ProductionMethod': 'last',
    'OilProduced': 'sum',
    'GasProduced': 'sum',
    'WaterProduced': 'sum',
    'DaysProduced': 'sum'  # NOTE that handling of this field differs
}
ak_prod_2022 = ak_prod_2022.groupby(by=["API10"],
                                    as_index=False).agg(agg_funcs_2).reset_index(drop=True)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['ALASKA', 'oil_agg'] = ak_prod_2022.OilProduced.sum()
before_after_table.at['ALASKA', 'gas_agg'] = ak_prod_2022.GasProduced.sum()

# =============================================================================
# %% ALASKA - Location data
# =============================================================================
# Read OGIM wells
ak_wells = ogim_wells_usa.query("STATE_PROV == 'ALASKA'")

# Read well-locations data
# ak_wells = pd.read_excel(r"North_America\United_States_\State_Raw_Data\Alaska\Location\2023-10-26\wells.xlsx")
print(ak_wells.columns)
print(ak_wells.head())

# =============================================================================
# %% ALASKA - Merge and clean
# =============================================================================
ak_prod_merged = pd.merge(ak_wells,
                          ak_prod_2022,
                          how="right",
                          left_on="FAC_ID",
                          right_on='API10')
# Convert to GeoDataFrame not necessary, as well's geometry column is preserved
print(ak_prod_merged.head())
print(ak_prod_merged.columns)

# Indicate prod year and ENTITY type
ak_prod_merged["prod_year"] = 2022
ak_prod_merged["entity_type"] = "WELL"

# Check locational accuracy, Plot data
figAK = scatterMaps(
    ak_prod_merged,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr='GasProduced',
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="AK well-level gas production (2022),\n ",
    saveFigPath=results_folder + "AK_2022_production.tiff"
)

# =============================================================================
# %% ALASKA - Integration
# =============================================================================
ak_integrated, ak_errors = integrate_production(
    ak_prod_merged,
    src_ref_id="186, 246",  # FIXED
    src_date="2024-07-10",  # date of production data download
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    on_offshore="ON_OFFSHORE",
    state_prov="ALASKA",
    fac_name="FAC_NAME",
    fac_id="API10",
    fac_type='FAC_TYPE',
    spud_date="SPUD_DATE",
    comp_date="COMP_DATE",
    drill_type='DRILL_TYPE',
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="OilProduced",
    gas_mcf="GasProduced",
    water_bbl="WaterProduced",
    # condensate_bbl=None,
    prod_days="DaysProduced",
    prod_year="prod_year",
    entity_type="entity_type",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# plt.hist(ak_integrated.PROD_DAYS)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('ALASKA',
                                             before_after_table,
                                             ak_integrated)


save_spatial_data(
    ak_integrated,
    "alaska_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% ARKANSAS [2022] - WIP - Read + aggregate production
# =============================================================================
# Read all tables from the MS Access database
# NOTE: individual table names MUST be manually renamed to remove white spaces
# (e.g., `PRU Master` to `PRUMaster`), or else `read_msAccess()` will fail
fp = r'arkansas/AOGC.mdb'
tableNamesIdx_ar, tableNames_ar, dfs_ar = read_msAccess(fp,
                                                        table_subset=['Prod',
                                                                      'WellMaster_SideTrack',
                                                                      'UICMonitor'])
name2index = dict(zip(tableNames_ar, tableNamesIdx_ar))

# -----------------------------------------------------------------------------
# Read the "Production" table from the Access DB into its own dataframe.
prod_index = name2index.get('Prod')
ar_prod_all = dfs_ar[prod_index]
print(ar_prod_all.columns)
print(ar_prod_all.head())

# Filter to 2022 only
ar_prod_2022 = ar_prod_all[ar_prod_all.RptDate.dt.year == 2022].reset_index(drop=True)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['ARKANSAS', 'oil_original'] = ar_prod_2022.OilProd.sum()  # is in BBL
before_after_table.at['ARKANSAS', 'gas_original'] = ar_prod_2022.GasProd.sum()  # is in MCF

# -----------------------------------------------------------------------------
# Read the UICMonitor table
uic_index = name2index.get('UICMonitor')
ar_uic_all = dfs_ar[uic_index]
print(ar_uic_all.columns)
print(ar_uic_all.head())

# Filter to 2022 only
ar_uic_2022 = ar_uic_all[ar_uic_all.RptDate.dt.year == 2022].reset_index(drop=True)
ar_uic_2022.Vol_Liq.sum()
ar_uic_2022.Vol_Gas.sum()

# -----------------------------------------------------------------------------
# Read the "WellMaster" table into its own dataframe. This table is needed to
# join production records, via PruID, to a well API (and subsequently a lat-long)
wellmaster_sidetrack_index = name2index.get('WellMaster_SideTrack')
ar_wellmaster_sidetrack = dfs_ar[wellmaster_sidetrack_index]
# Reduce to just columns I need for the join
pru_to_api = ar_wellmaster_sidetrack[['PRUID', 'API_WellNo']]
# Drop records where a PRUID is NOT mapped to an API
pru_to_api = pru_to_api[pru_to_api.PRUID.notna()].reset_index(drop=True)

# TODO - remove duplicate records for PRUIDs
# # There are three pairs of duplicate PRUID values. After inspecting them all in
# # the AOGC online portal, I've determined they should be dropped so the production
# # volumes from a specific PRUID doesn't get double counted.
# # Sort records so that null latitude values are listed last, then drop duplicate
# # PRUIDs while keeping the "first" record (the ones with a lat-long)
# api_to_pru = api_to_pru.sort_values(by=['Latitude'],
#                                     ascending=[True],
#                                     na_position='last')
# api_to_pru = api_to_pru.drop_duplicates(subset=['PRUID'], keep='first')

# if len(pru_to_api.API_WellNo.unique()) == len(pru_to_api.PRUID.unique()):
pru_to_api_dict = dict(zip(pru_to_api.PRUID, pru_to_api.API_WellNo))

# -----------------------------------------------------------------------------
# Aggregate monthly production to an annual total for each PRUID
agg_funcs_ar = {'PruNumber': 'first',
                'OilProd': 'sum',
                'GasProd': 'sum',
                'WtrProd': 'sum'}
ar_prod_agg = ar_prod_2022.groupby(by=['PruID'],
                                   as_index=False).agg(agg_funcs_ar)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['ARKANSAS', 'oil_agg'] = ar_prod_agg.OilProd.sum()
before_after_table.at['ARKANSAS', 'gas_agg'] = ar_prod_agg.GasProd.sum()

# Add a new column to `ar_prod_agg` that contains the API number for that PruID
ar_prod_agg['api'] = ar_prod_agg.PruID.map(pru_to_api_dict)
# =============================================================================
# %% ARKANSAS - Location data
# =============================================================================
# Read OGIM wells
ar_wells = ogim_wells_usa.query("STATE_PROV == 'ARKANSAS'")
print(ar_wells.columns)
print(ar_wells.head())

# =============================================================================
# %% ARKANSAS - Merge and clean
# =============================================================================
ar_prod_merge = pd.merge(ar_prod_agg,
                         ar_wells,
                         left_on='api',
                         right_on="FAC_ID",
                         how="left")

# Convert df to gdf, since CRS got dropped during the merge
ar_prod_merge = gpd.GeoDataFrame(ar_prod_merge,
                                 geometry='geometry',
                                 crs=4326)

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
ar_prod_merge = ar_prod_merge[ar_prod_merge.geometry.notna()].reset_index(drop=True)
# ar_prod_merge = ar_prod_merge[~ar_prod_merge.geometry.is_empty].reset_index(drop=True)

# ------------------------------------------------------
# Plot data
figAR = scatterMaps(
    ar_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GasProd",
    dataScaling=10000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="AR well-level gas production (2022),\n ",
    saveFigPath=results_folder + "AR_2022_production.tiff"
)

# =============================================================================
# %% ARKANSAS - Integration
# =============================================================================
ar_integrated, ar_errors = integrate_production(
    ar_prod_merge,
    src_date="2024-08-30",
    category="OIL AND NATURAL GAS PRODUCTION",
    src_ref_id="187, 253",  # DONE
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Arkansas",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_TYPE",
    spud_date='SPUD_DATE',
    comp_date='COMP_DATE',
    drill_type='DRILL_TYPE',
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="OilProd",
    gas_mcf="GasProd",
    water_bbl="WtrProd",
    # condensate_bbl=None,
    # prod_days="DaysProducing",
    prod_year="2022",
    entity_type="WELL",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('ARKANSAS',
                                             before_after_table,
                                             ar_integrated)

save_spatial_data(
    ar_integrated,
    "arkansas_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# ======================================================
# %% CALIFORNIA [2022] - Read + aggregate production
# ======================================================
# Read in production datasets
# These datasets have repeated APIs because data is stored monthly
fp = r"california\2022CaliforniaOilAndGasWellMonthlyProduction.csv"
ca_prod = pd.read_csv(fp)
print(ca_prod.columns)
print(ca_prod.head())

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['CALIFORNIA', 'oil_original'] = ca_prod.OilorCondensateProduced.sum()
before_after_table.at['CALIFORNIA', 'gas_original'] = ca_prod.GasProduced.sum()
# -----------------------------------------------------------------------------

# There are some entries with more production days than possible per month
# (for ex., 31 producing days reported in June which has 30 days)
# Repair any of these cases so only the maximum days in each month is reported

# Create a DateTime version of the ProductionReportDate value
ca_prod['ProductionReport_datetime'] = pd.to_datetime(ca_prod['ProductionReportDate'])

# If a record is for a month with 30 days AND it reports more than 30 days of
# production, make it report only 30 days instead
mask_30_day_month = ca_prod['ProductionReport_datetime'].dt.month.isin([4, 6, 9, 11])
mask_over_30_days_producing = ca_prod.DaysProducing > 30
ca_prod.loc[mask_30_day_month & mask_over_30_days_producing, 'DaysProducing'] = 30

# If a record is for a month with 28 days (Feb) AND it reports more than 28 days of
# production, make it report only 28 days instead
mask_february = ca_prod['ProductionReport_datetime'].dt.month.isin([2])
mask_over_28_days_producing = ca_prod.DaysProducing > 28
ca_prod.loc[mask_february & mask_over_28_days_producing, 'DaysProducing'] = 28

# ------------------------------------------------------
# Before aggregating production to annual, convert API from an integer to a string
# (and add the leading zero that is part of the state code portion of the API)
ca_prod.APINumber = ca_prod.APINumber.astype(str)
ca_prod['API12'] = "0" + ca_prod.APINumber
# Convert API-12 into API-10 (well locations are at the API-10 level)
ca_prod['API10'] = ca_prod.API12.str[:-2]
dupes = ca_prod[ca_prod.duplicated(subset=['API10',
                                           'ProductionReport_datetime'],
                                   keep=False)]

# FIRST, group the API-10 rows so that there's only one API-10 record per month.
# This step prevents DaysProducing for wells being double-counted and above 365 in a year
ca_agg_funcs_1 = {
    'WellTypeCode': 'last',
    'OilorCondensateProduced': 'sum',
    'GasProduced': 'sum',
    'WaterProduced': 'sum',
    'DaysProducing': 'max'  # CHECK
}
ca_prod = ca_prod.groupby(by=['API10',
                              'ProductionReport_datetime'],
                          as_index=False).agg(ca_agg_funcs_1)

# THEN, group by API-10s so that you get one record for that well's 2022 annual production
ca_agg_funcs_2 = {
    'WellTypeCode': 'last',
    'OilorCondensateProduced': 'sum',
    'GasProduced': 'sum',
    'WaterProduced': 'sum',
    'DaysProducing': 'sum'  # CHECK
}
ca_prod_agg = ca_prod.groupby(by=['API10'], as_index=False).agg(ca_agg_funcs_2)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['CALIFORNIA', 'oil_agg'] = ca_prod_agg.OilorCondensateProduced.sum()
before_after_table.at['CALIFORNIA', 'gas_agg'] = ca_prod_agg.GasProduced.sum()

# =============================================================================
# %% CALIFORNIA - Read wells
# =============================================================================
# Read OGIM wells
ca_wells = ogim_wells_usa.query("STATE_PROV == 'CALIFORNIA'")
print(ca_wells.columns)
print(ca_wells.head())

# =============================================================================
# %% CALIFORNIA - Merge and clean
# =============================================================================
ca_prod_merge = pd.merge(ca_prod_agg,
                         ca_wells,
                         left_on='API10',
                         right_on="FAC_ID",
                         how="left")

# Convert df to gdf, since CRS got dropped during the merge
ca_prod_merge = gpd.GeoDataFrame(ca_prod_merge,
                                 geometry='geometry',
                                 crs=4326)

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
ca_prod_merge = ca_prod_merge[ca_prod_merge.geometry.notna()].reset_index()
ca_prod_merge = ca_prod_merge[~ca_prod_merge.geometry.is_empty].reset_index()

# ------------------------------------------------------
# Plot data
figCA = scatterMaps(
    ca_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GasProduced",
    dataScaling=10000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="CA well-level gas production (2022),\n ",
    saveFigPath=results_folder + "CA_2022_production.tiff"
)

# Indicate prod year and ENTITY type
# ------------------------------------------------------
ca_prod_merge["prod_year"] = 2022
ca_prod_merge["entity_type"] = "WELL"

# =============================================================================
# %% CALIFORNIA - Integration
# =============================================================================
ca_integrated, ca_errors = integrate_production(
    ca_prod_merge,
    src_date="2024-07-10",  # updated on daily basis
    category="OIL AND NATURAL GAS PRODUCTION",
    src_ref_id="188, 247",  # DONE
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="California",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_TYPE",
    spud_date='SPUD_DATE',
    # comp_date=None,
    drill_type='DRILL_TYPE',
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="OilorCondensateProduced",
    gas_mcf="GasProduced",
    water_bbl="WaterProduced",
    # condensate_bbl=None,
    prod_days="DaysProducing",
    prod_year="prod_year",
    entity_type="entity_type",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('CALIFORNIA',
                                             before_after_table,
                                             ca_integrated)

save_spatial_data(
    ca_integrated,
    "california_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# Quick check of production data
# summ_CA = check_production_stats(ca_integrated,
#                                  ca_prod,
#                                  "OilorCondensateProduced",
#                                  "GasProduced")

# ======================================================
# %% COLORADO [2022] - Read + aggregate production data
# Data available for download here: https://ecmc.state.co.us/data2.html#/downloads
# ======================================================
# Read production data from the MS acc database
# fp_prod = r"North_America\United_States_\State_Raw_Data\Colorado\Production\CO 2022 Annual Production.xlsx"

# fp = r'colorado/CO 2022 Annual Production Summary-xp.mdb'
# tableNamesIdx_co, tableNames_co, dfs_co = read_msAccess(fp)

fp = r'colorado/2022 Colorado Annual Production.xlsx'
co_prod = pd.read_excel(fp, converters={'name': str,
                                        'api_county_code': str,
                                        'api_seq_num': str,
                                        'sidetrack_num': str})
print(co_prod.columns)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['COLORADO', 'oil_original'] = co_prod.oil_prod.sum()
before_after_table.at['COLORADO', 'gas_original'] = co_prod.gas_prod.sum()
# -----------------------------------------------------------------------------

# Create complete API number from separate columns
co_prod["api_num"] = '05-' + co_prod['api_county_code'] + '-' + co_prod['api_seq_num'] + '-' + co_prod['sidetrack_num']

# Convert API-12 into API-10
co_prod['API10'] = co_prod.api_num.astype(str).str[:-3]

# ------------------------------------------------------
# Handle multiple rows for one API number
print("Number of unique APIs versus number of records in production data = ", [len(co_prod["api_num"].unique()), co_prod.shape[0]])

# There are a small number of API-12s that report more than one row of
# production in a single month, due to one of two cases:
# (A) a row reports a well's production from one of many formations/pools that
# the API has access to; the well was operated by 1 company for the whole year
# (B) the well, which draws from a single formation, changed operators part way
# through the year; each row represents production under a specific ownership
dupes = co_prod[co_prod.duplicated(subset=['api_num'], keep=False)]

# FIRST, group rows so that there's one row for each API-12 + operator combo
# for the year. This step prevents Prod_days for wells being double-counted and
# above 365 in a year
agg_funcs_1 = {
    'report_year': 'last',
    'Prod_days': 'max',  # NOTE that handling of this field differs
    'oil_prod': 'sum',
    'gas_prod': 'sum',
    'flared_vented': 'sum',
    'gas_used_on_lease': 'sum',
    'water_prod': 'sum',
    'API10': 'last'
}
co_prod_agg = co_prod.groupby(by=['api_num', 'name'],
                              as_index=False).agg(agg_funcs_1)

# THEN, group rows so that there's only one API-12 record for the year. The
# production volumes & producing days from each operator's "stint" are summed.
agg_funcs_2 = {
    'report_year': 'last',
    'Prod_days': 'sum',  # NOTE that handling of this field differs
    'oil_prod': 'sum',
    'gas_prod': 'sum',
    'flared_vented': 'sum',
    'gas_used_on_lease': 'sum',
    'water_prod': 'sum',
    'API10': 'last'
}
co_prod_agg = co_prod_agg.groupby(by=['api_num'],
                                  as_index=False).agg(agg_funcs_2)

# FINALLY, group by API-10s so that you get one record for the 2022 annual
# production of that surface location.
agg_funcs_3 = {
    'report_year': 'last',
    'Prod_days': 'max',  # NOTE that handling of this field differs
    'oil_prod': 'sum',
    'gas_prod': 'sum',
    'flared_vented': 'sum',
    'gas_used_on_lease': 'sum',
    'water_prod': 'sum'
}
co_prod_agg = co_prod_agg.groupby(by=['API10'],
                                  as_index=False).agg(agg_funcs_3)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['COLORADO', 'oil_agg'] = co_prod_agg.oil_prod.sum()
before_after_table.at['COLORADO', 'gas_agg'] = co_prod_agg.gas_prod.sum()

# =============================================================================
# %% COLORADO - Location data
# =============================================================================
# Read OGIM wells
co_wells = ogim_wells_usa.query("STATE_PROV == 'COLORADO'")
print(co_wells.columns)
print(co_wells.head())

# # =============================================================================
# %% COLORADO - Merge and clean
# =============================================================================
co_prod_merge = pd.merge(co_prod_agg,
                         co_wells,
                         how="left",
                         left_on='API10',
                         right_on='FAC_ID').reset_index(drop=True)

print("Total # of records in prod data = ", co_prod_agg.shape[0], " VERSUS merged data = ", co_prod_merge.shape[0])
print("Total # of records with NULL lat values = ", co_prod_merge[co_prod_merge.LATITUDE.isnull()].shape[0])

# Drop any rows that have a null geometry (i.e., wells where we have a
# location for them, but no production record to join it to)
co_prod_merge = co_prod_merge[co_prod_merge.geometry.notna()].reset_index(drop=True)

# Convert to GeoDataFrame, since merging turned the gdf into a df
co_prod_merge = co_prod_merge.set_geometry('geometry')

# ------------------------------------------------------
# Plot data
figCO = scatterMaps(
    co_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="gas_prod",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="CO well-level gas production (2022),\n ",
    saveFigPath=results_folder + "CO_2022_production.tiff"
)

figCO = scatterMaps(
    co_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="oil_prod",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Oil production (barrels/year)",
    figTitle="CO well-level oil production (2022),\n ",
    saveFigPath=results_folder + "CO_2022_OIL_production.tiff"
)


# =============================================================================
# %% COLORADO - Integration
# =============================================================================
co_integrated, co_errors = integrate_production(
    co_prod_merge,
    src_date="2024-01-09",  # based on edit date in ZIP file
    src_ref_id="189, 248",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Colorado",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type='FAC_TYPE',
    spud_date="SPUD_DATE",
    comp_date="COMP_DATE",
    # drill_type=None,
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="oil_prod",
    gas_mcf="gas_prod",
    water_bbl="water_prod",
    # condensate_bbl=None,
    prod_days="Prod_days",
    prod_year=2022,
    entity_type="WELL",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('COLORADO',
                                             before_after_table,
                                             co_integrated)

save_spatial_data(
    co_integrated,
    "colorado_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# summ_CO = check_production_stats(co_prod_integrated, co_prod2, "oil_prod", "gas_prod")


# ======================================================
# %% KANSAS [2022] - Read + aggregate production data
# KANSAS production data available at the lease level https://www.kgs.ku.edu/Magellan/Field/lease.html
# ======================================================
# Read .txt data
fp_gas_ks = r"kansas\gas_leases_2020_present.txt"
ks_gas = pd.read_csv(fp_gas_ks, sep=",", header=0)
print(ks_gas.head())
print(ks_gas.columns)

fp_oil_ks = r"kansas\oil_leases_2020_present.txt"
ks_oil = pd.read_csv(fp_oil_ks, sep=",", header=0)
print(ks_oil.head())
print(ks_oil.columns)

# Do both tables have the same columns?
# NOTE that the column "production" in each table has different meanings and units!!!
print(ks_gas.columns == ks_oil.columns)  # TRUE

# ------------------------------------------------------
# Select for 2022 records
ks_gas["year"] = pd.to_datetime(ks_gas["MONTH-YEAR"]).dt.year
ks_gas_2022 = ks_gas.query("year == 2022").reset_index(drop=True)

ks_oil["year"] = pd.to_datetime(ks_oil["MONTH-YEAR"]).dt.year
ks_oil_2022 = ks_oil.query("year == 2022").reset_index(drop=True)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['KANSAS', 'oil_original'] = ks_oil_2022.PRODUCTION.sum()  # NOTE that the column "production" in each table has different meanings and units!!!
before_after_table.at['KANSAS', 'gas_original'] = ks_gas_2022.PRODUCTION.sum()
# -----------------------------------------------------------------------------


# AGGREGATE Production by Lease ID
agg_funcs = {
    'LEASE': 'last',
    'DOR_CODE': 'last',
    'API_NUMBER': 'last',
    'FIELD': 'last',
    'PRODUCING_ZONE': 'last',
    'OPERATOR': 'last',
    # 'COUNTY': 'last',
    'LATITUDE': 'last',
    'LONGITUDE': 'last',
    'PRODUCT': 'last',
    'WELLS': 'last',
    'PRODUCTION': 'sum'  # NOTE that the column "production" in each table has different meanings and units!!!
    # "GAS_MCF": 'sum',
    # "OIL_BBL": 'sum'
}

ks_gas_agg = ks_gas_2022.groupby(by="LEASE_KID",
                                 as_index=False).agg(agg_funcs).reset_index(drop=True)

ks_oil_agg = ks_oil_2022.groupby(by="LEASE_KID",
                                 as_index=False).agg(agg_funcs).reset_index(drop=True)


# Change the col name "production" to to the specific hydrocarbon being reported
ks_gas_agg["GAS_MCF"] = ks_gas_agg["PRODUCTION"]
ks_oil_agg["OIL_BBL"] = ks_oil_agg["PRODUCTION"]

print('Are there leases that report both oil AND gas production?')
print(ks_gas_agg["LEASE_KID"].isin(ks_oil_agg["LEASE_KID"]).value_counts())  # should all be FALSE?

# If there are none, it's safe to simply CONCATENATE BOTH TABLES
ks_prod = pd.concat([ks_gas_agg, ks_oil_agg]).reset_index(drop=True)
print(ks_prod.head())
print("TOTAL # OF RECORDS = ", [ks_prod.shape[0], ks_gas_2022.shape[0], ks_oil_2022.shape[0]])
print(f'Unique LEASE_KID in oil and gas = {len(ks_prod.LEASE_KID.unique())}')
print(f'Number of rows = {len(ks_prod)}')

# After concatenating, fill the null production cells with zero, because if a
# lease did not report any oil or gas production in a given year, we are assuming
# it produced none of that hydrocarbon.
ks_prod["GAS_MCF"] = ks_prod["GAS_MCF"].fillna(0)
ks_prod["OIL_BBL"] = ks_prod["OIL_BBL"].fillna(0)


# Exclude ALL Null LAT and LON
ks_prod = ks_prod[~ks_prod["LATITUDE"].isnull()].reset_index(drop=True)

# Convert to GeoDataFrame
ks_prod = gpd.GeoDataFrame(ks_prod,
                           geometry=gpd.points_from_xy(ks_prod.LONGITUDE,
                                                       ks_prod.LATITUDE),
                           crs="epsg:4326")

print(ks_prod["GAS_MCF"].min(), ks_prod["GAS_MCF"].max())
plt.hist(ks_prod["GAS_MCF"], density=True)
print("TOTAL OIL AND GAS IN 2022 = ", [ks_prod["OIL_BBL"].sum(), ks_prod["GAS_MCF"].sum()])

# Unabbreviate product / facility type
ks_prod["fac_type"] = ks_prod["PRODUCT"].replace({"O": "OIL", "G": "GAS"})

# Rename the column "LEASE" to "LEASE_NAME", to distinguish it from the string
# 'lease' that I want to write in the ENTITY_TYPE field
ks_prod = ks_prod.rename(columns={"LEASE": "LEASE_NAME"}, errors="raise")

# ------------------------------------------------------
# Plot data to preview it
figKS = scatterMaps(
    ks_prod,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="OIL_BBL",
    dataScaling=100,
    colorMapName="plasma",
    dataLabel="Oil production (BBL/year)",
    figTitle="KS lease-level oil production (2022),\n ",
    saveFigPath=results_folder + "KS_2022_oil_production.tiff"
)

figKSgas = scatterMaps(
    ks_prod,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GAS_MCF",
    dataScaling=1000,
    colorMapName="plasma",
    dataLabel="Gas production (MCF/year)",
    figTitle="KS lease-level GAS production (2022),\n ",
    saveFigPath=results_folder + "KS_2022_gas_production.tiff"
)

# =============================================================================
# %% KANSAS - Integration
# =============================================================================
ks_integrated, ks_errors = integrate_production(
    ks_prod,
    src_date="2024-07-27",  # publication date from website
    src_ref_id="192, 249",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="KANSAS",
    fac_name="LEASE_NAME",
    fac_id="LEASE_KID",
    fac_type="fac_type",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    fac_status=None,
    op_name="OPERATOR",
    oil_bbl="OIL_BBL",
    gas_mcf="GAS_MCF",
    # water_bbl=None,
    # condensate_bbl=None,
    prod_days=None,
    prod_year=2022,
    entity_type="LEASE",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('KANSAS',
                                             before_after_table,
                                             ks_integrated)

save_spatial_data(
    ks_integrated,
    "kansas_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# summ_KS = check_production_stats(ks_integrated, ks_prod_gdf, "OIL_BBL", "GAS_MCF")


# ------------------------------------------------------
# %% KENTUCKY [2022] - Read + aggregate production data
# - Production data available here https://eec.ky.gov/Natural-Resources/Oil-and-Gas/Resources/Pages/Production-Reports.aspx
# - Includes separate files for oil and gas production for each month in a given year
# ------------------------------------------------------
# Read data files and concatenate together into one df.
# There are separate Excel files for each year and hydrocarbon type
prod_file_path = "kentucky"
prod_files_ky = os.listdir(prod_file_path)

ky_prod_dfs = []

for file in tqdm(prod_files_ky):
    if file.endswith('.xlsx'):
        print(file)
        df = pd.read_excel(os.path.join(prod_file_path, file))
        if "Year 2017" in df.columns:
            df.rename(columns={'Year 2017': 'Year'}, inplace=True)
        if "PERMIT" in df.columns:
            df.rename(columns={'PERMIT': 'Permit'}, inplace=True)
        if "Oil" in file:
            # df['prod_type'] = "OIL"
            ky_prod_oil = df.copy()
        elif "Gas" in file:
            # df["prod_type"] = "GAS"
            ky_prod_gas = df.copy()

# Merge, rather than concatenate, the records, so that wells that report both
# oil and gas production report those volumes in the same record.
ky_prod_all = ky_prod_oil.merge(ky_prod_gas,
                                how='outer',
                                on=['Year',
                                    'County',
                                    'Permit',
                                    'Company',
                                    'Lease_NM',
                                    'Well_No',
                                    'PoolName',
                                    'Formation',
                                    'LAT',
                                    'LONG'],
                                suffixes=('_oil', '_gas'))


# ky_prod_all = pd.concat(ky_prod_dfs).reset_index(drop=True)
print(ky_prod_all.head())
ky_prod_all.columns


# Sum the total gas and total oil production for each row (which is a specific
# lease plus year), and put the results in a new column
gas_prod_cols = [col for col in ky_prod_all if col.endswith('Gas')]
ky_prod_all['total_gas_prod'] = ky_prod_all[gas_prod_cols].sum(axis=1)
oil_prod_cols = [col for col in ky_prod_all if col.endswith('Oil')]
ky_prod_all['total_oil_prod'] = ky_prod_all[oil_prod_cols].sum(axis=1)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['KENTUCKY', 'oil_original'] = ky_prod_all.total_oil_prod.sum()
before_after_table.at['KENTUCKY', 'gas_original'] = ky_prod_all.total_gas_prod.sum()
# -----------------------------------------------------------------------------

# Aggregate production rows by Permit number AND year
agg_funcs_ky = {
    'County': 'last',
    'Company': 'last',
    'Lease_NM': 'last',
    'Well_No': 'last',
    'LAT': 'last',
    'LONG': 'last',
    'total_gas_prod': 'sum',
    'total_oil_prod': 'sum'
}
ky_prod_agg = ky_prod_all.groupby(by=["Year", "Permit"], as_index=False).agg(agg_funcs_ky).reset_index(drop=True)


# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['KENTUCKY', 'oil_agg'] = ky_prod_agg.total_oil_prod.sum()
before_after_table.at['KENTUCKY', 'gas_agg'] = ky_prod_agg.total_gas_prod.sum()

# ------------------------------------------------------
# Remove any points with null geometries
ky_prod_agg = ky_prod_agg[~ky_prod_agg.LAT.isnull()].reset_index(drop=True)
print("======================================")
print(ky_prod_agg.LAT.min(), ky_prod_agg.LONG.min(), ky_prod_agg.LAT.max(), ky_prod_agg.LONG.max())

# There are some erroneous lat/lon values in this dataset!
ky_prod_agg.loc[ky_prod_agg.LONG == 82.422426, 'LONG'] = -82.422426

# Create GeoDataFrame
ky_prod_agg = gpd.GeoDataFrame(ky_prod_agg,
                               geometry=gpd.points_from_xy(ky_prod_agg.LONG,
                                                           ky_prod_agg.LAT),
                               crs="epsg:4326")

# Assign lease names/ well names
create_concatenated_well_name(ky_prod_agg,
                              'Lease_NM',
                              'Well_No',
                              'well_name_new')

# ------------------------------------------------------
# Select 2022 data for visualization of gas production
prod_ky20 = ky_prod_agg.query("(Year==2022)").reset_index(drop=True)
prod_ky20.head()

# Plot data
figKY = scatterMaps(
    prod_ky20,
    lat_lon=True,
    lat_attribute="LAT",
    lon_attribute="LONG",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="total_gas_prod",
    dataScaling=2500,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="Kentucky well-level gas production (2022), ",
    saveFigPath=results_folder + "KY_2022_production.tiff"
)


# =============================================================================
# %% KENTUCKY - Integration
# =============================================================================
ky_integrated, ky_prod_err = integrate_production(
    ky_prod_agg,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="KENTUCKY",
    src_ref_id="155",  # DONE
    src_date="2023-04-15",  # this is a guess?
    on_offshore="Onshore",
    fac_name='well_name_new',
    fac_id="Permit",
    # fac_type=None,
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # fac_status=None,
    op_name="Company",
    oil_bbl="total_oil_prod",
    gas_mcf="total_gas_prod",
    # water_bbl=None,
    # condensate_bbl=None,
    # prod_days=None,
    prod_year="Year",
    entity_type="LEASE",  # FIXME , this should be wells!
    fac_latitude="LAT",
    fac_longitude="LONG"
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('KENTUCKY',
                                             before_after_table,
                                             ky_integrated)

save_spatial_data(
    ky_integrated,
    "kentucky_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


# summ_KY = check_production_stats(ky_prod_, prod_ky20, "OIL", "GAS")

# Quick check of production data
# summ_KY = check_production_stats(ky_prod_, prod_ky4, "OIL", "GAS")
# ky_prod_.query("GAS_MCF >0")["GAS_MCF"].sum()

# =============================================================================
# %% LOUISIANA [2022]
# =============================================================================
# fp_la_prod = r'louisiana/Oil and Gas Detail Production by Month.csv'
la_prod = pd.read_csv(r'louisiana/Oil and Gas Detail Production by Month.csv')  # monthly production table by LUW
print(la_prod.columns)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['LOUISIANA', 'oil_original'] = la_prod['Oil Production (Barrels)'].sum()
before_after_table.at['LOUISIANA', 'gas_original'] = la_prod['Gas Production (MCFs)'].sum()
before_after_table.at['LOUISIANA', 'cond_original'] = la_prod['Condensate Oil'].sum()


# Aggregate monthly production into annual production by LUW Code
agg_funcs = {
    "Field ID": "first",
    "Field Name": "first",
    "Operator Name": "first",
    "Luw Type Description": "first",
    "Parish Name": "first",
    "Well Count": "max",
    "Oil Production (Barrels)": "sum",
    "Gas Production (MCFs)": "sum",
    "Condensate Oil": "sum"
}
la_prod_agg = la_prod.groupby(by="LUW Code", as_index=False).agg(agg_funcs)


# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['LOUISIANA', 'oil_agg'] = la_prod_agg['Oil Production (Barrels)'].sum()
before_after_table.at['LOUISIANA', 'gas_agg'] = la_prod_agg['Gas Production (MCFs)'].sum()
before_after_table.at['LOUISIANA', 'cond_agg'] = la_prod_agg['Condensate Oil'].sum()

# =============================================================================
# %% LOUISIANA - Read well location and related tables
# =============================================================================
# Read well table (includes lat-long and serial number)
la_wells = pd.read_csv(r'louisiana/Well Information.csv',
                       encoding='cp1252')

# Read table that associates LUW Codes to well serial number
la_luw2wells = pd.read_csv(r'louisiana/Wells by LUW Code.csv')
# Reduce this table to only the fields that I need
la_luw2wells = la_luw2wells[['LUW Code',
                             'Well Serial Num',
                             'LUW Type Code Description']]

# Join the LUW Code info to the la_wells dataframe via serial number, so that
# each la_well (and its lat-long) is associated with a LUW code
la_wells_luw = la_wells.merge(la_luw2wells,
                              how='left',
                              on='Well Serial Num',
                              suffixes=('_x', '_y'))
# NOTE: This join will introduce duplicate records in the dataset, because a
# small number (1,935) of well serial numbers belong to two LUWs (I think
# because the well draws from 2 different formations/fields?)
# In most cases though, one LUW has multiple wells on it.
len(la_wells_luw)
len(la_wells_luw['Well Serial Num'].unique())

# Because a LUW is necessary to associate production with a location,
# make a table of only wells whose serial number matched a reporting LUW
wells_with_luw = la_wells_luw[la_wells_luw['LUW Code'].notna()]
wells_with_luw = wells_with_luw[wells_with_luw['LUW Code'] != 'ZZZZZZ']

# =============================================================================
# %% LOUISIANA - Aggregate wells into LUW centroids, then merge with production
# =============================================================================
# Turn the well df into a gdf, dropping any wells without coordinates first
wells_with_luw = wells_with_luw[wells_with_luw.Latitude.notna()].reset_index(drop=True)
wells_with_luw_locs = gpd.GeoDataFrame(wells_with_luw,
                                       geometry=gpd.points_from_xy(wells_with_luw.Longitude,
                                                                   wells_with_luw.Latitude),
                                       crs="epsg:4326")
# Filter out well points outside of the general state area
# Don't want to use a simple clip bc I'd be removing offshore wells too
wells_with_luw_locs = wells_with_luw_locs[wells_with_luw_locs['Latitude'].between(28, 34)]
wells_with_luw_locs = wells_with_luw_locs[wells_with_luw_locs['Longitude'].between(-95, -86)]

# wells_with_luw_locs.plot()

# -----------------------------------------------------
# Dissolve the well record gdf based on LUW code. In instances where there are
# multiple wells / APIs associated with a single LUW, a multi-part point geometry
# will be associated with the PRU. Finally, create centroids out of all the
# multi-point features, and then assign this single-point-only array of geometries
# to the gdf's geometry column.
wells_with_luw_agg = {
    'Operator Name': 'first',
    'Parish Name': 'first',
    'Field Name': 'first',
    'API Num': 'count'}

luw_geoms = wells_with_luw_locs.dissolve(by='LUW Code', aggfunc=wells_with_luw_agg)
luw_geoms = luw_geoms.reset_index(drop=False)
print(luw_geoms.geometry.type.value_counts())
print(luw_geoms.geometry.isna().value_counts())  # should all be false

single_point_geoms_only = luw_geoms.geometry.centroid
luw_geoms['geometry'] = single_point_geoms_only

# -----------------------------------------------------
# Convert LUW Code column from string to int64, so that it can be joined
# with our production data properly later on
luw_geoms['LUW Code'].replace({'ZZZZZZ': 0}, inplace=True)
luw_geoms['LUW Code'] = luw_geoms['LUW Code'].astype(int)

# Merge LUW-level production volumes with LUW-level geometries
la_prod_merge = pd.merge(la_prod_agg,
                         luw_geoms,
                         on="LUW Code",
                         how="left")
# The CRS of this table gets dropped during pd.merge(), so recast as gdf
la_prod_merge = gpd.GeoDataFrame(la_prod_merge,
                                 geometry='geometry',
                                 crs='epsg:4326')

la_prod_merge['long'] = la_prod_merge.geometry.x
la_prod_merge['lat'] = la_prod_merge.geometry.y

# =============================================================================
# # %% LA checking my work
# # =============================================================================
# joinedtowells = la_prod_merge[la_prod_merge['Operator Name_y'].notna()]

# notjoinedtowells = la_prod_merge[la_prod_merge['Operator Name_y'].isna()]
# notjoinedtowells['Oil Production (Barrels)'].sum()

# =============================================================================
# %% LOUISIANA - Integration
# =============================================================================
la_integrated, la_prod_err = integrate_production(
    la_prod_merge,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="LOUISIANA",
    src_ref_id="155, 250",  # DONE
    src_date="2024-09-04",
    # on_offshore="Onshore",  # FIXME
    # fac_name='well_name_new',
    fac_id="LUW Code",
    fac_type='Luw Type Description',
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # fac_status=None,
    op_name="Operator Name_x",
    oil_bbl='Oil Production (Barrels)',
    gas_mcf='Gas Production (MCFs)',
    # water_bbl=None,
    condensate_bbl='Condensate Oil',
    # prod_days=None,
    prod_year="2022",
    entity_type="LEASE",  # FIXME
    fac_latitude="lat",
    fac_longitude="long"
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('LOUISIANA',
                                             before_after_table,
                                             la_integrated)

save_spatial_data(
    la_integrated,
    "louisiana_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# =============================================================================
# %% MICHIGAN [2022] - Read + aggregate production data

# production data FTP site URL: ftp://ftp.deq.state.mi.us/geowebface
# =============================================================================
# Read all tables from the MS Access database
# NOTE: individual table names MUST be manually renamed to remove white spaces
# (e.g., `PRU Master` to `PRUMaster`), or else `read_msAccess()` will fail
# fp = "North_America//United_States_//State_Raw_Data//Michigan//Production//Oil&GasProduction 9-1-2022.accdb"
fp = r'michigan/Oil&GasProduction 3-1-2023.accdb'
tableNamesIdx_mi, tableNames_mi, dfs_mi = read_msAccess(fp,
                                                        table_subset=['Production',
                                                                      'PRUWells'])

# Read the "Production" table from the Access DB into its own dataframe.
# Production volumes are reported monthly for each production reporting
# unit (PRU), which may include one or multiple wells.
name2index = dict(zip(tableNames_mi, tableNamesIdx_mi))
prod_index = name2index.get('Production')
mi_prod_all = dfs_mi[prod_index]
print(mi_prod_all.columns)
print(mi_prod_all.head())

# Filter for 2022 production records only
mi_prod_all["report_datetime"] = pd.to_datetime(mi_prod_all["RptDate"])
mask_2022 = (mi_prod_all['report_datetime'].dt.year == 2022)
mi_prod = mi_prod_all[mask_2022].reset_index(drop=True)
# print("Total oil and gas production in mmbarrels and bcf = {} and {} , respectively \n".format((mi_prod["OilProd"].sum() / 1e6 + mi_prod["CondProd"].sum() / 1e6), mi_prod["GasSold"].sum() / 1e6))

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['MICHIGAN', 'oil_original'] = mi_prod.OilProd.sum()
before_after_table.at['MICHIGAN', 'gas_original'] = mi_prod.GasSold.sum()  # FIXME is this field right?
before_after_table.at['MICHIGAN', 'cond_original'] = mi_prod.CondProd.sum()
# -----------------------------------------------------------------------------

# Aggregate month-specific rows by PRUNumber to arrive at one row per PRU,
# containing that PRU's annual production.
mi_prod["count_months"] = 1  # Add a field to count the months a PRU produced
agg_funcs = {
    "OpNo": "first",  # Operator Number
    "OilProd": "sum",
    "CondProd": "sum",
    "NGLProd": "sum",
    "WtrProd": "sum",
    "GasSold": "sum",
    "count_months": "sum"
}
mi_prod_agg = mi_prod.groupby(by="PRUNumber", as_index=False).agg(agg_funcs)
# Estimate production days based on number of months a PRU was producing
mi_prod_agg["prod_days"] = mi_prod_agg["count_months"] * 30.4
mi_prod_agg["prod_days"] = mi_prod_agg["prod_days"].round()

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['MICHIGAN', 'oil_agg'] = mi_prod_agg.OilProd.sum()
before_after_table.at['MICHIGAN', 'gas_agg'] = mi_prod_agg.GasSold.sum()
before_after_table.at['MICHIGAN', 'cond_agg'] = mi_prod_agg.CondProd.sum()

# =============================================================================
# %% MICHIGAN - Read well location and attribute info
# =============================================================================
# Read the 'PRUWells' table, which links PRU numbers to API number(s).
# It includes attribute info on wells, but NOT their lat-long location.
pru_wells = dfs_mi[name2index.get('PRUWells')]
cols2keep = ['PRUNumber',
             'API_WellNo',
             'County',
             'Wl_Permit',
             'Well_NM',
             'Well_Typ',
             'Wl_Status',
             'Dt_Status']
pru_wells = pru_wells[cols2keep]
pru_wells.head()

# Format API-14 number as a string API-10, no hyphens (for joining with OGIM wells)
pru_wells['API10'] = pru_wells.API_WellNo.astype(str).str[:10]
# pru_wells.API_WellNo = pru_wells.API_WellNo.apply(lambda x: '{}-{}-{}-{}-{}'.format(x[0:2], x[2:5], x[5:10], x[10:12], x[12:14]))

# ------------------------------------------------------
# Read OGIM wells
mi_well_locs = ogim_wells_usa.query("STATE_PROV == 'MICHIGAN'")
print(mi_well_locs.columns)
print(mi_well_locs.head())

# ------------------------------------------------------
# Merge well attributes and well locations based on API
pru_wells_locs = pd.merge(pru_wells,
                          mi_well_locs,
                          left_on='API10',
                          right_on="FAC_ID",
                          how="left")
print(pru_wells_locs.head())

# =============================================================================
# %% MICHIGAN - Aggregate wells into PRU centroids, then merge with production
# =============================================================================
# Turn the well df into a gdf, dropping any wells without coordinates first
pru_wells_locs = pru_wells_locs[pru_wells_locs.LATITUDE.notna()].reset_index(drop=True)
pru_wells_locs = gpd.GeoDataFrame(pru_wells_locs,
                                  geometry=gpd.points_from_xy(pru_wells_locs.LONGITUDE,
                                                              pru_wells_locs.LATITUDE),
                                  crs="epsg:4326")

# Dissolve the well record gdf based on PRU number. In instances where there are
# multiple wells / APIs associated with a single PRU, a multi=part point geometry
# will be associated with the PRU. Finally, create centroids out of all the
# multi-point features, and then assign this single-point-only array of geometries
# to the gdf's geometry column.
mi_wells_agg = {
    'OPERATOR': 'first',
    'County': 'first',
    'SRC_REF_ID': 'first',
    'SRC_DATE': 'first',
    'ON_OFFSHORE': 'first'}
# FIXME - Many well attributes that get dropped at this point,
# because it doesn't make sense to just retain a single well's operator, status,
# etc. and ascribe that to an entire PRU. Ask Mark whether any should be kept.

pru_geoms = pru_wells_locs.dissolve(by='PRUNumber', aggfunc=mi_wells_agg)
pru_geoms = pru_geoms.reset_index(drop=False)
print(pru_geoms.geometry.type.value_counts())
print(pru_geoms.geometry.isna().value_counts())

single_point_geoms_only = pru_geoms.geometry.centroid
pru_geoms['geometry'] = single_point_geoms_only

# -----------------------------------------------------
# Merge PRU-level production volumes with PRU-level geometries
mi_prod_merge = pd.merge(mi_prod_agg,
                         pru_geoms,
                         on="PRUNumber",
                         how="left")
# The CRS of this table gets dropped during pd.merge(), so recast as gdf
mi_prod_merge = gpd.GeoDataFrame(mi_prod_merge,
                                 geometry='geometry',
                                 crs='epsg:4326')

print("=================================")
print("Total oil in original versus merged dataset = {} versus {} million barrels".format((mi_prod["OilProd"].sum() / 1e6 + mi_prod["CondProd"].sum() / 1e6), (mi_prod_merge["OilProd"].sum() / 1e6 + mi_prod_merge["CondProd"].sum() / 1e6)))

# Drop any rows that have a null geometry (i.e., wells where we have a
# location for them, but no production record to join it to)
mi_prod_merge = mi_prod_merge[mi_prod_merge.geometry.notna()].reset_index(drop=True)
mi_prod_merge['Long'], mi_prod_merge['Lat'] = mi_prod_merge.geometry.x, mi_prod_merge.geometry.y
# ------------------------------------------------------
# Plot data
figNM = scatterMaps(
    mi_prod_merge,
    lat_lon=True,
    lat_attribute="Lat",
    lon_attribute="Long",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GasSold",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="MI PRU-level gas production (2022),\n ",
    saveFigPath=results_folder + "MI_2022_production.tiff"
)

# ------------------------------------------------------
# Check attribute names to ensure they are entered correctly
# Note that because these are mostly multi-well reporting units, spud dates, completion dates and well trajectory are not reported
# ------------------------------------------------------
# Indicate prod year and ENTITY type
mi_prod_merge["prod_year"] = 2022
mi_prod_merge["entity_type"] = "PRODUCTION REPORTING UNIT"
# Create custom representation of PRU number, for inclusion in the FAC_ID column
mi_prod_merge['ID'] = 'PRU ' + mi_prod_merge.PRUNumber.map('{:.0f}'.format)

# =============================================================================
# %% MICHIGAN - Integration
# =============================================================================
mi_integrated, mi_errors = integrate_production(
    mi_prod_merge,
    src_date="2023-03-01",
    src_ref_id="195, 251",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Michigan",
    # fac_name="",
    fac_id="ID",
    # fac_type="",
    spud_date=None,
    comp_date=None,
    drill_type=None,
    # fac_status='Status_y',
    op_name="OPERATOR",
    oil_bbl="OilProd",
    gas_mcf="GasSold",
    water_bbl="WtrProd",
    condensate_bbl="CondProd",
    prod_days="prod_days",
    prod_year="prod_year",
    entity_type="entity_type",
    fac_latitude='Lat',
    fac_longitude='Long'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('MICHIGAN',
                                             before_after_table,
                                             mi_integrated)

save_spatial_data(
    mi_integrated,
    "michigan_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# summ_MI = check_production_stats(mi_integrated, prod_mi_2021, "OilProd", "GasSold")

# ======================================================
# %% MISSISSIPPI [2022] - Read + aggregate production
# Mississippi production data available here https://www.ogb.state.ms.us/proddata.php
# ======================================================
# Read in production data file
ms_prod = pd.read_csv('mississippi/QueryResults_2022.csv')
# Filter production by Year
ms_prod = ms_prod.query("Year == 2022").reset_index(drop=True)
print(ms_prod.head())
print(ms_prod.columns)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['MISSISSIPPI', 'oil_original'] = ms_prod.OilProd.sum()  # TODO should we use the SOLD fields?
before_after_table.at['MISSISSIPPI', 'gas_original'] = ms_prod.GasProd.sum()
# -----------------------------------------------------------------------------

# Replace production values stored as NaNs with 0s
cols_with_nulls = ['OilProd', 'GasProd', 'WaterProd', 'ProdDays']
ms_prod[cols_with_nulls] = ms_prod[cols_with_nulls].fillna(0)

# Create a string API14 column
ms_prod['API14'] = ms_prod.WellID.astype(str)

# Since well-level data uses API10, create an API10 column
ms_prod['API10'] = ms_prod.API14.str[:-4]

# ------------------------------------------------------
# There are some entries with more production days than possible per month
# (for ex., 31 producing days reported in February)
# Repair any of these cases so only the maximum days in each month is reported

# If a record is for a month with 30 days AND it reports more than 30 days of
# production, make it report only 30 days instead
mask_30_day_month = ms_prod['Month'].isin([4, 6, 9, 11])
mask_over_30_days_producing = ms_prod.ProdDays > 30
ms_prod.loc[mask_30_day_month & mask_over_30_days_producing, 'ProdDays'] = 30

# If a record is for a month with 28 days (Feb) AND it reports more than 28 days of
# production, make it report only 28 days instead
mask_february = ms_prod['Month'].isin([2])
mask_over_28_days_producing = ms_prod.ProdDays > 28
ms_prod.loc[mask_february & mask_over_28_days_producing, 'ProdDays'] = 28

# ------------------------------------------------------
# FIRST, group production by API-14, so the 12 months of records for a specific
# API14 are combined
agg_funcs = {
    # 'API14': 'first',
    'API10': 'last',
    'Year': 'last',
    'WellNameNumber': 'last',
    'OperatorName': 'last',
    'ProdOperatorName': 'last',
    'FieldName': 'last',
    'CountyName': 'last',
    'ProdDays': 'sum',  # NOTE different treatment
    'OilProd': 'sum',
    'WaterProd': 'sum',
    'GasProd': 'sum'
}


ms_prod = ms_prod.groupby(by="API14", as_index=False).agg(agg_funcs)
print(ms_prod.head())

# See if there are still multiple entries for each API-10. (Yes, there are)
dupes = ms_prod[ms_prod.API10.duplicated(keep=False)]

# ------------------------------------------------------
# THEN, group production by API-10, so production from multiple bottom holes
# are combined
agg_funcs_2 = {
    # 'API10': 'last',
    'Year': 'last',
    'WellNameNumber': 'last',
    'OperatorName': 'last',
    'ProdOperatorName': 'last',
    'FieldName': 'last',
    'CountyName': 'last',
    'ProdDays': 'max',  # NOTE different treatment
    'OilProd': 'sum',
    'WaterProd': 'sum',
    'GasProd': 'sum'
}

ms_prod = ms_prod.groupby(by="API10", as_index=False).agg(agg_funcs_2)
print(ms_prod.head())

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['MISSISSIPPI', 'oil_agg'] = ms_prod.OilProd.sum()
before_after_table.at['MISSISSIPPI', 'gas_agg'] = ms_prod.GasProd.sum()

# ======================================================
# %% MISSISSIPPI - Location data
# ======================================================
# Read OGIM wells
ms_wells = ogim_wells_usa.query("STATE_PROV == 'MISSISSIPPI'")
print(ms_wells.columns)
print(ms_wells.head())

# ======================================================
# %% MISSISSIPPI - Merge and clean
# ======================================================
ms_prod_merged = pd.merge(ms_prod,
                          ms_wells,
                          left_on='API10',
                          right_on='FAC_ID',
                          how="left").reset_index(drop=True)
print(ms_prod_merged.head())
print(ms_prod_merged.columns)

# Convert to GeoDataFrame, since merging turned the gdf into a df
ms_prod_merged = ms_prod_merged.set_geometry('geometry')

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
ms_prod_merged = ms_prod_merged[ms_prod_merged.geometry.notna()].reset_index()

# ----------------------------------------------------------------------------
# Visualize gas production
fig1 = scatterMaps(
    ms_prod_merged,
    lat_lon=True,
    lat_attribute='LATITUDE',
    lon_attribute='LONGITUDE',
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr='GasProd',
    dataScaling=1000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="MS well-level gas production (2022), ",
    saveFigPath=results_folder + "MS_2022_production.tiff"
)
# Visualize OIL production
fig1 = scatterMaps(
    ms_prod_merged,
    lat_lon=True,
    lat_attribute='LATITUDE',
    lon_attribute='LONGITUDE',
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr='OilProd',
    dataScaling=100,
    colorMapName="plasma",
    dataLabel="Oil production (barrels/year)",
    figTitle="MS well-level oil production (2022), ",
    saveFigPath=results_folder + "MS_2022_oil_production.tiff"
)

# fig, ax = plt.subplots(figsize=(5, 2), nrows=1, ncols=2)
# ax[0].hist(ms_prod_merged["GasProd"], density=True)
# ax[1].hist(ms_prod_merged["OilProd"], density=True)


# =============================================================================
# %% MISSISSIPPI - Integration
# =============================================================================
ms_integrated, ms_errors = integrate_production(
    ms_prod_merged,
    src_date="2023-10-23",
    src_ref_id="199, 207",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Mississippi",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type="FAC_TYPE",
    # spud_date=None,  # both N/A
    # comp_date=None,
    drill_type='DRILL_TYPE',
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="OilProd",
    gas_mcf="GasProd",
    water_bbl="WaterProd",
    # condensate_bbl=None,
    prod_days="ProdDays",
    prod_year="Year",
    entity_type="WELL",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('MISSISSIPPI',
                                             before_after_table,
                                             ms_integrated)

save_spatial_data(
    ms_integrated,
    "mississippi_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# Quick check of production data
# summ_MS = check_production_stats(ms_integrated, ms_prod_merged, "OilProd", "GasProd")

# =============================================================================
# %% MONTANA [2022] - Read + aggregate production data
# =============================================================================
# Read monthly well-level production data
mt_prod = pd.read_csv(r'montana/MT_HistoricalWellProduction.tab', sep='\t')
print(mt_prod.columns)
print(mt_prod.head())
mt_prod['datetimes'] = pd.to_datetime(mt_prod['Rpt_Date'], format='%m/%d/%Y', errors='coerce')
mt_prod['year'] = mt_prod['datetimes'].dt.year

mt_prod_2022 = mt_prod.query("year == 2022")

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['MONTANA', 'oil_original'] = mt_prod_2022.BBLS_OIL_COND.sum()
before_after_table.at['MONTANA', 'gas_original'] = mt_prod_2022.MCF_GAS.sum()

# Note: Although API-14 values are listed, all of the values end with '0000' so
# they are equal to API-10 values
mt_prod_2022['API_str'] = mt_prod_2022.API_WellNo.astype(str)
mt_prod_2022['API_str'].str.endswith('0000').value_counts()  # TRUE

# -----------------------------------------------------------------------------
# There are some entries with more production days than possible per month
# (for ex., 31 producing days reported in June which has 30 days)
# Repair any of these cases so only the maximum days in each month is reported

# If a record is for a month with 30 days AND it reports more than 30 days of
# production, make it report only 30 days instead
mask_30_day_month = mt_prod_2022['datetimes'].dt.month.isin([4, 6, 9, 11])
mask_over_30_days_producing = mt_prod_2022.DAYS_PROD > 30
mt_prod_2022.loc[mask_30_day_month & mask_over_30_days_producing, 'DAYS_PROD'] = 30

# If a record is for a month with 28 days (Feb) AND it reports more than 28 days of
# production, make it report only 28 days instead
mask_february = mt_prod_2022['datetimes'].dt.month.isin([2])
mask_over_28_days_producing = mt_prod_2022.DAYS_PROD > 28
mt_prod_2022.loc[mask_february & mask_over_28_days_producing, 'DAYS_PROD'] = 28

# A small number of monthly production records report an impossible number of
# production days (like 270 days in a month). Since we can't really know what
# they intended to write, set these values to n/a instead
mt_prod_2022.loc[mt_prod_2022.DAYS_PROD > 31, 'DAYS_PROD'] = np.nan

# -----------------------------------------------------------------------------
# There are a small number of APIs that report more than one row of production
# in a single month, and that's because each row reports the production from a
# particular formation/pool that API has access to.
dupes = mt_prod_2022[mt_prod_2022.duplicated(subset=['API_str',
                                                     'Rpt_Date'],
                                             keep=False)]

# FIRST, group the API-14 rows so that there's only one API-14 record per month.
# This step prevents DaysProduced for wells being double-counted and above 365 in a year
agg_mt_1 = {'Lease_Unit': 'last',
            'CoName': 'last',
            'BBLS_OIL_COND': 'sum',
            'MCF_GAS': 'sum',
            'BBLS_WTR': 'sum',
            'DAYS_PROD': 'max'  # NOTE that handling of this field differs
            }
mt_prod_2022 = mt_prod_2022.groupby(by=['API_str', 'Rpt_Date'],
                                    as_index=False).agg(agg_mt_1)

# THEN, group by API-14s so that you get one record for that well's 2022 annual production
agg_mt_2 = {'Lease_Unit': 'last',
            'CoName': 'last',
            'BBLS_OIL_COND': 'sum',
            'MCF_GAS': 'sum',
            'BBLS_WTR': 'sum',
            'DAYS_PROD': 'sum'  # NOTE that handling of this field differs
            }

mt_prod_2022_agg = mt_prod_2022.groupby(by=['API_str'],
                                        as_index=False).agg(agg_mt_2)

# =============================================================================
# %% MONTANA - Location data
# =============================================================================
# Read OGIM wells
mt_wells = ogim_wells_usa.query("STATE_PROV == 'MONTANA'")

print(mt_wells.columns)
print(mt_wells.head())

# =============================================================================
# %% MONTANA - Merge and clean
# =============================================================================
mt_prod_merge = pd.merge(mt_prod_2022_agg,
                         mt_wells,
                         left_on='API_str',
                         right_on="FAC_ID",
                         how="left")

# Convert df to gdf, since CRS got dropped during the merge
mt_prod_merge = gpd.GeoDataFrame(mt_prod_merge,
                                 geometry='geometry',
                                 crs=4326)

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
mt_prod_merge = mt_prod_merge[mt_prod_merge.geometry.notna()].reset_index()

# ------------------------------------------------------
# Plot data
figMT = scatterMaps(
    mt_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="MCF_GAS",
    dataScaling=10000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="MT well-level gas production (2022),\n ",
    saveFigPath=results_folder + "MT_2022_production.tiff"
)

# =============================================================================
# %% MONTANA - Integration
# =============================================================================
mt_integrated, mt_errors = integrate_production(
    mt_prod_merge,
    src_ref_id="209, 243",  # DONE
    src_date="2024-07-15",
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    on_offshore="ONSHORE",
    state_prov="MONTANA",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type='FAC_TYPE',
    spud_date="SPUD_DATE",
    comp_date="COMP_DATE",
    drill_type='DRILL_TYPE',
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="BBLS_OIL_COND",
    gas_mcf="MCF_GAS",
    water_bbl="BBLS_WTR",
    # condensate_bbl=None,
    prod_days="DAYS_PROD",
    prod_year="2022",
    entity_type="WELL",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('MONTANA',
                                             before_after_table,
                                             mt_integrated)

save_spatial_data(
    mt_integrated,
    "montana_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


# =============================================================================
# %% NEW MEXICO - Read + aggregate production data

# api_st_cde	American Petroleum Institute unique state identifier
# api_cnty_cde	American Petroleum Institute county code
# api_well_idn	American Petroleum Institute unique well code, by county
# pool_idn	    Identifier code for specific pool
# prodn_mth	    Month in which production occurred
# prodn_yr	    Year that production occurred
# ogrid_cde	    Oil & Gas Reporting ID
# prd_knd_cde	Product Kind Code (Oil, Gas, or Water)
# eff_dte	    The date which the RECORD is effective
# amend_ind	    Identifies submitted form as an amended form
# c115_wc_stat_cde  Status Code reported on C115
# prod_amt	    Volume of fluid produced (MCF or BBLS)
# prodn_day_num	Number of days well completion produced or injected
# mod_dte	    Last modified
# =============================================================================
fp = r'new_mexico\FTP_data_set\nm_2022_production_wells_from_FTP.csv'
nm_prod = pd.read_csv(fp)
print(nm_prod.columns)
print(nm_prod.head)

# Create separate columns for Oil, Gas, and Water production
nm_prod.prd_knd_cde = nm_prod.prd_knd_cde.str.strip()  # remove trailing spaces
nm_prod.loc[nm_prod.prd_knd_cde == 'O', 'OIL_BBL'] = nm_prod.prod_amt
nm_prod.loc[nm_prod.prd_knd_cde == 'G', 'GAS_MCF'] = nm_prod.prod_amt
nm_prod.loc[nm_prod.prd_knd_cde == 'W', 'WATER_BBL'] = nm_prod.prod_amt

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['NEW MEXICO', 'oil_original'] = nm_prod.OIL_BBL.sum()
before_after_table.at['NEW MEXICO', 'gas_original'] = nm_prod.GAS_MCF.sum()

# ------------------------------------------------------
# There are some entries with more production days than possible per month
# (for ex., 37 producing days reported in July)
# Repair any of these cases so only the maximum days in each month is reported

# If a record is for a month with 30 days AND it reports more than 30 days of
# production, make it report only 30 days instead
mask_30_day_month = nm_prod['prodn_mth'].isin([4, 6, 9, 11])
mask_over_30_days_producing = nm_prod.prodn_day_num > 30
nm_prod.loc[mask_30_day_month & mask_over_30_days_producing, 'prodn_day_num'] = 30

# If a record is for a month with 28 days (Feb) AND it reports more than 28 days of
# production, make it report only 28 days instead
mask_february = nm_prod['prodn_mth'].isin([2])
mask_over_28_days_producing = nm_prod.prodn_day_num > 28
nm_prod.loc[mask_february & mask_over_28_days_producing, 'prodn_day_num'] = 28

# Finally, if there are any remaining records with more than 31 production days
# reported, set that total to 31 days
nm_prod.loc[nm_prod.prodn_day_num > 31, 'prodn_day_num'] = 31

# -----------------------------------------------------------------------------
# Aggregate monthly production to an annual total for each well

# Create API field on which to aggregate
nm_prod.api_cnty_cde = nm_prod.api_cnty_cde.astype(str).str.zfill(3)
nm_prod.api_well_idn = nm_prod.api_well_idn.astype(str).str.zfill(5)
nm_prod['api'] = nm_prod.api_st_cde.astype(str) + '-' + nm_prod.api_cnty_cde + '-' + nm_prod.api_well_idn.astype(str)

# FIRST, group by API *and* Month so that there's only one API record per month.
# This step prevents DaysProduced for wells being double-counted and above 365 in a year
agg_funcs_nm_1 = {'OIL_BBL': 'sum',
                  'GAS_MCF': 'sum',
                  'WATER_BBL': 'sum',
                  # 'ogrid_cde': 'first',
                  'prodn_day_num': 'max'}  # NOTE that handling of this field differs
nm_prod_agg = nm_prod.groupby(by=['api', 'prodn_mth'],
                              as_index=False).agg(agg_funcs_nm_1)

# THEN, group by APIs so that you get one record for that well's 2022 annual production
agg_funcs_nm_2 = {'OIL_BBL': 'sum',
                  'GAS_MCF': 'sum',
                  'WATER_BBL': 'sum',
                  # 'ogrid_cde': 'first',
                  'prodn_day_num': 'sum'}  # NOTE that handling of this field differs
nm_prod_agg = nm_prod.groupby(by=['api'],
                              as_index=False).agg(agg_funcs_nm_2)

# If there are any APIs that produced zero oil, gas, or water all year, drop these records
mask_no_prod = ((nm_prod_agg.OIL_BBL == 0) & (nm_prod_agg.GAS_MCF == 0) & (nm_prod_agg.WATER_BBL == 0))
nm_prod_agg = nm_prod_agg[~mask_no_prod].reset_index(drop=True)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['NEW MEXICO', 'oil_agg'] = nm_prod_agg.OIL_BBL.sum()
before_after_table.at['NEW MEXICO', 'gas_agg'] = nm_prod_agg.GAS_MCF.sum()


# =============================================================================
# %% NEW MEXICO - Location data
# =============================================================================
# Read OGIM wells
nm_wells = ogim_wells_usa.query("STATE_PROV == 'NEW MEXICO'").reset_index(drop=True)
print(nm_wells.columns)
print(nm_wells.head())

# =============================================================================
# %% NEW MEXICO - Merge and clean
# =============================================================================
nm_prod_merge = pd.merge(nm_prod_agg,
                         nm_wells,
                         left_on='api',
                         right_on="FAC_ID",
                         how="left")

# Convert df to gdf, since CRS got dropped during the merge
nm_prod_merge = gpd.GeoDataFrame(nm_prod_merge,
                                 geometry='geometry',
                                 crs=4326)

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
nm_prod_merge = nm_prod_merge[nm_prod_merge.geometry.notna()].reset_index(drop=True)

figNM = scatterMaps(
    nm_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GAS_MCF",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="NM well-level gas production (2022),\n ",
    saveFigPath=results_folder + "NM_2022_production.tiff"
)


# =============================================================================
# %% NEW MEXICO - integration
# =============================================================================
nm_integrated, nm_prod_err = integrate_production(
    nm_prod_merge,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="NEW MEXICO",
    src_ref_id="156, 210",  # FIXME
    src_date="2024-06-25",  # FIXME
    on_offshore="Onshore",
    fac_name='WELL_NAME',
    fac_id="api",
    fac_type="FAC_TYPE",
    spud_date="SPUD_DATE",
    comp_date='COMP_DATE',
    drill_type='DRILL_TYPE',
    fac_status="FAC_STATUS",
    op_name="OPERATOR",
    oil_bbl="OIL_BBL",
    gas_mcf="GAS_MCF",
    water_bbl="WATER_BBL",
    # condensate_bbl=None,
    prod_days="prodn_day_num",
    prod_year=2022,
    entity_type="WELL",
    fac_latitude="LATITUDE",
    fac_longitude="LONGITUDE"
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('NEW MEXICO',
                                             before_after_table,
                                             nm_integrated)

save_spatial_data(
    nm_integrated,
    "new_mexico_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


# ======================================================
# %% NEW YORK [2022] - Read + aggregate production data
# ======================================================
"""
Data from New York's Department of Environmental Conservation
 - Website for well location data: https://www.dec.ny.gov/energy/1603.html
 - Website for well production data: https://www.dec.ny.gov/energy/36159.html
 - Production data includes data for the years 2010 through 2020.
"""
ny_prod = pd.read_csv('new_york//Prod2022.csv')
print(ny_prod.columns)
print(ny_prod.head())

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['NEW YORK', 'oil_original'] = ny_prod.OilProd.sum()
before_after_table.at['NEW YORK', 'gas_original'] = ny_prod.GasProd.sum()
# -----------------------------------------------------------------------------

# KEEP ONLY IMPORTANT ATTRIBUTES
ny_prod = ny_prod[['API_WellNo',
                   'County',
                   'Well_Typ',
                   'Wl_Status',
                   'Well_Nm',
                   'MonthProd',
                   'GasProd',
                   'WaterProd',
                   'OilProd',
                   'Year']]

# Assume an average of 30.4 days per year to estimate # of production days
ny_prod["prod_days"] = ny_prod["MonthProd"] * 30.4
ny_prod["prod_days"] = ny_prod["prod_days"].round().astype(int)

# Convert APIs from int to string
ny_prod['API_WellNo'] = ny_prod.API_WellNo.astype(str)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['NEW YORK', 'oil_agg'] = ny_prod.OilProd.sum()
before_after_table.at['NEW YORK', 'gas_agg'] = ny_prod.GasProd.sum()

# =============================================================================
# %% NEW YORK - Location data
# =============================================================================
# Read OGIM wells
ny_wells = ogim_wells_usa.query("STATE_PROV == 'NEW YORK'")
print(ny_wells.columns)
print(ny_wells.head())

# =============================================================================
# %% NEW YORK - Merge and clean
# =============================================================================
ny_prod_merge = pd.merge(ny_prod,
                         ny_wells,
                         left_on="API_WellNo",
                         right_on='FAC_ID',
                         how="left")
print(ny_prod_merge.head())

# Convert to GeoDataFrame, since merging turned the gdf into a df
ny_prod_merge = ny_prod_merge.set_geometry('geometry')

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
ny_prod_merge = ny_prod_merge[ny_prod_merge.geometry.notna()].reset_index()

# ------------------------------------------------------
# Plot data
fig1 = scatterMaps(
    ny_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GasProd",
    dataScaling=2000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="New York well-level gas production (2022), ",
    saveFigPath=results_folder + "NY_well_level_production.tiff"
)
# ------------------------------------------------------
# %% NEW YORK - Integration
# ------------------------------------------------------
ny_integrated, ny_prod_err = integrate_production(
    ny_prod_merge,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="NEW YORK",
    src_ref_id="154, 211",  # DONE
    src_date="2023-07-05",  # production source date from webpage
    on_offshore="Onshore",
    fac_name='FAC_NAME',
    fac_id='FAC_ID',
    fac_type='FAC_TYPE',
    spud_date="SPUD_DATE",
    comp_date="COMP_DATE",
    drill_type="DRILL_TYPE",
    fac_status='FAC_STATUS',
    op_name='OPERATOR',
    oil_bbl='OilProd',
    gas_mcf='GasProd',
    water_bbl='WaterProd',
    condensate_bbl=None,
    prod_days="prod_days",
    prod_year=2022,
    entity_type="WELL",
    fac_latitude="LATITUDE",
    fac_longitude="LONGITUDE"
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('NEW YORK',
                                             before_after_table,
                                             ny_integrated)

save_spatial_data(
    ny_integrated,
    "new_york_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# summ_NY = check_production_stats(ny_integrated, ny_prod, "OilProd", "GasProd")


# =============================================================================
# %% NORTH DAKOTA [2022] - Read + aggregate production data (w/ well coords)
# Production data downloaded from: https://www.dmr.nd.gov/oilgas/mprindex.asp
# =============================================================================
fp_xlsx = r"north_dakota//"

# Read individual Excel files
nd_prod_dfs = []
files = os.listdir(fp_xlsx)
for file in files:
    if file.endswith('.xlsx') and file.startswith('2022'):
        # print(file)
        df = pd.read_excel(os.path.join(fp_xlsx, file), sheet_name=0)
        nd_prod_dfs.append(df)

# Concatenate result
nd_prod = pd.concat(nd_prod_dfs).reset_index(drop=True)
print("==============================")
print(nd_prod.columns)
print(nd_prod.head())

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['NORTH DAKOTA', 'oil_original'] = nd_prod.Oil.sum()
before_after_table.at['NORTH DAKOTA', 'gas_original'] = nd_prod.Gas.sum()  # TODO should we use GasSold instead?
# -----------------------------------------------------------------------------
# There are a small number of APIs that report more than one row of production
# in a single month, and that's because each row reports the production from a
# particular POOL that API has access to.
dupes = nd_prod[nd_prod.duplicated(subset=['API_WELLNO', 'ReportDate'], keep=False)]

# FIRST, group the rows so that there's only one API-14 record PER MONTH.
# This sums together production from 2+ different pools.
# This step prevents Days Produced for wells being double-counted and above 365 in a year
agg_funcs1 = {
    'Company': 'last',
    'WellName': 'last',
    'County': 'last',
    # 'FieldName': 'last',
    # 'Pool': 'last',
    'Oil': 'sum',
    'Wtr': 'sum',
    'Days': 'max',  # different treatment
    'Gas': 'sum',
    'GasSold': 'sum',
    'Flared': 'sum',
    'Lat': 'last',
    'Long': 'last'
}
nd_prod_ag = nd_prod.groupby(by=['API_WELLNO',
                                 'ReportDate'],
                             as_index=False).agg(agg_funcs1)

# Should have zero dupes for API and month now
dupes = nd_prod_ag[nd_prod_ag.duplicated(subset=['API_WELLNO', 'ReportDate'], keep=False)]

# THEN, group by APIs so that you get one record for that well's 2022 annual
# production.
agg_funcs2 = {
    'Company': 'last',
    'WellName': 'last',
    'County': 'last',
    'Oil': 'sum',
    'Wtr': 'sum',
    'Days': 'sum',  # different treatment
    'Gas': 'sum',
    'GasSold': 'sum',
    'Flared': 'sum',
    'Lat': 'last',
    'Long': 'last'
}
nd_prod_agg = nd_prod_ag.groupby(by=["API_WELLNO"], as_index=False).agg(agg_funcs2)

# Convert to GDF
nd_prod_agg = gpd.GeoDataFrame(nd_prod_agg,
                               geometry=gpd.points_from_xy(nd_prod_agg.Long,
                                                           nd_prod_agg.Lat),
                               crs="epsg:4326")

# Indicate prod year and ENTITY type
nd_prod_agg["prod_year"] = 2022
nd_prod_agg["entity_type"] = "WELL"

# ------------------------------------------------------
# Check accuracy of point locations
# Plot data
figNM = scatterMaps(
    nd_prod_agg,
    lat_lon=True,
    lat_attribute="Lat",
    lon_attribute="Long",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="Gas",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="ND well-level gas production (2022),\n ",
    saveFigPath=results_folder + "ND_2022_production.tiff"
)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['NORTH DAKOTA', 'oil_agg'] = nd_prod_agg.Oil.sum()
before_after_table.at['NORTH DAKOTA', 'gas_agg'] = nd_prod_agg.Gas.sum()

# =============================================================================
# %% NORTH DAKOTA - Integration
# =============================================================================
nd_integrated, nd_errors = integrate_production(
    nd_prod_agg,
    src_date="2022-12-31",  # I am totally guessing.
    src_ref_id="200",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="North Dakota",
    fac_name="WellName",
    fac_id="API_WELLNO",
    fac_type=None,
    spud_date=None,
    comp_date=None,
    drill_type=None,
    fac_status=None,
    op_name="Company",
    oil_bbl="Oil",
    gas_mcf="Gas",
    water_bbl="Wtr",
    condensate_bbl=None,
    prod_days="Days",
    prod_year="prod_year",
    entity_type="entity_type",
    fac_latitude='Lat',
    fac_longitude='Long'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('NORTH DAKOTA',
                                             before_after_table,
                                             nd_integrated)

save_spatial_data(
    nd_integrated,
    "north_dakota_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# summ_ND = check_production_stats(nd_integrated, nd_prod_agg, "Oil", "Gas")


# ======================================================
# %% OHIO [2022] - Read + aggregate production
# updated weekly
# Data source: https://ohiodnr.gov/wps/portal/gov/odnr/discover-and-learn/safety-conservation/about-odnr/oil-gas/oil-gas-resources/featured-content-3
# ======================================================
# Read MS Access data; takes a long time
# fp = "North_America\\United_States_\\State_Raw_Data\\Ohio\\Rbdmsd97_09.12.2023\\Rbdmsd97.mdb"
fp = 'ohio\\Rbdmsd97.mdb'
tableNamesIdx_oh, tableNames_oh, dfs_oh = read_msAccess(fp,
                                                        table_subset=['Production',
                                                                      'Well',
                                                                      'tblLocational'])

# create a dictionary that maps the index (number) of each MS Access table to
# the table's name, so I can select dataframes from `dfs` by Name instead of
# just their index number. (The state of Ohio seems to have changed the order...)
name2index = dict(zip(tableNames_oh, tableNamesIdx_oh))

# What's the index number that refers to the 'Production' table?
# Read in the production data, and keep only the columns I'm interested in
prod_index = name2index.get('Production')
oh_prod = dfs_oh[prod_index][['API_WELLNO',
                              'PRODUCTION_YEAR',
                              'OWNER_NAME',
                              'OIL',
                              'GAS',
                              'BRINE',
                              'DAYS',
                              'DateFirstProd',
                              'MaximumStorageCapacity',
                              'NumberOilStorageTanks']]
print(oh_prod.dtypes)

# How far back does production data go?
print(list(sorted(oh_prod.PRODUCTION_YEAR.unique())))

# Extract production data for year(s) you're interested in
prodyears2keep = ['2022']
oh_prod = oh_prod[oh_prod.PRODUCTION_YEAR.isin(prodyears2keep)]

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['OHIO', 'oil_original'] = oh_prod.OIL.sum()
before_after_table.at['OHIO', 'gas_original'] = oh_prod.GAS.sum()
# -----------------------------------------------------------------------------

# In the original dataset, some wells claim to have produced OVER 365 days a year
# Manually override these assumed-erroneous values.
oh_prod.loc[oh_prod.DAYS > 365, 'DAYS'] = 365

# =============================================================================
# %% OHIO - Location data
# =============================================================================
# Extract Well-Level attribute data, and keep only the columns I need
well_index = name2index.get('Well')
oh_wells_noloc = dfs_oh[well_index][['API_WELLNO',
                                     'WL_STATUS',
                                     'DT_STATUS',
                                     'DT_SPUD',
                                     'DT_COMP',
                                     'WELL_NM',
                                     'WELL_TYP',
                                     'WELL_NO',
                                     'OPNO']]
# Reformat ID fields for merging with locational info
oh_wells_noloc["LOCATION_ID"] = oh_wells_noloc["API_WELLNO"]
oh_wells_noloc['OPNO'] = oh_wells_noloc['OPNO'].astype('Int64')


# Extract Well-Level Location data, and keep only the columns I need
tbl_loc_index = name2index.get('tblLocational')
tbl_loc = dfs_oh[tbl_loc_index][["LOCATION_ID",
                                 "WH_LAT",
                                 "WH_LONG",
                                 "SLANT"]]
# Only select locations that are not null
tbl_loc = tbl_loc[(tbl_loc.WH_LAT.notnull() & tbl_loc.WH_LONG.notnull())]

# a few longitudes are erroneously positive; fix them
tbl_loc.loc[tbl_loc.WH_LONG > 0, 'WH_LONG'] = tbl_loc.WH_LONG * -1

# Merge the two datasets: location and attribute information
oh_wells = pd.merge(tbl_loc,
                    oh_wells_noloc,
                    on="LOCATION_ID",
                    how="left")

print(f'Number of records in original locational dataset = {tbl_loc.shape[0]}')
print(f'Compare: number of records in merged dataset = {oh_wells.shape[0]}')

# =============================================================================
# %% OHIO - Merge and clean
# =============================================================================
# Merge well locations with prod data based on common API_WELLNO
oh_prod_merge = pd.merge(oh_prod, oh_wells, on="API_WELLNO", how='left')
print(oh_prod_merge.columns)
print(oh_prod_merge.head())

# ------------------------------------------------------
# Exclude any records with NULL LAT AND LON
oh_prod_merge = oh_prod_merge[~oh_prod_merge.WH_LAT.isnull()]

# ------------------------------------------------------
# Create GeoDataFrame
oh_prod_merge = gpd.GeoDataFrame(oh_prod_merge,
                                 geometry=gpd.points_from_xy(oh_prod_merge.WH_LONG,
                                                             oh_prod_merge.WH_LAT),
                                 crs="epsg:4326")
print(oh_prod_merge.head())

# ------------------------------------------------------
# Preview 2022 production data on a map
# 2022 production data
prod_oh_2022 = oh_prod_merge.query("PRODUCTION_YEAR == '2022'")
fig1 = scatterMaps(
    prod_oh_2022,
    lat_lon=True,
    lat_attribute="WH_LAT",
    lon_attribute="WH_LONG",
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GAS",
    dataScaling=40000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="Ohio 2022 well-level gas production, ",
    saveFigPath=results_folder + "OH_2022_production_GAS.tiff"
)

fig1OIL = scatterMaps(
    prod_oh_2022,
    lat_lon=True,
    lat_attribute="WH_LAT",
    lon_attribute="WH_LONG",
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="OIL",
    dataScaling=5000,
    colorMapName="plasma",
    dataLabel="Oil production (BBL/year)",
    figTitle="Ohio 2022 well-level gas production, ",
    saveFigPath=results_folder + "OH_2022_production_OIL.tiff"
)

# ------------------------------------------------------
# Check Drill configuration
print(oh_prod_merge.SLANT.unique())
oh_prod_merge = replace_row_names(oh_prod_merge,
                                  colName="SLANT",
                                  dict_names={'V': 'VERTICAL',
                                              'D': 'DIRECTIONAL',
                                              'O': 'OTHER HORIZONTAL',
                                              'H': 'HORIZONTAL',
                                              'G': 'N/A'})

# ------------------------------------------------------
# Check well types
# Check "Codes" table
print(oh_prod_merge.WELL_TYP.unique())
dict_names2 = {
    'OG_R': 'OIL AND GAS',
    'UN_R': 'N/A',
    'ST_R': 'STRATIGRAPHIC TEST',
    'SW_R': 'CLASS II DISPOSAL WELLS BRINE/WASTE',
    'MN_R': 'MONITOR/OBSERVATION',
    'ER_R': 'CLASS II ENHANCED RECOVERY',
    'OG': 'OIL AND GAS',
    'GS_R': 'GAS STORAGE',
    'WS': 'WATER SUPPLY',
    'BR_R': 'BRINE PRODUCTION',
    'OG': 'OIL AND GAS',
    np.nan: 'N/A'
}
oh_prod_merge = replace_row_names(oh_prod_merge,
                                  colName="WELL_TYP",
                                  dict_names=dict_names2)

# ------------------------------------------------------
# Date fields
clean_a_date_field(oh_prod_merge, 'DT_SPUD')
clean_a_date_field(oh_prod_merge, 'DT_COMP')

# ------------------------------------------------------
print(oh_prod_merge.WL_STATUS.unique())
dict_status = {
    'PR': 'PRODUCING',
    'PA': 'PLUGGED AND ABANDONED',
    'DM': 'DOMESTIC WELL',
    'HP': 'HISTORICAL PRODUCTION WELL',
    'SI': 'SHUT-IN',
    'I4': 'TEMPORARILY INACTIVE - FOURTH YEAR',
    'DR': 'WELL DRILLED',
    'DG': 'DRILLING',
    'FR': 'FINAL RESTORATION',
    'PB': 'PLUGGED BACK',
    'AI': 'ACTIVE INJECTION',
    'IA': 'DRILLED, INACTIVE',
    'DA': 'DRY AND ABANDONED',
    'OP': 'ORPHAN WELL - PENDING',
    'ND': 'NOT DRILLED',
    'NF': 'FIELD INSPECTED, WELL NOT FOUND',
    'LH': 'LOST HOLE',
    'I3': 'TEMPORARILY INACTIVE - THIRD YEAR',
    'WW': 'PLUGGED BACK FOR WATER WELL',
    'UN': 'N/A',
    'OR': 'ORPHAN WELL - READY',
    'SW': 'STORAGE WELL',
    'EX': 'PERMIT EXPIRED',
    # 'WP':  # FIXME
    np.nan: 'N/A'
}
oh_prod_merge = replace_row_names(oh_prod_merge,
                                  colName="WL_STATUS",
                                  dict_names=dict_status)

# ------------------------------------------------------
# Convert prod year to integers
oh_prod_merge['PRODUCTION_YEAR'] = oh_prod_merge['PRODUCTION_YEAR'].astype(int)

# ------------------------------------------------------
# Check for TOTAL PRODUCTION AS REPORTED IN ORIGINAL PRODUCTION DATA versus MERGED DATASET\
print("Gas production, oil production in original (2022) dataset = ", round(prod_oh_2022.GAS.sum() / 1e9, 2), " Tcf and ", round(prod_oh_2022.OIL.sum() / 1e6, 2), " million barrels")
print("Gas production, oil production in MERGED dataset = ", round(oh_prod_merge.GAS.sum() / 1e9, 2), " Tcf and ", round(oh_prod_merge.OIL.sum() / 1e6, 2), " million barrels")

# ------------------------------------------------------
# NOTE: THERE ARE FEW RECORDS WITH ANNUAL PRODUCTION DAYS > 365 DAYS IN THE YEAR
# FOR now, we are keeping the data as is, but we need to figure out WHY these values are weird
print("==> Number of records with production days > 365 = ", oh_prod_merge.query("DAYS > 365").shape[0])
print(oh_prod_merge.query("DAYS > 365"))

# =============================================================================
# %% OHIO - Integration
# =============================================================================
oh_integrated, oh_prod_err = integrate_production(
    oh_prod_merge,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="OHIO",
    src_ref_id="213",  # DONE
    src_date="2024-06-25",
    on_offshore="Onshore",
    fac_name='WELL_NM',
    fac_id="API_WELLNO",
    fac_type="WELL_TYP",
    spud_date="DT_SPUD",
    comp_date="DT_COMP",
    drill_type="SLANT",
    fac_status='WL_STATUS',
    op_name="OWNER_NAME",
    oil_bbl="OIL",
    gas_mcf="GAS",
    water_bbl='BRINE',  # Use 'BRINE' production to represent 'WATER' production
    condensate_bbl=None,
    prod_days="DAYS",
    prod_year="PRODUCTION_YEAR",
    entity_type="WELL",
    fac_latitude="WH_LAT",
    fac_longitude="WH_LONG"
)


# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('OHIO',
                                             before_after_table,
                                             oh_integrated)
# Check summary stats
# original_2022 = oh_prod_1522.query("PRODUCTION_YEAR == ['2022']").reset_index(drop=True)
# summ_OH = check_production_stats(oh_integrated, original_2022, "OIL", "GAS")

save_spatial_data(
    oh_integrated,
    "ohio_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# ======================================================
# %% PENNSYLVANIA [2022] - Unconventional - Read + aggregate production
# ======================================================
# Read production data for Unconventional wells
pa_u_prod = pd.read_csv("pennsylvania\\OilGasProduction_2022.csv")
pa_u_prod = pa_u_prod[pa_u_prod.WIRE_LABEL.str.contains('Unconventional')].reset_index(drop=True)
print(pa_u_prod.head().iloc[:, 0:10])
print(*pa_u_prod.columns, sep='\n')

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['PENNSYLVANIA_U', 'oil_original'] = pa_u_prod.OIL_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_U', 'gas_original'] = pa_u_prod.GAS_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_U', 'cond_original'] = pa_u_prod.CONDENSATE_QUANTITY.sum()
# -----------------------------------------------------------------------------

# Data format is rows of monthly well production - group by WELL_PERMIT_NUM
# to arrive at one row/well with annaul production

# Sort records so that the most recent month of reporting is FIRST, and
# therefore the most recent status/operator is listed first
pa_u_prod['PRODUCTION_PERIOD_END_DATE'] = pd.to_datetime(pa_u_prod['PRODUCTION_PERIOD_END_DATE'])
pa_u_prod_sorted = pa_u_prod.sort_values(by='PRODUCTION_PERIOD_END_DATE',
                                         ascending=False,
                                         na_position='last')

agg_f = {
    'FARM': 'first',  # well name and number
    'WELL_STATUS': 'first',
    'WELL_NO': 'first',
    'SPUD_DATE': 'first',
    'GAS_QUANTITY': 'sum',
    'GAS_OPERATING_DAYS': 'sum',
    'CONDENSATE_QUANTITY': 'sum',
    'CONDENSATE_OPERATING_DAYS': 'sum',
    'OIL_QUANTITY': 'sum',
    'OIL_OPERATING_DAYS': 'sum',
    'AVERAGE_IND': 'first',
    'CLIENT': 'first',
    'COUNTY': 'first',
    'LATITUDE_DECIMAL': 'first',
    'LONGITUDE_DECIMAL': 'first',
    'UNCONVENTIONAL_IND': 'first',
    'CONFIG_CODE': 'first',
    'WELL_CODE_DESC': 'first'
}

pa_u_prod_agg = pa_u_prod_sorted.groupby(by="PERMIT_NUM").agg(agg_f).reset_index(drop=False)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['PENNSYLVANIA_U', 'oil_agg'] = pa_u_prod_agg.OIL_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_U', 'gas_agg'] = pa_u_prod_agg.GAS_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_U', 'cond_agg'] = pa_u_prod_agg.CONDENSATE_QUANTITY.sum()

# ======================================================
# %% PENNSYLVANIA - Unconventional - Clean
# ======================================================
# Check for any errors in lat/lon
pa_u_prod_agg = pa_u_prod_agg[~pa_u_prod_agg.LONGITUDE_DECIMAL.isnull()]
print("Total # of records with null lat/lon values = ", len(pa_u_prod_agg[pa_u_prod_agg.LONGITUDE_DECIMAL.isnull()]))

print("CHECKING MIN and MAX Lat/Lon values in dataset")
print([pa_u_prod_agg.LONGITUDE_DECIMAL.min(), pa_u_prod_agg.LONGITUDE_DECIMAL.max(), pa_u_prod_agg.LATITUDE_DECIMAL.min(), pa_u_prod_agg.LATITUDE_DECIMAL.max()])

# If there are location errors in this dataset [e.g., the wells with longitude == -1.0], drop them
# print(prod_gp1.query("LONGITUDE_DECIMAL == -1.0"))
# pa_u_prod_agg = pa_u_prod_agg.query('LONGITUDE_DECIMAL != -1.0')

# ------------------------------------------------------
# Create GDF
pa_u_prod_agg = gpd.GeoDataFrame(pa_u_prod_agg,
                                 geometry=gpd.points_from_xy(
                                     pa_u_prod_agg.LONGITUDE_DECIMAL,
                                     pa_u_prod_agg.LATITUDE_DECIMAL),
                                 crs="epsg:4326")

# ------------------------------------------------------
# Preview 2022 production data
# Plot data
fig1 = scatterMaps(
    pa_u_prod_agg,
    lat_lon=True,
    lat_attribute="LATITUDE_DECIMAL",
    lon_attribute="LONGITUDE_DECIMAL",
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GAS_QUANTITY",
    dataScaling=100000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="Pennsylvania 2022 well-level gas production (Mcf), ",
    saveFigPath=results_folder + "PA_2022_production.tiff"
)

# ------------------------------------------------------
# Spud date format
print(pa_u_prod_agg.SPUD_DATE.unique())
clean_a_date_field(pa_u_prod_agg, 'SPUD_DATE')

# ------------------------------------------------------
# Well configuration or drill type
print(pa_u_prod_agg.CONFIG_CODE.unique())
pa_u_prod_agg = replace_row_names(pa_u_prod_agg,
                                  colName="CONFIG_CODE",
                                  dict_names={'Horizontal Well': 'HORIZONTAL',
                                              'Vertical Well': 'VERTICAL',
                                              'Deviated Well': 'DEVIATED'})

# ------------------------------------------------------
# Check well status
print(pa_u_prod_agg.WELL_STATUS.unique())
pa_u_prod_agg = replace_row_names(pa_u_prod_agg,
                                  colName="WELL_STATUS",
                                  dict_names={'Plugged OG Well': 'PLUGGED'})

# ------------------------------------------------------
# Production days - Each well reports its operating days for gas, oil, and
# condensate separately. Pick out the largest number of days from the three
# options, and use that number as our "number of producing days" for the well
pa_u_prod_agg['prod_days'] = pa_u_prod_agg[['GAS_OPERATING_DAYS',
                                            'OIL_OPERATING_DAYS',
                                            'CONDENSATE_OPERATING_DAYS']].max(axis=1)

# Create production year column
pa_u_prod_agg['year_'] = 2022

check_fac_type_status_drill(pa_u_prod_agg,
                            type_="WELL_CODE_DESC",
                            status_="WELL_STATUS")

pa_u_prod_agg["WELL_TYPE"] = pa_u_prod_agg["WELL_CODE_DESC"].replace({'COMB. OIL&GAS': 'OIL AND GAS'})

# ======================================================
# %% PENNSYLVANIA - Unconventional wells - Integration
# ======================================================
pa_u_prod_integrated, pa_prod_err = integrate_production(
    pa_u_prod_agg,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="PENNSYLVANIA",
    src_ref_id="215",
    src_date="2024-06-25",  # download date, since not sure when it was last updated
    on_offshore="Onshore",
    fac_name="FARM",
    fac_id="PERMIT_NUM",
    fac_type="WELL_CODE_DESC",
    spud_date="SPUD_DATE",
    # comp_date=None,
    drill_type="CONFIG_CODE",
    fac_status='WELL_STATUS',
    op_name="CLIENT",
    oil_bbl="OIL_QUANTITY",
    gas_mcf="GAS_QUANTITY",
    # water_bbl=None,
    condensate_bbl="CONDENSATE_QUANTITY",
    prod_days="prod_days",
    prod_year="year_",
    entity_type="WELL",
    fac_latitude="LATITUDE_DECIMAL",
    fac_longitude="LONGITUDE_DECIMAL"
)

# ------------------------------------------------------
# Convert gas and oil and condensate to float  # TODO why are these values changed to strings during integration?
# ------------------------------------------------------
# pa_u_prod_integrated["GAS_MCF"] = pa_u_prod_integrated["GAS_MCF"].astype(float)
# pa_u_prod_integrated["OIL_BBL"] = pa_u_prod_integrated["OIL_BBL"].astype(float)
# pa_u_prod_integrated["WATER_BBL"] = pa_u_prod_integrated["WATER_BBL"].astype(float)
# pa_u_prod_integrated["CONDENSATE_BBL"] = pa_u_prod_integrated["CONDENSATE_BBL"].astype(float)


# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('PENNSYLVANIA_U',
                                             before_after_table,
                                             pa_u_prod_integrated)

save_spatial_data(
    pa_u_prod_integrated,
    "pennsylvania_unconventional_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# Quick check of production data in original data and in integrated dataset
# summ_PA_UNCONV = check_production_stats(pa_u_prod_integrated,
#                                         pa_u_prod_agg,
#                                         "OIL_QUANTITY",
#                                         "GAS_QUANTITY")

# ======================================================
# %% PENNSYLVANIA - Conventional - Read + aggregate
# ======================================================
pa_c_prod = pd.read_csv("pennsylvania\\OilGasProduction_2022.csv")
pa_c_prod = pa_c_prod[pa_c_prod.WIRE_LABEL.str.contains('Conventional')].reset_index(drop=True)
# Select only records that reflect 2022 production
pa_c_prod = pa_c_prod.query("PERIOD_ID == '2022-0'")

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['PENNSYLVANIA_C', 'oil_original'] = pa_c_prod.OIL_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_C', 'gas_original'] = pa_c_prod.GAS_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_C', 'cond_original'] = pa_c_prod.CONDENSATE_QUANTITY.sum()
# -----------------------------------------------------------------------------

# There are some instances of duplicated PERMIT_NUMs across rows, with different CLIENT values
# TODO - if each of these rows have production and we sum them, are we double
# counting some production volumnes?

# Group by WELL_PERMIT_NUM
agg_f = {
    'FARM': 'first',
    'WELL_STATUS': 'first',
    'WELL_NO': 'first',
    'SPUD_DATE': 'first',
    'GAS_QUANTITY': 'sum',
    'GAS_OPERATING_DAYS': 'sum',
    'CONDENSATE_QUANTITY': 'sum',
    'CONDENSATE_OPERATING_DAYS': 'sum',
    'OIL_QUANTITY': 'sum',
    'OIL_OPERATING_DAYS': 'sum',
    'AVERAGE_IND': 'first',
    'CLIENT': 'first',
    'COUNTY': 'first',
    'LATITUDE_DECIMAL': 'first',
    'LONGITUDE_DECIMAL': 'first',
    'UNCONVENTIONAL_IND': 'first',
    'CONFIG_CODE': 'first',
    'WELL_CODE_DESC': 'first'
}

pa_c_prod_agg = pa_c_prod.groupby(by=["PERMIT_NUM", "FARM", "CLIENT"], as_index=False).agg(agg_f).reset_index(drop=False)
# pa_c_prod_agg = pa_c_prod.groupby(by=["PERMIT_NUM"], as_index=False).agg(agg_f).reset_index(drop=False)
print(pa_c_prod_agg.head())

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['PENNSYLVANIA_C', 'oil_agg'] = pa_c_prod_agg.OIL_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_C', 'gas_agg'] = pa_c_prod_agg.GAS_QUANTITY.sum()
before_after_table.at['PENNSYLVANIA_C', 'cond_agg'] = pa_c_prod_agg.CONDENSATE_QUANTITY.sum()

# ======================================================
# %% PENNSYLVANIA - Conventional - Clean
# ======================================================
# Remove any records with null geometries
pa_c_prod_agg = pa_c_prod_agg[~pa_c_prod_agg.LONGITUDE_DECIMAL.isnull()]

print("CHECKING MIN and MAX Lat/Lon values in dataset")
print([pa_c_prod_agg.LONGITUDE_DECIMAL.min(),
       pa_c_prod_agg.LONGITUDE_DECIMAL.max(),
       pa_c_prod_agg.LATITUDE_DECIMAL.min(),
       pa_c_prod_agg.LATITUDE_DECIMAL.max()])

# Create GeoDataFrame
pa_c_prod_agg = gpd.GeoDataFrame(pa_c_prod_agg,
                                 geometry=gpd.points_from_xy(
                                     pa_c_prod_agg.LONGITUDE_DECIMAL,
                                     pa_c_prod_agg.LATITUDE_DECIMAL),
                                 crs="epsg:4326")

# ------------------------------------------------------
# Spud date format
print(pa_c_prod_agg.SPUD_DATE.unique())
clean_a_date_field(pa_c_prod_agg, 'SPUD_DATE')

# ------------------------------------------------------
# Well configuration or drill type
print(pa_c_prod_agg.CONFIG_CODE.unique())
pa_c_prod_agg = replace_row_names(pa_c_prod_agg,
                                  colName="CONFIG_CODE",
                                  dict_names={'Horizontal Well': 'HORIZONTAL',
                                              'Vertical Well': 'VERTICAL',
                                              'Deviated Well': 'DEVIATED',
                                              'Undetermined': 'N/A'})

# ------------------------------------------------------
# Check well status
print(pa_c_prod_agg.WELL_STATUS.unique())
pa_c_prod_agg = replace_row_names(pa_c_prod_agg,
                                  colName="WELL_STATUS",
                                  dict_names={'Plugged OG Well': 'PLUGGED'})

# ------------------------------------------------------
# Production days - Each well reports its operating days for gas, oil, and
# condensate separately. Pick out the largest number of days from the three
# options, and use that number as our "number of producing days" for the well
pa_c_prod_agg['prod_days'] = pa_c_prod_agg[['GAS_OPERATING_DAYS',
                                            'OIL_OPERATING_DAYS',
                                            'CONDENSATE_OPERATING_DAYS']].max(axis=1)

# Year
pa_c_prod_agg['year_'] = 2022

check_fac_type_status_drill(pa_c_prod_agg,
                            type_="CONFIG_CODE",
                            status_="WELL_CODE_DESC",
                            drill_="CONFIG_CODE")

# ======================================================
# %% PENNSYLVANIA - Conventional wells - Integration
# ======================================================
pa_c_prod_integrated, pa_prod_conv_err = integrate_production(
    pa_c_prod_agg,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="PENNSYLVANIA",
    src_ref_id="215",
    src_date="2024-06-25",  # download date, since not sure when it was last updated
    on_offshore="Onshore",
    fac_name="FARM",
    fac_id="PERMIT_NUM",
    fac_type="WELL_CODE_DESC",
    spud_date="SPUD_DATE",
    comp_date=None,
    drill_type="CONFIG_CODE",
    fac_status='WELL_STATUS',
    op_name="CLIENT",
    oil_bbl="OIL_QUANTITY",
    gas_mcf="GAS_QUANTITY",
    # water_bbl=None,
    condensate_bbl="CONDENSATE_QUANTITY",
    prod_days="prod_days",
    prod_year="year_",
    entity_type="WELL",
    fac_latitude="LATITUDE_DECIMAL",
    fac_longitude="LONGITUDE_DECIMAL"
)
# ------------------------------------------------------
# Convert gas and oil and condensate to float  # TODO why are these values changed to strings during integration?
# ------------------------------------------------------
pa_c_prod_integrated["GAS_MCF"] = pa_c_prod_integrated["GAS_MCF"].astype(float)
pa_c_prod_integrated["OIL_BBL"] = pa_c_prod_integrated["OIL_BBL"].astype(float)
pa_c_prod_integrated["WATER_BBL"] = pa_c_prod_integrated["WATER_BBL"].astype(float)
pa_c_prod_integrated["CONDENSATE_BBL"] = pa_c_prod_integrated["CONDENSATE_BBL"].astype(float)

fig1 = scatterMaps(
    pa_c_prod_integrated,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GAS_MCF",
    dataScaling=1000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="Pennsylvania 2022 well-level gas production (Mcf), \n conventional wells",
    saveFigPath=results_folder + "PA_2022_conv.tiff"
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('PENNSYLVANIA_C',
                                             before_after_table,
                                             pa_c_prod_integrated)

save_spatial_data(
    pa_c_prod_integrated,
    "pennsylvania_conventional_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)

# Quick check of production data
# summ_PA_CONV = check_production_stats(pa_c_prod_integrated, pa_conv_gdf, "OIL_QUANTITY", "GAS_QUANTITY")

# =============================================================================
# %% UTAH [2022]- Read + aggregate production data
# Production data available from https://oilgas.ogm.utah.gov/oilgasweb/data-center/dc-main.xhtml
# =============================================================================
# Read Utah oil and gas production data, which is reported monthly
ut_prod = pd.read_csv(r'utah/Production2020To2024.csv')
print(ut_prod.head())
print(ut_prod.columns)

ut_prod.API = ut_prod.API.astype(str)

# Query for only 2022 production
ut_prod["report_date"] = pd.to_datetime(ut_prod["ReportPeriod"])
ut_prod["report_year"] = pd.to_datetime(ut_prod["ReportPeriod"]).dt.year
ut_prod_2022 = ut_prod.query("report_year == 2022").reset_index(drop=True)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['UTAH', 'oil_original'] = ut_prod_2022.Oil.sum()
before_after_table.at['UTAH', 'gas_original'] = ut_prod_2022.Gas.sum()
# -----------------------------------------------------------------------------

# Handle multiple rows for one API number
# Each API-10 in `ut_prod` can appear in 2+ rows, with a row containing
# that surface location's production from multiple rock formations or multiple
# well bores.

# FIRST, group the rows so that there's only one API-12 (API + bore) record per month.
# This step prevents DaysProducing for wells being double-counted and above 365 in a year
ut_agg_funcs_1 = {
    'Operator': 'last',
    'WellType': 'last',
    'Oil': 'sum',
    'Gas': 'sum',
    'Water': 'sum',
    'report_year': 'last',
    'DaysProd': 'max'
}
ut_prod_agg = ut_prod_2022.groupby(by=['API',
                                       'WellBore',
                                       'report_date'],
                                   as_index=False).agg(ut_agg_funcs_1)

# THEN, group by API-12 so that you get one record for that API-12's 2022 annual production
ut_agg_funcs_2 = {
    'Operator': 'last',
    'WellType': 'last',
    'Oil': 'sum',
    'Gas': 'sum',
    'Water': 'sum',
    'report_year': 'last',
    'DaysProd': 'sum'  # sum the volumes produced in each month
}
ut_prod_agg = ut_prod_agg.groupby(by=['API', 'WellBore'],
                                  as_index=False).agg(ut_agg_funcs_2)


# FINALLY, group by API-10s so you get one record for the year per surface location
ut_agg_funcs_3 = {
    'Operator': 'last',
    'WellType': 'last',
    'Oil': 'sum',
    'Gas': 'sum',
    'Water': 'sum',
    'report_year': 'last',
    'DaysProd': 'max'  # 2+ bores can produce at once, so don't sum them
}
ut_prod_agg = ut_prod_agg.groupby(by=['API'],
                                  as_index=False).agg(ut_agg_funcs_3)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['UTAH', 'oil_agg'] = ut_prod_agg.Oil.sum()
before_after_table.at['UTAH', 'gas_agg'] = ut_prod_agg.Gas.sum()

# =============================================================================
# %% UTAH - Read wells
# Read OGIM wells
ut_wells = ogim_wells_usa.query("STATE_PROV == 'UTAH'")
print(ut_wells.columns)
print(ut_wells.head())

# =============================================================================
# %% UTAH - Merge and clean
# =============================================================================
ut_prod_merge = pd.merge(ut_prod_agg,
                         ut_wells,
                         left_on='API',
                         right_on='FAC_ID',
                         how='left')

# Convert to GeoDataFrame, since merging turned the gdf into a df
ut_prod_merge = ut_prod_merge.set_geometry('geometry')

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
ut_prod_merge = ut_prod_merge[ut_prod_merge.geometry.notna()].reset_index()

print(ut_prod_merge.columns)

# ------------------------------------------------------
# Check if volumes remain the same after aggregation
print("Total oil production in original dataset = ", round(ut_prod_2022["Oil"].sum() / 1e6, 2))
print("Total oil production in merged dataset = ", round(ut_prod_merge["Oil"].sum() / 1e6, 2))

print("Total gas production in original dataset = ", round(ut_prod_2022["Gas"].sum() / 1e6, 2))
print("Total gas production in merged dataset = ", round(ut_prod_merge["Gas"].sum() / 1e6, 2))

# ------------------------------------------------------
# Plot production
fig1 = scatterMaps(
    ut_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="Gas",
    dataScaling=7500,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="Utah well-level gas production (2022), ",
    saveFigPath=results_folder + "UT_2022_production.tiff"
)

# =============================================================================
# %% UTAH - Integration
# =============================================================================
ut_integrated, ut_errors = integrate_production(
    ut_prod_merge,
    src_date="2024-07-16",  # daily from what I can tell in teh "Received" column!
    src_ref_id="202, 218",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Utah",
    fac_name="FAC_NAME",
    fac_id="FAC_ID",
    fac_type='FAC_TYPE',
    spud_date='SPUD_DATE',
    comp_date='COMP_DATE',
    drill_type="DRILL_TYPE",
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="Oil",
    gas_mcf="Gas",
    water_bbl="Water",
    # condensate_bbl=None,
    prod_days="DaysProd",
    prod_year="report_year",
    entity_type="WELL",
    fac_latitude='LATITUDE',
    fac_longitude='LONGITUDE'
)
# Histogram of producing days for each well in ingtegrated result
plt.hist(ut_integrated.PROD_DAYS)
# summ_UT = check_production_stats(ut_integrated, ut_prod, "Oil", "Gas")

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('UTAH',
                                             before_after_table,
                                             ut_integrated)

save_spatial_data(
    ut_integrated,
    "utah_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


# ======================================================
# %% WEST VIRGINIA [2022] - Read + aggregate production data

# Well production data available from the WV [DEP](https://dep.wv.gov/oil-and-gas/databaseinfo/Pages/default.aspx)
# Location data for wells available from the new data hub [WV Open Data](https://data-wvdep.opendata.arcgis.com/datasets/WVDEP::all-dep-wells/about)
# Production data includes data for the years 2010 through 2022.
# Well locations reported separately from oil and gas production data
# Units used. Gas = MCF (1,000 cubic feet). Oil, Condensate, Water = Barrels (42 gallons).
# =============================================================================
fp1 = "west_virginia//2023-07-18 2022 Production File.xlsx"
wv_2022 = pd.read_excel(fp1)

print(wv_2022.head())
print(wv_2022.columns)

# Calculate total gas [Mcf] and liquids [bbl]
wv_2022['total_gas_mcf'] = wv_2022["Total_Gas"]
wv_2022['total_liq_bbl'] = wv_2022["Total_Oil"]
wv_2022['total_water_bbl'] = wv_2022["Total_Water"]
# TODO - should we add NGL into any of these fields? wv_data_prod_all["Total_NGL"]

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['WEST VIRGINIA', 'oil_original'] = wv_2022.total_liq_bbl.sum()
before_after_table.at['WEST VIRGINIA', 'gas_original'] = wv_2022.total_gas_mcf.sum()
# -----------------------------------------------------------------------------

# Keep only relevant attributes in the production data table
wv_prod = wv_2022[['Year',
                   'API',
                   'County',
                   'Reporting_RP',
                   'Operator',
                   'Well Type',
                   'total_gas_mcf',
                   'total_liq_bbl',
                   'total_water_bbl']]
# Convert API from int to string (they will all be API-10)
wv_prod.API = wv_prod.API.astype(str)

# ------------------------------------------------------
# Some APIs have multiple records associated with them;
# Aggregate these records so each API only has one annual production record
# The multiple records are due to the same well/API reporting production from
# two or more unique "Reporting_RP" values, whatever that means
# However, the API always only has one Operator associated with it.
wv_agg_dict = {
    'Year': 'first',
    'Well Type': 'first',
    'total_gas_mcf': 'sum',
    'total_liq_bbl': 'sum',
    'total_water_bbl': 'sum'
}

wv_prod_agg = wv_prod.groupby(by=['API',
                                  'Operator'],
                              as_index=False).agg(wv_agg_dict)

wv_prod_agg.head()

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['WEST VIRGINIA', 'oil_agg'] = wv_prod_agg.total_liq_bbl.sum()
before_after_table.at['WEST VIRGINIA', 'gas_agg'] = wv_prod_agg.total_gas_mcf.sum()

# =============================================================================
# %% WEST VIRGINIA - Read wells
# =============================================================================
# Read OGIM wells
wv_wells = ogim_wells_usa.query("STATE_PROV == 'WEST VIRGINIA'")
print(wv_wells.columns)
print(wv_wells.head())

# =============================================================================
# %% WEST VIRGINIA - Merge and clean
# =============================================================================
# Merge wells and production
wv_prod_merge = pd.merge(wv_prod_agg,
                         wv_wells,
                         left_on='API',
                         right_on="FAC_ID",
                         how='left')
print(wv_prod_merge.head())

# Convert to GeoDataFrame, since merging turned the gdf into a df
wv_prod_merge = wv_prod_merge.set_geometry('geometry')

# Drop any rows with null geometries (i.e., production APIs that didn't get
# joined to a well location)
wv_prod_merge = wv_prod_merge[wv_prod_merge.geometry.notna()].reset_index()
wv_prod_merge = wv_prod_merge[~wv_prod_merge.geometry.is_empty].reset_index()

# Map of gas production data for 2022
wv_prod_merge = wv_prod_merge.query("Year == 2022").reset_index(drop=True)
fig1 = scatterMaps(
    wv_prod_merge,
    lat_lon=True,
    lat_attribute="LATITUDE",
    lon_attribute="LONGITUDE",
    figWidth=16,
    figHeight=8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="total_gas_mcf",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="West Virginia well-level gas production (2022), ",
    saveFigPath=results_folder + "WV_2022_production.tiff"
)

# =============================================================================
# %% WEST VIRGINIA - Integration
# =============================================================================
wv_integrated, wv_prod_err = integrate_production(
    wv_prod_merge,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="WEST VIRGINIA",
    src_ref_id="151, 152",  # DONE
    src_date="2023-07-18",  # pub date in production data Excel filename
    on_offshore="ON_OFFSHORE",
    fac_name='FAC_NAME',
    fac_id="API",
    fac_type="FAC_TYPE",
    # spud_date='SPUD_DATE',  # all values are null
    comp_date="COMP_DATE",
    drill_type="DRILL_TYPE",
    fac_status='FAC_STATUS',
    op_name="OPERATOR",
    oil_bbl="total_liq_bbl",
    gas_mcf="total_gas_mcf",
    water_bbl="total_water_bbl",
    # condensate_bbl=None,
    # prod_days=None,
    prod_year="Year",
    entity_type="WELL",
    fac_latitude="LATITUDE",
    fac_longitude="LONGITUDE"
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('WEST VIRGINIA',
                                             before_after_table,
                                             wv_integrated)

save_spatial_data(
    wv_integrated,
    "west_virigina_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


# ======================================================
# %% WYOMING [2022] - Read + aggregate production data
# DATA downloaded by 'All Counties by Selected Year' from this URL:
# https://pipeline.wyo.gov/rprodcountyyear.cfm?Oops=#oops#&RequestTimeOut=6500
# Production data INCLUDES lat-longs
# ======================================================
fp_prod = r'wyoming/Production-2022.xls'
# NOTE: while this file has a XLS extension, it's ACTUALLY a tab-delimited file
# So read it in as a tab-delimited CSV.
# This table contains the annual well production values for wells in all counties
wy_prod = pd.read_csv(fp_prod, sep='\t')

wy_prod = gpd.GeoDataFrame(wy_prod,
                           geometry=gpd.points_from_xy(wy_prod.Lon,
                                                       wy_prod.Lat),
                           crs="epsg:4326")
print(wy_prod.columns)
print(wy_prod.head())

# Indicate prod year and ENTITY type
wy_prod["prod_year"] = 2022
wy_prod["entity_type"] = "WELL"

# -----------------------------------------------------------------------------
# DEAL WITH PROD DAYS

# The value '99' represents nodata in the production days columns.
# Replace instances of '99' in these columns with nan
daycols = list(wy_prod.columns[wy_prod.columns.str.endswith('days')])
for daycol in daycols:
    wy_prod[daycol] = wy_prod[daycol].replace(99, np.nan)

# There are some entries with more production days than possible per month
# (for ex., 31 producing days reported in June which has 30 days)
# Repair any of these cases so only the maximum days in each month is reported

# If a record is for a month with 30 (or 28) days AND it reports more days of
# production than possible, make it report the max number of days in that month instead
months_not_31days = ['Apr', 'Jun', 'Sep', 'Nov', 'Feb']
for month in months_not_31days:
    colname = f'{month}_days'
    if month == 'Feb':
        mask_over_28_days_producing = wy_prod[colname] > 28
        wy_prod.loc[mask_over_28_days_producing, colname] = 28
    else:
        mask_over_30_days_producing = wy_prod[colname] > 30
        wy_prod.loc[mask_over_30_days_producing, colname] = 30

# ------------------------------------------------------

# For each record, sum the row's monthly values for GAS, OIL, WATER, PRODUCTION DAYS
col_suffixes = ['gas', 'oil', 'water', 'days']
outcolnames = ['GAS_MCF', 'OIL_BBL', 'WATER_BBL', 'PROD_DAYS']

for x, outcol in zip(col_suffixes, outcolnames):
    # print(x)
    column_list = [f'Jan_{x}',
                   f'Feb_{x}',
                   f'Mar_{x}',
                   f'Apr_{x}',
                   f'May_{x}',
                   f'Jun_{x}',
                   f'Jul_{x}',
                   f'Aug_{x}',
                   f'Sep_{x}',
                   f'Oct_{x}',
                   f'Nov_{x}',
                   f'Dec_{x}']
    # print(column_list)
    wy_prod[outcol] = wy_prod[column_list].sum(axis=1)

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['WYOMING', 'oil_original'] = wy_prod.OIL_BBL.sum()
before_after_table.at['WYOMING', 'gas_original'] = wy_prod.GAS_MCF.sum()

# Drop some columns that I no longer need
wy_prod = wy_prod.drop(wy_prod.iloc[:, 16:64], axis=1)
wy_prod = wy_prod.drop(wy_prod.iloc[:, 19:44], axis=1)
print(wy_prod.columns)
# -----------------------------------------------------------------------------

# Un-abbreviate drilling trajectory
print(wy_prod["Horiz_Dir"].unique())
wy_prod["DRILL_TYPE"] = wy_prod["Horiz_Dir"].replace({'N': 'N/A',
                                                      'D': 'DIRECTIONAL',
                                                      'H': 'HORIZONTAL'})

# ------------------------------------------------------
# Un-abbreviate facility types
# https://wogcc.wyo.gov/public-resources/help-with-website/codes-and-symbols
print(wy_prod["Wellclass"].unique())
dict_class = {
    'O': 'OIL',
    'G': 'GAS',
    'D': 'DISPOSAL',
    'MW': 'MONITORING',
    'I': 'INJECTION',
    'M': 'MONITORING',
    '5': 'N/A',
    'LW': 'WATER WELL',
    'GO': 'GAS ORPHANED',
    'S': 'SOURCE WELL',
    'ST': 'STRAT TEST',
    'OO': 'OIL ORPHANED',
    '1': 'N/A'
}
wy_prod["Wellclass"] = wy_prod["Wellclass"].replace(dict_class)

# ------------------------------------------------------
# Check min and max lat and lon
print("Max and min lon = ", wy_prod["Lon"].max(), " ", wy_prod["Lon"].min())

# Check locational accuracy - Plot data
figWY = scatterMaps(
    wy_prod,
    lat_lon=True,
    lat_attribute="Lat",
    lon_attribute="Lon",
    figWidth=16,
    figHeight=5.8,
    NA_SA_extent=False,
    showLegend=False,
    heatMap=True,
    colorAttr="GAS_MCF",
    dataScaling=30000,
    colorMapName="plasma",
    dataLabel="Gas production (Mcf/year)",
    figTitle="WY well-level gas production (2022),\n ",
    saveFigPath=results_folder + "WY_2022_production.tiff"
)

# =============================================================================
# %% WYOMING - Integration
# =============================================================================
wy_integrated, wy_errors = integrate_production(
    wy_prod,
    src_date="2024-07-09",
    src_ref_id="201",  # DONE
    category="OIL AND NATURAL GAS PRODUCTION",
    fac_alias="OIL_GAS_PROD",
    country="United States of America",
    state_prov="Wyoming",
    fac_name="Wellname",
    fac_id="Apino",
    fac_type='Wellclass',
    spud_date=None,
    comp_date=None,
    drill_type='DRILL_TYPE',
    fac_status=None,
    op_name="Company",
    oil_bbl="OIL_BBL",
    gas_mcf="GAS_MCF",
    water_bbl="WATER_BBL",
    condensate_bbl=None,
    prod_days="PROD_DAYS",
    prod_year=2022,
    entity_type="entity_type",
    fac_latitude='Lat',
    fac_longitude='Lon'
)

# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('WYOMING',
                                             before_after_table,
                                             wy_integrated)

save_spatial_data(
    wy_integrated,
    "wyoming_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)


print(merge_and_export)

# =============================================================================
# %% MERGE ALL STATE RESULTS (2022) INTO ONE
# This can be done even if all the integration code blocks above have not been
# run, because it relies on the already-exported geojson files
# =============================================================================
# print(blah)

fp = results_folder

all_files = glob.glob(fp + "\\*2022_.geojson")
all_prod = []

for file in all_files:
    file_shortname = file.split('\\')[-1]
    print(f'Reading {file_shortname}...')
    gdf = gpd.read_file(file)

    # # Fill GAS, OIL AND WATER WITH ZEROS
    # gdf["GAS_MCF"] = gdf["GAS_MCF"].replace({-999: 0, '-999': 0, -999.0: 0})
    # gdf["OIL_BBL"] = gdf["OIL_BBL"].replace({-999: 0, '-999': 0, -999.0: 0})
    # gdf["WATER_BBL"] = gdf["WATER_BBL"].replace({-999: 0, '-999': 0, -999.0: 0})
    # gdf["CONDENSATE_BBL"] = gdf["CONDENSATE_BBL"].replace({-999: 0, '-999': 0, -999.0: 0})
    all_prod.append(gdf)

# Conatenate
all_prod_v2 = pd.concat(all_prod).reset_index(drop=True)
# print(all_prod_v2.head())

# =============================================================================
# %% Export as shapefile and then create Excel report
# =============================================================================
# Change working directory to go up one folder,
# from 'Public_Production_v0\data' to just 'Public_Production_v0'
os.chdir("..")

todays_date = datetime.datetime.now().strftime("%Y-%m-%d")
all_prod_v2.to_file(f'{todays_date}_Production_2022.geojson',
                    encoding='utf-8',
                    driver='GeoJSON')
print('GeoJSON successfully exported!')

# Temporarily set the COUNTRY field as equal to the STATE_PROV field for the
# Excel report (so you can review by results by state)
all_prod_v2['COUNTRY'] = all_prod_v2['STATE_PROV']

out_excel_name = os.path.join(results_folder, f'{todays_date}_Production_2022.xlsx')
create_internal_review_spreadsheet(all_prod_v2, out_excel_name)


# =============================================================================
# %% Export before_after_table to a CSV
# =============================================================================
out_csv_name = os.path.join(results_folder, f'{todays_date}_before_after_integration_completeness.csv')
before_after_table.to_csv(out_csv_name)
