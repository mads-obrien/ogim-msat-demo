#!/usr/bin/env python
# coding: utf-8

# # <font color = 'darkblue'> Grid spatial data

# @momara, MSAT, REVISED 02-28-2022 for Permian gridding

# ========================================================

def utm_from_lon(lon):
    """
    utm_from_lon - UTM zone for a longitude

    Not right for some polar regions (Norway, Svalbard, Antartica)

    :param float lon: longitude
    :return: UTM zone number
    :rtype: int
    """
    from math import floor

    return floor( ( lon + 180 ) / 6) + 1

# ========================================================
def scale_bar(ax, 
              proj, 
              length, 
              location=(0.5, 0.05), 
              linewidth=3,
              units='km', m_per_unit=1000):
    """

    http://stackoverflow.com/a/35705477/1072212
    ax is the axes to draw the scalebar on.
    proj is the projection the axes are in
    location is center of the scalebar in axis coordinates ie. 0.5 is the middle of the plot
    length is the length of the scalebar in km.
    linewidth is the thickness of the scalebar.
    units is the name of the unit
    m_per_unit is the number of meters in a unit
    
    """
    import matplotlib.pyplot as plt
    from matplotlib import patheffects
    import matplotlib
    import cartopy.crs as ccrs
    
    # find lat/lon center to find best UTM zone
    x0, x1, y0, y1 = ax.get_extent(proj.as_geodetic())
    # Projection in metres
    utm = ccrs.UTM(utm_from_lon((x0+x1)/2))
    # Get the extent of the plotted area in coordinates in metres
    x0, x1, y0, y1 = ax.get_extent(utm)
    # Turn the specified scalebar location into coordinates in metres
    sbcx, sbcy = x0 + (x1 - x0) * location[0], y0 + (y1 - y0) * location[1]
    # Generate the x coordinate for the ends of the scalebar
    bar_xs = [sbcx - length * m_per_unit/2, sbcx + length * m_per_unit/2]
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
    left = x0+(x1-x0)*0.05
    # Plot the N arrow
    t1 = ax.text(left, sbcy, u'\u25B2\nN', transform=utm,
        horizontalalignment='center', verticalalignment='bottom',
        path_effects=buffer, zorder=2)
    # Plot the scalebar without buffer, in case covered by text buffer
    ax.plot(bar_xs, [sbcy, sbcy], transform=utm, color='k',
        linewidth=linewidth, zorder=3)

# ========================================================
def grid_data(gdf, 
              all_attrs,
              agg_functions,
              agg_attrs_idx=None,
              x_res=0.1, 
              y_res=0.1,
              mp_colormap="plasma",
              plotHeatMap=True,
              figWidth=12,
              figHeight=8,
              cbarLabel="Gas (Mcf)",
              regionShp=None,
              clipGridToRegion=True,
              figTitle=None,
              showScale=False,
              axisLabels=True,
              showStates=True,
              showBorders=True,
              showCoast=True,
              showLand=False,
              showOcean=False,
              showLakes=False,
              showRivers=False):
    """
    `grid_data` creates a grid of resolution `x_res`x`y_res` in degrees
    and aggregates the column `agg_attrs`. 
    
    Inputs:
    ------------
        gdf: geodataframe, the geodataframe with data to be gridded and aggregated
        all_attrs: list of strings, a list of attributes to be aggregated. Note the first attribute
        agg_functions: dict, dictionary of summary stats to generate for each of the agg_attrs
        agg_attrs: str, attribute for which a heatmap of the aggregated data is desired
        x_res: float, the x resolution in degrees
        y_res: float, the y resolution in degrees
        mp_colormap: str, the matplotlib colormap for the heatmap
        figWidth: float, width of heatmap figure
        figHeight: float, height of heatmap figure
        cbarLabel: str, label name for the colorbar
        regionShp: None or .shp path, shapefile representing AOI boundary, if needed
        clipGridToRegion: bool, if True, grid boundaries are defined by the boundary of the AOI
        showScale: bool, draws a scalebar on the figure
        figTitle: show figure title
        dataLabel: displays legend or colorbar axis label if heatmap is plotted
        showScale: bool, if True, displays a 100-km scale on the map
        dataScaling: float, allows for modification of scatter point marker size
        axisLabels: bool, if True, label x and y axis of figure object as Longitude and Latitude
        showStates: bool, if True, show state boundaries
        showBorders: bool, if True, show country borders
        showCoast: bool, if True, show coast line
        showLand: bool, if True, show land in light yellow
        showOcean: bool, if True, show ocean in light blue
        showLakes: bool, if True, show lakes
        showRivers: bool, if True, show rivers
        
    Returns:
    --------------
        density_gdf: gridded geodataframe of resolution [x_res, y_res] 
        fig: heatmap of aggregated `agg_attrs`
        
    Example:
    --------------
    # ============================
    # Generate fake data for geodataframe
    
    lat, lon = np.random.uniform(low=32, high=35, size=(100,)), np.random.uniform(low=-104, high=-101, size=(100,))
    gas_mcf, oil_bbl = np.random.uniform(low=0, high=5000, size=(100,)), np.random.uniform(low=0, high=250, size=(100,))
    df = pd.DataFrame(data=[lat, lon, gas_mcf, oil_bbl], index=['lat', 'lon', 'GAS_MCF', 'OIL_BBL']).T
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.lon, df.lat), crs="epsg:4326")
    # =============================
    all_attrs = ['GAS_MCF','OIL_BBL']
    agg_functions = {'GAS_MCF':'sum', 'OIL_BBL':'sum'}
    agg_attrs_idx = 0 # GAS starts at index 0 in `all_attrs` and `agg_functions`
    gdf_dens, fig = grid_data(gdf, 
              all_attrs,
              agg_functions,
              agg_attrs_idx=agg_attrs_idx,
              x_res=0.2, 
              y_res=0.2,
              mp_colormap="magma_r",
              plotHeatMap=True,
              figWidth=8,
              figHeight=5,
              cbarLabel="Gas (mcf)",
              regionShp=None,
              showScale=True)
    # =============================
        
    """

    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    import geopandas as gpd
    import matplotlib.ticker as ticker
    import cartopy.feature as cfeature
    from cartopy.io.shapereader import Reader
    import cartopy.io.shapereader as shpreader
    import cartopy.crs as ccrs
    from matplotlib.axes import Axes
    from cartopy.mpl.geoaxes import GeoAxes
    GeoAxes._pcolormesh_patched = Axes.pcolormesh
    from shapely.geometry import Polygon, Point, shape, mapping, MultiPoint
    import warnings
    warnings.filterwarnings("ignore")
    
    plt.rcParams["font.family"] = "arial" 
    
    # ========================================================
    # Create grid based on the bounds of gdf
    if clipGridToRegion==False:
        xmin,ymin,xmax,ymax =  gdf.total_bounds
    else:
        AOI = gpd.read_file(regionShp)
        xmin,ymin,xmax,ymax =  AOI.total_bounds

    # Set grid dimensions (e.g., 0.1 degrees)
    width = x_res 
    height = y_res

    # Set row/column values to iterate through
    rows = int(np.ceil((ymax-ymin) /  height))
    cols = int(np.ceil((xmax-xmin) / width))
    XleftOrigin = xmin
    XrightOrigin = xmin + width
    YtopOrigin = ymax
    YbottomOrigin = ymax- height
    
    polygons = []
    for i in range(cols):
        Ytop = YtopOrigin
        Ybottom =YbottomOrigin
        for j in range(rows):
            # Append to polygon list 
            polygons.append(Polygon([(XleftOrigin, Ytop), (XrightOrigin, Ytop), (XrightOrigin, Ybottom), (XleftOrigin, Ybottom)])) 
            Ytop = Ytop - height
            Ybottom = Ybottom - height
        XleftOrigin = XleftOrigin + width
        XrightOrigin = XrightOrigin + width

    # Create new geodataframe based on polygon list, set crs 
    grid = gpd.GeoDataFrame({'geometry':polygons})
    
    # Set crs
    if gdf.crs is None:
        print ("====================")
        print ("!!! CRS of geodataframe is not set!!! \n ==> Please set CRS first; <== TERMINATING program")
        sys.exit()
        
    else:
        if "epsg:4326" not in str(gdf.crs):
            print ("!!! Transform CRS to EPSG:4326 to continue analysis!!!\n ==> TERMINATING program")
            sys.exit()
        else:
            grid = grid.set_crs(gdf.crs)
    
    # ========================================================
    # SPATIAL JOIN grid with data
    # Create unique gridID's and wellID's column
    grid['gridID'] = [*range(0, len(grid))]
    
    # Perform spatial join with grid and gdf 
    dfjoin = gpd.sjoin(grid, gdf, how='right', op='contains') 
    
    # Fill null values with zeros
    for attr in list(dfjoin.columns):
        dfjoin[attr] = dfjoin[attr].fillna(0)
        
    # Group by gridID and sum values in dataset
    df2 = dfjoin.groupby("gridID").agg(agg_functions).reset_index()
    
    # Set column names for the merged dataset
    colnames = []
    for key in agg_functions:
        colName = agg_functions[key] + "_" + key
        colnames.append(colName)
        
    df2.columns = ['gridID'] + colnames 
    
    # Merge the resulting gdf with original grid dataframe
    density_df = grid.merge(df2, how='left', on='gridID') 

    # Fill na values with 0
    for attr in list(density_df.columns):
        density_df[attr] = density_df[attr].fillna(0)
    
    # Add attributes for centroid lat and long
    density_df['centroid_lon'] = density_df.geometry.centroid.x
    density_df['centroid_lat'] = density_df.geometry.centroid.y
    
    # ========================================================
    if plotHeatMap == True:
        
        # Plot a gridded heatmap of the parameter of choice
        fig = plt.figure(figsize = (figWidth, figHeight))
        
        # Figure decorations
        ax = fig.add_subplot(111, projection = ccrs.PlateCarree())
        
        if showLand==True:
            ax.add_feature(cfeature.LAND)
        if showOcean==True:
            ax.add_feature(cfeature.OCEAN)
        if showBorders==True:
            ax.add_feature(cfeature.BORDERS, linestyle='-', lw=1, edgecolor='gray')
        if showCoast==True:
            ax.add_feature(cfeature.COASTLINE, linestyle='-', lw=1, edgecolor='gray')
        if showLakes==True:
            ax.add_feature(cfeature.LAKES, alpha=0.5)
        if showRivers==True:
            ax.add_feature(cfeature.RIVERS)
        if showStates==True:
            ax.add_feature(cfeature.STATES.with_scale('10m'), lw=1.5, edgecolor='gray')

        ax.tick_params(direction='out', length=6, labelsize=17)
        ax.xaxis.set_visible(True)
        ax.yaxis.set_visible(True)
        
        #  Set extent
        if clipGridToRegion==False:
            dataLon, dataLat = density_df.centroid_lon, density_df.centroid_lat
        
            ax.set_extent([dataLon.min()-0.2, dataLon.max()+0.2, dataLat.min()-0.2, dataLat.max()+0.2], ccrs.PlateCarree())
        else:
            xmin,ymin,xmax,ymax =  AOI.total_bounds
            ax.set_extent([xmin-0.2, xmax+0.2, ymin-0.2, ymax+0.2], ccrs.PlateCarree())
            

        #  Plots the data onto map
        # ========================================================
        cmap_ = plt.get_cmap(mp_colormap)
        p1 = density_df.plot(ax=ax, column=density_df.columns[agg_attrs_idx+2], cmap=cmap_)

        # Colorbar
        cax = fig.add_axes([0.8, 0.12, 0.03,0.76])
        vmin_, vmax_ = np.min(density_df.iloc[:,agg_attrs_idx+2].values), np.max(density_df.iloc[:,agg_attrs_idx+2].values)
        sm = plt.cm.ScalarMappable(cmap=cmap_, norm=plt.Normalize(vmin=vmin_, vmax=vmax_))
        sm._A = []
        cbar1 = fig.colorbar(sm, cax=cax)
        cbar1.ax.tick_params(labelsize = 15, direction='in', length=6, width=1.5, grid_color='gray')
        cbar1.set_label(cbarLabel, fontsize = 18)

        # Axes labels
        if axisLabels==True:
            ax.set_ylabel("Latitude", fontsize = 20)
            ax.set_xlabel("Longitude", fontsize = 20)
            
        # Add AOI boundary, if desired
        if regionShp is not None:
            reader = shpreader.Reader(regionShp)
            area = list(reader.geometries())
            AREA = cfeature.ShapelyFeature(area, ccrs.PlateCarree())
            ax.add_feature(AREA, facecolor = 'none', edgecolor = 'darkred', lw = 2.5)
            
         # Add scale bar
        if showScale == True:
            scale_bar(ax, ccrs.PlateCarree(), 100, location = (0.25, 0.05))  # 100 km scale bar
            
        # Figure title, Includes an estimate of total production from the dataset
        if figTitle is not None:
            ax.set_title(figTitle + " ($Total = $" + str("{:,}".format(int(gdf[all_attrs[agg_attrs_idx]].sum())))+")", fontsize=18, fontweight='bold', loc='left')
            
        
        # Show degree symbols in X and Y axes
        ax.xaxis.set_major_formatter(ticker.FormatStrFormatter( "%.f\N{DEGREE SIGN}"))
        ax.yaxis.set_major_formatter(ticker.FormatStrFormatter( "%.f\N{DEGREE SIGN}"))
  
    return density_df, fig 

# Test Permian
# ========================================================
import geopandas as gpd
import pandas as pd

# Read file
fp = "drilling_info_\Permian_Monthly_Prodn_Data_Enverus_\_enverus_prodn_permian_Jan_Dec_2021_.pkl"
df = pd.read_pickle(fp)

# Convert to GeoDataFrame
gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.x, df.y), crs="epsg:4326")
gdf.head()

# Parameters
import time
startTime = time.time()
all_attrs = ['LiquidsProd_BBL', 'GasProd_MCF', 'Prod_BOE']

agg_functions = {'LiquidsProd_BBL':'sum', 
                 'GasProd_MCF':'sum', 
                 'Prod_BOE':'sum'}
# We want to visualize 'Prod_BOE', which is at position 2 in `all_attrs` and `Prod_BOE`
agg_attrs_idx=1

# Desired grid resolution
x_res, y_res = 0.05, 0.05

perm_grid, fig1 = grid_data(gdf, 
              all_attrs,
              agg_functions,
              agg_attrs_idx=agg_attrs_idx,
              x_res=x_res, 
              y_res=y_res,
              mp_colormap="plasma",
              plotHeatMap=True,
              figWidth=12,
              figHeight=8,
              cbarLabel="Gas production (Mcf)",
              regionShp=r"C:\Users\momara\OneDrive - MethaneSAT, LLC\OGIM\data_proprietary_\drilling_info_\Permian_Monthly_Prodn_Data_Enverus_\PermianBasin_Extent_201712.shp",
              clipGridToRegion=True,
              figTitle="Gas production",
              showScale=True,
              axisLabels=True,
              showStates=True,
              showBorders=True,
              showCoast=True,
              showLand=False,
              showOcean=False,
              showLakes=False,
              showRivers=False)

# ==========================
print ("==============")
print ("Time elapsed = ", round((time.time()-startTime)/60,1), "minutes")

