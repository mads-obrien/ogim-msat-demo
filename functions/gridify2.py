# -*- coding: utf-8 -*- 
"""
Created on Thu Sep 12 13:27:03 2024

UPDATE to pre-existing "gridify.py" --> pandas append method is deprecated, so modified to concat method

Produce equal-area grid squares across an area of interest, 
and calculate the (mean, count, etc.) of a second data layer by grid square.

@author: maobrien and kweatherby
"""


#%% Import required packages
import os
import geopandas as gpd
import pandas as pd
import numpy as np
import rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from shapely.geometry import Polygon
from datetime import datetime
from rasterio.enums import MergeAlg

def gridify(roi, length=4000, width=4000, clip2shape=True):
    '''
    Given a region of interest (ROI), 
    create a "fishnet" of grid polygons across the area, 
    with grid squares of a specified length and width,
    clipped to the shape of the region.
    DEPENDENCIES: geopandas, matplotlib, datetime
      
    ARGUMENTS:
    roi = Region Of Interest, i.e. geodataframe containing one or more polygon areas to be gridded
    length = number. length of desired grid square, in the units used by the targets' CRS. (i.e. meters)
    width = number. same as above.
    '''
    print(str(datetime.now().time())+" gridify() started")
    
    # Dissolves ROI into one single-part feature, in case of multi-polygons
    roi = roi.dissolve()

    print("Coordinate system of ROI:")
    print(roi.crs)
    print('Linear unit: '+roi.crs.axis_info[0].unit_name)
    mastergrid = gpd.GeoDataFrame(geometry = None)
    
    print('Creating grid length='+str(length)+' width='+str(width))
    for target in roi.geometry: # If only one polygon ROI is provided to the function that's fine, this loop will run once
        xmin, ymin, xmax, ymax = target.bounds
        cols = list(np.arange(xmin, xmax + width, width))
        rows = list(np.arange(ymin, ymax + length, length))
        # Create empty list to contain polygon grid squares
        polygons = [] 
        for x in cols[:-1]:
            for y in rows[:-1]:
                polygons.append(Polygon([(x,y), (x+width, y), (x+width, y+length), (x, y+length)]))
        # Convert list of poly geometries to a geodataframe
        grid = gpd.GeoDataFrame({'geometry':polygons})
        mastergrid = pd.concat([mastergrid, grid])
    
    # Specify CRS of grid (same as input ROI)
    mastergrid = mastergrid.set_crs(str(roi.crs))
    print("Coordinate system of grid squares:")
    print(str(mastergrid.crs))
    print('Linear unit: '+mastergrid.crs.axis_info[0].unit_name)
    
    if clip2shape==True:
        print("Clipping fishnet to ROI extents...")
        mastergrid = gpd.clip(mastergrid, roi)
        # Reset gridsquare index if clipping occurs
        mastergrid = mastergrid.reset_index(drop=True)
        
        
    print(str(datetime.now().time())+" gridify() finished!")
    return mastergrid


def grid_summarize(points, gridsquares, columndict=None):
    '''
    Returns a geodataframe with summarized attributes
    
    ARGUMENTS:
    points = point geodataframe
    columndict = a dictionary of each column you wish to summarize, 
        and the aggregating function you wish to use. 
        Possible values include 'sum', 'min', 'max', 'mean'
    gridsquares = polygon geodataframe, output of gridify()
    '''
    print(str(datetime.now().time())+" grid_summarize() started")
    
    # Spatial join points to empty gridsquares based on intersection
    # Returns df where rows = points within a gridsquare
    pointsinsquares = gpd.sjoin(gridsquares, points, how='inner', lsuffix='polys', rsuffix='points') 
    # reset_index() to make the 'index' of each gridsquare a column
    pointsinsquares = pointsinsquares.rename_axis('grid_index').reset_index(drop=False)
    # Create field with 1 as constant value, to be summed 
    pointsinsquares['pointcount'] = 1
    
    # create dictionary for aggregation functions
    aggdict = {'pointcount':'sum'}
    # Append any other aggregation functions specified by the user
    if columndict:
        aggdict.update(columndict)
    
    # Summarize the stats within each grid square
    gridsquaretotals = pointsinsquares.groupby(['grid_index']).agg(aggdict)
    gridsquaretotals = gridsquaretotals.reset_index()
    
    # Join the tabular stats to their gridsquare geometries
    output = gridsquares.merge(gridsquaretotals, left_index=True, right_on='grid_index', how='outer')
    # Fix the 'nan' indexes of gridsquares without datapoints
    output = output.reset_index(drop=True)
    # Fill all NA values in pointcount column with '0'
    output['pointcount'] = output['pointcount'].fillna(0)

    output = gpd.GeoDataFrame(output, geometry='geometry', crs=gridsquares.crs)
    print(str(datetime.now().time())+" grid_summarize() finished!")
    return output

def str_mode(x):
    '''
    Return the most frequently appearing string value in a series of strings
    (i.e., returning the "mode" of a string column)
    
    Parameters
    ----------
    x : a series or a column in a dataframe; must be of string type

    '''
    if len(x.value_counts()) == 0:
        return None
    else:
        return x.value_counts().index[0]
    
    
    
def merge_grid_summarize(basingrid, basingrid_ogim_points, basingrid_enverus_points):
    '''
    Parameters
    ----------
    basingrid : TYPE
        DESCRIPTION.
    basingrid_ogim_points : TYPE
        DESCRIPTION.
    basingrid_enverus_points : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    '''
    if not 'grid_index' in basingrid.columns:
        basingrid = basingrid.rename_axis('grid_index').reset_index(drop=False)
    else:
        pass
    # change identical column names prior to merge
    basingrid_ogim_points = basingrid_ogim_points.rename(columns={'pointcount':'count_ogim'})
    basingrid_enverus_points = basingrid_enverus_points.rename(columns={'pointcount':'count_enverus'})

    # Join Enverus stat columns to OGIM stat columns, using grid square index as merge field
    dfmerge = pd.merge(basingrid_ogim_points, basingrid_enverus_points, on='grid_index', how='outer')
    # merge point statistics columns to set of all basingrid cells; drop duplicate/uneeded geometry columns
    dfmerge_togrid = pd.merge(basingrid, dfmerge, on='grid_index', how='outer').drop(columns=['geometry_x','geometry_y'])
    # Convert intermediate df back into a gdf, 'reinstating' the geometry column
    output = gpd.GeoDataFrame(dfmerge_togrid, geometry='geometry', crs=basingrid.crs)
    # Drop any grid cells where NO points from either dataset appear in the grid square
    output = output.dropna(subset=['count_ogim','count_enverus'], how='all')
    # for Count fields, replace remaining nans with zeroes, to aid counting later on
    output.count_ogim = output.count_ogim.fillna(0)
    output.count_enverus = output.count_enverus.fillna(0)
    return output


def percentage_dif(ogim_col,enverus_col):
    '''
    Calculate the percentage difference across two numeric columns OR values, 
    while avoiding any "divide by zero" errors.
    '''
    if ogim_col == enverus_col:
        return 0
    if (enverus_col == 0) & (ogim_col !=0):
        return 999
    if (enverus_col != 0) & (ogim_col ==0):
        return -999
    try:
        return (((ogim_col - enverus_col) / ((ogim_col + enverus_col)/2)) * 100)
    except ZeroDivisionError:
        return float('inf')





def gdf_to_raster(gdf, value_column, output_file, resolution):
    """
    Convert a GeoDataFrame to a raster and save it to a file.
    
    ARGUMENTS:
        gdf (GeoDataFrame): The input GeoDataFrame with gridded geometries.
        value_column (str): The column in the GeoDataFrame to use for raster values.
        output_file (str): The file path to save the raster file.
        resolution (float): The resolution of the raster.
    """
    # Ensure the GeoDataFrame has a valid CRS
    if gdf.crs is None:
        raise ValueError("The GeoDataFrame must have a valid CRS.")
    
    # Get bounds and calculate raster dimensions
    bounds = gdf.total_bounds
    xmin, ymin, xmax, ymax = bounds
    width = int((xmax - xmin) / resolution)
    height = int((ymax - ymin) / resolution)
    
    # Create an affine transform for the raster
    transform = from_origin(xmin, ymax, resolution, resolution)
    
    # Prepare shapes and values for rasterization
    shapes = ((geom, value) for geom, value in zip(gdf.geometry, gdf[value_column]))
    
    # Create the raster
    raster = rasterize(
        shapes=shapes,
        out_shape=(height, width),
        transform=transform,
        fill=np.nan,
        dtype='float32'
    )
    
    # Write the raster to a file
    with rasterio.open(
        output_file,
        'w',
        driver='GTiff',
        height=height,
        width=width,
        count=1,
        dtype='float32',
        crs=gdf.crs.to_string(),
        transform=transform
    ) as dst:
        dst.write(raster, 1)
    
    print(f"Raster saved to {output_file}")



def raster_point_count(points, raster, output_path="output_raster.tif", nodata=0):
    """
    Counts the total number of points within each raster cell and outputs a raster.
    
    ARGUMENTS:
        points (GeoDataFrame): A GeoDataFrame containing point geometries
        raster (str): Path to the input raster file
        output_path (str): Path to save the output raster file
        nodata (float): Value for no-data cells in the output raster
    
    """
    # Read raster metadata
    with rasterio.open(raster) as src:
        raster_meta = src.meta.copy()
        transform = src.transform
        crs = src.crs

    # Ensure point data CRS matches raster CRS
    if points.crs != crs:
        points = points.to_crs(crs)

    # Validate geometries
    if not all(points.geometry.apply(lambda geom: geom.is_valid)):
        raise ValueError("Invalid geometries detected in points GeoDataFrame.")
    
    # Prepare shapes for rasterization
    shapes = ((geom, 1) for geom in points.geometry if geom is not None)

    # Create an empty array for raster output with the same dimensions as the input raster
    point_counts = rasterize(
        shapes=shapes,
        out_shape=(raster_meta['height'], raster_meta['width']),
        transform=transform,
        all_touched=False,  # Only count points within cell boundaries
        fill=nodata,  # No points default to 0
        merge_alg=rasterio.enums.MergeAlg.add,  # Sum counts for overlapping points
        dtype=rasterio.float32
    )

    # Update raster metadata
    raster_meta.update({
        "dtype": rasterio.float32,
        "nodata": nodata,
        "count": 1
    })

    # Write the aggregated raster to the specified output path
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with rasterio.open(output_path, "w", **raster_meta) as dst:
        dst.write(point_counts, 1)

    print(f"Point count raster saved to {output_path}")
    
    





def raster_point_aggregate(points, raster, output_dir="output_rasters", output_name='agg_rast', nodata=0, agg_function=None):
    """
    Aggregates point data within each raster cell and outputs one or more rasters

    #     default behavior:
    #         - Outputs raster with count of points in each cell

    #     optional behavior:
    #         - If agg_function provided, outputs additional rasters for each specified aggregation

    #     ARGUMENTS:
    #         points (GeoDataFrame): A GeoDataFrame containing point geometries
    #         raster (str): Path to the input raster file
    #         output_dir (str): Directory to save the output raster files
    #         output_name (str): Base string name for output raster file
    #         nodata (float): Value for no-data cells in the output raster
    #         agg_function (dict): Dictionary specifying fields and their aggregation functions ('value': 'sum', 'value': 'mean')
    """
    # Read raster metadata
    with rasterio.open(raster) as src:
        raster_meta = src.meta.copy()
        transform = src.transform
        crs = src.crs

    # Ensure point data CRS matches raster CRS
    if points.crs != crs:
        points = points.to_crs(crs)

    # Validate geometries
    if not all(points.geometry.apply(lambda geom: geom.is_valid)):
        raise ValueError("Invalid geometries detected in points GeoDataFrame.")

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    ####### default behavior: point count raster #######
    # Prepare shapes for rasterization (count points)
    shapes = ((geom, 1) for geom in points.geometry if geom is not None)

    point_counts = rasterize(
        shapes=shapes,
        out_shape=(raster_meta['height'], raster_meta['width']),
        transform=transform,
        all_touched=False,
        fill=nodata,
        merge_alg=MergeAlg.add,
        dtype=rasterio.float32
    )

    # Save the point count raster
    point_count_path = os.path.join(output_dir, output_name + "_pntcnt.tif")
    raster_meta.update({"dtype": rasterio.float32, "nodata": nodata, "count": 1})
    with rasterio.open(point_count_path, "w", **raster_meta) as dst:
        dst.write(point_counts, 1)
    print(f"Point count raster saved to {point_count_path}")

    ####### Additional aggregations if specified #######
    if agg_function:
        for field, agg_type in agg_function.items():
            if field not in points.columns:
                raise ValueError(f"Field '{field}' not found in points GeoDataFrame.")

            # Prepare shapes for rasterization (values from the specified field)
            shapes = ((geom, value) for geom, value in zip(points.geometry, points[field]) if geom is not None)

            if agg_type == 'sum':
                merge_alg = MergeAlg.add
            elif agg_type == 'mean':
                merge_alg = MergeAlg.mean
            elif agg_type == 'min':
                merge_alg = MergeAlg.min
            elif agg_type == 'max':
                merge_alg = MergeAlg.max
            else:
                raise ValueError(f"Unsupported aggregation type: {agg_type}")

            # Perform rasterization with the correct aggregation
            agg_array = rasterize(
                shapes=shapes,
                out_shape=(raster_meta['height'], raster_meta['width']),
                transform=transform,
                all_touched=False,
                fill=nodata,
                merge_alg=merge_alg,
                dtype=np.float32
            )

            # Save the aggregated raster
            agg_raster_path = os.path.join(output_dir, output_name + f"_{field}_{agg_type}.tif")
            with rasterio.open(agg_raster_path, "w", **raster_meta) as dst:
                dst.write(agg_array, 1)
            print(f"{agg_type.capitalize()} raster for '{field}' saved to {agg_raster_path}")

    print("All rasters generated successfully!")
