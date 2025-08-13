# -*- coding: utf-8 -*-


import numpy as np
import xarray as xr
import rasterio
from rasterio.transform import from_bounds
from rasterio.crs import CRS
import argparse
import os

def netcdf_to_geotiff(input_file, output_file=None, variable_name=None):
    """Convert NetCDF file to GeoTIFF
    
    Example usage:
        output_file = netcdf_to_geotiff('AMU_DARYA_area_weighted_flux_066.nc',
                                        output_file='AMU_DARYA_area_weighted_flux_066.tif')
    
    """
    
    # Open NetCDF file
    ds = xr.open_dataset(input_file)
    
    # Find lat/lon variables
    lat_var = next((name for name in ['lat', 'latitude', 'y'] if name in ds.variables), None)
    lon_var = next((name for name in ['lon', 'longitude', 'x'] if name in ds.variables), None)
    
    if not lat_var or not lon_var:
        raise ValueError(f"Could not find lat/lon variables in: {list(ds.variables.keys())}")
    
    # Get coordinates and data
    lats, lons = ds[lat_var].values, ds[lon_var].values
    data_vars = [var for var in ds.data_vars if var not in {lat_var, lon_var}]
    
    if not data_vars:
        raise ValueError("No data variables found")
    
    variable_name = variable_name or data_vars[0]
    data = ds[variable_name].values
    
    # Handle 3D data (take first layer)
    if data.ndim == 3:
        data = data[0]
    
    # Ensure correct orientation
    if data.shape != (len(lats), len(lons)):
        data = data.T
    
    # Don't flip - use data as-is from NetCDF
    
    # Create geospatial transform
    lon_min, lon_max = float(lons.min()), float(lons.max())
    lat_min, lat_max = float(lats.min()), float(lats.max())
    
    # Calculate pixel size
    lon_res = (lon_max - lon_min) / (len(lons) - 1)
    lat_res = (lat_max - lat_min) / (len(lats) - 1)
    
    # Adjust bounds to pixel edges (shift by half pixel)
    west = lon_min - lon_res / 2
    east = lon_max + lon_res / 2
    south = lat_min - lat_res / 2
    north = lat_max + lat_res / 2
    
    transform = from_bounds(west, south, east, north, len(lons), len(lats))
    
    # Handle output filename
    if not output_file:
        base_name = os.path.splitext(input_file)[0]
        output_file = f"{base_name}_{variable_name}.tif"
    
    # Handle NaN values
    nodata_value = None
    if np.isnan(data).any():
        nodata_value = -9999.0
        data = np.where(np.isnan(data), nodata_value, data)
    
    # Write GeoTIFF
    with rasterio.open(
        output_file, 'w',
        driver='GTiff',
        height=data.shape[0], width=data.shape[1], count=1,
        dtype=rasterio.float32,
        crs=CRS.from_epsg(4326),
        transform=transform,
        nodata=nodata_value,
        compress='lzw'
    ) as dst:
        dst.write(data.astype(np.float32), 1)
    
    ds.close()
    return output_file
