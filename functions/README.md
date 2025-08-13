# ogim-msat > functions
This folder contains helper functions that get called during data integration, data consolidation, or other analyses.


*ogimlib - includes the following functions:*
---

#### **`assign_offshore_label_to_us_data`**: 
  - This function appends `ONSHORE` or `OFFSHORE` attribute to GeoDataFrame based on whether each record falls within or outside of a predefined `offshore_boundary` which is a GeoDataFrame representing offshore boundaries. Tested on US data for wells.
  - Call signature: gdf_onoffshore = assign_offshore_label_to_us_data(gdf, offshore_boundary)
#### **`calculate_basin_area_km2`**: 
  - This function can be used to calculate basin area in km2. It reprojects the basin GeoDataFrame to ECKERT-IV projection and calculates the area based on this CRS.
#### **`calculate_pipeline_length_km`**:
  - Calculate the length (in km) of each pipeline segment in the GeoDataFrame. The function uses ECKERT-IV projection for length calculations. 
#### **`check_invalid_geoms`**: 
  - This function returns a list of records in the GeoDataFrame that have invalid geometries (e.g., None or -inf, inf)  
#### **`data_auto_download`**:
  - This function automatically downloads a .zip file or data file from the given url and extracts to a specific folder path 
#### **`explode_multi_geoms`**:
  - This function can be used to explode multigeometries in GeoDataFrame (e.g., MULTIPOINT to POINT) 
#### **`flatten_gdf_geometry`**:
  - Flatten multi-geometry collection (MultiPoint, 'MultiLineString', 'MultiPolygon')  
#### **`integrate_basins`**:
  - OGIM function for integrating basin boundary data
#### **`integrate_facs`**:
  - OGIM function for integrating facility-level data
#### **`integrate_flares`**:
  - OGIM function for integrating gas flaring detections data
#### **`integrate_pipelines`**:
  - OGIM function for integrating pipeline data
#### **`integrate_production`**:
  - OGIM function for integrating oil and gas production data
#### **`interactive_map`**:
  - Function for generating an interactive map (requires Jupyter Notebook) of point locatins of facilities  
#### **`random_imagery_check`**:
  - Randomly draw samples of point locations from the GeoDataFrame and retrieve Google Earth imagery at those locations. Requires Google Maps API 
#### **`read_msAccess`**:
  - Read Microsoft Access data into a Pandas DataFrame 
#### **`read_spatial_data`**:
  - Read .shp, .geojson, .gpkg, .gdb files into a Pandas GeoDataFrame
#### **`replace_row_names`**:
  - Map items in a specific column (`colName`) in a DataFrame to replacement values in `dict_names` (dictionary)
#### **`reproject_eckiv`**:
  - Reproject CRS of GeoDataFrame to Eckert-IV 
#### **`save_spatial_data`**:
  - Save GeoDataFrame as .shp, .geojson file  
#### **`sig_figures`**:
  - Return a specified number of significant figures for a given numeric value in a DataFrame 
#### **`transform_CRS`**:
  - Transform the CRS of the GeoDataFrame to another CRS, requires EPSG codes. Default: EPSG:4326  
#### **`transform_geom_3d_2d`**:
  - Transform 3D geometries in a GeoDataFrame (e.g., POINT Z) to 2D (e.g., POINT). Tested only on point geometries. 
#### **`translate_espanol`**:
  - Uses `googletrans` library (poor performance in some cases) to translate Spanish attributes to English
#### **`unzip_files_in_folder`**:
  - Function for automatically unzipping .zip files and extracting contents to a specified path
---

*gridify - includes the following functions:*
---
 - 'gridify' --> Create a "fishnet" of polygons, with specified length/width, across a region of interest 
 - 'grid_summarize' --> Summarize the attributes of points falling within each 'gridify' grid square
 - 'str_mode' --> Return the most frequently appearing string value in a series of strings
 - 'merge_grid_summarize' --> Merge two separate 'grid_summarize' results into one table
 - 'percentage_dif' --> Calculate the percentage difference across two numeric columns, while avoiding "divide by zero" errors
---

