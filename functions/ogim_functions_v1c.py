# OGIM bottom-up inventory helper functions
# Last updated on 02/16/2022
#===========================================================================
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import trange
import time
#===========================================================================
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Arial"
from shapely.geometry import Polygon, Point, shape, mapping, MultiPoint
import matplotlib
import matplotlib.ticker as mtick
#===========================================================================
"""
Function for reprojecting the geodataframe to an ECKERT IV CRS
@author: momara
"""
def reproject_eckiv(gdf, gdf_crs_epsg=4326):
    """Reproject the geodataframe to an Eckert IV equal area projection
    ...
    Inputs:
    ------
    gdf: geodataframe
        The geodataframe whose CRS will be converted 
    gdf_crs_epsg: required input for the EPSG code of the gdf's current CRS if it is not defined
    
    Returns:
    ------
    A new gdf with the reprojected CRS
    """
#====================================================================================================================
    # Check current CRS
    print ("===================")
    print ("Current CRS is: ", gdf.crs)
    # Define new CRS
    ECKERT_IV_STR = "+proj=eck4 +lon_0=0 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs"
    
    if gdf.crs is None:
        # Set crs of the dataframe first
        print ("===================")
        print ("NOTE: First set the correct CRS for this dataframe \n by specifying the appropriate EPSG code in the `gdf_crs_epsg` parameter")
        gdf = gdf.set_crs(epsg=gdf_crs_epsg)
        print ("===================")
        print ("Reprojecting to Eckert IV")
        gdf = gdf.to_crs(ECKERT_IV_STR)
        print ("===================")
        print ("Reprojecting finished. New CRS is: ", gdf.crs)
        
    else:
        # Reproject
        print ("===================")
        print ("Reprojecting to Eckert IV")
    
        gdf = gdf.to_crs(ECKERT_IV_STR)
        print ("===================")
        print ("Reprojecting finished. New CRS is: ", gdf.crs)
    
    return gdf

#====================================================================================================================
"""
Function for consolidating O&G feature attributes for each of the feature
datasets in the O&G infrastructure database
@author: momara
"""
def create_geodb(gdf,   append_objectID=True,
                        startFID=1,
                        category='NOT AVAILABLE',
                        country='NOT AVAILABLE', 
                        src_date=2021, 
                        source_url='url',
                        fac_id=None,
                        on_offshore='NOT AVAILABLE',
                        op_name=None,
                        fac_name=None,
                        drill_type=None,
                        fac_status=None,
                        fac_type=None,
                        install_date=None,
                        commodity=None,
                        capacity=None,
                        throughput=None,
                        pipe_length=None,
                        pipe_material=None,
                        basin_name=None,
                        field_name=None,
                        field_type=None,
                        shape_area=None,
                        source_rank=5,
                        temporal_rank=5,
                        spatial_rank=5):
    #===========================================================================

    """Create a geodataframe, update data schema
    Parameters:
    -----------
      gdf: The geodataframe, CRS=EckertIV
      append_OBJECT_ID: boolean, if True, `startID` must be specified for the OBJECT ID of the gdf
      startFID: int, starting index for the unique OBJECT_ID
      category: str, indicates one of the following classes:
                  (i) Oil and gas wells
                  (ii) Oil and natural gas platforms
                  (iii) Natural gas compressor stations
                  (iv) Natural gas processing plants
                  (v) Liquiefied natural gas (LNG) terminals
                  (vi) Oil and natural gas pipelines
                  (vii) Petroleum storage and terminals
                  (viii) Crude oil refineries
                  (ix) Natural gas flaring facilities
      country: str, name of the country
      src_date: str, date the data was last updated by the data owner
      src_url: str, url for the source data
      fac_id: str or float, depends on the specific format used by the data owner. For wells, this represents the American
                  Petroleum Institute number that identifies each unique well or the Universal Well Identifier
                  number, or well ID in the database.
      on_offshore: str, indicates whether the facility is on or offshore
      op_name: str, name of the operator
      fac_name: str, name of the facility
      drill_type: str, indicates the drilling trajectory of the well, e.g., VERTICAL, HORIZONTAL, DIRECTIONAL
      fac_status: str, indicates the facility status as of the date the data was last updated
      fac_type: str, for wells, indicates whether the facility is an oil, gas, or mixed oil and gas type;
                  could also be used to indicate whether the facility is a gathering compressor station or
                  a transmission compressor station
      install_date: str or datetime: indicates the date the facility was installed
                  (i) for wells, this could be based on the drilling date, the completion date, or the first
                      reported production date.
      commodity: str, specific for oil and gas pipelines, identifies the type of commodity being transported
                  e.g., oil vs gas vs hydrocarbon liquids
      capacity: float, the installed capacity of the facility. Important for:
                  (i) natural gas compressor stations and processing plants: use units of MMcfd
      throughput: float, the production or throughput volumes of oil and gas produced or compressed or processed
                  (i) for gas: use units of MMcfd
                  (ii) for oil: use units of kilo barrels per day
      pipe_length: indicates length of pipeline, units km
      pipe_mater: material used for in pipeline construction
      basin_name:  for oil and gas basins, specify name of the basin
      field_name:  for oil and gas fields, specify name of the field
      shape_area:  for either basins or fields, specify the area in km2, if available
      source_rank: int, an initial ranking of the quality of the source of the data, from low (1) to high (5)
      temporal_rank: int, an initial ranking of the temporal quality of the data based on when it was last updated
                  from (1) low to (5) high
      spatial_rank: int, an initial ranking of the spatial quality of the data based on the accuracy of point
                  locations, from (1) low to (5) high
    Returns:
    --------
      The new geodataframe, appropriately formatted with the different required attributes.
    """
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    #====================================================================================================================
    # Define feature attributes
    attrs_ = ['OBJECT_ID',
             'CATEGORY',
             'COUNTRY', 
             'SRC_DATE', 
             'SRC_URL',
             'FAC_ID',
             'FAC_NAME',
             'OFFSHORE',
             'OPER_NAME',
             'FAC_TYPE',
             'DRILL_TYPE',
             'INSTL_DATE',
             'FAC_STATUS',
             'COMMODITY',
             'CAPACITY',
             'THROUGHPUT',
             'PIPE_LEN',
             'PIPE_MATER',
             'BASIN',
             'FIELD',
             'FIELD_TYPE',
             'SHAPE_AREA',
             'SRC_RANK',
             'TEMP_RANK',
             'SPAT_RANK',
             'AVG_RANK',
             'geometry'
               ]

    # Create an empty gdb and add relevant data
    gdb_ = gpd.GeoDataFrame(columns = attrs_)
    if append_objectID == True:
        gdb_['OBJECT_ID'] = list (np.arange(startFID, gdf.shape[0]+startFID))
    else:
        gdb_['OBJECT_ID'] = list (np.arange(1, gdf.shape[0]+1))
    
    # Category, source date and source URL
    # Category, source date and source URL
    if category is None:
        gdb_['CATEGORY'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['CATEGORY'] = gdf[category]
        except:
            gdb_['CATEGORY'] = category
    
    # SOURCE DATE
    if src_date is None:
        gdb_['SRC_DATE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['SRC_DATE'] = gdf[src_date]
        except:
            gdb_['SRC_DATE'] = src_date
            
    # SOURCE URL
    if source_url is None:
        gdb_['SRC_URL'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['SRC_URL'] = gdf[source_url]
        except:
            gdb_['SRC_URL'] = source_url
    # Country
    # If available, the attribute name comes from the original dataset and should be entered in the 'country' parameter
    # Otherwise, accept the user value for the `country` parameter
    if not country:
        gdb_['COUNTRY'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['COUNTRY'] = gdf[country]
        except:
            gdb_['COUNTRY'] = country

    # Facility name
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_name' parameter
    if not fac_name:
        gdb_['FAC_NAME'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FAC_NAME'] = gdf[fac_name]
        except:
            raise KeyError("Please check the attribute name for `fac_name` in the original database or enter None")

    # Well API, UWI number or Facility ID
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_id' parameter
    if not fac_id:
        gdb_['FAC_ID'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FAC_ID'] = gdf[fac_id]
        except:
            raise KeyError("Please check the attribute name for `fac_id` in the original database or enter None")

    # Offshore or Onshore
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_id' parameter
    # Otherwise, accept the user value for the `on_offshore` parameter
    if not on_offshore:
        gdb_['OFFSHORE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['OFFSHORE'] = gdf[on_offshore]
        except:
            gdb_['OFFSHORE'] = on_offshore 
        
    # Facility status (e.g., abandoned, active, producing, suspended, operating, etc)
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_status' parameter
    if not fac_status:
        gdb_['FAC_STATUS'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FAC_STATUS'] = gdf[fac_status]
        except:
            raise KeyError("Please check the attribute name for `fac_status` in the original database or enter None")

    # Facility type (e.g., abandoned, active, producing, suspended, operating, etc)
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_type' parameter
    if not fac_type:
        gdb_['FAC_TYPE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FAC_TYPE'] = gdf[fac_type]
        except:
            raise KeyError("Please check the attribute name for `fac_type` in the original database or enter None")
    
    # Operator name
    # If available, the attribute name comes from the original dataset and should be entered in the 'op_name' parameter
    if not op_name:
        gdb_['OPER_NAME'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['OPER_NAME'] = gdf[op_name]
        except:
            raise KeyError("Please check the attribute name for `op_name` in the original database or enter None")

    # Drill Trajectory for wells (e.g., vertical, horizontal, directional)
    # If available, the attribute name comes from the original dataset and should be entered in the 'drill_type' parameter
    if not drill_type:
        gdb_['DRILL_TYPE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['DRILL_TYPE'] = gdf[drill_type]
        except:
            raise KeyError("Please check the attribute name for `drill_type` in the original database or enter None")
        
    # Installation date (could also be spud date or first production date)
    # If available, the attribute name comes from the original dataset and should be entered in the 'install_date' parameter
    if not install_date:
        gdb_['INSTL_DATE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['INSTL_DATE'] = gdf[install_date]
        except:
            raise KeyError("Please check the attribute name for `install_date` in the original database or enter None")
        
    # Commodity type (e.g., oil versus gas pipeline transportation)
    # If available, the attribute name comes from the original dataset and should be entered in the 'commodity' parameter
    if not commodity:
        gdb_['COMMODITY'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['COMMODITY'] = gdf[commodity]
        except:
            raise KeyError("Please check the attribute name for `commodity` in the original database or enter None")
        
    # Installed Capacity (e.g., for compressor station (MMcfd) or for crude oil refineries, Million barrels per day)
    # If available, the attribute name comes from the original dataset and should be entered in the 'capacity' parameter
    if not capacity:
        gdb_['CAPACITY'] = -9999
    else:
        try:
            gdb_['CAPACITY'] = gdf[capacity]
        except:
            raise KeyError("Please check the attribute name for `capacity` in the original database or enter None")
        
    # Reported Throughput (e.g., for compressor station (MMcfd), or oil and gas production for oil wells)
    # If available, the attribute name comes from the original dataset and should be entered in the 'throughput' parameter
    if not throughput:
        gdb_['THROUGHPUT'] = -9999
    else:
        try:
            gdb_['THROUGHPUT'] = gdf[throughput]
        except:
            raise KeyError("Please check the attribute name for `throughput` in the original database or enter None")
        
    # Pipeline length in km2, if available
    # If available, the attribute name comes from the original dataset and should be entered in the 'pipe_length' parameter
    if not pipe_length:
        gdb_['PIPE_LEN'] = -9999
    else:
        try:
            gdb_['PIPE_LEN'] = gdf[pipe_length]
        except:
            raise KeyError("Please check the attribute name for `pipe_length` in the original database or enter None")

    # Pipeline material, if available
    # If available, the attribute name comes from the original dataset and should be entered in the 'pipe_material' parameter
    if not pipe_material:
        gdb_['PIPE_MATER'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['PIPE_MATER'] = gdf[pipe_material]
        except:
            raise KeyError("Please check the attribute name for `pipe_material` in the original database or enter None")
        
    # Basin names
    # If available, the attribute name comes from the original dataset and should be entered in the 'basin_name' parameter
    if not basin_name:
        gdb_['BASIN'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BASIN'] = gdf[basin_name]
        except:
            raise KeyError("Please check the attribute name for `basin_name` in the original database or enter None")

    # Field names
    # If available, the attribute name comes from the original dataset and should be entered in the 'field_name' parameter
    if not field_name:
        gdb_['FIELD'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FIELD'] = gdf[field_name]
        except:
            raise KeyError("Please check the attribute name for `field_name` in the original database or enter None")

    # Field TYPE: Oil or gas field?
    # If available, the attribute name comes from the original dataset and should be entered in the 'field_type' parameter
    if not field_type:
        gdb_['FIELD_TYPE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FIELD_TYPE'] = gdf[field_type]
        except:
            raise KeyError("Please check the attribute name for `field_type` in the original database or enter None")

    # SHAPE area: area, in km2, of field or basin, if available
    # If available, the attribute name comes from the original dataset and should be entered in the 'shape_area' parameter
    if not shape_area:
        gdb_['SHAPE_AREA'] = -9999
    else:
        try:
            gdb_['SHAPE_AREA'] = gdf[shape_area]
        except:
            raise KeyError("Please check the attribute name for `shape_area` in the original database or enter None")

    #====================================================================================================================
    # How reliable is the source of the data?? Scale of 1 (lowest)-5 (highest)
    # ------|Government sources (highly reliable) | Company sources (highly reliable) | Data from individuals (May not be as reliable)
    gdb_['SRC_RANK'] = source_rank 
    
    # How frequently is the data updated??
    # ------|Look at date of last update  
    gdb_['TEMP_RANK'] = temporal_rank 
    
    # How accurate is the location information??
    # ------|Government sources are likely more reliable
    gdb_['SPAT_RANK'] = spatial_rank 
 
    ### GENERATE an average quality score based on the `Source_Rank`, `Temporal_Rank` and `Spatial_Rank`
        
    gdb_['AVG_RANK'] = 1/3*(gdb_.SRC_RANK + gdb_.TEMP_RANK + gdb_.SPAT_RANK)
    
    gdb_['geometry'] = gdf.geometry
    
    # Set CRS 
    print ("===\n Successfully applied schema to new gdf\n===")
    print ("CRS of original geodataframe is: ", gdf.crs)
    try:
        gdb_.crs = gdf.crs
    except:
        print ("*******NOTE: CRS of original geodataframe is not assigned*******")
        pass
    
    return gdb_

#====================================================================================================================
"""
Function for visualizing a random selection of point locations in the gdf
for an initial visual assessment of accuracy of point location data
@author: momara
"""
### Function for initial visualization ###
def map_visualize_init(gdf, center=[50,-103], 
                       zoom=10, 
                       ransamp_size=50,
                       height='500px', 
                       width='800px', 
                       showMSAT=True,
                       MSAT_Targets_Path=None):
    """
    Visaulize a random selection of point location data on a map
    Parameters:
    -----------
        gdf: The geodataframe
        center: lat, lon representing center of the map
        ransamp_size: number of random samples to draw from gdf for initial visualization
        height, width: height and width of map in pixels
        showMSAT: boolean, if True, displays MethaneSAT current target areas
        MSAT_Targets_Path: path to shapefile for MSAT Target areas
      
    Returns:
    --------
        map: Map showing randomly selected well locations in satellite imagery
    """
    import leafmap.leafmap as leafmap
    import numpy as np
    import geopandas as gpd
    
    
    style = {
            "stroke": True,
            "color": "darkred",
            "weight": 2,
            "opacity": 1,
            "fill": True,
            "fillColor": "darkred",
            "fillOpacity": 0.1,
            }

    hover_style = {"fillOpacity": 0.1}
    
    m = leafmap.Map(height=height, width=width, center=center, zoom=zoom)
    # Add MethaneSAT polygon boundaries
    if showMSAT == True:
        try:
            msat_polys = gpd.read_file(MSAT_Targets_Path)
            m.add_gdf(msat_polys, layer_name="MethaneSAT Targets", style=style, hover_style=hover_style)
        except:
            print ("===MethaneSAT targets not plotted, check path===")
            pass
    
    # Randomly select the first50
    try:
        ran_samps = np.random.choice(gdf.OBJECT_ID, size=ransamp_size, replace=False)
        gdf_ = gdf[gdf.OBJECT_ID.isin(list(ran_samps))]
    except:
        ran_samps = np.random.choice(gdf.OBJECT_ID, size=gdf.shape[0], replace=False)
        gdf_ = gdf[gdf.OBJECT_ID.isin(list(ran_samps))]

    # Plot
    m.add_gdf(gdf_, layer_name="Facilities", style=style, hover_style=hover_style)
    m.add_tile_layer(url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}", name="Google Satellite", attribution="GoogleMaps")

    return m
#====================================================================================================================
"""
Reading geodatabases, shapefiles, kmls and kmzs
@author: momara
"""

def read_gdb(path_to_gdb, 
             layer_name=None, 
             preview=True, 
             table_gradient=False):
    """
    Read an ESRI geodatabase into a geodataframe
    Inputs:
    ---
    path_to_gdb: path to geodatabase
    layer_name: if specified, reads data specific to that layer
    preview: display data head
    table_gradient: shows a gradient scale for the attribute values
    
    Returns:
    ---
    A geodataframe of the same file
    
    """
    import geopandas as gpd
    
    if layer_name is None:
        gdf = gpd.read_file(path_to_gdb)
        print ('---------------------------------------')
        print ('Total # of features in dataset = %d' % gdf.shape[0])
        print (gdf.columns)
    
        if table_gradient == True:
            display (gdf.head().style.background_gradient(cmap='Blues'))
        else:
            display (gdf.head())
    else:
        gdf = gpd.read_file(path_to_gdb, layer=layer_name)
        print ('---------------------------------------')
        print ('Total # of features in dataset = %d' % gdf.shape[0])
        print (gdf.columns)
    
        if table_gradient == True:
            display (gdf.head().style.background_gradient(cmap='Blues'))
        else:
            display (gdf.head())
            
    return gdf
        
        
# Function for reading shapefile        
    
def read_shpFile(path_to_shp, 
                 table_gradient=False):
    """
    Read a shapefile into a geodataframe
    Inputs:
    ---
    path_to_shp: path to shapefile
    table_gradient: automatically displays a preview of the dataframe, showing a gradient scale for the columns
    
    Returns:
    ---
    A geodataframe of the same file
    
    """
    import geopandas as gpd
    
    gdf = gpd.read_file(path_to_shp)
    print ('---------------------------------------')
    print ('Total # of features in dataset = %d' % gdf.shape[0])
    print (gdf.columns)
    
    if table_gradient == True:
        display (gdf.head().style.background_gradient(cmap='Blues'))
    else:
        display (gdf.head())
    
    return gdf  

# Function for reading kmz
def read_kmzFile(path_to_kmz):
    
    """
    One disadvantage of this function is that it will only read the first layer of the .kmz. 
    ---
    TO-DO: Modify script to read multiple layers of the .kmz file
    """
    
    import fiona
    from zipfile import ZipFile
    import geopandas as gpd

    fiona.drvsupport.supported_drivers['libkml'] = 'rw' # enable KML support which is disabled by default
    fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # enable KML support which is disabled by default
    
    # Read
    kmz = ZipFile(path_to_kmz, 'r')
    kml_name = path_to_kmz.split("\\")[3].strip().split('.')[0] + '.kml'
    gdf = gpd.read_file(kmz.open(kml_name, 'r'), driver='KML')
    print ('---------------------------------------')
    print ('Total # of features in dataset = %d' % gdf.shape[0])
    print (gdf.columns)
    display (gdf.head())

    return gdf  

def read_kmlLayer(path_to_kml):
    """
    This function reads a kml file and returns the layer names contained in the file as a list
    
    """
    import fiona
    from zipfile import ZipFile
    import geopandas as gpd
    
    fiona.drvsupport.supported_drivers['libkml'] = 'rw' # enable KML support which is disabled by default
    fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # enable KML support which is disabled by default
    
    # KML names
    layer_names = fiona.listlayers(path_to_kml)
    
    # List layer names
    print (layer_names)
    
    return layer_names
    
def read_kmlFile(path_to_kml, layer_names, 
                 return_outputs=False, 
                 read_layer_by_layer=False):
    
    """
    This function reads a kml file for each layer
    
    layer_names: A list of layer names as strings
    """
    
    import fiona
    from zipfile import ZipFile
    import geopandas as gpd

    fiona.drvsupport.supported_drivers['libkml'] = 'rw' # enable KML support which is disabled by default
    fiona.drvsupport.supported_drivers['LIBKML'] = 'rw' # enable KML support which is disabled by default
    gpd.io.file.fiona.drvsupport.supported_drivers['KML'] = 'rw'
    
    # Read kml
    if read_layer_by_layer == True:
        all_data = []
    
        for layer in range(len(layer_names)):
            if return_outputs == True:
                print ('---------------------------------------')
                print ("Now reading layer = ", layer_names[layer])
                layer_name = str (layer_names[layer])
                kml_ = gpd.read_file(path_to_kml, driver="KML", layer=str(layer_name))
                print ('---------------------------------------')
                print ('Total # of features in dataset = %d' % kml_.shape[0])
            else:
                layer_name = str (layer_names[layer])
                kml_ = gpd.read_file(path_to_kml, driver="KML", layer=str(layer_name))
            # Append results to all_data
            all_data.append(kml_)
            
    else:
        kml_ = gpd.read_file(path_to_kml, driver="KML")
        all_data = kml_.copy()

    return all_data

# =======================================================================
# # Download Google images
# # =======================================================================
# Define function for splitting data into n equal sizes

def chunks(lst, n):
    """Yield successive n-sized chunks from list."""
    return [lst[i:i + n] for i in range(0, len(lst), n)]

# =======================================================================
def return_Google_images(gdf, 
                         gdf_crs_epsg=4326,
                         num_images=10,
                         fac_category="wells",
                         region=None,
                         zoom=19, 
                         height=400, 
                         width=400, 
                         api_key='GOOGLE_MAPS_API_KEY', 
                         fp="Google_images_"
                         ):
    """
    Return Google images at `data_sel_` lat and lon
    ---
    gdf: geodataframe with crs='Eckert IV'
    gdf_crs_epsg: int, specify the EPSG code for the current gdf's CRS if it is None
    fac_category: indicates the facility category, e.g., wells, compressor stations, processing plants, etc
    region: country or basin where the facility is located
    num_images: number of images to sample, should be a multiple of 10
    zoom: zoom level of the data
    height: height in pixels of the image
    width: width in pixels of the image
    api_key: Google maps api key
    ---
    """
    import urllib
    import urllib.request as ur
    from tqdm import tqdm
    import numpy as np
    import geopandas as gpd
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    import cv2
    import glob
    
    # =========================================================================
    # First do a random selection of facilities
    # Geodataframe must have a unique object ID
    
##    ran_ids = np.random.choice(gdf.OBJECT_ID, size=num_images, replace=False)
##    gdf_sel = gdf[gdf.OBJECT_ID.isin(ran_ids)]

    gdf_sel = gdf.sample(n=num_images, replace=False)
    
    # Check CRS
    if gdf_sel.crs is None:
        # Set crs of the dataframe first
        print ("===================")
        print ("NOTE: First set the correct CRS for this dataframe \n by specifying the appropriate EPSG code in the `gdf_crs_epsg` parameter")
        print ("===================")
        print ("Setting appropriate CRS as defined by the EPSG code in `gdf_crs_epsg` parameter")
        gdf_sel = gdf_sel.set_crs(epsg=gdf_crs_epsg)
    
    # Set new CRS as WGS 1984 to enable retrieval of lat, lon in decimal degrees from Google Maps
    print ("Transforming CRS to EPSG:4326")
    gdf_sel2 = gdf_sel.to_crs(4326)
    
    # lat and lon in degrees
    if gdf_sel2.geometry.type.iloc[0] == 'Point':
        gdf_sel2['Longitude'] = gdf_sel2.geometry.x
        gdf_sel2['Latitude'] = gdf_sel2.geometry.y
    else:
        gdf_sel2['Longitude'] = gdf_sel2.centroid.x
        gdf_sel2['Latitude'] = gdf_sel2.centroid.y

    # Next, iterate through each row of the gdf and retrieve Google Maps imagery
    
    print ("===>Retrieving images")
    for idx, row in tqdm(gdf_sel2.iterrows(), total=gdf_sel2.shape[0], desc='Running_images_: ' + region):
        lat_, lon_ = row.Latitude, row.Longitude
        img_src = 'http://maps.googleapis.com/maps/api/staticmap?scale=1&center=' + str (lat_) + ',' + str(lon_)+ '&zoom=' + str(zoom) +'&size='+str(width)+'x'+str(height)+'&maptype=satellite&key='+api_key
        ur.urlretrieve(img_src, fp +'\\_'+ region + "_" + fac_category + "_"+ str(idx) + "_" + str(lat_) + '_' + str(lon_) + "_.png")
        
    # =========================================================================
    # Next, read saved .png files and plot them
    print ("===>Reading and plotting retrieved images")
    
    files = glob.glob(fp + "\\_" + region + "_*.png")
    # The PDF document
    pdf_pages = PdfPages(fp + "\\" + region + "_" + fac_category + "_images_.pdf")
    # Split the files into 10 equal chunks
    chunks_ = chunks(files, 10)
    for idx2 in range(len(chunks_)):
        nrows_, ncols_ = 2, 5
        fig, ax_ = plt.subplots(nrows=nrows_,ncols=ncols_, figsize=(14, 6), facecolor='w', edgecolor='k')
        fig.subplots_adjust(hspace = .5, wspace=0.000)
        ax_ = ax_.ravel()
        # Image ids
        image_ids = chunks_[idx2]
        for idx in range(10):
            img_ = cv2.imread(image_ids[idx])
            ax_[idx].imshow(img_)
            ax_[idx].axis('off')
            # Show lat and lon
            lat_str, lon_str = str(round(float(image_ids[idx].split("_")[-3:][:2][0]),6)), str(round(float(image_ids[idx].split("_")[-3:][:2][1]),6))
            # Append titles
            ax_[idx].set_title(region + ": \n" + lat_str + ",\n" + lon_str)
        # Save files
        plt.savefig(pdf_pages, format='pdf')
    # Close pdf
    pdf_pages.close()

    # Save gdf_sel2 as a .csv file
    print ("===>Saving .csv of retrieved lat and lon for images")
    gdf_sel2.drop('geometry',axis=1).to_csv(fp +'\\_'+ region + "_" + fac_category + "_.csv") 
    
    print ("===>Program successfully finished")
    
    return gdf_sel2

# =========================================================================
# Saving geodataframe as shapefile
# Important that the geodataframe is of the appropriate CRS

def save_as_shp(gdf, file_name, out_path):
    """
   Save geodataframe as an ESRI shapefile
   Inputs
   ------
   gdf: geodataframe
   file_name: str, file name for the output file
   out_path: str, path to output the file
   Returns
   -------
   Shapefile saved to specified file path
    """
    gdf.to_file(out_path + file_name + "_.shp", encoding='utf-8' )
    print ("===Successfully saved .shp to specified path===")

# Saving as geojson
def save_as_geojson(gdf, file_name, out_path):
    """
    Save geodataframe as a geojson
    Inputs
    ------
    gdf: geodataframe
    file_name: str, file name for the output file
    out_path: str, path to output the file
    Returns
    -------
    GeoJSON saved to specified file path
    """
    gdf.to_file(out_path + file_name + "_.geojson", driver='GeoJSON', encoding='utf-8')
    print ("===Successfully saved .geojson to specified path===")

#============================================================================

#================================================================
def calculate_epsg(longitude, latitude):
    '''
    Function returns an EPSG code given a latitude and longitude
    
    Inputs:
    ---
    longitude: Longitude, in decimal degrees (WGS 194)
    latitude: Latitude, in decimal degrees (WGS 1984)
    
    Returns:
    ---
    The European Survey Petroleum Group's (EPSG) code
    
    ---
    @author: momara
    
    '''
    import numpy as np
    # The EPSG code is 32600+zone for positive latitudes and 32700+zone for negatives.
    EPSG = 32700 - (np.sign(latitude)+1)/2 * 100 + np.floor((180+longitude)/6) + 1
    
    return EPSG

#================================================================
# Function for converting from one CRS to WGS 1984
def transform_CRS(gdf, 
                  target_epsg_code="epsg:4326",
                  appendLatLon=True
                 ):
    """
    Transform the gdf from its original CRS to another CRS//known EPSG code, default "epsg:4326"
    
    Inputs:
    ---
        gdf:    geodataframe with known geometry type and CRS
        target_epsg_code:    the new EPSG code for the geodataframe. Default: EPSG:4326
        appendLatLon:    bool, if desired, adds lat and lon to the new gdf
    
    Returns:
    ---
        gdf_new: a new geodataframe whose CRS has been transformed to the the target CRS. Also, two columns representing
                latitude (`latitude_calc`) and longitude (`longitude_calc`) are added to `gdf_new`
    ---
    @author: momara
    """
    import geopandas as gpd
    import warnings
    warnings.filterwarnings("ignore")
    
    # Check CRS
    # Check CRS of reference geodataframe
    crs_gdf = gdf.crs
    if crs_gdf is not None:
        print ("=======================")
        print ("CRS of original gdf is: ", crs_gdf)
    else:
        raise ValueError ("!! CRS of gdf is not set !!")
    
    # Transform to target CRS: use EPSG codes
    gdf_new = gdf.to_crs(target_epsg_code)
    
    # Append latitude and longitude [decimal degrees if EPSG:4326] attributes to gdf_new
    if appendLatLon==True:
        gdf_new['longitude_calc'] = gdf_new.geometry.x
        gdf_new['latitude_calc'] = gdf_new.geometry.y
    
    print ("=======================")
    print ("CRS of new gdf is: ", gdf_new.crs)
    
    return gdf_new

#================================================================
# Function for aggregating well-level attributes to site-level attributes

def wells2sites(gdf, 
                aggreg_funcs,
                radius_m=20, 
                print_results=True):
    '''
    Generates a geodataframe with well site attributes based on well-level information.
    
    Approach
    --------
        We begin by identifying the appropriate CRS based on the UTM coordinate system, generating unique EPSG codes
        for each latitude/longitude record in the UTM zone(s)
            We then create a buffer polygon of radius `radius_m` [meters] around each location, then merge overlapping buffers
        so that the merged polygon approximates the site boundary if there are multiple wells on the site.
            Finally, a unique `TARGET_FID`, representing the facility id is set for each site. The well-level attributes are
        consolidated into site-level attributes based on the `aggreg_funcs` dictionary.
        
    Inputs:
    ------
        gdf:    geodataframe of wells, CRS must be defined. 
        agg_funcs: dictionary of Pandas aggregation functions for consolidating well-level attributes to site-level attributes;
               key-value pairs indicating `key`: attribute/field/column name and `value`: statistical operator for
               aggregation, e.g., `sum`, `mean`, `first`, `last`, etc. The dictionary {Join_Count:'sum'} should be included
               in aggreg_funcs to count the total number of wells on each site.
                e.g. aggreg_funcs = {'lon':'mean', 'lat':'mean', 'CurrentOperator':'first', 'Join_Count':'sum'}
        radius:    float, radius for the buffer
    
    Returns:
    -------
        two geodataframes [CRS: EPSG:4326] with a `TARGET_FID` field for:
            (i) the well level data (all_data_wells)
            (ii) the site-level data (all_data_sites)
        A polygon geodataframe (`all_polys`) is also generated showing the merged polygons representing `radius_m` buffer around each wellhead
        
    Dependencies:
    ------------
        pyproj: pip install pyproj
        Pandas, GeoPandas, Numpy
        trange from tqdm: pip install tqdm
    
    ------------
    @author: momara
    '''
    # Time check
    import time
    startTime = time.time()
    # Libraries
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    from pyproj import Proj, transform
    from tqdm import trange
    import warnings
    warnings.filterwarnings("ignore")
    
    #================================================================
    # Check CRS of reference geodataframe
    crs_gdf = gdf.crs
    if crs_gdf is not None:
        print ("=======================")
        print ("CRS of original gdf is: ", crs_gdf)
        # Transform to target CRS: use EPSG codes
        gdf_new = gdf.to_crs('epsg:4326')
    
        # Append latitude and longitude [decimal degrees if EPSG:4326] attributes to gdf_new
        gdf_new['lon_calc'] = gdf_new.geometry.x
        gdf_new['lat_calc'] = gdf_new.geometry.y
        
        print ("=======================")
        print ("CRS of new gdf is: ", gdf_new.crs)
        print ("=======================")
        
    else:
        raise ValueError ("!! Error: CRS not set on original gdf !!")

    # Add epsg to each row in the dataframe
    gdf_sel = gdf_new.copy()
    gdf_sel['epsg_code'] = calculate_epsg(gdf_sel['lon_calc'], gdf_sel['lat_calc'])
    
    # determine unique epsg
    unique_epsg_codes = gdf_sel.epsg_code.unique()
    
    # Loop through each unique code
    all_data_sites = []
    all_data_wells = []
    all_polys = []
    
    len_fids_update = 0
    
    for idx in trange(len(unique_epsg_codes), desc='Running EPSG=> '):
        
        # EPSG codes
        epsg_code_ = unique_epsg_codes[idx]
        # data selected
        data_sel = gdf_sel.query('epsg_code == @epsg_code_')
        # define crs for this dataset
        try:
            data_sel = data_sel.to_crs("EPSG:" + str(int(epsg_code_)))
            
        except:
            print ("!! CRSError; check lat/lon values; Returned EPSG = ", epsg_code_, "\n !! Skipping over these data !!")
            continue
        
        # buffer    
        data_sel2 = data_sel.copy()
        data_sel2['geometry'] = data_sel2.buffer(radius_m)
        # unary union: We can collapse these circles into a single shapely MultiPolygon geometry
        mp = data_sel2.geometry.unary_union
        df_diss = gpd.GeoDataFrame(geometry=[mp])
        # break multipolygon into individual polygons
        dissolved_df = df_diss.explode().reset_index(drop=True)
        # set CRS of new dataframe
        dissolved_df.crs = data_sel2.crs
        # NEXT, 
        # create unique id for each well site
        len_fids = dissolved_df.shape[0]

        dissolved_df['TARGET_FID'] = [*range(len_fids_update, len_fids+len_fids_update)]
        #===============================================================
        # For counting # of wells per site
        data_sel['Join_Count'] = 1
        # spatial join of wells within well site (buff) polygons
        try:
            data_sel.drop(columns = ['index_right'], inplace = True)
        except:
            pass
        
        wellsOnSites = gpd.sjoin(data_sel, dissolved_df, how = "left", op = "within")
    
        #================================================================
        
        site_attrs = wellsOnSites.groupby(by='TARGET_FID').agg(aggreg_funcs)
        
        # Update len_fids_update
        len_fids_update = len_fids_update + len_fids
        
        #================================================================
        # DETERMINE CENTROID LATITUDE AND LONGITUDE
        ## polygon centroid
        dissolved_df['centroid'] = dissolved_df['geometry'].centroid
        dissolved_df = dissolved_df.set_geometry('centroid')
        # Centroid in degrees lat and lon
        lon, lat = dissolved_df.geometry.centroid.x, dissolved_df.geometry.centroid.y
        inProj, outProj = Proj(init='EPSG:' + str(int(epsg_code_))), Proj(init='epsg:4326')
        lon2,lat2 = transform(inProj,outProj,lon,lat)
        dissolved_df['centroid_lon'] = lon2
        dissolved_df['centroid_lat'] = lat2
        # Change geometry back to polygon
        dissolved_df = dissolved_df.set_geometry('geometry')
        dissolved_df = dissolved_df.to_crs("EPSG:4326")
        
        # Merge site_attrs with dissolved_df #
        site_attrs2 = pd.merge(site_attrs, dissolved_df, how='left', on='TARGET_FID')
        
        #================================================================
        # drop geometry column before appending, as this geometry column 
        # inherits the well site polygon geometry
        
        site_attrs2 = site_attrs2.drop(columns=['geometry'])
        
        # Set geometry again using the `centroid` attribute [POINT]
        site_attrs2['geometry'] = site_attrs2.centroid
        
        # drop centroid
        site_attrs2 = site_attrs2.drop(columns=['centroid'])
        
        site_attrs2 = gpd.GeoDataFrame(site_attrs2, geometry='geometry')
         # Append to list of dataframes
        
        all_data_sites.append(site_attrs2)
        all_data_wells.append(wellsOnSites)
        all_polys.append(dissolved_df)
        
    # Concatenate
    all_polys = pd.concat(all_polys)
    all_polys = all_polys.drop(columns=['centroid']).reset_index()
    
    all_data_wells = pd.concat(all_data_wells).reset_index()
    all_data_sites = pd.concat(all_data_sites).reset_index()
    
    # Preliminary result check
    if print_results==True:
        print ('================================================================')
        print ('Total # of records in original dataset = ', gdf_sel.shape[0])
        print ('Total # of records in spatial joined dataset = ', all_data_sites.Join_Count.sum())
        print ('Total # of sites in spatial joined dataset = ', len (all_data_sites.TARGET_FID.unique()))
        print ('Idx = ', idx, "; Max number of records per site = ", all_data_sites.Join_Count.max())
        print ('Idx = ', idx, "; Average # of records per site = ", all_data_sites.Join_Count.sum() / all_data_sites.shape[0])
        print ('================================================================')
        print ('Time elapsed = ', (time.time() - startTime)/60, " minutes")
        #=======================================================================   
    
    # These three gdfs can then be saved as .shp or .geojson
    
    #=======================================================================
    # Set CRS on final dataset to EPSG:4326
    all_polys = all_polys.to_crs("epsg:4326")
    all_data_wells = all_data_wells.to_crs("epsg:4326")
    all_data_sites = all_data_sites.to_crs("epsg:4326")
    #=======================================================================
    
    return all_polys, all_data_wells, all_data_sites

#=======================================================================     
def create_geodb_flares(gdf,   append_objectID=True,
                        startFID=1,
                        country=None, 
                        src_date=2021, 
                        source_url='url',
                        fac_id=None,
                        fac_type=None,
                        avg_temp=None,
                        bcm_2012=None,
                        bcm_2013=None,
                        bcm_2014=None,
                        bcm_2015=None,
                        bcm_2016=None,
                        bcm_2017=None,
                        bcm_2018=None,
                        bcm_2019=None,
                        bcm_2020=None,
                        og_segment=None,
                        source_rank=5,
                        temporal_rank=5,
                        spatial_rank=5):
    #===========================================================================

    """Create a geodataframe, update data schema
    Parameters:
    -----------
      gdf: The geodataframe, CRS=EckertIV
      append_OBJECT_ID: boolean, if True, `startID` must be specified for the OBJECT ID of the gdf
      startFID: int, starting index for the unique OBJECT_ID
      country: str, name of the country
      src_date: str, date the data was last updated by the data owner
      src_url: str, url for the source data
      fac_id: str or float, depends on the specific format used by the data owner. For wells, this represents the American
                  Petroleum Institute number that identifies each unique well or the Universal Well Identifier
                  number, or well ID in the database.
      fac_type: str, if available, type of facility: e.g. lng for liquefied natural gas
      avg_temp: mean estimated gas flaring temperature in Kelvin
      bcm_2012: estimated flared volume in 2012
      bcm_2013: estimated flared volume in 2013
      bcm_2014: estimated flared volume in 2014
      bcm_2015: estimated flared volume in 2015
      bcm_2016: estimated flared volume in 2016
      bcm_2017: estimated flared volume in 2017
      bcm_2018: estimated flared volume in 2018
      bcm_2019: estimated flared volume in 2019
      bcm_2020: estimated flared volume in 2020
      og_segment: upstream, midstream or downstream
      
      source_rank: int, an initial ranking of the quality of the source of the data, from low (1) to high (5)
      temporal_rank: int, an initial ranking of the temporal quality of the data based on when it was last updated
                  from (1) low to (5) high
      spatial_rank: int, an initial ranking of the spatial quality of the data based on the accuracy of point
                  locations, from (1) low to (5) high
    Returns:
    --------
      The new geodataframe, appropriately formatted with the different required attributes.
    """
    import numpy as np
    import pandas as pd
    import geopandas as gpd
    #====================================================================================================================
    # Define feature attributes
    attrs_ = ['OBJECT_ID',
              'COUNTRY', 
              'SRC_DATE', 
              'SRC_URL',
              'FAC_ID',
              'FAC_TYPE',
              'AVG_TEMP',
              'OG_SEGMENT',
              'BCM_2012',
              'BCM_2013',
              'BCM_2014',
              'BCM_2015',
              'BCM_2016',
              'BCM_2017',
              'BCM_2018',
              'BCM_2019',
              'BCM_2020',
              'SRC_RANK',
              'TEMP_RANK',
              'SPAT_RANK',
              'AVG_RANK',
              'geometry'
               ]

    # Create an empty gdb and add relevant data
    gdb_ = gpd.GeoDataFrame(columns = attrs_)
    if append_objectID == True:
        gdb_['OBJECT_ID'] = list (np.arange(startFID, gdf.shape[0]+startFID))
    else:
        gdb_['OBJECT_ID'] = list (np.arange(1, gdf.shape[0]+1))
    
    # Category, source date and source URL
    gdb_['SRC_DATE'] = src_date
    gdb_['SRC_URL'] = source_url

    # Country
    # If available, the attribute name comes from the original dataset and should be entered in the 'country' parameter
    # Otherwise, accept the user value for the `country` parameter
    if not country:
        gdb_['COUNTRY'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['COUNTRY'] = gdf[country]
        except:
            gdb_['COUNTRY'] = country

    # Well API, UWI number or Facility ID
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_id' parameter
    if not fac_id:
        gdb_['FAC_ID'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FAC_ID'] = gdf[fac_id]
        except:
            raise KeyError("Please check the attribute name for `fac_id` in the original database or enter None")

    # Facility type (e.g., abandoned, active, producing, suspended, operating, etc)
    # If available, the attribute name comes from the original dataset and should be entered in the 'fac_type' parameter
    if not fac_type:
        gdb_['FAC_TYPE'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['FAC_TYPE'] = gdf[fac_type]
        except:
            raise KeyError("Please check the attribute name for `fac_type` in the original database or enter None")
    
    # Operator name
    # If available, the attribute name comes from the original dataset and should be entered in the 'op_name' parameter
    if not avg_temp:
        gdb_['AVG_TEMP'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['AVG_TEMP'] = gdf[avg_temp]
        except:
            raise KeyError("Please check the attribute name for `avg_temp` in the original database or enter None")

    # Drill Trajectory for wells (e.g., vertical, horizontal, directional)
    # If available, the attribute name comes from the original dataset and should be entered in the 'drill_type' parameter
    if not og_segment:
        gdb_['OG_SEGMENT'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['OG_SEGMENT'] = gdf[og_segment]
        except:
            raise KeyError("Please check the attribute name for `og_segment` in the original database or enter None")
        
    # Installation date (could also be spud date or first production date)
    # If available, the attribute name comes from the original dataset and should be entered in the 'install_date' parameter
    if not bcm_2012:
        gdb_['BCM_2012'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2012'] = gdf[bcm_2012]
        except:
            raise KeyError("Please check the attribute name for `bcm_2012` in the original database or enter None")
    # 2013
    if not bcm_2013:
        gdb_['BCM_2013'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2013'] = gdf[bcm_2013]
        except:
            raise KeyError("Please check the attribute name for `bcm_2013` in the original database or enter None")
    # 2014
    if not bcm_2014:
        gdb_['BCM_2014'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2014'] = gdf[bcm_2014]
        except:
            raise KeyError("Please check the attribute name for `bcm_2014` in the original database or enter None")
    # 2015
    if not bcm_2015:
        gdb_['BCM_2015'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2015'] = gdf[bcm_2015]
        except:
            raise KeyError("Please check the attribute name for `bcm_2015` in the original database or enter None")
            
    # 2016
    if not bcm_2016:
        gdb_['BCM_2016'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2016'] = gdf[bcm_2016]
        except:
            raise KeyError("Please check the attribute name for `bcm_2016` in the original database or enter None")
            
    # 2017
    if not bcm_2017:
        gdb_['BCM_2017'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2017'] = gdf[bcm_2017]
        except:
            raise KeyError("Please check the attribute name for `bcm_2017` in the original database or enter None")
    
    # 2018
    if not bcm_2018:
        gdb_['BCM_2018'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2018'] = gdf[bcm_2018]
        except:
            raise KeyError("Please check the attribute name for `bcm_2018` in the original database or enter None")
    
    # 2019
    if not bcm_2019:
        gdb_['BCM_2019'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2019'] = gdf[bcm_2019]
        except:
            raise KeyError("Please check the attribute name for `bcm_2019` in the original database or enter None")
    
    # 2020
    if not bcm_2020:
        gdb_['BCM_2020'] = 'NOT AVAILABLE'
    else:
        try:
            gdb_['BCM_2020'] = gdf[bcm_2020]
        except:
            raise KeyError("Please check the attribute name for `bcm_2020` in the original database or enter None")
            
    #====================================================================================================================
    # How reliable is the source of the data?? Scale of 1 (lowest)-5 (highest)
    # ------|Government sources (highly reliable) | Company sources (highly reliable) | Data from individuals (May not be as reliable)
    gdb_['SRC_RANK'] = source_rank 
    
    # How frequently is the data updated??
    # ------|Look at date of last update  
    gdb_['TEMP_RANK'] = temporal_rank 
    
    # How accurate is the location information??
    # ------|Government sources are likely more reliable
    gdb_['SPAT_RANK'] = spatial_rank 
 
    ### GENERATE an average quality score based on the `Source_Rank`, `Temporal_Rank` and `Spatial_Rank`
        
    gdb_['AVG_RANK'] = 1/3*(gdb_.SRC_RANK + gdb_.TEMP_RANK + gdb_.SPAT_RANK)
    
    gdb_['geometry'] = gdf.geometry
    
    # Set CRS 
    print ("===\n Successfully applied schema to new gdf\n===")
    print ("CRS of original geodataframe is: ", gdf.crs)
    try:
        gdb_.crs = gdf.crs
    except:
        print ("*******NOTE: CRS of original geodataframe is not assigned*******")
        pass
    
    return gdb_

#====================================================================================================================
# Summarize key attributes

def summarize_geodb(df, 
                    country='alberta', 
                    fac_category="wells",
                    data_source="Government - Alberta Energy Regulator",
                    num_data_sources=1,
                    avg_rank=5,
                    savePath = "Public_Data\\data\\North_America\\Canada\\Alberta"):
    """
    Return summary stats for feature count
    and boolean for key attributes in the geodataframe
    """
    cnt1, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr9, attr10, attr11 = [], [], [], [], [], [], [], [], [], [], [], []
    
    cols = ['COUNTRY', 'DATA_SOURCE', 'NUM_DATA_SOURCES', 'AVG_RANK', 'FACILITY_CATEGORY','FEATURE_COUNT','OPER_NAME', 'FAC_TYPE', 'DRILL_TYPE','FAC_STATUS','INSTL_DATE', 
            'CAPACITY', 'THROUGHPUT', 'PIPE_LEN', 'PIPE_MATER', 'BASIN', 'FIELD'
           ]
    # ============================
    
    print ("====================")
    print ("Total # of features in dataset = ", df.shape[0])
    cnt1 = df.shape[0]
    print ("====================")
    
    print (df.OPER_NAME.unique())
    if len(df.OPER_NAME.unique()) != 1:
        attr1 = "YES"
    else:
        attr1 = "NO"
    print ("====================")
    print (df.FAC_TYPE.unique())
    if len(df.FAC_TYPE.unique()) != 1:
        attr2 = "YES"
    else:
        attr2 = "NO"
    print ("====================")
    print (df.DRILL_TYPE.unique())
    if len(df.DRILL_TYPE.unique()) != 1:
        attr3 = "YES"
    else:
        attr3 = "NO"
    print ("====================")
    print (df.FAC_STATUS.unique())
    if len(df.FAC_STATUS.unique()) != 1:
        attr4 = "YES"
    else:
        attr4 = "NO"
    print ("====================")
    print (df.INSTL_DATE.unique())
    if len(df.INSTL_DATE.unique()) != 1:
        attr5 = "YES"
    else:
        attr5 = "NO"
    print ("====================")
    print (df.CAPACITY.unique())
    if len(df.CAPACITY.unique()) != 1:
        attr6 = "YES"
    else:
        attr6 = "NO"
    print ("====================")
    print (df.THROUGHPUT.unique())
    if len(df.THROUGHPUT.unique()) != 1:
        attr7 = "YES"
    else:
        attr7 = "NO"
    print ("====================")
    print (df.PIPE_LEN.unique())
    if len(df.PIPE_LEN.unique()) != 1:
        attr8 = "YES"
    else:
        attr8 = "NO"
    print ("====================")
    print (df.PIPE_MATER.unique())
    if len(df.PIPE_MATER.unique()) != 1:
        attr9 = "YES"
    else:
        attr9 = "NO"
    print ("====================")
    print (df.BASIN.unique())
    if len(df.BASIN.unique()) != 1:
        attr10 = "YES"
    else:
        attr10 = "NO"
    print ("====================")
    
    print (df.FIELD.unique())
    if len(df.FIELD.unique()) != 1:
        attr11 = "YES"
    else:
        attr11 = "NO"
    print ("====================")
    
    # Create summary dataframe
    data1 = [country, data_source, num_data_sources, avg_rank, fac_category, cnt1, attr1, attr2, attr3, attr4, attr5, attr6, attr7, attr8, attr9, attr10, attr11]
    
    df_ = pd.DataFrame(data1, index=cols).T
    
    display(df_.head())
    
    # Save as excel spreadsheet
    df_.to_excel(savePath + "\\" + country + "_summary_attributes_" + fac_category + "_.xlsx")
    
    return df_

# =============================================================================================
# Plotting scatter map 
# # ==============================
# PLOT POINTS
#  FUNCTION for adding SCALES to maps

from math import floor
import matplotlib.pyplot as plt
from matplotlib import patheffects
import matplotlib
import os
import cartopy.crs as ccrs 
from matplotlib.axes import Axes
from cartopy.mpl.geoaxes import GeoAxes
GeoAxes._pcolormesh_patched = Axes.pcolormesh
import cartopy.feature as cfeature
import matplotlib.ticker as mticker
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from mpl_toolkits.axes_grid1 import make_axes_locatable
from matplotlib.colors import BoundaryNorm
from matplotlib.ticker import MaxNLocator
from cartopy.io.shapereader import Reader
import cartopy.io.shapereader as shpreader

import tqdm

def utm_from_lon(lon):
    """
    utm_from_lon - UTM zone for a longitude
    Not right for some polar regions (Norway, Svalbard, Antartica)
    :param float lon: longitude
    :return: UTM zone number
    :rtype: int
    """
    return floor( ( lon + 180 ) / 6) + 1

def scale_bar(ax, proj, length, location=(0.5, 0.05), linewidth=3,
              units='km', m_per_unit=1000):
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

def scatterMaps(gdf, 
                lat_lon=True,
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
    #======================================
    # For setting extents
    # Check that data is in EPSG:4326
    if gdf.crs == "EPSG:4326" and showPipelines==False:
        if lat_lon == True:
            dataLon, dataLat = gdf['longitude'], gdf['latitude'] # If latitude and longitude labels are included as attributes in the data
        else:
            dataLon, dataLat = gdf.geometry.x, gdf.geometry.y # For geodataframe in EPSG:4326 with no latitude/longitude attributes
            gdf['longitude'] = dataLon
            gdf['latitude'] = dataLat
        # Creates the map
        fig = plt.figure(figsize = (figWidth, figHeight))
        
        # Fig. axis decorations
        ca_map = fig.add_subplot(111, projection = ccrs.PlateCarree())
        
        if showLand==True:
            ca_map.add_feature(cfeature.LAND)
        if showOcean==True:
            ca_map.add_feature(cfeature.OCEAN)
        if showBorders==True:
            ca_map.add_feature(cfeature.BORDERS, linestyle='-', lw=1, edgecolor='gray')
        if showCoast==True:
            ca_map.add_feature(cfeature.COASTLINE, linestyle='-', lw=1, edgecolor='gray')
        if showLakes==True:
            ca_map.add_feature(cfeature.LAKES, alpha = 0.5)
        if showRivers==True:
            ca_map.add_feature(cfeature.RIVERS)
        if showStates==True:
            ca_map.add_feature(cfeature.STATES.with_scale('10m'), lw = 0.1, edgecolor='gray')

        ca_map.tick_params(direction = 'out', length = 6, labelsize = 14)
        
        # If desired to aoverlay polygon boundary (.shp) of specific region on the map
        if fpShp is not None:
            reader = shpreader.Reader(fpShp)
            area = list(reader.geometries())
            AREA = cfeature.ShapelyFeature(area, ccrs.PlateCarree())
    
        if showArea == True:
            ca_map.add_feature(AREA, facecolor = 'none', edgecolor = 'darkred', lw = 3)
        
        # Set approximate map extent
        if NA_SA_extent==True:
            ca_map.set_extent([-160, -30, -60, 75], ccrs.PlateCarree())
        elif NA_SA_extent==False:
            try:
                ca_map.set_extent([dataLon.min()-0.5, dataLon.max()+0.5, dataLat.min()-0.5, dataLat.max()+0.5], ccrs.PlateCarree())
            except:
                print ("Not setting map extent")
                pass

        ca_map.xaxis.set_visible(True)
        ca_map.yaxis.set_visible(True)


        # Plots the data onto map
        if heatMap == False:
            scatter_ = ca_map.scatter(gdf['longitude'], gdf['latitude'], 
                                s=markerSize, label=dataLabel,
                                c=markerColor,
                                transform=ccrs.PlateCarree())
        elif heatMap == True:
            scatter_ = ca_map.scatter(gdf['longitude'], 
                                      gdf['latitude'], 
                                      s=gdf[colorAttr]/dataScaling, 
                                      label=dataLabel,
                                      c=gdf[colorAttr], 
                                      cmap = plt.get_cmap(colorMapName), 
                                      transform = ccrs.PlateCarree())
            

            # Colorbar
            cbar = plt.colorbar(scatter_, ax = ca_map)
            cbar.set_label(dataLabel, fontsize = 16)
            cbar.ax.tick_params(labelsize = 14)
            
        
        # Axes labels
        if axisLabels==True:
            ca_map.set_ylabel("Latitude", fontsize = 18)
            ca_map.set_xlabel("Longitude", fontsize = 18)

        # Add scale bar
        if showScale == True:
            scale_bar(ca_map, ccrs.PlateCarree(), 100, location = (0.25, 0.05))  # 100 km scale bar
            
        # Figure title
        if figTitle is not None:
            ca_map.set_title(figTitle + " $n = $" + str(gdf.shape[0]), fontsize=18, fontweight='bold', loc='left')
            
        # Show legend
        if showLegend==True:
            ca_map.legend(fontsize=16, fancybox=True, shadow=True, loc='lower left',markerscale=4.)
        
        # Show degree symbols in X and Y axes
        ca_map.xaxis.set_major_formatter(ticker.FormatStrFormatter( "%.f\N{DEGREE SIGN}"))
        ca_map.yaxis.set_major_formatter(ticker.FormatStrFormatter( "%.f\N{DEGREE SIGN}"))

#         fig.tight_layout()

    elif showPipelines == True:
        # Creates the map
        fig = plt.figure(figsize = (figWidth, figHeight))
        
        # Fig. axis decorations
        ca_map = fig.add_subplot(111, projection = ccrs.PlateCarree())
        
        if showLand==True:
            ca_map.add_feature(cfeature.LAND)
        if showOcean==True:
            ca_map.add_feature(cfeature.OCEAN)
        if showBorders==True:
            ca_map.add_feature(cfeature.BORDERS, linestyle='-', lw=1, edgecolor='gray')
        if showCoast==True:
            ca_map.add_feature(cfeature.COASTLINE, linestyle='-', lw=1, edgecolor='gray')
        if showLakes==True:
            ca_map.add_feature(cfeature.LAKES, alpha = 0.5)
        if showRivers==True:
            ca_map.add_feature(cfeature.RIVERS)
        if showStates==True:
            ca_map.add_feature(cfeature.STATES.with_scale('10m'), lw = 0.1, edgecolor='gray')

        ca_map.tick_params(direction = 'out', length = 6, labelsize = 14)
        
        # If desired to aoverlay polygon boundary (.shp) of specific region on the map
        if fpShp is not None:
            reader = shpreader.Reader(fpShp)
            area = list(reader.geometries())
            AREA = cfeature.ShapelyFeature(area, ccrs.PlateCarree())
    
        if showArea == True:
            ca_map.add_feature(AREA, facecolor = 'none', edgecolor = 'darkred', lw = 3)
        
        # Set approximate map extent
        if NA_SA_extent==True:
            ca_map.set_extent([-160, -30, -60, 75], ccrs.PlateCarree())
        elif NA_SA_extent==False:
            try:
                ca_map.set_extent([dataLon.min()-0.5, dataLon.max()+0.5, dataLat.min()-0.5, dataLat.max()+0.5], ccrs.PlateCarree())
            except:
                print ("Not setting map extent")
                pass

        ca_map.xaxis.set_visible(True)
        ca_map.yaxis.set_visible(True)
        
        # Plot
        pipes_ = gdf.plot(ax=ca_map, ls='-', color='blue', lw=2,transform = ccrs.PlateCarree())
        
        # Axes labels
        if axisLabels==True:
            ca_map.set_ylabel("Latitude", fontsize = 18)
            ca_map.set_xlabel("Longitude", fontsize = 18)

        # Add scale bar
        if showScale == True:
            scale_bar(ca_map, ccrs.PlateCarree(), 100, location = (0.25, 0.05))  # 100 km scale bar
            
        # Figure title
        if figTitle is not None:
            ca_map.set_title(figTitle + " total length = " + str(int(gdf[pipe_len_attrName].sum())) + " km", fontsize=18, fontweight='bold', loc='left')
            
        # Show legend
        if showLegend==True:
            ca_map.legend(fontsize=16, fancybox=True, shadow=True, loc='lower left',markerscale=4.)
        
        # Show degree symbols in X and Y axes
        ca_map.xaxis.set_major_formatter(ticker.FormatStrFormatter( "%.f\N{DEGREE SIGN}"))
        ca_map.yaxis.set_major_formatter(ticker.FormatStrFormatter( "%.f\N{DEGREE SIGN}"))

        print ("!! CRS is not set or not in EPSG:4326!!")
    
    return fig
###

# =====================================================================================================================
# =====================================================================================================================
# DATA CONSOLIDATION FUNCTIONS

def check_null_geometries(gdf_, id_attr=None):
    """
    ->Checks for any NULL geometries in gdf
    
    Inputs:
    ---
        id_attr is the attribute name--OBJECT_ID or OGIM_ID that would be used to identify any NULLs
        
    Returns:
    ---
        nulls_comp: A list of the IDs where geoemtries are NULLs
        gdf_nulls: A geodataframe of all the NULL geometries
    
    """
    
    nulls_comp = []
    for idx, row in gdf_.iterrows():
        if row.geometry is None:
            nulls_comp.append(row[id_attr])
    else:
        pass
    print ("=====================")
    print ("Number of features with NULL geometries = ", len(nulls_comp))
    print ("=====================")
    
    # Return IDs and gdf of nulls
    gdf_nulls = gdf_[gdf_[id_attr].isin(nulls_comp)]
    
    display(gdf_nulls.head())
    
    
    return nulls_comp, gdf_nulls

# Standardize nulls
def standardize_nulls(gpd_, attrs_=['A','B','C']):
    """
    ->Replace NULLs with "NOT AVAILABLE", -9999, or "1990-01-01"
    
    Inputs:
    ---
        gpd_: GeoDataFrame
        attrs_ : a list of attributes to check for missing records and replace with standard missing data keys
        
    Returns:
    ---
        gpd_: with standardized NULLs
    
    """
    if 'FAC_ID' in attrs_ or 'STATE_PROV' in attrs_ or 'FAC_NAME' in attrs_ or 'OPER_NAME' in attrs_ or 'FAC_TYPE' in attrs_ or 'DRILL_TYPE' in attrs_ or 'FAC_STATUS'in attrs_ or 'COMMODITY' in attrs_ or 'PIPE_MATER' in attrs_:
        try:
            gpd_['FAC_ID'] = [gpd_.FAC_ID.iloc[x] if gpd_.FAC_ID.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['FAC_NAME'] = [gpd_.FAC_NAME.iloc[x] if gpd_.FAC_NAME.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['FAC_NAME'] = ["NOT AVAILABLE" if gpd_.FAC_NAME.iloc[x] == "Untitled Placemark" else gpd_.FAC_NAME.iloc[x] for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['STATE_PROV'] = [gpd_.STATE_PROV.iloc[x] if gpd_.STATE_PROV.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['OPER_NAME'] = [gpd_.OPER_NAME.iloc[x] if gpd_.OPER_NAME.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['FAC_TYPE'] = [gpd_.FAC_TYPE.iloc[x] if gpd_.FAC_TYPE.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['DRILL_TYPE'] = [gpd_.DRILL_TYPE.iloc[x] if gpd_.DRILL_TYPE.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            trgpd_['FAC_STATUS'] = [gpd_.FAC_STATUS.iloc[x] if gpd_.FAC_STATUS.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['COMMODITY'] = [gpd_.COMMODITY.iloc[x] if gpd_.COMMODITY.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['PIPE_MATER'] = [gpd_.PIPE_MATER.iloc[x] if gpd_.PIPE_MATER.iloc[x] is not None else "NOT AVAILABLE" for x in range(gpd_.shape[0])]
        except:
            pass
        
    if 'INSTL_DATE' in attrs_:
        try:
            gpd_['INSTL_DATE'] = [gpd_.INSTL_DATE.iloc[x] if gpd_.INSTL_DATE.iloc[x] is not None else "1900-01-01" for x in range(gpd_.shape[0])]
            gpd_['INSTL_DATE'] = [gpd_.INSTL_DATE.iloc[x] if gpd_.INSTL_DATE.iloc[x] != "NOT AVAILABLE" else "1900-01-01" for x in range(gpd_.shape[0])]
        except:
            pass
        
    if 'PIPELEN_KM' in attrs_ or 'AREA_KM2' in attrs_ or 'CAPACITY' in attrs_:
        try:
            gpd_['PIPELEN_KM'] = [gpd_.PIPELEN_KM.iloc[x] if gpd_.PIPELEN_KM.iloc[x] is not None else -9999 for x in range(gpd_.shape[0])]
            gpd_['PIPELEN_KM'] = [gpd_.PIPELEN_KM.iloc[x] if gpd_.PIPELEN_KM.iloc[x] != "NOT AVAILABLE" else -9999 for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['AREA_KM2'] = [gpd_.AREA_KM2.iloc[x] if gpd_.AREA_KM2.iloc[x] is not None else -9999 for x in range(gpd_.shape[0])]
            gpd_['AREA_KM2'] = [gpd_.AREA_KM2.iloc[x] if gpd_.AREA_KM2.iloc[x] != "NOT AVAILABLE" else -9999 for x in range(gpd_.shape[0])]
        except:
            pass
        try:
            gpd_['CAPACITY'] = [gpd_.CAPACITY.iloc[x] if gpd_.CAPACITY.iloc[x] is not None else -9999 for x in range(gpd_.shape[0])]
            gpd_['CAPACITY'] = [gpd_.CAPACITY.iloc[x] if gpd_.CAPACITY.iloc[x] != "NOT AVAILABLE" else -9999 for x in range(gpd_.shape[0])]
        except:
            pass
        
    return gpd_
            
# =========================================
# Helper function for appending LAT and LON in decimal degrees to database
def append_lat_lon(gdf_, reorder_columns=False):
    """
    This function appends lat and lon in decimal degrees to the geodataframe
    reorder_columns reorders the columns so that lat and lon are third and second last columns in the geodatafrmae before 'geometry'
    """
    # Check that CRS is EPSG: 4326
    if gdf_.crs == "epsg:4326":
        lon_, lat_ = gdf_.geometry.x, gdf_.geometry.y
        # Then append to geodataframe
        gdf_['longitude'] = round(lon_,6)
        gdf_['latitude'] = round(lat_,6)
    else:
        print ("!!CRS is either not set or is not EPSG:4326")
        
    if reorder_columns==True:
        cols = gdf_.columns.tolist()
        cols = cols[:-3] + cols[-2:] + cols[-3:-2]
        gdf_ = gdf_[cols]

    return gdf_
# ==============================

# Read the data
# ==============================
def read_shapefiles_data(fp = None, showOverview=True):
    """
    Read .shp data from folder and concatenate to a single geodataframe
    fp is file folde path
    """
    
    import geopandas as gpd
    
    all_files = glob.glob(fp + "\\*.shp")
    data_ = []

    for file in all_files:
        gdf_ = gpd.read_file(file)
        # Check crs
        if "epsg:4326" in str(gdf_.crs):
            data_.append(gdf_)
        
        else:
            print ("***CRS in NOT epsg:4326, Transforming now***")
            dat_transf = gdf_.to_crs("epsg:4326")
            data_.append(data_transf)
         
    # Concatenate
    all_results = pd.concat(data_)
    
    if showOverview==True:
        print ("=========================================")
        print ("Total # of features in dataset = {}".format(all_results.shape[0]))
        display(all_results.head())
    
    return all_results

def transform_3d_2d(gdf):
    """
    Transform 3D geometries (e.g., POINT Z) to 2 D (e.g., POINT)
    """
    import geopandas as gpd
    import shapely
    import shapely.wkt
    
    gdf2 = gdf.copy()
    
    geoms_ = []
    
    for idx, row in gdf2.iterrows():
        new_geom = shapely.ops.transform(lambda x, y, z=None: (x,y), row.geometry).wkt
        geoms_.append(new_geom)
        
    geoms2_ = []
    for ptt in geoms_:
        geomX = shapely.wkt.loads(ptt)
        geoms2_.append(geomX)
        
    # =================================
    gdf2.set_geometry(geoms2_, inplace=True)
    
    return gdf2


# Transform to "ECKERT_IV", calculate pipeline length in km and reconvert to EPSG 4326
def pipes_preprocess_km(gdf, attrName=None):
    """
    Transform to ECKERT_IV and then calculate length in km, and then
    transform back to EPSG 4326
    
    attrName is the name of the attribute for pipeline length, e.g., PIPE_LEN
    """
    # Check if CRS is defined
    
    if gdf.crs is None:
        print ("CRS of gdf is not set!!!")
        print ("===>Will assume EPSG 4326 and continue<===")
        
    gdf_eckIV = reproject_eckiv(gdf)
    
    pipe_lengths = []
    for idx, row in gdf_eckIV.iterrows():
        pipe_len = row.geometry.length #m
        pipe_lengths.append(pipe_len/1000) # Convert to km
        
    # Append results to geodataframe
    gdf_eckIV[attrName] = pipe_lengths
    
    # Then convert back to EPSG 4326
    gdf_4326 = transform_CRS(gdf_eckIV, appendLatLon=False)
    
    return gdf_4326

# Function for converting from one CRS to WGS 1984
def transform_CRS(gdf, 
                  target_epsg_code="epsg:4326",
                  appendLatLon=False
                 ):
    """
    Transform the gdf from its original CRS to another CRS//known EPSG code, default "epsg:4326"
    
    Inputs:
    ---
        gdf:    geodataframe with known geometry type and CRS
        target_epsg_code:    the new EPSG code for the geodataframe. Default: EPSG:4326
        appendLatLon:    bool, if desired, adds lat and lon to the new gdf
    
    Returns:
    ---
        gdf_new: a new geodataframe whose CRS has been transformed to the the target CRS. Also, two columns representing
                latitude (`latitude_calc`) and longitude (`longitude_calc`) are added to `gdf_new`
    ---
    @author: momara
    """
    import geopandas as gpd
    import warnings
    warnings.filterwarnings("ignore")
    
    # Check CRS
    # Check CRS of reference geodataframe
    crs_gdf = gdf.crs
    if crs_gdf is not None:
        print ("=======================")
        print ("CRS of original gdf is: ", crs_gdf)
    else:
        raise ValueError ("!! CRS of gdf is not set !!")
    
    # Transform to target CRS: use EPSG codes
    gdf_new = gdf.to_crs(target_epsg_code)
    
    # Append latitude and longitude [decimal degrees if EPSG:4326] attributes to gdf_new
    if appendLatLon==True:
        gdf_new['longitude_calc'] = gdf_new.geometry.x
        gdf_new['latitude_calc'] = gdf_new.geometry.y
    
    print ("=======================")
    print ("CRS of new gdf is: ", gdf_new.crs)
    
    return gdf_new

