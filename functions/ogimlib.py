# OGIM Library (ogimlib)
# Last updated June 2022
# Last updated by: @momara
# ===========================================================================
# Dependencies
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import trange
from tqdm import tqdm
import time
import datetime
# ===========================================================================
import matplotlib.pyplot as plt
plt.rcParams["font.family"] = "Arial"
from shapely.geometry import Polygon, Point, shape, mapping, MultiPoint
from shapely.validation import make_valid
import shapely.wkt
import matplotlib
import matplotlib.ticker as mtick
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.image as mpimage
import os
import sys
import math
import glob
# ===========================================================================
# Bokeh
from bokeh.io import output_file, show, export_png
from bokeh.models import ColumnDataSource, GMapOptions
from bokeh.plotting import gmap
from bokeh.layouts import row
# ===========================================================================
from urllib.request import urlopen, urlretrieve
import io
from io import BytesIO
import zipfile
from zipfile import ZipFile
# ===========================================================================
import html
import folium

# Leafmap
# ===========================================================================
import leafmap.leafmap as leafmap

# pyodbc
# ===========================================================================
import pyodbc

# Python Class Object for Oil and Gas facilities
# Encoding
# ===========================================================================
UNICODE_ENCODING = 'utf-8'
NULL_STRING = u'N/A'   # used to indicate null data for a string-type attribute in the facility class
NULL_NUMERIC = -999  # used to indicate null data for numeric-type attribute in facility class
NULL_DATE = "1900-01-01"   # used to indicate null data for date attribute in facility class

class OGIMFacs(object):
    """
    Class object for OGIM facility-level data sourced from public sources.
        - Oil and natural gas wells
        - Offshore platforms
        - Natural gas compressor stations
        - Gathering and processing facilities
        - LNG facility
        - Crude oil refinery
        - Petroleum terminals
        - Tank batteries
        - Injection and disposal facilities
    """
    def __init__(self, 
        ogim_id=NULL_NUMERIC, 
        category=NULL_STRING, 
        country=NULL_STRING, 
        state_prov=NULL_STRING, 
        src_ref_id=NULL_STRING, 
        src_date=NULL_DATE,
        on_offshore=NULL_STRING,
        fac_name=NULL_STRING,
        fac_id=NULL_STRING,
        fac_type=NULL_STRING,
        fac_status=NULL_STRING,
        op_name=NULL_STRING,
        spud_date=NULL_DATE,
        comp_date=NULL_DATE,
        drill_type=NULL_STRING,
        install_date=NULL_DATE,
        commodity=NULL_STRING,
        liq_capacity_bpd=NULL_NUMERIC,
        liq_throughput_bpd=NULL_NUMERIC,
        gas_capacity_mmcfd=NULL_NUMERIC,
        gas_throughput_mmcfd=NULL_NUMERIC,
        num_compr_units=NULL_NUMERIC,
        num_storage_tanks=NULL_NUMERIC,
        site_hp=NULL_NUMERIC,
        fac_latitude=NULL_NUMERIC,
        fac_longitude=NULL_NUMERIC
        ):
        
        # Set data that should be string
        attrs_strings = {
            'CATEGORY': category,
            'COUNTRY': country,
            'STATE_PROV': state_prov,
            'ON_OFFSHORE': on_offshore,
            'SRC_REF_ID': src_ref_id,
            'FAC_NAME': fac_name,
            'FAC_ID': fac_id,
            'FAC_TYPE': fac_type,
            'FAC_STATUS': fac_status,
            'DRILL_TYPE': drill_type,
            'OPERATOR': op_name,
            'COMMODITY': commodity,
            
            }
        
        for attribute, input_parameter in attrs_strings.items():
            if input_parameter is NULL_STRING:
                setattr(self, attribute, NULL_STRING)
            elif type(input_parameter) is str:
                setattr(self, attribute, input_parameter)
            elif type(input_parameter) is float or type(input_parameter) is int:
                setattr(self, attribute, str(input_parameter))
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_STRING)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_STRING)
                        
        # Set data that should be date [YYYY-MM-DD]
        attrs_dates = {
            'SRC_DATE': src_date,
            'INSTALL_DATE': install_date,
            'SPUD_DATE': spud_date,
            'COMP_DATE': comp_date
            }
        
        for attribute, input_parameter in attrs_dates.items():
            if input_parameter is NULL_DATE:
                setattr(self, attribute, NULL_DATE)
            elif type(input_parameter) is str: 
                setattr(self, attribute, input_parameter)
            else:
                if input_parameter is None or type(input_parameter) is None:
                    setattr(self, attribute, NULL_DATE)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_DATE)
    
        # Set data for attributes that should be numeric
        attrs_numeric = {
            'OGIM_ID': ogim_id,
            'LIQ_CAPACITY_BPD': liq_capacity_bpd,
            'LIQ_THROUGHPUT_BPD': liq_throughput_bpd,
            'GAS_CAPACITY_MMCFD': gas_capacity_mmcfd,
            'GAS_THROUGHPUT_MMCFD': gas_throughput_mmcfd,
            'NUM_COMPR_UNITS': num_compr_units,
            'NUM_STORAGE_TANKS': num_storage_tanks,
            'SITE_HP': site_hp,
            'LONGITUDE': fac_longitude,
            'LATITUDE': fac_latitude
            }
        
        for attribute, input_parameter in attrs_numeric.items():
            if input_parameter is NULL_NUMERIC or input_parameter == -9999:
                setattr(self, attribute, NULL_NUMERIC)
            elif type(input_parameter) is float or type(input_parameter) is int:
                    setattr(self, attribute, input_parameter)
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_NUMERIC)
                else:
                    try:
                        setattr(self, attribute, float(input_parameter))  
                    except:
                        print("Error trying to create facility with parameter {0} for attribute {1}.".format(input_parameter, attribute))

    # OGIM facility representation
        
    def __repr__(self):
        """
        Representation of the O&G facility.
        """
        
        data_ = {
            'OGIM_ID': self.OGIM_ID, 
            'CATEGORY': self.CATEGORY, 
            'COUNTRY': self.COUNTRY, 
            'STATE_PROV': self.STATE_PROV, 
            'SRC_REF_ID': self.SRC_REF_ID, 
            'SRC_DATE': self.SRC_DATE, 
            'ON_OFFSHORE': self.ON_OFFSHORE,
            'FAC_NAME': self.FAC_NAME,
            'FAC_ID': self.FAC_ID, 
            'FAC_TYPE': self.FAC_TYPE, 
            'FAC_STATUS': self.FAC_STATUS, 
            'OPERATOR': self.OPERATOR, 
            'SPUD_DATE': self.SPUD_DATE,
            'COMP_DATE': self.COMP_DATE,
            'DRILL_TYPE': self.DRILL_TYPE,
            'INSTALL_DATE': self.INSTALL_DATE, 
            'COMMODITY': self.COMMODITY,
            'LIQ_CAPACITY_BPD': self.LIQ_CAPACITY_BPD,
            'LIQ_THROUGHPUT_BPD': self.LIQ_THROUGHPUT_BPD,
            'GAS_CAPACITY_MMCFD': self.GAS_CAPACITY_MMCFD,
            'GAS_THROUGHPUT_MMCFD': self.GAS_THROUGHPUT_MMCFD,
            'NUM_COMPR_UNITS': self.NUM_COMPR_UNITS,
            'NUM_STORAGE_TANKS': self.NUM_STORAGE_TANKS,
            'SITE_HP': self.SITE_HP,
            'LATITUDE': self.LATITUDE, 
            'LONGITUDE': self.LONGITUDE
            }
        
        return str(data_)
    
# =========================================================
# Ensure numeric data [not lat/lon] are rounded to 3 significant figures

def sig_figures(
    x, 
    n=3
    ):
    """Return 'x' rounded to 'n' significant digits."""
    y=abs(x)
    if np.isnan(y): return np.nan
    if y <= sys.float_info.min: return 0.0
    return round(x, int(n-math.ceil(math.log10(y))))

# Integrate facilities
# =========================================================

def integrate_facs(
    gdf,
    starting_ids: int=0,
    category: str = None,
    fac_alias: str = None,
    country: str = None,
    state_prov: str = None,
    src_ref_id: str = None,
    src_date: str = None,
    on_offshore: str = None,
    fac_name: str = None,
    fac_id: str = None,
    fac_type: str = None,
    spud_date: str = None,
    comp_date: str = None,
    drill_type: str = None,
    install_date: str = None,
    fac_status: str = None,
    op_name: str = None,
    commodity: str = None,
    liq_capacity_bpd: float = None,
    liq_throughput_bpd: float = None,
    gas_capacity_mmcfd: float = None,
    gas_throughput_mmcfd: float = None,
    num_compr_units: int = None,
    num_storage_tanks: int = None,
    site_hp: float = None,
    fac_latitude: float = None,
    fac_longitude: float = None
    ):
    
    """Integrate OGIM facility level data sourced from public sources
    
    Inputs:
    ---
        starting_ids:       starting OGIM_ID for this dataset
        category:           str, indicates one of the following O&G infra category:
                            (i) Oil and natural gas wells
                            (ii) Natural gas compressor stations
                            (iii) Offshore platforms
                            (iv) Gathering and processing facilities
                            (v) Tank batteries
                            (vi) Petroleum terminals
                            (vii) LNG facilities
                            (viii) Injection and disposal facilities (e.g., underground storage facilities)
                            (ix) Equipment and components (e.g., valves, compressor engines, dehydrators, etc)
                            (x) Crude oil refineries
        fac_alias:          One of the following alias (WELLS, COMPR_PROC, REFINERY, LNG_STORAGE, OTHER) represents facility type for which this data is being integrated.
                            Needed for outputting the correct attributes specific to the facility category,
                            (i) WELLS (for well-level data)
                            (ii) COMPR_PROC (for compressor stations and processing facilities)
                            (iii) REFINERY (for crude oil refineries)
                            (iv) LNG_STORAGE (for LNG and other storage facilities, including tank batteries, injection and disposal, petroleum terminals)
                            (v) OTHER (for other facilities, e.g. equipment and components category)
        country:            str, name of the country
        state_prov:         str, name of state or province
        src_date:           str, date the data was last updated by the data owner
        src_ref_id:         str, a reference ID for the data source. Additional info is contained in a standalone reference table.
        fac_id:             str or float, depends on the specific format used by the data owner. For wells, this represents the American
                            Petroleum Institute number that identifies each unique well or the Universal Well Identifier
                            number, or well ID in the database.
        on_offshore:        str, indicates whether the facility is on or offshore
        op_name:            str, name of the operator
        fac_name:           str, name of the facility
        fac_status:         str, indicates the facility status as of the date the data was last updated
        fac_type:           str, for wells, indicates whether the facility is an oil, gas, or mixed oil and gas type;
                            could also be used to indicate whether the facility is a gathering compressor station or
                            a transmission compressor station
        spud_date:          Indicates when the well was spudded/commencement of drilling operations
        comp_date:          Indicates when the well was completed for routine production
        drill_type:         Indicates the drilling configuration for the well, e.g., vertical, horizontal, directional.
        install_date:       str or datetime: indicates the date the facility was installed. Applies to all other facilities except wells                  
        commodity:          str, if available, commodity handled or produced by the facility (e.g., crude oil, bitumen, ethanol, etc)
        liq_capacity_bpd:   design capacity of the facility for handling liquids (barrels per day)
        liq_throughput_bpd: actual liquid throughput at facility (barrels per day)
        gas_capacity_mmcfd: design capacity for handling gas at facility (million cubic feet per day)
        gas_througput_mmcfd:actual gas throughput at facility (million cubic feet per day)
        num_compr_units:    number of compressor units at the compressor station or processing plant
        num_storage_tanks:  number of storage tanks at tank batteries, petroleum terminals, refineries, etc
        site_hp:            total engine horsepower for the facility, if available
        fac_latitude:       float, latitude of the facility location, WGS 1984 (EPSG:4326)
        fac_longitude:      float, longitude of the facility location, WGS 1984 (EPSG: 4326)
        
    Returns:
    --------
      The new geodataframe, properly formatted with the different required attributes.
      
    Example:
    ---
    # Fake data
    data = ["WELL 01", "API-01","OPERATOR A-Z", "2020-01-01", "VERTICAL", 31.4, -103.4]
    col_names = ["well_name", 'well_api', "operator_name", "spud_date", "drill_trajectory", "latitude", "longitude"]
    df = pd.DataFrame(data=data, index=col_names).T
    gdf = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.longitude, df.latitude), crs="epsg:4326")

    # Integration
    wells_gdf, errors_wells = integrate_facs(
        gdf,
        starting_ids=1,
        category="Oil and natural gas wells",
        fac_alias="WELLS",
        country="US",
        state_prov="Texas",
        src_ref_id="1",
        src_date="2022-04-07",
        on_offshore="Onshore",
        fac_name="well_name",
        fac_id="well_api",
        fac_type=None,
        spud_date="spud_date",
        comp_date=None,
        drill_type="drill_trajectory",
        install_date=None,
        fac_status=None,
        op_name="operator_name",
        commodity=None,
        liq_capacity_bpd=None,
        liq_throughput_bpd=None,
        gas_capacity_mmcfd=None,
        gas_throughput_mmcfd=None,
        num_compr_units=None,
        num_storage_tanks=None,
        site_hp=None,
        fac_latitude="latitude",
        fac_longitude="longitude"
        )
    """
    
    starting_ids = starting_ids # Specify the starting ID for this dataset
    all_facs_ = [] 
    attributes_ = [
        'OGIM_ID',
        'CATEGORY',
        'COUNTRY',
        'STATE_PROV',
        'SRC_REF_ID',
        'SRC_DATE',
        'ON_OFFSHORE',
        'FAC_NAME',
        'FAC_ID',
        'FAC_TYPE',
        'FAC_STATUS',
        'OPERATOR',
        'SPUD_DATE',
        'COMP_DATE',
        'DRILL_TYPE',
        'INSTALL_DATE',
        'COMMODITY',
        'LIQ_CAPACITY_BPD',
        'LIQ_THROUGHPUT_BPD',
        'GAS_CAPACITY_MMCFD',
        'GAS_THROUGHPUT_MMCFD',
        'NUM_COMPR_UNITS',
        'NUM_STORAGE_TANKS',
        'SITE_HP',
        'LATITUDE',
        'LONGITUDE',
        ]

    # GDF attributes
    # =========================================================
    category = category
    country  = country
    state_prov = state_prov
    src_ref_id = src_ref_id
    src_date = src_date
    on_offshore = on_offshore
    # =========================================================
    fac_name = fac_name
    fac_id = fac_id
    fac_type = fac_type
    spud_date = spud_date
    comp_date = comp_date
    drill_type = drill_type
    install_date = install_date
    fac_status = fac_status
    op_name = op_name
    commodity = commodity
    liq_capacity_bpd = liq_capacity_bpd
    liq_throughput_bpd = liq_throughput_bpd
    gas_capacity_mmcfd = gas_capacity_mmcfd
    gas_throughput_mmcfd = gas_throughput_mmcfd
    num_compr_units = num_compr_units
    num_storage_tanks = num_storage_tanks
    site_hp = site_hp
    fac_latitude = fac_latitude
    fac_longitude = fac_longitude

    # =========================================================
    error_logs_, error_log_desc = [], [] # For storing possible errors in data entries
    
    for idx_, row in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        # Specify attributes
        # CATEGORY
        try:
            category2 = row[category]
        except:
            category2 = category
        
        # SR_REF_ID
        try:
            src_ref_id2 = row[src_ref_id]
        except:
            src_ref_id2 = src_ref_id
        # COUNTRY
        try:
            country2 = row[country]
        except:
            country2 = country
        # STATE_PROV
        try:
            state_prov2 = row[state_prov]
        except:
            state_prov2 = state_prov
    
        # ON_OFFSHORE
        try:
            on_offshore2 = row[on_offshore]
        except:
            on_offshore2 = on_offshore
            
        # SOURCE_DATE
        try:
            src_date2 = row[src_date]
        except:
            src_date21 = src_date
            try:
                src_date22 = float(src_date21[0:4]) # If no error, then date entered properly
                src_date2 = src_date21
            except:
                raise KeyError("Invalid source date `src_date` field")
    
        # FACILITY NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_name2 = row[fac_name]
        except:
            if fac_name is not None:
                error_logs_.append(fac_name)
                error_log_desc.append("FAC_NAME")
                fac_name2 = NULL_STRING
            else:
                fac_name2 = NULL_STRING
        
        # FACILITY ID
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_id2 = row[fac_id]
        except:
            if fac_id is not None:
                error_logs_.append(fac_id)
                error_log_desc.append("FAC_ID")
                fac_id2 = NULL_STRING
            else:
                fac_id2 = NULL_STRING
        
        # FACILITY TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_type2 = row[fac_type]
        except:
            if fac_type is not None:
                error_logs_.append(fac_type)
                error_log_desc.append("FAC_TYPE")
                fac_type2 = NULL_STRING
            else:
                fac_type2 = NULL_STRING
                
        # DRILL TYPE
        try:
            drill_type2 = row[drill_type]
        except:
            if drill_type is not None:
                error_logs_.append(drill_type)
                error_log_desc.append("DRILL_TYPE")
                drill_type2 = NULL_STRING
            else:
                drill_type2 = NULL_STRING
        
        # SPUD DATE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            spud_date2 = row[spud_date]
        except:
            if spud_date is not None:
                error_logs_.append(spud_date)
                error_log_desc.append("SPUD_DATE")
                spud_date2 = NULL_DATE
            else:
                spud_date2 = NULL_DATE
                
        # COMPLETION DATE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            comp_date2 = row[comp_date]
        except:
            if comp_date is not None:
                error_logs_.append(comp_date)
                error_log_desc.append("COMP_DATE")
                comp_date2 = NULL_DATE
            else:
                comp_date2 = NULL_DATE
        
        # INSTALLATION DATE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            install_date2 = row[install_date]
        except:
            if install_date is not None:
                error_logs_.append(install_date)
                error_log_desc.append("INSTALL_DATE")
                install_date2 = NULL_DATE
            else:
                install_date2 = NULL_DATE
        
        # FACILITY STATUS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_status2 = row[fac_status]
        except:
            if fac_status is not None:
                error_logs_.append(fac_status)
                error_log_desc.append("FAC_STATUS")
                fac_status2 = NULL_STRING
            else:
                fac_status2 = NULL_STRING
        
        # OPERATOR NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            op_name2 = row[op_name]
        except:
            if op_name is not None:
                error_logs_.append(op_name)
                error_log_desc.append("OPERATOR")
                op_name2 = NULL_STRING
            else:
                op_name2 = NULL_STRING
                
        # COMMODITY
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            commodity2 = row[commodity]
        except:
            if commodity is not None:
                error_logs_.append(commodity)
                error_log_desc.append("COMMODITY")
                commodity2 = NULL_STRING
            else:
                commodity2 = NULL_STRING
            
        # LIQUIDS CAPACITY
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            liq_capacity_bpd2 = row[liq_capacity_bpd]
            liq_capacity_bpd2 = sig_figures(liq_capacity_bpd2, n=3)
        except:
            if liq_capacity_bpd is not None:
                error_logs_.append(liq_capacity_bpd)
                error_log_desc.append("LIQ_CAPACITY_BPD")
                liq_capacity_bpd2 = NULL_NUMERIC
            else:
                liq_capacity_bpd2 = NULL_NUMERIC
            
        # LIQUIDS THROUGHPUT
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            liq_throughput_bpd2 = row[liq_throughput_bpd]
            liq_throughput_bpd2 = sig_figures(liq_throughput_bpd2, n=3)
        except:
            if liq_throughput_bpd is not None:
                error_logs_.append(liq_throughput_bpd)
                error_log_desc.append("LIQ_THROUGHPUT_BPD")
                liq_throughput_bpd2 = NULL_NUMERIC
            else:
                liq_throughput_bpd2 = NULL_NUMERIC
            
        # GAS CAPACITY
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            gas_capacity_mmcfd2 = row[gas_capacity_mmcfd]
            gas_capacity_mmcfd2 = sig_figures(gas_capacity_mmcfd2, n=3)
        except:
            if gas_capacity_mmcfd is not None:
                error_logs_.append(gas_capacity_mmcfd)
                error_log_desc.append("GAS_CAPACITY_MMCFD")
                gas_capacity_mmcfd2 = NULL_NUMERIC
            else:
                gas_capacity_mmcfd2 = NULL_NUMERIC
            
        # GAS THROUGHPUT
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            gas_throughput_mmcfd2 = row[gas_throughput_mmcfd]
            gas_throughput_mmcfd2 = sig_figures(gas_throughput_mmcfd2, n=3)
        except:
            if gas_throughput_mmcfd is not None:
                error_logs_.append(gas_throughput_mmcfd)
                error_log_desc.append("GAS_THROUGHPUT_MMCFD")
                gas_throughput_mmcfd2 = NULL_NUMERIC
            else:
                gas_throughput_mmcfd2 = NULL_NUMERIC
                
        # NUMBER OF COMPRESSOR UNITS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            num_compr_units2 = row[num_compr_units]
        except:
            if num_compr_units is not None:
                error_logs_.append(num_compr_units)
                error_log_desc.append("NUM_COMPR_UNITS")
                num_compr_units2 = NULL_NUMERIC
            else:
                num_compr_units2 = NULL_NUMERIC
                
        # NUMBER OF STORAGE TANKS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            num_storage_tanks2 = row[num_storage_tanks]
        except:
            if num_storage_tanks is not None:
                error_logs_.append(num_storage_tanks)
                error_log_desc.append("NUM_STORAGE_TANKS")
                num_storage_tanks2 = NULL_NUMERIC
            else:
                num_storage_tanks2 = NULL_NUMERIC
  
        # SITE HORSEPOWER
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            site_hp2 = row[site_hp]
        except:
            if site_hp is not None:
                error_logs_.append(site_hp)
                error_log_desc.append("SITE_HP")
                site_hp2 = NULL_NUMERIC
            else:
                site_hp2 = NULL_NUMERIC
        
        # FAC LATITUDE
        try:
            fac_latitude2 = row[fac_latitude]
        except:
            fac_latitude2 = fac_latitude
        
        # FAC LONGITUDE
        try:
            fac_longitude2 = row[fac_longitude]
        except:
            fac_longitude2 = fac_longitude
        
        # =========================================================
        # Generate Well OBJECT for each well
    
        id_ = (starting_ids) + (idx_)
        
        ogim_fac = OGIMFacs(
            ogim_id=id_,
            category=category2,
            country=country2,
            state_prov=state_prov2,
            src_ref_id=src_ref_id2,
            src_date=src_date2,
            on_offshore=on_offshore2,
            fac_name=fac_name2,
            fac_id=fac_id2,
            fac_type=fac_type2,
            spud_date=spud_date2,
            comp_date=comp_date2,
            drill_type=drill_type2,
            install_date=install_date2,
            fac_status=fac_status2,
            op_name=op_name2,
            commodity=commodity2,
            liq_capacity_bpd=liq_capacity_bpd2,
            liq_throughput_bpd=liq_throughput_bpd2,
            gas_capacity_mmcfd=gas_capacity_mmcfd2,
            gas_throughput_mmcfd=gas_throughput_mmcfd2,
            num_compr_units=num_compr_units2,
            num_storage_tanks=num_storage_tanks2,
            site_hp=site_hp2,
            fac_latitude=fac_latitude2,
            fac_longitude=fac_longitude2
            )
    
        # Build attributes 
        OGIM_ID = ogim_fac.OGIM_ID                          # int
        
        CATEGORY = ogim_fac.CATEGORY                        # str
        CATEGORY = CATEGORY.upper()
        
        COUNTRY = ogim_fac.COUNTRY                          # str
        COUNTRY = COUNTRY.upper()
        
        STATE_PROV = ogim_fac.STATE_PROV                    # str
        STATE_PROV = STATE_PROV.upper()
        
        SRC_REF_ID = ogim_fac.SRC_REF_ID                    # str
        SRC_REF_ID = SRC_REF_ID.upper()
        
        SRC_DATE = ogim_fac.SRC_DATE                        # str
        
        ON_OFFSHORE = ogim_fac.ON_OFFSHORE                  # str
        ON_OFFSHORE = ON_OFFSHORE.upper()
    
        FAC_NAME = ogim_fac.FAC_NAME                        # str
        # Standardize text styling
        FAC_NAME = FAC_NAME.upper()                         # str
    
        FAC_ID = ogim_fac.FAC_ID                            # str
        FAC_ID = FAC_ID.upper()
    
        FAC_TYPE = ogim_fac.FAC_TYPE                        # str
        # Standardize text styling
        FAC_TYPE = FAC_TYPE.upper()                         # str
        
        SPUD_DATE = ogim_fac.SPUD_DATE                      # str
        
        COMP_DATE = ogim_fac.COMP_DATE                      # str
        
        DRILL_TYPE = ogim_fac.DRILL_TYPE
        DRILL_TYPE = DRILL_TYPE.upper()
    
        INSTALL_DATE = ogim_fac.INSTALL_DATE                # str
        
        FAC_STATUS = ogim_fac.FAC_STATUS                    # str
        FAC_STATUS = FAC_STATUS.upper()
    
        OPERATOR = ogim_fac.OPERATOR                        # str
        OPERATOR = OPERATOR.upper()
        
        COMMODITY = ogim_fac.COMMODITY
        COMMODITY = COMMODITY.upper()
        
        LIQ_CAPACITY_BPD = ogim_fac.LIQ_CAPACITY_BPD        # float
        if np.isnan(LIQ_CAPACITY_BPD):
            LIQ_CAPACITY_BPD = NULL_NUMERIC
            
        LIQ_THROUGHPUT_BPD = ogim_fac.LIQ_THROUGHPUT_BPD    # float
        if np.isnan(LIQ_THROUGHPUT_BPD):
            LIQ_THROUGHPUT_BPD = NULL_NUMERIC
            
        GAS_CAPACITY_MMCFD = ogim_fac.GAS_CAPACITY_MMCFD    # float
        if np.isnan(GAS_CAPACITY_MMCFD):
            GAS_CAPACITY_MMCFD = NULL_NUMERIC
            
        GAS_THROUGHPUT_MMCFD = ogim_fac.GAS_THROUGHPUT_MMCFD# float
        if np.isnan(GAS_THROUGHPUT_MMCFD):
            GAS_THROUGHPUT_MMCFD = NULL_NUMERIC
            
        NUM_COMPR_UNITS = ogim_fac.NUM_COMPR_UNITS          # int
        if np.isnan(NUM_COMPR_UNITS):
            NUM_COMPR_UNITS = NULL_NUMERIC
            
        NUM_STORAGE_TANKS = ogim_fac.NUM_STORAGE_TANKS      # int
        if np.isnan(NUM_STORAGE_TANKS):
            NUM_STORAGE_TANKS = NULL_NUMERIC
            
        SITE_HP = ogim_fac.SITE_HP                          # float
        if np.isnan(SITE_HP):
            SITE_HP = NULL_NUMERIC
    
        LATITUDE = ogim_fac.LATITUDE                        # float
        LONGITUDE = ogim_fac.LONGITUDE                      # float
    
        # Format lat and lon to 5 decimal places
        LATITUDE = float(u"{:.5f}".format(LATITUDE))
        LONGITUDE = float(u"{:.5f}".format(LONGITUDE))
    
        # Create DataFrame
        
        df_ = pd.DataFrame([
            OGIM_ID, CATEGORY, COUNTRY, STATE_PROV, SRC_REF_ID, \
            SRC_DATE, ON_OFFSHORE, FAC_NAME, FAC_ID, FAC_TYPE, \
            FAC_STATUS, OPERATOR, SPUD_DATE, COMP_DATE, DRILL_TYPE,  \
            INSTALL_DATE, COMMODITY, LIQ_CAPACITY_BPD, LIQ_THROUGHPUT_BPD, GAS_CAPACITY_MMCFD, \
            GAS_THROUGHPUT_MMCFD, NUM_COMPR_UNITS, NUM_STORAGE_TANKS, SITE_HP, LATITUDE, 
            LONGITUDE
            ], 
            index=attributes_
            ).T
    
        all_facs_.append(df_)
    # =========================================================
    # Concatenate facility data
    all_facs_df = pd.concat(all_facs_)
    
    # =========================================================
    # Create GeoDataFrame
    all_facs_gdf = gpd.GeoDataFrame(all_facs_df, geometry=gpd.points_from_xy(all_facs_df.LONGITUDE, all_facs_df.LATITUDE), crs="epsg:4326")
    
    # Reset index
    all_facs_gdf2 = all_facs_gdf.reset_index()
    all_facs_gdf3 = all_facs_gdf2.drop(columns=['index'])
    
    # Error logs
    error_logs2 = list(dict.fromkeys(error_logs_))
    error_logs_desc2 = list(dict.fromkeys(error_log_desc))
    
    if len(error_logs2) > 0: # and error_logs2 is not None:
        print ("*** There are possible errors in assigned attribute names! \n Please check error_logs *** \n =========== \n {} for attributes {}".format(error_logs2, error_logs_desc2))
        
    # GeoDataFrame attributes for specific facility categories
    # WELLS attributes
    attrs_WELLS = [
        'OGIM_ID','CATEGORY','COUNTRY','STATE_PROV','SRC_REF_ID',\
        'SRC_DATE','ON_OFFSHORE','FAC_NAME','FAC_ID','FAC_TYPE',\
        'FAC_STATUS','OPERATOR', 'SPUD_DATE','COMP_DATE','DRILL_TYPE',\
        'LATITUDE','LONGITUDE', 'geometry'
        ]
    
    # COMPRESSOR STATIONS AND PROCESSING FACILITY attributes
    attrs_COMPR_PROC = [
        'OGIM_ID','CATEGORY','COUNTRY','STATE_PROV','SRC_REF_ID',\
        'SRC_DATE','ON_OFFSHORE','FAC_NAME','FAC_ID','FAC_TYPE',\
        'FAC_STATUS','OPERATOR','INSTALL_DATE','COMMODITY','LIQ_CAPACITY_BPD',\
        'LIQ_THROUGHPUT_BPD','GAS_CAPACITY_MMCFD','GAS_THROUGHPUT_MMCFD','NUM_COMPR_UNITS','NUM_STORAGE_TANKS',\
        'SITE_HP','LATITUDE','LONGITUDE', 'geometry'
        ]
    
    # REFINERY attributes
    attrs_REFINERY = [
        'OGIM_ID','CATEGORY','COUNTRY','STATE_PROV','SRC_REF_ID',\
        'SRC_DATE','ON_OFFSHORE','FAC_NAME','FAC_ID','FAC_TYPE',\
        'FAC_STATUS','OPERATOR','INSTALL_DATE','COMMODITY','LIQ_CAPACITY_BPD',\
        'LIQ_THROUGHPUT_BPD','NUM_STORAGE_TANKS','LATITUDE','LONGITUDE', 'geometry'
        ]
    
    # LNG_STORAGE attributes (for petroleum terminals, LNG facilities, tank batteries, injection and disposal facilities)
    attrs_LNG_STORAGE = [
        'OGIM_ID','CATEGORY','COUNTRY','STATE_PROV','SRC_REF_ID',\
        'SRC_DATE','ON_OFFSHORE','FAC_NAME','FAC_ID','FAC_TYPE',\
        'FAC_STATUS','OPERATOR','INSTALL_DATE','COMMODITY','LIQ_CAPACITY_BPD',\
        'LIQ_THROUGHPUT_BPD','GAS_CAPACITY_MMCFD','GAS_THROUGHPUT_MMCFD','NUM_STORAGE_TANKS','LATITUDE',\
        'LONGITUDE', 'geometry'
        ]
    
    # OTHER attributes (e.g., equipment and components category, offshore platforms, stations-other)
    attrs_OTHER = [
        'OGIM_ID','CATEGORY','COUNTRY','STATE_PROV','SRC_REF_ID',\
        'SRC_DATE','ON_OFFSHORE','FAC_NAME','FAC_ID','FAC_TYPE',\
        'FAC_STATUS','OPERATOR','INSTALL_DATE','COMMODITY','LATITUDE','LONGITUDE', 'geometry'
        ]
    
    # Generate final GeoDataFrame
    # =========================================================
    if fac_alias == "WELLS" or fac_alias == "Wells" or fac_alias == "wells":
        final_gdf = all_facs_gdf3[attrs_WELLS]
    
    elif fac_alias == "COMPR_PROC" or fac_alias == "Compressor Stations" or fac_alias == "Processing Plant" or fac_alias == "Processing Facilities" or fac_alias == "Compr_proc" or fac_alias == "Processing":
        final_gdf = all_facs_gdf3[attrs_COMPR_PROC]
        
    elif fac_alias == "REFINERY" or fac_alias == "Refinery" or fac_alias == "Crude Oil Refinery":
        final_gdf = all_facs_gdf3[attrs_REFINERY]
        
    elif fac_alias == "LNG_STORAGE" or fac_alias == "LNG" or fac_alias == "Storage" or fac_alias == "Petroleum Terminals" or fac_alias == "Tank Batteries" or fac_alias == "Injection and disposal":
        final_gdf = all_facs_gdf3[attrs_LNG_STORAGE]
        
    elif fac_alias == "OTHER" or fac_alias == "Equipment_Components":
        final_gdf = all_facs_gdf3[attrs_OTHER]
        
    # Preview
    print(final_gdf.head())
    
    return final_gdf, error_logs2


# Database schema for wells
# ===========================================================================

schema_WELLS = {
    'geometry': 'Point',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'DRILL_TYPE': 'str',
        'SPUD_DATE': 'str',
        'COMP_DATE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'LATITUDE': 'float',
        'LONGITUDE': 'float'
        }
    }

# Database schema for compressor stations and processing facilities
# ===========================================================================

schema_COMPR_PROC = {
    'geometry': 'Point',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'INSTALL_DATE': 'str',
        'COMMODITY': 'str',
        'LIQ_CAPACITY_BPD': 'float',
        'LIQ_THROUGHPUT_BPD': 'float',
        'GAS_CAPACITY_MMCFD': 'float',
        'GAS_THROUGHPUT_MMCFD':'float',
        'NUM_COMPR_UNITS':'int32',
        'NUM_STORAGE_TANKS':'int32',
        'SITE_HP':'float',
        'LATITUDE': 'float',
        'LONGITUDE': 'float'
        }
    }

# Database schema for refineries
# ===========================================================================
    
schema_REFINERY = {
    'geometry': 'Point',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'INSTALL_DATE': 'str',
        'COMMODITY': 'str',
        'LIQ_CAPACITY_BPD': 'float',
        'LIQ_THROUGHPUT_BPD': 'float',
        'NUM_STORAGE_TANKS':'int32',
        'LATITUDE': 'float',
        'LONGITUDE': 'float'
        }
    }

# Database schema for LNG and STORAGE facilities
# ===========================================================================
    
schema_LNG_STORAGE = {
    'geometry': 'Point',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'INSTALL_DATE': 'str',
        'COMMODITY': 'str',
        'LIQ_CAPACITY_BPD': 'float',
        'LIQ_THROUGHPUT_BPD': 'float',
        'GAS_CAPACITY_MMCFD': 'float',
        'GAS_THROUGHPUT_MMCFD':'float',
        'NUM_STORAGE_TANKS':'int',
        'LATITUDE': 'float',
        'LONGITUDE': 'float'
        }
    }

# Database schema for other facility types (e.g., stations-other, equipment and components)
# ===========================================================================
    
schema_OTHER = {
    'geometry': 'Point',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'INSTALL_DATE': 'str',
        'COMMODITY': 'str',
        'LATITUDE': 'float',
        'LONGITUDE': 'float'
        }
    }  

# Database schema for PIPELINES
# ===========================================================================

schema_PIPELINES = {
    'geometry': 'LineString',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'INSTALL_DATE': 'str',
        'COMMODITY': 'str',
        'LIQ_CAPACITY_BPD':'float',
        'LIQ_THROUGHPUT_BPD':'float',
        'GAS_CAPACITY_MMCFD':'float',
        'GAS_THROUGHPUT_MMCFD':'float',
        'PIPE_DIAMETER_MM':'float',
        'PIPE_LENGTH_KM':'float',
        'PIPE_MATERIAL':'str'
        }
    } 

# Database schema for BASINS, FIELDS, PLAYS, LICENSE BLOCKS
# ===========================================================================

schema_BASINS = {
    'geometry': 'Polygon',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'NAME': 'str',
        'RESERVOIR_TYPE': 'str',
        'OPERATOR': 'str',
        'AREA_KM2': 'float'
        }
    }

# Database schema for OIL, GAS PRODUCTION
# ===========================================================================

schema_OIL_GAS_PROD = {
    'geometry': 'Point',
    'properties': {
        'OGIM_ID': 'int32',
        'CATEGORY': 'str',
        'COUNTRY': 'str',
        'STATE_PROV': 'str',
        'SRC_REF_ID': 'str',
        'SRC_DATE': 'str',
        'ON_OFFSHORE': 'str',
        'FAC_NAME': 'str',
        'FAC_ID': 'str',
        'FAC_TYPE': 'str',
        'FAC_STATUS': 'str',
        'OPERATOR': 'str',
        'SPUD_DATE': 'str',
        'COMP_DATE': 'str',
        'DRILL_TYPE': 'str',
        'OIL_BBL': 'float',
        'GAS_MCF': 'float',
        'WATER_BBL': 'float',
        'CONDENSATE_BBL': 'float',
        'PROD_DAYS': 'int32',
        'PROD_YEAR': 'int32',
        'ENTITY_TYPE': 'str',
        'LATITUDE': 'float',
        'LONGITUDE': 'float'
        }
    }

# ===========================================================================
# Transforming CRS to EPSG:4326 (default)
# ===========================================================================

def transform_CRS(
    gdf, 
    target_epsg_code="epsg:4326",
    appendLatLon: bool = False
    ):
    """ Transform the gdf from its original CRS to another CRS//known EPSG code, default "epsg:4326"
    
    Inputs:
    ---
        gdf:              geodataframe with known geometry type and CRS
        target_epsg_code: the new EPSG code for the geodataframe. Default: EPSG:4326
        appendLatLon:     bool, if desired, adds lat and lon to the new gdf
    
    Returns:
    ---
        gdf_new: a new geodataframe whose CRS has been transformed to the the target CRS. 
        Also, if appendLatLon is True, two columns representing latitude (`latitude_calc`) and longitude (`longitude_calc`) are added to `gdf_new`
    
    Dependencies:
    ---
        # import geopandas as gpd
            
    """
    # Check CRS
    # Check CRS of reference geodataframe
    crs_gdf = gdf.crs
    if crs_gdf is not None:
        print ("=======================")
        print ("CRS of original gdf is: ", crs_gdf)
    else:
        raise ValueError("!! CRS of gdf is not set !!")
    
    # Transform to target CRS: use EPSG codes
    gdf_new = gdf.to_crs(target_epsg_code)
    
    # Append latitude and longitude [decimal degrees if EPSG:4326] attributes to gdf_new
    if appendLatLon==True:
        if 'Point' in gdf_new.geom_type.unique()[0]:
            gdf_new['longitude_calc'] = gdf_new.geometry.x
            gdf_new['latitude_calc'] = gdf_new.geometry.y
        elif 'Polygon' in gdf_new.geom_type.unique()[0] or 'MultiPolygon' in gdf_new.geom_type.unique():
            gdf_new['longitude_calc'] = gdf_new.geometry.centroid.x
            gdf_new['latitude_calc'] = gdf_new.geometry.centroid.y
    
    print ("=======================")
    print ("CRS of new gdf is: ", gdf_new.crs)
    
    return gdf_new

# ===========================================================================

def replace_row_names(
    gdf, 
    colName: str, 
    dict_names: dict
    ):
        
    """ Map items in a specific column (`colName`) to replacement values in `dict_names`
    # Standardize fac status description
    # =================================
    # EXAMPLE:
    colName = "WELL_ACTIV"
    dict_names = {'ABAN':'Abandoned',
              'ACT':'Active',
              'CANC':'Cancelled',
              'CASE':'Cased',
              'COMP':'Completed',
              'DRIL':'Drilling',
              'DSUS':'Drilling suspended',
              'GAST':'Gas testing',
              'PRES':'Prep to resume',
              'PSPD':'Prep to spud',
              'SUSP':'Suspended',
              'WAG':'Well authorization granted',
              'ABNZ':'Abandoned zone',
              'XXXX':'NOT AVAILABLE'
             }
    # =================================
    bc_wellsv3 = replace_row_names(bc_wellsv2, colName, dict_names)
    
    """
    # Standardize description
    print ("========================")
    print ("Original list of unique attributes = ", gdf[colName].unique())
    cols_= gdf[colName].map(dict_names).fillna(gdf[colName])
    # preview unique types again
    gdf[colName] = cols_
    
    print ("========================")
    print (gdf[colName].unique())
    
    return gdf

# ===========================================================================
# Function for reading shapefiles, GeoJSONs and Geodatabases

def read_spatial_data(
    path_to_file, 
    layer_name: str = None,
    specify_encoding: bool = False,
    data_encoding: 'Unicode' = 'utf-8',
    table_gradient: bool = False
    ):
        
    """ Read a .SHP, .GeoJSON or .GDB into a Pandas GeoDataFrame
        
    Inputs:
    ---
        path_to_file:   path to file
        layer_name:     str, specify layer name for .gdb file
        specify_encoding:  if True, the encoding specified in "data_encoding" will be used to read the data  
        table_gradient: bool, if True, automatically displays a preview of the dataframe, showing a gradient scale for numerical columns
    
    Returns:
    ---
        A GeoDataFrame of the same file
            
    Dependencies:
    ---
        GeoPandas
    """
    fp_id_ = path_to_file[-10:] # Extract the last ten characters in the file name
        
    if ".shp" in fp_id_ or ".SHP" in fp_id_ or ".geojson" in fp_id_ or ".GeoJSON" in fp_id_:
        if specify_encoding == True:
            gdf = gpd.read_file(path_to_file, encoding=data_encoding)
        else:
            gdf = gpd.read_file(path_to_file)
        print ('---------------------------------------')
        print ('Total # of features in dataset = %d' % gdf.shape[0])
        print (gdf.columns)
    
        if table_gradient == True:
            print(gdf.head().style.background_gradient(cmap='Blues'))
        else:
            print(gdf.head())
                
    elif ".gdb" in fp_id_ or ".GDB" in fp_id_ or ".gpkg" in fp_id_ or ".GPKG" in fp_id_:
        if layer_name is None:
            if specify_encoding == True:
                gdf = gpd.read_file(path_to_file, encoding=data_encoding)
            else:
                gdf = gpd.read_file(path_to_file)
            print ('---------------------------------------')
            print ('Total # of features in dataset = %d' % gdf.shape[0])
            print (gdf.columns)
    
            if table_gradient == True:
                print(gdf.head().style.background_gradient(cmap='Blues'))
            else:
                print(gdf.head())
        else:
            if specify_encoding == True:
                gdf = gpd.read_file(path_to_file, layer=layer_name, encoding=data_encoding)
            else:
                gdf = gpd.read_file(path_to_file)
            print ('---------------------------------------')
            print ('Total # of features in dataset = %d' % gdf.shape[0])
            print (gdf.columns)
    
            if table_gradient == True:
                print(gdf.head().style.background_gradient(cmap='Blues'))
            else:
                print(gdf.head())
    return gdf  

# ===========================================================================
# Automatically download spatial data

def data_auto_download(
    url: str,
    region: str = None,
    category: str = None,
    createFolder: bool = False,
    export_path: str = None,
    fileName: str = None
    ):
        
    """ This function automatically downloads a .zip file or data file from the given url
    and extracts to a specific folder path
    
    Inputs:
    ---
        url:          str, path to URL
        region:       str, name of the region for which data is being downloaded. This will also be used in the folde path
        category:     str, facility category (e.g., wells, compressor stations, facilities, etc)
        createFolder: bool, if True, a new folder is created to the current working directory for storing the data as long as it doesnt exist already
        export_path:  str, if createFolder is False, then `export_path` must be specified for storing the downloaded data
        fileName:     str, name of the file with appropriate extension (e.g., .xlsx, .csv)
            
    Dependencies:
    ---
        import os
        from urllib.request import urlopen, urlretrieve
        import io
        from io import BytesIO
        from zipfile import ZipFile
    """
    if createFolder:
        # Create a new folder
        export_path = os.getcwd()+"\\auto_downloads\\" + region + "\\" + category
        
        # Check whether the specified path exists or not
        if os.path.exists(export_path) == True:
            pass
        else:
            # Create a new directory
            os.makedirs(export_path, exist_ok=True)
    else:
        export_path = export_path
    
    #".ZIP FILE"
    if ".zip" in url:
        response = urlopen(url)
    
        zipFile = ZipFile(BytesIO(response.read()))
    
        # Extract to folder
        print ("==============================")
        print ("Download completed, now extracting file to folder")
        zipFile.extractall(path=export_path)
        print ("==============================")
        print ("Download and extraction complete for {} ".format(url))
    else:
        urlretrieve(url, export_path + "\\" + fileName)

# ===========================================================================
# Retrieve imagery at a selected number of locations

def chunks(lst, n):
    """Yield successive n-sized chunks from list."""
    return [lst[i:i + n] for i in range(0, len(lst), n)]

def random_imagery_check(
    gdf_,
    api_key: str = None,
    zoom_level: int=18,
    gdf_crs_epsg: str = None,
    ran_sample: bool = True,
    num_images: int=100,
    region_name: str = None,
    fac_category: str = None,
    save_path: str = None,
    save_pdf: bool = False
    ):
    
    """Randomly draw samples from the GeoDataFrame and retrieve Google Earth Imagery
    
    Inputs:
    ---
        gdf_:         GeoDataFrame, CRS: EPSG: 4326, with location attributes including "LATITUDE" and "LONGITUDE"
        api_ky:       Google Maps API Key
        zoom_level:   Zoom level
        gdf_crs_epsg: If CRS is not set on data, specify it here to automatically set
        ran_sample:   boolean, if True, draw random sample of number "num_images" from dataset
        num_images:   number of images to plot
        region_name:  name of region
        fac_category: facility category for data being plotted
        save_path:    path to folder where imagery will be saved.
        save_pdf:     save the downloaded images in one pdf document.
        
    Returns:
    ---
        .pngs for the selected data to plot
        
    """
    # Random sample
    if ran_sample == True:
        gdf_sel = gdf_.sample(n=num_images, replace=False)
    else:
        gdf_sel = gdf_.copy()
        
    # # Check CRS
    if gdf_sel.crs is None or gdf_sel.crs != "epsg:4326":
        # Set crs of the dataframe first
        print ("===================")
        print ("NOTE: First set the correct CRS for this dataframe \n by specifying the appropriate EPSG code in the `gdf_crs_epsg` parameter")
        print ("===================")
        print ("Setting appropriate CRS as defined by the EPSG code in `gdf_crs_epsg` parameter")
        gdf_sel = gdf_sel.set_crs(epsg=gdf_crs_epsg)
        
        # Set new CRS as WGS 1984 to enable retrieval of lat, lon in decimal degrees from Google Maps
        print ("Transforming CRS to EPSG:4326")
        gdf_sel = gdf_sel.to_crs(4326)
    
        # lat and lon in degrees
        if gdf_sel.geometry.type.iloc[0] == 'Point':
            gdf_sel['LONGITUDE'] = gdf_sel.geometry.x
            gdf_sel['LATITUDE'] = gdf_sel.geometry.y
            
    elif gdf_sel.crs == "epsg:4326":
        if gdf_sel.geometry.type.iloc[0] == 'Point':
            gdf_sel['LONGITUDE'] = gdf_sel.geometry.x
            gdf_sel['LATITUDE'] = gdf_sel.geometry.y

    # Next, iterate through each row of the gdf and retrieve Google Maps imagery
    print ("===>Retrieving images<===")
    
    map_figs = []
    for idx, row1 in tqdm(gdf_sel.iterrows(), total=gdf_sel.shape[0]):
#         output_file(region_name + "_"  + str(idx) + ".html")
        lat_, lon_ = row1.LATITUDE, row1.LONGITUDE
        map_1 = GMapOptions(lat=lat_, lng=lon_, map_type="satellite", zoom=zoom_level)
        p1 = gmap(api_key, map_1, title=region_name + ": " + str(lat_) + ", " + str(lon_))
        # Add cross at actual lat and lon
        p1.triangle(lon_, lat_, size=16, color="#c02942", alpha=0.9)
        
        export_png(row([p1]), filename=save_path + "\\" + str(idx) + "_" + region_name + "_" + fac_category + "_"  + str(lat_) + "_" + str(lon_) + "_.png", width=1500, height=1500)
        
    # =========================================================================
    # Next, read saved .png files and plot them
    print ("===> Reading and plotting retrieved images <===")
    
    files = glob.glob(save_path + "\\*.png")
    # The PDF document
    if save_pdf == True:
        pdf_pages = PdfPages(save_path + "\\" + region_name + "_" + fac_category + "_images_.pdf")
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
                img_ = mpimage.imread(image_ids[idx])
                ax_[idx].imshow(img_)
                ax_[idx].axis('off')
                # Show lat and lon
                lat_str, lon_str = str(image_ids[idx].split("_")[:-2][3]), str(image_ids[idx].split("_")[:-1][4])
                #    Append titles
                ax_[idx].set_title(region_name + ": \n" + lat_str + ",\n" + lon_str)
            # Save files
            plt.savefig(pdf_pages, format='pdf')
        # Close pdf
        pdf_pages.close()

    # Save gdf_sel2 as a .csv file
    print ("===>Saving .csv of retrieved lat and lon for images <===")
    gdf_sel.drop('geometry',axis=1).to_csv(save_path +'\\_'+ region_name + "_" + fac_category + "_.csv") 
    
    print ("===>Program successfully finished<===")
    
# =========================================================================
# Saving GeoDataFrames as .shp or .geojson

def save_spatial_data(
    gdf, 
    file_name: str = None,
    schema_def: bool = False,
    schema: dict = None,
    file_type: str="ESRI_SHP",
    out_path: str = None,
    data_encoding:str="utf-8",
    ):
    
    """Save geodataframe as an ESRI shapefile or GeoJSON
    
    Inputs
    ---
       gdf:       geodataframe
       file_name: str, file name for the output file [format: country_stateprov_infracategory_], e.g., canada_saskatchewan_oil_gas_wells
       schema_def: bool: if True, the "schema" parameter should be defined based on the "OGIM_Schema" def
       schema:    Use specific schema, e.g., for wells (schema_WELLS), gathering and processing facilities (schema_COMPR_PROC), etc
       file_type: either "ESRI_SHP" for .shapefile or "GeoJSON". If GeoJSON, gdf must have CRS of EPSG:4326
       out_path:  str, path to output the file
       data_encoding: Default encoding is "utf-8"
       
    Returns
    ---
        Shapefile or GeoJSON saved to specified file folder
    """
    if file_type == "ESRI_SHP" and schema_def == True:
        gdf.to_file(out_path + file_name + "_.shp", encoding=data_encoding, schema=schema)
    elif file_type == "GeoJSON" and schema_def == True:
        gdf.to_file(out_path + file_name + "_.geojson", encoding=data_encoding, schema=schema, driver="GeoJSON")
    elif file_type == "ESRI_SHP" and schema_def == False:
        gdf.to_file(out_path + file_name + "_.shp", encoding=data_encoding)
    elif file_type == "GeoJSON" and schema_def == False:
        gdf.to_file(out_path + file_name + "_.geojson", encoding=data_encoding, driver="GeoJSON")
    
    print ("===Successfully saved {0} to specified path===".format(file_type))

# =========================================================================
# Extract the .zip files in this folder

def unzip_files_in_folder(
    dir_name, 
    create_save_path: bool = False, 
    save_path: str = None,
    remove_zip: bool = True
    ):
    
    """Function for unzipping files in a folder
    
    Inputs:
    ---
        dir_name: full path to folder with zipped files
        create_save_path: bool, if True, function automatically creates a "unzipped_files_" sub-folder in "dir_name"
        save-path: if `create_save_path` is False, the path to save extracted files must be entered here
        remove_zip: bool, if True, the original .zip folders are removed from directory
    
    """
    # Get current working directory
    cwd_ = os.getcwd()
    
    path_to_zip_file = dir_name
    extension = ".zip"
    
    # Change directory from working dir to dir with files
    os.chdir(dir_name) 
    
    # Path for saving unzipped files
    if create_save_path == True:
        # Create a new folder
        export_path = os.getcwd()+"\\unzipped_files\\" 
        
        # Check whether the specified path exists or not
        if os.path.exists(export_path) == True:
            pass
        else:
            # Create a new directory
            os.makedirs(export_path, exist_ok=True)
    else:
        if save_path is not None:
            export_path = save_path
        else:
            raise ValueError("***save_path not set and `create_save_path` is False***")
    
    # Loop through each .zip item in the directory, unzip 
    for item in os.listdir(dir_name): # loop through items in dir
        if item.endswith(extension): # check for ".zip" extension
            file_name = os.path.abspath(item) # get full path of files
            zip_ref = zipfile.ZipFile(file_name) # create zipfile object
            zip_ref.extractall(export_path) # extract file to dir
            zip_ref.close() # close file
            
            if remove_zip == True:
                os.remove(file_name) 
            
    # Change directory back to initial working directory
    os.chdir(cwd_)
    
# =========================================================================
# Transform 3D geometries to 2D geometries
def transform_geom_3d_2d(gdf):
    """ Transform 3D geometries in a GeoDataFrame (e.g., POINT Z) to 2D (e.g., POINT)"""
    
    geoms_ = []
    
    # A copy of the original GeoDataFrame
    gdf2 = gdf.copy()
    
    # Iterate through each row in the dataset
    for idx, row in tqdm(gdf2.iterrows(), total=gdf2.shape[0]):
        new_geom = shapely.ops.transform(lambda x, y, z=None: (x,y), row.geometry).wkt
        geoms_.append(new_geom)
        
    geoms2_ = []
    for ptt in geoms_:
        geomX = shapely.wkt.loads(ptt)
        geoms2_.append(geomX)
        
    # =================================
    gdf3 = gdf2.set_geometry(geoms2_, crs=gdf2.crs)
    
    # Preview
    print(gdf3.head())
    
    return gdf3

# Explode multipart geometries
# =========================================================================
def explode_multi_geoms(gdf):
    """Explode multi-part geometries into single geometries """
    
    return gdf.explode(column='geometry', ignore_index=True)

# Check invalid geometries in spatial data
# =========================================================================

def check_invalid_geoms(
    gdf, 
    id_attr=None
    ):
    
    """Checks for any `None` or `inf` geometries in gdf
    
    Inputs:
    ---
        gdf: GeoDataFrame with geometry attributes
        id_attr: is the attribute name--OBJECT_ID or OGIM_ID that would be used to index the null features
        
    Returns:
    ---
        nulls_list: A list of the IDs where geometries are invalid, based on `id_attr`
        gdf_nulls: A geodataframe of all the invalid geometries
        
    Dependencies:
    ---
        geopandas
        tqdm
        shapely
    """
    
    nulls_list = []
    for idx_, row_ in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        if row_.geometry is None:
            nulls_list.append(row_[id_attr])
        elif "inf" in row_.geometry.wkt:
            nulls_list.append(row_[id_attr])
        else:
            pass
    print ("=====================")
    print ("Number of features with INVALID geometries = ", len(nulls_list))
    print ("=====================")
    
    # Return IDs and gdf of nulls
    gdf_nulls = gdf[gdf[id_attr].isin(nulls_list)]
    
    print(gdf_nulls.head())

    return nulls_list, gdf_nulls


# =========================================================================
def repair_invalid_polygon_geometries(gdf_, geom_col_name='geometry'):
    '''Repair any invalid polygon geometries in a GeoDataFrame.

    NOTE: Some of the "repaired" geometries might become GeometryCollection
    type after running `make_valid()`. (A GeometryCollection is a mix of
    polygons and lines, or polygons & points).
    If, after making a geometry valid, a record has a geometry of type
    GeometryCollection, this function breaks apart the GeometryCollection and
    only keep the Polygon or Multipolygon portion of the geometry for the
    resulting gdf.

    Parameters
    ----------
    gdf_ : GeoPandas GeoDataFrame
        GeoDataFrame containing records with Polygon or MultiPolygon geometries.
    geom_col_name : string, optional
        Name of the designated gometry column in `gdf`. The default is 'geometry'.

    Returns
    -------
    gdf : GeoPandas GeoDataFrame
        Copy of input `gdf_`. If any geometries in `gdf_` were invalid, the
        output `gdf` contains repaired versions of those geometries.

    '''
    # If none of the records in the input gdf_ contain polygons, then exit the
    # function. (Shapely only detects "invalidity" in polygon features.)
    gdf_geom_types = [x.geom_type for x in gdf_.geometry]
    if not any('Polygon' in s for s in gdf_geom_types):
        print('!!! Function should be used on polygon-type geometry records.')
        print('Check that your input gdf contains polygon features and try again.')
        return

    # Make a copy so the original data / geoms are not edited
    gdf = gdf_.copy(deep=True)

    # `make_valid` function can't be vectorized across the GeoSeries, so use a
    # lambda function to apply `make_valid` to each geometry one row at a time.
    # TODO - need to edit this code once we start using Shapely 2.0+
    gdf.geometry = gdf.apply(lambda row: make_valid(row.geometry), axis=1)

    # Look for rows with GeometryCollection type geometries, and turn them into
    # just Polygon or MultiPolygon geometries
    for i, row in gdf.iterrows():
        g = row.geometry
        if g.geom_type == 'GeometryCollection':
            print(f'Record {i} has {len(g.geoms)} components in its GeometryCollection:')
            geom_types = [x.geom_type for x in g.geoms]
            print(geom_types)
            # TODO - comment on why this logic gate is necessary.
            # if any('Line' in s for s in geom_types) and any('Polygon' in s for s in geom_types) and len(g.geoms) == 2:
                # print('Line and Polygon situation with two components')
            for part in g.geoms:
                if part.geom_type == 'MultiPolygon' or part.geom_type == 'Polygon':
                    gdf.at[i, 'geometry'] = part
                    print(f'Record {i} repaired.')
    return gdf



# Interactive map for visualizing a random sample of points in Google Earth imagery
# =========================================================================

def interactive_map(
    gdf, 
    center=[50,-103], 
    zoom=10, 
    random_sample=False,
    num_samples=50,
    height='500px', 
    width='800px', 
    showMSAT=False,
    MSAT_Targets_Path=None
    ):
    
    """Visaulize a random selection of point location data on an interactive map, basemap is Google Earth
    
    Inputs:
    -----------
        gdf: The geodataframe with data to visualize. Preferred CRS is EPSG:4326 
        center: lat, lon representing center of the map
        random_sample: if True, draw a random sample of size (num_samples) from gdf for visualization
        num_samples: number of random samples to draw from gdf for initial visualization
        height, width: height and width of map in pixels
        showMSAT: boolean, if True, displays MethaneSAT current target areas
        MSAT_Targets_Path: path to shapefile for MSAT Target areas
      
    Returns:
    --------
        Map showing randomly selected well locations in satellite imagery
        
    Dependencies:
    --------
        leafmap
        numpy (np)
        geopandas (gpd)
        
    """
    # Map style
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
    if random_sample == True:
        try:
            gdf_ = gdf.sample(n=num_samples, replace=False)
        except:
            # If error, use all of the data from the dataset
            print ("num_samples is greater than total # of features in dataset \n ::::: Using all the features in the dataset")
            gdf_ = gdf.sample(n=gdf.shape[0])
    else:
        gdf_ = gdf.copy()

    # Plot
    m.add_gdf(gdf_, layer_name="Facilities", style=style, hover_style=hover_style)
    m.add_tile_layer(url="https://mt1.google.com/vt/lyrs=y&x={x}&y={y}&z={z}", name="Google Satellite", attribution="GoogleMaps")

    return m

# Interactive map for visualizing a random sample of points in Google Earth imagery
# =========================================================================

def reproject_eckiv(
    gdf, 
    gdf_crs_epsg=4326
    ):
    
    """Reproject the geodataframe to an Eckert IV equal area projection

    Inputs:
    ------
        gdf: geodataframe: The geodataframe whose CRS will be converted 
        gdf_crs_epsg: required input for the EPSG code of the gdf's current CRS if it is not defined
    
    Returns:
    ------
        A new gdf with the reprojected CRS
    """
    # =========================================================================
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


# Transform to "ECKERT_IV", calculate pipeline length in km and reconvert to EPSG 4326
# =========================================================================

def calculate_pipeline_length_km(
    gdf: 'GeoDataFrame', 
    attrName: 'str' = "PIPELINE_LENGTH_KM"
    ):

    """ Function for calculating length of pipeline segment in km, if not available in the dataframe
    The function transforms the CRS to ECKERT_IV for spatial data manipulation,
    and then iterates through each rowm calculating the segment length in km, before transforming the CRS back to EPSG:4326
    
    Inputs: 
        attrName: is the name of the attribute for pipeline length, e.g., PIPELINE_LENGTH_KM. This will be appended to gdf
    """
    # Check if CRS is defined
    
    if gdf.crs is None:
        print ("***CRS of gdf is not set!*** \n ==> Please set CRS first::: \n Terminating program...")
        sys.exit()
    
    # Reporject to Eckert IV
    gdf_eckIV = reproject_eckiv(gdf)
    
    # Calculation of pipeline lengths
    pipe_lengths = []
    
    for idx3, row3 in gdf_eckIV.iterrows():
        pipe_len = row3.geometry.length #m
        pipe_lengths.append(pipe_len/1000) # Convert to km
        
        
    # Append results to geodataframe
    gdf_eckIV[attrName] = pipe_lengths
    
    # Then convert back to EPSG 4326
    gdf_4326 = transform_CRS(gdf_eckIV, appendLatLon=False)
    
    return gdf_4326

def calculate_basin_area_km2(
    gdf: 'GeoDataFrame', 
    attrName: 'str' = "AREA_KM2"
    ):

    """ Function for calculating area of basin, shale play, or field in km2
    The function transforms the CRS to ECKERT_IV for spatial data manipulation,
    and then iterates through each row calculating the area in km2, before transforming the CRS back to EPSG:4326
    
    Inputs: 
        attrName: is the name of the attribute for area, e.g., AREA_KM2. This will be appended to gdf
    """
    # Check if CRS is defined
    
    if gdf.crs is None:
        print ("***CRS of gdf is not set!*** \n ==> Please set CRS first::: \n Terminating program...")
        sys.exit()
    
    # Reporject to Eckert IV
    gdf_eckIV = reproject_eckiv(gdf)
    
    # Calculation of pipeline lengths
    _areas = []
    
    for idx3, row3 in gdf_eckIV.iterrows():
        _area = row3.geometry.area #m2
        _areas.append(_area/(1000*1000)) # Convert to km
        
    # Append results to geodataframe
    gdf_eckIV[attrName] = _areas
    
    # Then convert back to EPSG 4326
    gdf_4326 = transform_CRS(gdf_eckIV, appendLatLon=False)
    
    return gdf_4326

# ===============================================================================
# OGIM pipeline data integration 

class OGIMPipelines(object):
    """
    Class object for OGIM pipeline data sourced from public sources.
        - Oil and natural gas pipeline data
    """
    def __init__(self, 
        ogim_id=NULL_NUMERIC, 
        category=NULL_STRING, 
        country=NULL_STRING, 
        state_prov=NULL_STRING, 
        src_ref_id=NULL_STRING, 
        src_date=NULL_DATE,
        on_offshore=NULL_STRING,
        fac_name=NULL_STRING,
        fac_id=NULL_STRING,
        fac_type=NULL_STRING,
        fac_status=NULL_STRING,
        op_name=NULL_STRING,
        install_date=NULL_DATE,
        commodity=NULL_STRING,
        liq_capacity_bpd=NULL_NUMERIC,
        liq_throughput_bpd=NULL_NUMERIC,
        gas_capacity_mmcfd=NULL_NUMERIC,
        gas_throughput_mmcfd=NULL_NUMERIC,
        pipe_diameter_mm=NULL_NUMERIC,
        pipe_length_km=NULL_NUMERIC,
        pipe_material=NULL_STRING
        ):
        
        # Set data that should be string
        attrs_strings = {
            'CATEGORY': category,
            'COUNTRY': country,
            'STATE_PROV': state_prov,
            'ON_OFFSHORE': on_offshore,
            'SRC_REF_ID': src_ref_id,
            'FAC_NAME': fac_name,
            'FAC_ID': fac_id,
            'FAC_TYPE': fac_type,
            'FAC_STATUS': fac_status,
            'OPERATOR': op_name,
            'COMMODITY': commodity,
            'PIPE_MATERIAL': pipe_material
            }
        
        for attribute, input_parameter in attrs_strings.items():
            if input_parameter is NULL_STRING:
                setattr(self, attribute, NULL_STRING)
            elif type(input_parameter) is str:
                setattr(self, attribute, input_parameter)
            elif type(input_parameter) is float or type(input_parameter) is int:
                setattr(self, attribute, str(input_parameter))
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_STRING)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_STRING)
                        
        # Set data that should be date [YYYY-MM-DD]
        attrs_dates = {
            'SRC_DATE': src_date,
            'INSTALL_DATE': install_date
            }
        
        for attribute, input_parameter in attrs_dates.items():
            if input_parameter is NULL_DATE:
                setattr(self, attribute, NULL_DATE)
            elif type(input_parameter) is str: 
                setattr(self, attribute, input_parameter)
            else:
                if input_parameter is None or type(input_parameter) is None:
                    setattr(self, attribute, NULL_DATE)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_DATE)
    
        # Set data for attributes that should be numeric
        attrs_numeric = {
            'OGIM_ID': ogim_id,
            'LIQ_CAPACITY_BPD': liq_capacity_bpd,
            'LIQ_THROUGHPUT_BPD': liq_throughput_bpd,
            'GAS_CAPACITY_MMCFD': gas_capacity_mmcfd,
            'GAS_THROUGHPUT_MMCFD': gas_throughput_mmcfd,
            'PIPE_DIAMETER_MM': pipe_diameter_mm,
            'PIPE_LENGTH_KM': pipe_length_km
            }
        
        for attribute, input_parameter in attrs_numeric.items():
            if input_parameter is NULL_NUMERIC or input_parameter == -9999:
                setattr(self, attribute, NULL_NUMERIC)
            elif type(input_parameter) is float or type(input_parameter) is int:
                    setattr(self, attribute, input_parameter)
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_NUMERIC)
                else:
                    try:
                        setattr(self, attribute, float(input_parameter))  
                    except:
                        print("Error trying to create facility with parameter {0} for attribute {1}.".format(input_parameter, attribute))

    # OGIM facility representation
        
    def __repr__(self):
        """
        Representation of the O&G facility.
        """
        
        data_ = {
            'OGIM_ID': self.OGIM_ID, 
            'CATEGORY': self.CATEGORY, 
            'COUNTRY': self.COUNTRY, 
            'STATE_PROV': self.STATE_PROV, 
            'SRC_REF_ID': self.SRC_REF_ID, 
            'SRC_DATE': self.SRC_DATE, 
            'ON_OFFSHORE': self.ON_OFFSHORE,
            'FAC_NAME': self.FAC_NAME,
            'FAC_ID': self.FAC_ID, 
            'FAC_TYPE': self.FAC_TYPE, 
            'FAC_STATUS': self.FAC_STATUS, 
            'OPERATOR': self.OPERATOR, 
            'INSTALL_DATE': self.INSTALL_DATE, 
            'COMMODITY': self.COMMODITY,
            'LIQ_CAPACITY_BPD': self.LIQ_CAPACITY_BPD,
            'LIQ_THROUGHPUT_BPD': self.LIQ_THROUGHPUT_BPD,
            'GAS_CAPACITY_MMCFD': self.GAS_CAPACITY_MMCFD,
            'GAS_THROUGHPUT_MMCFD': self.GAS_THROUGHPUT_MMCFD,
            'PIPE_DIAMETER_MM': self.PIPE_DIAMETER_MM,
            'PIPE_LENGTH_KM': self.PIPE_LENGTH_KM,
            'PIPE_MATERIAL': self.PIPE_MATERIAL
            }
        
        return str(data_)
    
# =========================================================

def integrate_pipelines(
    gdf: 'GeoDataFrame',
    starting_ids: int=0,
    category: str = None,
    fac_alias: str = "PIPELINES",
    country: str = None,
    state_prov: str = None,
    src_ref_id: str = None,
    src_date: str = None,
    on_offshore: str = None,
    fac_name: str = None,
    fac_id: str = None,
    fac_type: str = None,
    install_date: str = None,
    fac_status: str = None,
    op_name: str = None,
    commodity: str = None,
    liq_capacity_bpd: float = None,
    liq_throughput_bpd: float = None,
    gas_capacity_mmcfd: float = None,
    gas_throughput_mmcfd: float = None,
    pipe_diameter_mm: float = None,
    pipe_length_km: float = None,
    pipe_material: str = None
    ):
    
    """Integrate OGIM data for pipelines
    
    Inputs:
    ---
        starting_ids:       starting OGIM_ID for this dataset
        category:           str, indicates O&G infra category:
                            (i) Oil and natural gas pipelines
        fac_alias:          One of the following alias (WELLS, COMPR_PROC, REFINERY, LNG_STORAGE, OTHER) represents facility type for which this data is being integrated.
                            Needed for outputting the correct attributes specific to the facility category,
                            (i) WELLS (for well-level data)
                            (ii) COMPR_PROC (for compressor stations and processing facilities)
                            (iii) REFINERY (for crude oil refineries)
                            (iv) LNG_STORAGE (for LNG and other storage facilities, including tank batteries, injection and disposal, petroleum terminals)
                            (v) OTHER (for other facilities, e.g. equipment and components category)
        country:            str, name of the country
        state_prov:         str, name of state or province
        src_date:           str, date the data was last updated by the data owner
        src_ref_id:         str, a reference ID for the data source. Additional info is contained in a standalone reference table.
        fac_id:             str or float, depends on the specific format used by the data owner. For wells, this represents the American
                            Petroleum Institute number that identifies each unique well or the Universal Well Identifier
                            number, or well ID in the database.
        on_offshore:        str, indicates whether the facility is on or offshore
        op_name:            str, name of the operator
        fac_name:           str, name of the facility
        fac_status:         str, indicates the facility status as of the date the data was last updated
        fac_type:           str, for wells, indicates whether the facility is an oil, gas, or mixed oil and gas type;
                            could also be used to indicate whether the facility is a gathering compressor station or
                            a transmission compressor station
        install_date:       str or datetime: indicates the date the facility was installed. Applies to all other facilities except wells                  
        commodity:          str, if available, commodity handled or produced by the facility (e.g., crude oil, bitumen, ethanol, etc)
        liq_capacity_bpd:   design capacity of the facility for handling liquids (barrels per day)
        liq_throughput_bpd: actual liquid throughput at facility (barrels per day)
        gas_capacity_mmcfd: design capacity for handling gas at facility (million cubic feet per day)
        gas_througput_mmcfd:actual gas throughput at facility (million cubic feet per day)
        pipe_diameter_mm:   if available, diameter of pipeline in millimeters
        pipe_length_km:     if available, length of pipeline in kilometers
        pipe_material:      if available, material used for pipeline construction (e.g., steel, cast iron, plastic, etc)
        
    Returns:
    --------
      The new geodataframe, properly formatted with the different required attributes.
      
    """
    
    starting_ids = starting_ids # Specify the starting ID for this dataset
    all_facs_ = [] 
    attributes_ = [
        'OGIM_ID',
        'CATEGORY',
        'COUNTRY',
        'STATE_PROV',
        'SRC_REF_ID',
        'SRC_DATE',
        'ON_OFFSHORE',
        'FAC_NAME',
        'FAC_ID',
        'FAC_TYPE',
        'FAC_STATUS',
        'OPERATOR',
        'INSTALL_DATE',
        'COMMODITY',
        'LIQ_CAPACITY_BPD',
        'LIQ_THROUGHPUT_BPD',
        'GAS_CAPACITY_MMCFD',
        'GAS_THROUGHPUT_MMCFD',
        'PIPE_DIAMETER_MM',
        'PIPE_LENGTH_KM',
        'PIPE_MATERIAL'
        ]

    # GDF attributes
    # =========================================================
    category = category
    country  = country
    state_prov = state_prov
    src_ref_id = src_ref_id
    src_date = src_date
    on_offshore = on_offshore
    # =========================================================
    fac_name = fac_name
    fac_id = fac_id
    fac_type = fac_type
    install_date = install_date
    fac_status = fac_status
    op_name = op_name
    commodity = commodity
    liq_capacity_bpd = liq_capacity_bpd
    liq_throughput_bpd = liq_throughput_bpd
    gas_capacity_mmcfd = gas_capacity_mmcfd
    gas_throughput_mmcfd = gas_throughput_mmcfd
    pipe_diameter_mm = pipe_diameter_mm
    pipe_length_km = pipe_length_km
    pipe_material = pipe_material

    # =========================================================
    error_logs_, error_log_desc = [], [] # For storing possible errors in data entries
    
    for idx_, row in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        # Specify attributes
        # Specify attributes
        # CATEGORY
        try:
            category2 = row[category]
        except:
            category2 = category
        
        # SR_REF_ID
        try:
            src_ref_id2 = row[src_ref_id]
        except:
            src_ref_id2 = src_ref_id
        # COUNTRY
        try:
            country2 = row[country]
        except:
            country2 = country
        # STATE_PROV
        try:
            state_prov2 = row[state_prov]
        except:
            state_prov2 = state_prov
    
        # ON_OFFSHORE
        try:
            on_offshore2 = row[on_offshore]
        except:
            on_offshore2 = on_offshore
            
        # SOURCE_DATE
        try:
            src_date2 = row[src_date]
        except:
            src_date21 = src_date
            try:
                src_date22 = float(src_date21[0:4]) # If no error, then date entered properly
                src_date2 = src_date21
            except:
                raise KeyError("Invalid source date `src_date` field")
    
        # FACILITY NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_name2 = row[fac_name]
        except:
            if fac_name is not None:
                error_logs_.append(fac_name)
                error_log_desc.append("FAC_NAME")
                fac_name2 = NULL_STRING
            else:
                fac_name2 = NULL_STRING
        
        # FACILITY ID
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_id2 = row[fac_id]
        except:
            if fac_id is not None:
                error_logs_.append(fac_id)
                error_log_desc.append("FAC_ID")
                fac_id2 = NULL_STRING
            else:
                fac_id2 = NULL_STRING
        
        # FACILITY TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_type2 = row[fac_type]
        except:
            if fac_type is not None:
                error_logs_.append(fac_type)
                error_log_desc.append("FAC_TYPE")
                fac_type2 = NULL_STRING
            else:
                fac_type2 = NULL_STRING
                
        # INSTALLATION DATE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            install_date2 = row[install_date]
        except:
            if install_date is not None:
                error_logs_.append(install_date)
                error_log_desc.append("INSTALL_DATE")
                install_date2 = NULL_DATE
            else:
                install_date2 = NULL_DATE
        
        # FACILITY STATUS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_status2 = row[fac_status]
        except:
            if fac_status is not None:
                error_logs_.append(fac_status)
                error_log_desc.append("FAC_STATUS")
                fac_status2 = NULL_STRING
            else:
                fac_status2 = NULL_STRING
        
        # OPERATOR NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            op_name2 = row[op_name]
        except:
            if op_name is not None:
                error_logs_.append(op_name)
                error_log_desc.append("OPERATOR")
                op_name2 = NULL_STRING
            else:
                op_name2 = NULL_STRING
                
        # COMMODITY
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            commodity2 = row[commodity]
        except:
            if commodity is not None:
                error_logs_.append(commodity)
                error_log_desc.append("COMMODITY")
                commodity2 = NULL_STRING
            else:
                commodity2 = NULL_STRING
            
        # LIQUIDS CAPACITY
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            liq_capacity_bpd2 = row[liq_capacity_bpd]
            liq_capacity_bpd2 = sig_figures(liq_capacity_bpd2, n=3)
        except:
            if liq_capacity_bpd is not None:
                error_logs_.append(liq_capacity_bpd)
                error_log_desc.append("LIQ_CAPACITY_BPD")
                liq_capacity_bpd2 = NULL_NUMERIC
            else:
                liq_capacity_bpd2 = NULL_NUMERIC
            
        # LIQUIDS THROUGHPUT
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            liq_throughput_bpd2 = row[liq_throughput_bpd]
            liq_throughput_bpd2 = sig_figures(liq_throughput_bpd2, n=3)
        except:
            if liq_throughput_bpd is not None:
                error_logs_.append(liq_throughput_bpd)
                error_log_desc.append("LIQ_THROUGHPUT_BPD")
                liq_throughput_bpd2 = NULL_NUMERIC
            else:
                liq_throughput_bpd2 = NULL_NUMERIC
            
        # GAS CAPACITY
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            gas_capacity_mmcfd2 = row[gas_capacity_mmcfd]
            gas_capacity_mmcfd2 = sig_figures(gas_capacity_mmcfd2, n=3)
        except:
            if gas_capacity_mmcfd is not None:
                error_logs_.append(gas_capacity_mmcfd)
                error_log_desc.append("GAS_CAPACITY_MMCFD")
                gas_capacity_mmcfd2 = NULL_NUMERIC
            else:
                gas_capacity_mmcfd2 = NULL_NUMERIC
            
        # GAS THROUGHPUT
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            gas_throughput_mmcfd2 = row[gas_throughput_mmcfd]
            gas_throughput_mmcfd2 = sig_figures(gas_throughput_mmcfd2, n=3)
        except:
            if gas_throughput_mmcfd is not None:
                error_logs_.append(gas_throughput_mmcfd)
                error_log_desc.append("GAS_THROUGHPUT_MMCFD")
                gas_throughput_mmcfd2 = NULL_NUMERIC
            else:
                gas_throughput_mmcfd2 = NULL_NUMERIC
                
        # PIPE DIAMETER
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            pipe_diameter2 = row[pipe_diameter_mm]
            pipe_diameter2 = sig_figures(pipe_diameter2, n=3)
        except:
            if pipe_diameter_mm is not None:
                error_logs_.append(pipe_diameter_mm)
                error_log_desc.append("PIPE_DIAMETER_MM")
                pipe_diameter2 = NULL_NUMERIC
            else:
                pipe_diameter2 = NULL_NUMERIC
                
        # PIPE LENGTH
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            pipe_length2 = row[pipe_length_km]
            pipe_length2 = sig_figures(pipe_length2, n=3)
        except:
            if pipe_length_km is not None:
                error_logs_.append(pipe_length_km)
                error_log_desc.append("NUM_STORAGE_TANKS")
                pipe_length2 = NULL_NUMERIC
            else:
                pipe_length2 = NULL_NUMERIC
  
        # PIPE MATERIAL
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            pipe_material2 = row[pipe_material]
        except:
            if pipe_material is not None:
                error_logs_.append(pipe_material)
                error_log_desc.append("PIPE_MATERIAL")
                pipe_material2 = NULL_STRING
            else:
                pipe_material2 = NULL_STRING
        
        # =========================================================
        # Generate Well OBJECT for each well
    
        id_ = (starting_ids) + (idx_)
        
        ogim_fac = OGIMPipelines(
            ogim_id=id_,
            category=category2,
            country=country2,
            state_prov=state_prov2,
            src_ref_id=src_ref_id2,
            src_date=src_date2,
            on_offshore=on_offshore2,
            fac_name=fac_name2,
            fac_id=fac_id2,
            fac_type=fac_type2,
            install_date=install_date2,
            fac_status=fac_status2,
            op_name=op_name2,
            commodity=commodity2,
            liq_capacity_bpd=liq_capacity_bpd2,
            liq_throughput_bpd=liq_throughput_bpd2,
            gas_capacity_mmcfd=gas_capacity_mmcfd2,
            gas_throughput_mmcfd=gas_throughput_mmcfd2,
            pipe_diameter_mm=pipe_diameter2,
            pipe_length_km=pipe_length2,
            pipe_material=pipe_material2
            )
    
        # Build attributes 
        OGIM_ID = ogim_fac.OGIM_ID                          # int
        
        CATEGORY = ogim_fac.CATEGORY                        # str
        CATEGORY = CATEGORY.upper()
        
        COUNTRY = ogim_fac.COUNTRY                          # str
        COUNTRY = COUNTRY.upper()
        
        STATE_PROV = ogim_fac.STATE_PROV                    # str
        STATE_PROV = STATE_PROV.upper()
        
        SRC_REF_ID = ogim_fac.SRC_REF_ID                    # str
        SRC_REF_ID = SRC_REF_ID.upper()
        
        SRC_DATE = ogim_fac.SRC_DATE                        # str
        
        ON_OFFSHORE = ogim_fac.ON_OFFSHORE                  # str
        ON_OFFSHORE = ON_OFFSHORE.upper()
    
        FAC_NAME = ogim_fac.FAC_NAME                        # str
        # Standardize text styling
        FAC_NAME = FAC_NAME.upper()                         # str
    
        FAC_ID = ogim_fac.FAC_ID                            # str
        FAC_ID = FAC_ID.upper()
    
        FAC_TYPE = ogim_fac.FAC_TYPE                        # str
        # Standardize text styling
        FAC_TYPE = FAC_TYPE.upper()                         # str
    
        INSTALL_DATE = ogim_fac.INSTALL_DATE                # str
        
        FAC_STATUS = ogim_fac.FAC_STATUS                    # str
        FAC_STATUS = FAC_STATUS.upper()
    
        OPERATOR = ogim_fac.OPERATOR                        # str
        OPERATOR = OPERATOR.upper()
        
        COMMODITY = ogim_fac.COMMODITY
        COMMODITY = COMMODITY.upper()
        
        LIQ_CAPACITY_BPD = ogim_fac.LIQ_CAPACITY_BPD        # float
        if np.isnan(LIQ_CAPACITY_BPD):
            LIQ_CAPACITY_BPD = NULL_NUMERIC
            
        LIQ_THROUGHPUT_BPD = ogim_fac.LIQ_THROUGHPUT_BPD    # float
        if np.isnan(LIQ_THROUGHPUT_BPD):
            LIQ_THROUGHPUT_BPD = NULL_NUMERIC
            
        GAS_CAPACITY_MMCFD = ogim_fac.GAS_CAPACITY_MMCFD    # float
        if np.isnan(GAS_CAPACITY_MMCFD):
            GAS_CAPACITY_MMCFD = NULL_NUMERIC
            
        GAS_THROUGHPUT_MMCFD = ogim_fac.GAS_THROUGHPUT_MMCFD# float
        if np.isnan(GAS_THROUGHPUT_MMCFD):
            GAS_THROUGHPUT_MMCFD = NULL_NUMERIC
            
        PIPE_DIAMETER_MM = ogim_fac.PIPE_DIAMETER_MM          # int
        if np.isnan(PIPE_DIAMETER_MM):
            PIPE_DIAMETER_MM = NULL_NUMERIC
            
        PIPE_LENGTH_KM = ogim_fac.PIPE_LENGTH_KM              # int
        if np.isnan(PIPE_LENGTH_KM):
            NUM_STORAGE_TANKS = NULL_NUMERIC
            
        PIPE_MATERIAL = ogim_fac.PIPE_MATERIAL
        PIPE_MATERIAL = PIPE_MATERIAL.upper()
    
        # Create DataFrame
        
        df_ = pd.DataFrame([
            OGIM_ID, CATEGORY, COUNTRY, STATE_PROV, SRC_REF_ID, \
            SRC_DATE, ON_OFFSHORE, FAC_NAME, FAC_ID, FAC_TYPE, \
            FAC_STATUS, OPERATOR, INSTALL_DATE, COMMODITY, LIQ_CAPACITY_BPD, \
            LIQ_THROUGHPUT_BPD, GAS_CAPACITY_MMCFD, GAS_THROUGHPUT_MMCFD, PIPE_DIAMETER_MM, PIPE_LENGTH_KM, \
            PIPE_MATERIAL
            ], 
            index=attributes_
            ).T
    
        all_facs_.append(df_)
    # =========================================================
    # Concatenate facility data
    all_facs_df = pd.concat(all_facs_)
    
    # Create GeoDataFrame
    final_gdf = all_facs_df.reset_index().set_geometry(gdf.geometry, crs=gdf.crs)
    final_gdf2 = final_gdf.drop(columns=['index'])

    # Error logs
    error_logs2 = list(dict.fromkeys(error_logs_))
    error_logs_desc2 = list(dict.fromkeys(error_log_desc))
    
    if len(error_logs2) > 0: # and error_logs2 is not None:
        print ("*** There are possible errors in assigned attribute names! \n Please check error_logs *** \n =========== \n {} for attributes {}".format(error_logs2, error_logs_desc2))
        
    # Preview
    print(final_gdf2.head())
    
    return final_gdf2, error_logs2

# =========================================================
# ========================================================================================
# O&G fields and basins

class OGIMBasin(object):
    """Class object for the following categories:
        - Oil and natural gas basins
        - Oil and natural gas licence blocks
        - Oil and natural gas fields
        - Oil and natural gas plays
    """
    def __init__(self, 
        ogim_id=NULL_NUMERIC, 
        category=NULL_STRING, 
        country=NULL_STRING, 
        state_prov=NULL_STRING, 
        src_ref_id=NULL_STRING, 
        src_date=NULL_DATE,
        on_offshore=NULL_STRING,
        _name=NULL_STRING,
        op_name=NULL_STRING,
        reservoir_type=NULL_STRING,
        _area_km2=NULL_NUMERIC
        ):
        
        # Set data that should be string
        attrs_strings = {
            'CATEGORY': category,
            'COUNTRY': country,
            'STATE_PROV': state_prov,
            'ON_OFFSHORE': on_offshore,
            'SRC_REF_ID': src_ref_id,
            'NAME': _name,
            'OPERATOR': op_name,
            'RESERVOIR_TYPE': reservoir_type
            }
        
        for attribute, input_parameter in attrs_strings.items():
            if input_parameter is NULL_STRING:
                setattr(self, attribute, NULL_STRING)
            elif type(input_parameter) is str:
                setattr(self, attribute, input_parameter)
            elif type(input_parameter) is float or type(input_parameter) is int:
                setattr(self, attribute, str(input_parameter))
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_STRING)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_STRING)
                        
        # Set data that should be date [YYYY-MM-DD]
        attrs_dates = {
            'SRC_DATE': src_date
            }
        
        for attribute, input_parameter in attrs_dates.items():
            if input_parameter is NULL_DATE:
                setattr(self, attribute, NULL_DATE)
            elif type(input_parameter) is str: 
                setattr(self, attribute, input_parameter)
            else:
                if input_parameter is None or type(input_parameter) is None:
                    setattr(self, attribute, NULL_DATE)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_DATE)
    
        # Set data for attributes that should be numeric
        attrs_numeric = {
            'OGIM_ID': ogim_id,
            'AREA_KM2': _area_km2
            }
        
        for attribute, input_parameter in attrs_numeric.items():
            if input_parameter is NULL_NUMERIC or input_parameter == -9999:
                setattr(self, attribute, NULL_NUMERIC)
            elif type(input_parameter) is float or type(input_parameter) is int:
                    setattr(self, attribute, input_parameter)
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_NUMERIC)
                else:
                    try:
                        setattr(self, attribute, float(input_parameter))  
                    except:
                        print("Error trying to create facility with parameter {0} for attribute {1}.".format(input_parameter, attribute))

    # OGIM license block, field, license block
    def __repr__(self):
        """Representation of the O&G basin, field, license block, shale play
        """
        
        data_ = {
            'OGIM_ID': self.OGIM_ID, 
            'CATEGORY': self.CATEGORY, 
            'COUNTRY': self.COUNTRY, 
            'STATE_PROV': self.STATE_PROV, 
            'SRC_REF_ID': self.SRC_REF_ID, 
            'SRC_DATE': self.SRC_DATE, 
            'ON_OFFSHORE': self.ON_OFFSHORE,
            'NAME': self.NAME,
            'RESERVOIR_TYPE': self.RESERVOIR_TYPE, 
            'OPERATOR': self.OPERATOR, 
            'AREA_KM2': self.AREA_KM2
            }
        
        return str(data_)
    
## Integrate basins
## =========================================================

def integrate_basins(
    gdf,
    starting_ids: int=0,
    category: str = None,
    fac_alias: str = "OIL_GAS_BASINS",
    country: str = None,
    state_prov: str = None,
    src_ref_id: str = None,
    src_date: str = None,
    on_offshore: str = None,
    _name: str = None,
    reservoir_type: str = None,
    op_name: str = None,
    _area_km2: float = None
    ):
    
    """Integrate OGIM basin-level data sourced from public sources
    
    Inputs:
    ---
        starting_ids:       starting OGIM_ID for this dataset
        category:           str, indicates one of the following O&G category:
                            (i) Oil and natural gas basins
                            (ii) Oil and natural gas fields
                            (iii) Oil and natural gas license blocks
        fac_alias:          Useful for specifying database schema, default for this facility category is "OIL_GAS_BASINS"
        country:            name of the country
        state_prov:         name of state or province
        src_date:           date the data was last updated by the data owner
        src_ref_id:         a reference ID for the data source. Additional info is contained in a standalone reference table.
        on_offshore:        indicates whether the facility is on or offshore
        op_name:            name of the operator
        reservoir_type:     indicates whether the resevoir is an oil, gas, or oil and gas reservoir
        _area_km2:          indicates area of basin, field or license block in square kilometers
    Returns:
    --------
      The new geodataframe, properly formatted with the different required attributes.
      
    """
    
    starting_ids = starting_ids # Specify the starting OGIM ID for this dataset
    all_basins_ = [] 
    attributes_ = [
        'OGIM_ID',
        'CATEGORY',
        'COUNTRY',
        'STATE_PROV',
        'SRC_REF_ID',
        'SRC_DATE',
        'ON_OFFSHORE',
        'NAME',
        'RESERVOIR_TYPE',
        'OPERATOR',
        'AREA_KM2'
        ]

    # GDF attributes
    # =========================================================
    category = category
    country  = country
    state_prov = state_prov
    src_ref_id = src_ref_id
    src_date = src_date
    on_offshore = on_offshore
    # =========================================================
    _name = _name
    reservoir_type = reservoir_type
    op_name = op_name
    _area_km2 = _area_km2

    # =========================================================
    error_logs_, error_log_desc = [], [] # For storing possible errors in data entries
    
    for idx_, row in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        # Specify attributes
        # Specify attributes
        # CATEGORY
        try:
            category2 = row[category]
        except:
            category2 = category
        
        # SR_REF_ID
        try:
            src_ref_id2 = row[src_ref_id]
        except:
            src_ref_id2 = src_ref_id
        # COUNTRY
        try:
            country2 = row[country]
        except:
            country2 = country
        # STATE_PROV
        try:
            state_prov2 = row[state_prov]
        except:
            state_prov2 = state_prov
    
        # ON_OFFSHORE
        try:
            on_offshore2 = row[on_offshore]
        except:
            on_offshore2 = on_offshore
            
        # SOURCE_DATE
        try:
            src_date2 = row[src_date]
        except:
            src_date21 = src_date
            try:
                src_date22 = float(src_date21[0:4]) # If no error, then date entered properly
                src_date2 = src_date21
            except:
                raise KeyError("Invalid source date `src_date` field")
    
        # NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            _name2 = row[_name]
        except:
            if _name is not None:
                error_logs_.append(_name)
                error_log_desc.append("NAME")
                _name2 = NULL_STRING
            else:
                _name2 = NULL_STRING
        
        # FIELD TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            reservoir_type2 = row[reservoir_type]
        except:
            if reservoir_type is not None:
                error_logs_.append(reservoir_type)
                error_log_desc.append("RESERVOIR_TYPE")
                reservoir_type2 = NULL_STRING
            else:
                reservoir_type2 = NULL_STRING
        
        # OPERATOR NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        # In some cases, the analyst can calculate this attribute before integration,
        # for example, using the `calculate_basin_area_km2` function
        try:
            op_name2 = row[op_name]
        except:
            if op_name is not None:
                error_logs_.append(op_name)
                error_log_desc.append("OPERATOR")
                op_name2 = NULL_STRING
            else:
                op_name2 = NULL_STRING
            
        # AREA KM2
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            _area_km22 = row[_area_km2]
            _area_km22 = sig_figures(_area_km22, n=3)
        except:
            if _area_km2 is not None:
                error_logs_.append(_area_km2)
                error_log_desc.append("AREA_KM2")
                _area_km22 = NULL_NUMERIC
            else:
                _area_km22 = NULL_NUMERIC
                
        # =========================================================
        # Generate basin OBJECT for each basin
    
        id_ = (starting_ids) + (idx_)
        
        ogim_fac = OGIMBasin(
            ogim_id=id_,
            category=category2,
            country=country2,
            state_prov=state_prov2,
            src_ref_id=src_ref_id2,
            src_date=src_date2,
            on_offshore=on_offshore2,
            _name=_name2,
            reservoir_type=reservoir_type2,
            op_name=op_name2,
            _area_km2=_area_km22
            )
    
        # Build attributes 
        OGIM_ID = ogim_fac.OGIM_ID                          # int
        
        CATEGORY = ogim_fac.CATEGORY                        # str
        CATEGORY = CATEGORY.upper()
        
        COUNTRY = ogim_fac.COUNTRY                          # str
        COUNTRY = COUNTRY.upper()
        
        STATE_PROV = ogim_fac.STATE_PROV                    # str
        STATE_PROV = STATE_PROV.upper()
        
        SRC_REF_ID = ogim_fac.SRC_REF_ID                    # str
        SRC_REF_ID = SRC_REF_ID.upper()
        
        SRC_DATE = ogim_fac.SRC_DATE                        # str
        
        ON_OFFSHORE = ogim_fac.ON_OFFSHORE                  # str
        ON_OFFSHORE = ON_OFFSHORE.upper()
    
        NAME = ogim_fac.NAME                                # str
        # Standardize text styling
        NAME = NAME.upper()                                 # str
    
        RESERVOIR_TYPE = ogim_fac.RESERVOIR_TYPE            # str
        # Standardize text styling
        RESERVOIR_TYPE = RESERVOIR_TYPE.upper()             # str
        
        OPERATOR = ogim_fac.OPERATOR                        # str
        OPERATOR = OPERATOR.upper()
        
        AREA_KM2 = ogim_fac.AREA_KM2                        # float
        if np.isnan(AREA_KM2):
            AREA_KM2 = NULL_NUMERIC
            
        # Create DataFrame

        df_ = pd.DataFrame([
            OGIM_ID, CATEGORY, COUNTRY, STATE_PROV, SRC_REF_ID, \
            SRC_DATE, ON_OFFSHORE, NAME, RESERVOIR_TYPE, OPERATOR, \
            AREA_KM2
            ], 
            index=attributes_
            ).T
    
        all_basins_.append(df_)
    # =========================================================
    # Concatenate facility data
    all_basins_df = pd.concat(all_basins_)
    
    # Create GeoDataFrame
    final_gdf = all_basins_df.reset_index().set_geometry(gdf.geometry, crs=gdf.crs)
    final_gdf2 = final_gdf.drop(columns=['index'])

    # Error logs
    error_logs2 = list(dict.fromkeys(error_logs_))
    error_logs_desc2 = list(dict.fromkeys(error_log_desc))
    
    if len(error_logs2) > 0: # and error_logs2 is not None:
        print ("*** There are possible errors in assigned attribute names! \n Please check error_logs *** \n =========== \n {} for attributes {}".format(error_logs2, error_logs_desc2))
        
    # Preview
    print(final_gdf2.head())
    
    return final_gdf2, error_logs2

# ===============================================================

# OGIM Class object 
class OGIMProduction(object):
    """Class object for OGIM O&G production data from public sources."""
    def __init__(self, 
        ogim_id=NULL_NUMERIC, 
        category=NULL_STRING, 
        country=NULL_STRING, 
        state_prov=NULL_STRING, 
        src_ref_id=NULL_STRING, 
        src_date=NULL_DATE,
        on_offshore=NULL_STRING,
        fac_name=NULL_STRING,
        fac_id=NULL_STRING,
        fac_type=NULL_STRING,
        fac_status=NULL_STRING,
        op_name=NULL_STRING,
        spud_date=NULL_DATE,
        comp_date=NULL_DATE,
        drill_type=NULL_STRING,
        oil_bbl=NULL_NUMERIC,
        gas_mcf=NULL_NUMERIC,
        water_bbl=NULL_NUMERIC,
        condensate_bbl=NULL_NUMERIC,
        prod_days=NULL_NUMERIC,
        prod_year=NULL_NUMERIC,
        entity_type=NULL_STRING,
        fac_latitude=NULL_NUMERIC,
        fac_longitude=NULL_NUMERIC
        ):
        
        # Set data that should be string
        attrs_strings = {
            'CATEGORY': category,
            'COUNTRY': country,
            'STATE_PROV': state_prov,
            'ON_OFFSHORE': on_offshore,
            'SRC_REF_ID': src_ref_id,
            'FAC_NAME': fac_name,
            'FAC_ID': fac_id,
            'FAC_TYPE': fac_type,
            'FAC_STATUS': fac_status,
            'DRILL_TYPE': drill_type,
            'OPERATOR': op_name,
            'ENTITY_TYPE': entity_type
            }
        
        for attribute, input_parameter in attrs_strings.items():
            if input_parameter is NULL_STRING:
                setattr(self, attribute, NULL_STRING)
            elif type(input_parameter) is str:
                setattr(self, attribute, input_parameter)
            elif type(input_parameter) is float or type(input_parameter) is int:
                setattr(self, attribute, str(input_parameter))
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_STRING)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_STRING)
                        
        # Set data that should be date [YYYY-MM-DD]
        attrs_dates = {
            'SRC_DATE': src_date,
            'SPUD_DATE': spud_date,
            'COMP_DATE': comp_date
            }
        
        for attribute, input_parameter in attrs_dates.items():
            if input_parameter is NULL_DATE:
                setattr(self, attribute, NULL_DATE)
            elif type(input_parameter) is str: 
                setattr(self, attribute, input_parameter)
            else:
                if input_parameter is None or type(input_parameter) is None:
                    setattr(self, attribute, NULL_DATE)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_DATE)
    
        # Set data for attributes that should be numeric
        attrs_numeric = {
            'OGIM_ID': ogim_id,
            'OIL_BBL': oil_bbl,
            'GAS_MCF': gas_mcf,
            'WATER_BBL': water_bbl,
            'CONDENSATE_BBL': condensate_bbl,
            'PROD_DAYS': prod_days,
            'PROD_YEAR': prod_year,
            'LATITUDE': fac_latitude,
            'LONGITUDE': fac_longitude
            }
        
        for attribute, input_parameter in attrs_numeric.items():
            if input_parameter is NULL_NUMERIC:
                setattr(self, attribute, NULL_NUMERIC)
            elif type(input_parameter) is float or type(input_parameter) is int:
                    setattr(self, attribute, input_parameter)
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_NUMERIC)
                else:
                    try:
                        setattr(self, attribute, float(input_parameter))  
                    except:
                        print("Error trying to create facility with parameter {0} for attribute {1}.".format(input_parameter, attribute))

    # OGIM facility representation
        
    def __repr__(self):
        """
        Representation of the O&G facility.
        """
        
        data_ = {
            'OGIM_ID': self.OGIM_ID, 
            'CATEGORY': self.CATEGORY, 
            'COUNTRY': self.COUNTRY, 
            'STATE_PROV': self.STATE_PROV, 
            'SRC_REF_ID': self.SRC_REF_ID, 
            'SRC_DATE': self.SRC_DATE, 
            'ON_OFFSHORE': self.ON_OFFSHORE,
            'FAC_NAME': self.FAC_NAME,
            'FAC_ID': self.FAC_ID, 
            'FAC_TYPE': self.FAC_TYPE, 
            'FAC_STATUS': self.FAC_STATUS, 
            'OPERATOR': self.OPERATOR, 
            'SPUD_DATE': self.SPUD_DATE,
            'COMP_DATE': self.COMP_DATE,
            'DRILL_TYPE': self.DRILL_TYPE,
            'OIL_BBL': self.OIL_BBL,
            'GAS_MCF': self.GAS_MCF,
            'WATER_BBL': self.WATER_BBL,
            'CONDENSATE_BBL': self.CONDENSATE_BBL,
            'PROD_DAYS': self.PROD_DAYS,
            'PROD_YEAR': self.PROD_YEAR,
            'ENTITY_TYPE': self.ENTITY_TYPE,
            'LATITUDE': self.LATITUDE, 
            'LONGITUDE': self.LONGITUDE
            }
        
        return str(data_)
    
# =========================================================

def integrate_production(
    gdf,
    starting_ids: int=0,
    category: str = None,
    fac_alias: str = "OIL_GAS_PROD",
    country: str = None,
    state_prov: str = None,
    src_ref_id: str = None,
    src_date: str = None,
    on_offshore: str = None,
    fac_name: str = None,
    fac_id: str = None,
    fac_type: str = None,
    spud_date: str = None,
    comp_date: str = None,
    drill_type: str = None,
    fac_status: str = None,
    op_name: str = None,
    oil_bbl: float = None,
    gas_mcf: float = None,
    water_bbl: float = None,
    condensate_bbl: float = None,
    prod_days: int = None,
    prod_year: int = None,
    entity_type: str = None,
    fac_latitude: float = None,
    fac_longitude: float = None
    ):
    
    """Integrate OGIM production data sourced from public sources
    
    Inputs:
    ---
        starting_ids:       starting OGIM_ID for this dataset
        category:           str, indicates one of the following O&G infra category:
                            (i) Oil and natural gas production
                            
        fac_alias:          "OIL_GAS_PROD", for defining data schema
        country:            str, name of the country
        state_prov:         str, name of state or province
        src_date:           str, date the data was last updated by the data owner
        src_ref_id:         str, a reference ID for the data source. Additional info is contained in a standalone reference table.
        fac_id:             str or float, depends on the specific format used by the data owner. For wells, this represents the American
                            Petroleum Institute number that identifies each unique well or the Universal Well Identifier
                            number, or well ID in the database.
        on_offshore:        str, indicates whether the facility is on or offshore
        op_name:            str, name of the operator
        fac_name:           str, name of the facility
        fac_status:         str, indicates the facility status as of the date the data was last updated
        fac_type:           str, for wells, indicates whether the facility is an oil, gas, or mixed oil and gas type
        spud_date:          Indicates when the well was spudded/commencement of drilling operations
        comp_date:          Indicates when the well was completed for routine production
        drill_type:         Indicates the drilling configuration for the well, e.g., vertical, horizontal, directional.
        oil_bbl:            oil produced per entity per year in barrels
        gas_mcf:            gas produced per entity per year in mcf
        water_bbl:          water produced per entity per year in barrels
        condensate_bbl:     condensate produced per entity per year in barrels
        prod_days:          number of production days per year
        prod_year:          year for which production data are reported
        entity_type:        type of production entity (e.g., well, well site, lease, field)
        fac_latitude:       float, latitude of the facility location, WGS 1984 (EPSG:4326)
        fac_longitude:      float, longitude of the facility location, WGS 1984 (EPSG: 4326)
        
    Returns:
    --------
      The new geodataframe, properly formatted with the different required attributes.
      
    """
    
    starting_ids = starting_ids # Specify the starting ID for this dataset
    all_facs_ = [] 
    attributes_ = [
        'OGIM_ID',
        'CATEGORY',
        'COUNTRY',
        'STATE_PROV',
        'SRC_REF_ID',
        'SRC_DATE',
        'ON_OFFSHORE',
        'FAC_NAME',
        'FAC_ID',
        'FAC_TYPE',
        'FAC_STATUS',
        'OPERATOR',
        'SPUD_DATE',
        'COMP_DATE',
        'DRILL_TYPE',
        'OIL_BBL',
        'GAS_MCF',
        'WATER_BBL',
        'CONDENSATE_BBL',
        'PROD_DAYS',
        'PROD_YEAR',
        'ENTITY_TYPE',
        'LATITUDE',
        'LONGITUDE',
        ]

    # GDF attributes
    # =========================================================
    category = category
    country  = country
    state_prov = state_prov
    src_ref_id = src_ref_id
    src_date = src_date
    on_offshore = on_offshore
    # =========================================================
    fac_name = fac_name
    fac_id = fac_id
    fac_type = fac_type
    spud_date = spud_date
    comp_date = comp_date
    drill_type = drill_type
    fac_status = fac_status
    op_name = op_name
    oil_bbl = oil_bbl
    gas_mcf = gas_mcf
    water_bbl = water_bbl
    condensate_bbl = condensate_bbl
    prod_days = prod_days
    prod_year = prod_year
    entity_type = entity_type
    fac_latitude = fac_latitude
    fac_longitude = fac_longitude

    # =========================================================
    error_logs_, error_log_desc = [], [] # For storing possible errors in data entries
    
    for idx_, row in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        # Specify attributes
        # CATEGORY
        try:
            category2 = row[category]
        except:
            category2 = category
        
        # SR_REF_ID
        try:
            src_ref_id2 = row[src_ref_id]
        except:
            src_ref_id2 = src_ref_id
        # COUNTRY
        try:
            country2 = row[country]
        except:
            country2 = country
        # STATE_PROV
        try:
            state_prov2 = row[state_prov]
        except:
            state_prov2 = state_prov
    
        # ON_OFFSHORE
        try:
            on_offshore2 = row[on_offshore]
        except:
            on_offshore2 = on_offshore
            
        # SOURCE_DATE
        try:
            src_date2 = row[src_date]
        except:
            src_date21 = src_date
            try:
                src_date22 = float(src_date21[0:4]) # If no error, then date entered properly
                src_date2 = src_date21
            except:
                raise KeyError("Invalid source date `src_date` field")
    
        # FACILITY NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_name2 = row[fac_name]
        except:
            if fac_name is not None:
                error_logs_.append(fac_name)
                error_log_desc.append("FAC_NAME")
                fac_name2 = NULL_STRING
            else:
                fac_name2 = NULL_STRING
        
        # FACILITY ID
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_id2 = row[fac_id]
        except:
            if fac_id is not None:
                error_logs_.append(fac_id)
                error_log_desc.append("FAC_ID")
                fac_id2 = NULL_STRING
            else:
                fac_id2 = NULL_STRING
        
        # FACILITY TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_type2 = row[fac_type]
        except:
            if fac_type is not None:
                error_logs_.append(fac_type)
                error_log_desc.append("FAC_TYPE")
                fac_type2 = NULL_STRING
            else:
                fac_type2 = NULL_STRING
                
        # DRILL TYPE
        try:
            drill_type2 = row[drill_type]
        except:
            if drill_type is not None:
                error_logs_.append(drill_type)
                error_log_desc.append("DRILL_TYPE")
                drill_type2 = NULL_STRING
            else:
                drill_type2 = NULL_STRING
        
        # SPUD DATE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            spud_date2 = row[spud_date]
        except:
            if spud_date is not None:
                error_logs_.append(spud_date)
                error_log_desc.append("SPUD_DATE")
                spud_date2 = NULL_DATE
            else:
                spud_date2 = NULL_DATE
                
        # COMPLETION DATE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            comp_date2 = row[comp_date]
        except:
            if comp_date is not None:
                error_logs_.append(comp_date)
                error_log_desc.append("COMP_DATE")
                comp_date2 = NULL_DATE
            else:
                comp_date2 = NULL_DATE
        
        # FACILITY STATUS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_status2 = row[fac_status]
        except:
            if fac_status is not None:
                error_logs_.append(fac_status)
                error_log_desc.append("FAC_STATUS")
                fac_status2 = NULL_STRING
            else:
                fac_status2 = NULL_STRING
        
        # OPERATOR NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            op_name2 = row[op_name]
        except:
            if op_name is not None:
                error_logs_.append(op_name)
                error_log_desc.append("OPERATOR")
                op_name2 = NULL_STRING
            else:
                op_name2 = NULL_STRING
            
        # OIL_BBL
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            oil_bbl2 = row[oil_bbl]
            oil_bbl2 = sig_figures(oil_bbl2, n=5)
        except:
            if oil_bbl is not None:
                error_logs_.append(oil_bbl)
                error_log_desc.append("OIL_BBL")
                oil_bbl2 = NULL_NUMERIC
            else:
                oil_bbl2 = NULL_NUMERIC
            
        # GAS_MCF
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            gas_mcf2 = row[gas_mcf]
            gas_mcf2 = sig_figures(gas_mcf2, n=5)
        except:
            if gas_mcf2 is not None:
                error_logs_.append(gas_mcf)
                error_log_desc.append("GAS_MCF")
                gas_mcf2 = NULL_NUMERIC
            else:
                gas_mcf2 = NULL_NUMERIC
            
        # WATER BBL
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            water_bbl2 = row[water_bbl]
            water_bbl2 = sig_figures(water_bbl2, n=5)
        except:
            if water_bbl is not None:
                error_logs_.append(water_bbl)
                error_log_desc.append("WATER_BBL")
                water_bbl2 = NULL_NUMERIC
            else:
                water_bbl2 = NULL_NUMERIC
            
        # CONDENSATE BBL
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            condensate_bbl2 = row[condensate_bbl]
            condensate_bbl2 = sig_figures(condensate_bbl2, n=5)
        except:
            if condensate_bbl is not None:
                error_logs_.append(condensate_bbl)
                error_log_desc.append("CONDENSATE_BBL")
                condensate_bbl2 = NULL_NUMERIC
            else:
                condensate_bbl2 = NULL_NUMERIC
                
        # PROD DAYS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            prod_days2 = row[prod_days]
        except:
            if prod_days is not None:
                error_logs_.append(prod_days)
                error_log_desc.append("PROD_DAYS")
                prod_days2 = prod_days
            else:
                prod_days2 = NULL_NUMERIC
                
        # PROD YEAR
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            prod_year2 = row[prod_year]
        except:
            if prod_year is not None:
                error_logs_.append(prod_year)
                error_log_desc.append("PROD_YEAR")
                prod_year2 = prod_year
            else:
                prod_year2 = NULL_NUMERIC
  
        # ENTITY TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            entity_type2 = row[entity_type]
        except:
            if entity_type is not None:
                error_logs_.append(entity_type)
                error_log_desc.append("ENTITY_TYPE")
                entity_type2 = entity_type
            else:
                entity_type2 = NULL_STRING
        
        # FAC LATITUDE
        try:
            fac_latitude2 = row[fac_latitude]
        except:
            fac_latitude2 = fac_latitude
        
        # FAC LONGITUDE
        try:
            fac_longitude2 = row[fac_longitude]
        except:
            fac_longitude2 = fac_longitude
        
        # =========================================================
        # Generate Well OBJECT for each well
    
        id_ = (starting_ids) + (idx_)
        
        ogim_fac = OGIMProduction(
            ogim_id=id_,
            category=category2,
            country=country2,
            state_prov=state_prov2,
            src_ref_id=src_ref_id2,
            src_date=src_date2,
            on_offshore=on_offshore2,
            fac_name=fac_name2,
            fac_id=fac_id2,
            fac_type=fac_type2,
            spud_date=spud_date2,
            comp_date=comp_date2,
            drill_type=drill_type2,
            fac_status=fac_status2,
            op_name=op_name2,
            oil_bbl=oil_bbl2,
            gas_mcf=gas_mcf2,
            condensate_bbl=condensate_bbl2,
            prod_days=prod_days2,
            prod_year=prod_year2,
            entity_type=entity_type2,
            fac_latitude=fac_latitude2,
            fac_longitude=fac_longitude2
            )
    
        # Build attributes 
        OGIM_ID = ogim_fac.OGIM_ID                          # int
        
        CATEGORY = ogim_fac.CATEGORY                        # str
        CATEGORY = CATEGORY.upper()
        
        COUNTRY = ogim_fac.COUNTRY                          # str
        COUNTRY = COUNTRY.upper()
        
        STATE_PROV = ogim_fac.STATE_PROV                    # str
        STATE_PROV = STATE_PROV.upper()
        
        SRC_REF_ID = ogim_fac.SRC_REF_ID                    # str
        SRC_REF_ID = SRC_REF_ID.upper()
        
        SRC_DATE = ogim_fac.SRC_DATE                        # str
        
        ON_OFFSHORE = ogim_fac.ON_OFFSHORE                  # str
        ON_OFFSHORE = ON_OFFSHORE.upper()
    
        FAC_NAME = ogim_fac.FAC_NAME                        # str
        # Standardize text styling
        FAC_NAME = FAC_NAME.upper()                         # str
    
        FAC_ID = ogim_fac.FAC_ID                            # str
        FAC_ID = FAC_ID.upper()
    
        FAC_TYPE = ogim_fac.FAC_TYPE                        # str
        # Standardize text styling
        FAC_TYPE = FAC_TYPE.upper()                         # str
        
        SPUD_DATE = ogim_fac.SPUD_DATE                      # str
        
        COMP_DATE = ogim_fac.COMP_DATE                      # str
        
        DRILL_TYPE = ogim_fac.DRILL_TYPE
        DRILL_TYPE = DRILL_TYPE.upper()
        
        FAC_STATUS = ogim_fac.FAC_STATUS                    # str
        FAC_STATUS = FAC_STATUS.upper()
    
        OPERATOR = ogim_fac.OPERATOR                        # str
        OPERATOR = OPERATOR.upper()
        
        ENTITY_TYPE = ogim_fac.ENTITY_TYPE                       # str
        ENTITY_TYPE = ENTITY_TYPE.upper()
        
        OIL_BBL = ogim_fac.OIL_BBL                          # float
        if np.isnan(OIL_BBL):
            OIL_BBL = NULL_NUMERIC
            
        GAS_MCF = ogim_fac.GAS_MCF                          # float
        if np.isnan(GAS_MCF):
            GAS_MCF = NULL_NUMERIC
            
        CONDENSATE_BBL = ogim_fac.CONDENSATE_BBL            # float
        if np.isnan(CONDENSATE_BBL):
            CONDENSATE_BBL = NULL_NUMERIC
            
        WATER_BBL = ogim_fac.WATER_BBL                      # float
        if np.isnan(WATER_BBL):
            WATER_BBL = NULL_NUMERIC
            
        PROD_DAYS = ogim_fac.PROD_DAYS                      # int
        if np.isnan(PROD_DAYS):
            PROD_DAYS = NULL_NUMERIC
            
        PROD_YEAR = ogim_fac.PROD_YEAR                      # int
        if np.isnan(PROD_YEAR):
            PROD_YEAR = NULL_NUMERIC
    
        LATITUDE = ogim_fac.LATITUDE                        # float
        LONGITUDE = ogim_fac.LONGITUDE                      # float
    
        # Format lat and lon to 5 decimal places
        LATITUDE = float(u"{:.5f}".format(LATITUDE))
        LONGITUDE = float(u"{:.5f}".format(LONGITUDE))
    
        # Create DataFrame
        
        df_ = pd.DataFrame([
            OGIM_ID, CATEGORY, COUNTRY, STATE_PROV, SRC_REF_ID, \
            SRC_DATE, ON_OFFSHORE, FAC_NAME, FAC_ID, FAC_TYPE, \
            FAC_STATUS, OPERATOR, SPUD_DATE, COMP_DATE, DRILL_TYPE,  \
            OIL_BBL, GAS_MCF, WATER_BBL, CONDENSATE_BBL, PROD_DAYS, \
            PROD_YEAR, ENTITY_TYPE, LATITUDE, LONGITUDE
            ], 
            index=attributes_
            ).T
    
        all_facs_.append(df_)
    # =========================================================
    # Concatenate facility data
    all_facs_df = pd.concat(all_facs_)
    
    # =========================================================
    # Create GeoDataFrame
    all_facs_gdf = gpd.GeoDataFrame(all_facs_df, geometry=gpd.points_from_xy(all_facs_df.LONGITUDE, all_facs_df.LATITUDE), crs="epsg:4326")
    
    # Reset index
    all_facs_gdf2 = all_facs_gdf.reset_index()
    all_facs_gdf3 = all_facs_gdf2.drop(columns=['index'])
    
    # Error logs
    error_logs2 = list(dict.fromkeys(error_logs_))
    error_logs_desc2 = list(dict.fromkeys(error_log_desc))
    
    if len(error_logs2) > 0: # and error_logs2 is not None:
        print ("*** There are possible errors in assigned attribute names! \n Please check error_logs *** \n =========== \n {} for attributes {}".format(error_logs2, error_logs_desc2))
        
    # Preview
    print(all_facs_gdf3.head())
    
    return all_facs_gdf3, error_logs2


# =========================================================
# Reading data from Microsoft Access database
# ===================================================
def read_msAccess(pathToFile, table_subset=None):
    """ Function for reading MS Access data

    Inputs:
    ---
        pathToFile: str
            Path to MS access file
        table_subset : list, optional
            List (length one or more) of table name(s) from the provided
            MS Access file that will be read into Pandas DataFrames. If no
            `table_subset` parameter is defined, then all tables present in
            the MS Access file will be read in by default.

    Returns:
    ---
        tableNamesIdx: Indices for names of tables in MS Access data
        tableNames: Names of tables in MS Access data
        dfs: a list of data for each table in MS Access data

    Dependencies:
    ---
        pyodbc (conda install -c anaconda pyodbc)
        pandas

    """
    # Create connection to file
    conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + pathToFile + ";")
    conn = pyodbc.connect(conn_str)

    # Find all tables in database
    cursor = conn.cursor()
    tableNames = []
    for i in cursor.tables(tableType='TABLE'):
        print("====================")
        print("+++ Table Names +++")
        print(i.table_name)
        tableNames.append(i.table_name)

    # If user provides a list of specific tables they want to be read,
    # re-define the list of tableNames that will be iterated over and loaded
    # into dataframes by this function
    if table_subset:
        if type(table_subset) == str:
            list_ = []
            table_subset = list_.append(table_subset)
        if type(table_subset) == list:
            tableNames = table_subset

    # Read data into pandas df
    dfs = []
    tableNamesIdx = []
    for idx in range(len(tableNames)):
        df = pd.read_sql('select * from ' + tableNames[idx], conn)
        dfs.append(df)
        tableNamesIdx.append(idx)

    return tableNamesIdx, tableNames, dfs


def get_msAccess_table_names(pathToFile):
    """Return the table name(s) within a Microsoft Access db file

    Inputs:
    ---
        pathToFile: Path to MS access file

    Returns:
    ---
        tableNames: Names of tables in MS Access data

    Dependencies:
    ---
        pyodbc (conda install -c anaconda pyodbc)
        pandas

    """
    # Create connection to file
    conn_str = (r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};'
                r'DBQ=' + pathToFile + ";")
    conn = pyodbc.connect(conn_str)

    # Find all tables in database
    cursor = conn.cursor()
    tableNames = []
    for i in cursor.tables(tableType='TABLE'):
        # print(i.table_name)
        tableNames.append(i.table_name)

    return tableNames


# =========================================================
def flatten_gdf_geometry(
    gdf: 'GeoDataFrame', 
    geom_type: 'shapely.geometry'
    ):
    
    """Flatten multi-geometry collection (MultiPoint, 'MultiLineString', 'MultiPolygon')
    
    Inputs:
    ---
        gdf: GeoDataFrame with multi-geometry type
        geom_type: Current 3D geometry type for the dataset: MultiPoint, MultiLineString, MultiPolygon
        
    Returns:
    ---
        new_gdf_geom: Same dataframe as gdf with flattened geometries
        
    """
    # Geometry
    geometry = gdf.geometry
    flattened_geometry = []

    flattened_gdf = gpd.GeoDataFrame()

    for geom in geometry:
        if geom.type in ['GeometryCollection', 'MultiPoint', 'MultiLineString', 'MultiPolygon']:
            for subgeom in geom:
                if subgeom.type==geom_type:
                    flattened_geometry.append(subgeom)
        else:
            if geom.type==geom_type:
                flattened_geometry.append(geom)
            else:
                flattened_geometry.append(geom)

    flattened_gdf.geometry=flattened_geometry
    
    # Then set gdf's geometry 
    new_gdf_geom = gdf.set_geometry(flattened_gdf.geometry)

    return new_gdf_geom

# =========================================================
# Dictionary of US state names and codes
dict_us_states = {
    'AL': 'ALABAMA',
    'AK': 'ALASKA',
    'AZ': 'ARIZONA',
    'AR': 'ARKANSAS',
    'CA': 'CALIFORNIA',
    'CO': 'COLORADO',
    'CT': 'CONNECTICUT',
    'DE': 'DELAWARE',
    'DC': 'DISTRICT OF COLUMBIA',
    'FL': 'FLORIDA',
    'GA': 'GEORGIA',
    'HI': 'HAWAII',
    'ID': 'IDAHO',
    'IL': 'ILLINOIS',
    'IN': 'INDIANA',
    'IA': 'IOWA',
    'KS': 'KANSAS',
    'KY': 'KENTUCKY',
    'LA': 'LOUISIANA',
    'ME': 'MAINE',
    'MD': 'MARYLAND',
    'MA': 'MASSACHUSETTS',
    'MI': 'MICHIGAN',
    'MN': 'MINNESOTA',
    'MS': 'MISSISSIPPI',
    'MO': 'MISSOURI',
    'MT': 'MONTANA',
    'NE': 'NEBRASKA',
    'NV': 'NEVADA',
    'NH': 'NEW HAMPSHIRE',
    'NJ': 'NEW JERSEY',
    'NM': 'NEW MEXICO',
    'NY': 'NEW YORK',
    'NC': 'NORTH CAROLINA',
    'ND': 'NORTH DAKOTA',
    'OH': 'OHIO',
    'OK': 'OKLAHOMA',
    'OR': 'OREGON',
    'PA': 'PENNSYLVANIA',
    'RI': 'RHODE ISLAND',
    'SC': 'SOUTH CAROLINA',
    'SD': 'NORTH DAKOTA',
    'TN': 'TENNESSEE',
    'TX': 'TEXAS',
    'UT': 'UTAH',
    'VT': 'VERMONT',
    'VA': 'VIRGINIA',
    'WA': 'WASHINGTON',
    'WV': 'WEST VIRGINIA',
    'WI': 'WISCONSIN',
    'WY': 'WYOMING'
    }

# Assigning on and offshore labels to data
# =========================================================

def assign_offshore_label_to_us_data(
    gdf: 'GeoDataFrame',
    offshore_boundary: 'GeoDataFrame'
    ):
    
    """Append onshore/offshore attribute to gdf based on whether record falls within or outside of `offshore_boundary`"""
    
    # Check CRS
    if gdf.crs != offshore_boundary.crs:
        print("CRS don't match! \n Results may not be accurate!")
    
    # Check that index is unique
    if gdf.index.is_unique == False:
        print("GeoDataFrame indices are not unique! \n Results may not be accurate!")
    
    # Points within boundary
    points_within_ = gpd.sjoin(gdf, offshore_boundary, op='within')
    points_within_['on_offshore'] = 'OFFSHORE'
    
    print("===========\n Total number of records within polygon = {}".format(points_within_.shape[0]))
    
    # Points outside of boundary
    points_outside_ = gdf[~gdf.index.isin(points_within_.index)]
    points_outside_['on_offshore'] = 'ONSHORE'
    
    # Merge the two datasets
    gdf2 = pd.concat([points_within_, points_outside_]).sort_index()
    
    # Display results
    print(gdf2.head())
    
    return gdf2

# =========================================================
def translate_espanol(
    translateFromList: bool = False,
    listToTranslate: list = None,
    gdf: 'GeoDataFrame' = None,
    attrName: 'GeoDataFrame attribute' = None,
    printTranslations: bool = True
    ):
    
    """ Return a dictionary for unique features in `attrName` or `listToTranslate` indicating ES-EN translations
    
    Dependencies: 
    ---
        googletrans (pip install googletrans)
        then: import googletrans
              from googletrans import Translator
    """
    
    if translateFromList == False:
        unique_ = list(gdf[attrName].unique())
        # Check if None in list or np.nan
        if None in unique_:
            print("Note: `None` in attrName`")
            unique_ = list(filter(None, unique_))
    else:
        unique_ = listToTranslate

    # Create translator object [requires googletrans library]
    translator = Translator()
    
    en_unique_ = [translator.translate(unique_[x], src='es', dest='en').text for x in range(len(unique_))]
    
    # Create dictionary
    en_unique2_ = dict(zip(unique_, en_unique_))
    
    if printTranslations == True:
        pprint.pprint(en_unique2_)
    
    return en_unique2_


# OGIM Class object 
class OGIMFlares(object):
    """Class object for OGIM flaring detected site based on VIIRS"""
    def __init__(self, 
        ogim_id=NULL_NUMERIC, 
        category=NULL_STRING, 
        country=NULL_STRING, 
        state_prov=NULL_STRING, 
        src_ref_id=NULL_STRING, 
        src_date=NULL_DATE,
        on_offshore=NULL_STRING,
        fac_name=NULL_STRING,
        fac_id=NULL_STRING,
        fac_type=NULL_STRING,
        fac_status=NULL_STRING,
        op_name=NULL_STRING,
        gas_flared_mmcf=NULL_NUMERIC,
        avg_temp=NULL_NUMERIC,
        days_clear_observs=NULL_NUMERIC,
        flare_year=NULL_NUMERIC,
        segment_type=NULL_STRING,
        fac_latitude=NULL_NUMERIC,
        fac_longitude=NULL_NUMERIC
        ):
        
        # Set data that should be string
        attrs_strings = {
            'CATEGORY': category,
            'COUNTRY': country,
            'STATE_PROV': state_prov,
            'ON_OFFSHORE': on_offshore,
            'SRC_REF_ID': src_ref_id,
            'FAC_NAME': fac_name,
            'FAC_ID': fac_id,
            'FAC_TYPE': fac_type,
            'FAC_STATUS': fac_status,
            'OPERATOR': op_name,
            'SEGMENT_TYPE': segment_type
            }
        
        for attribute, input_parameter in attrs_strings.items():
            if input_parameter is NULL_STRING:
                setattr(self, attribute, NULL_STRING)
            elif type(input_parameter) is str:
                setattr(self, attribute, input_parameter)
            elif type(input_parameter) is float or type(input_parameter) is int:
                setattr(self, attribute, str(input_parameter))
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_STRING)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_STRING)
                        
        # Set data that should be date [YYYY-MM-DD]
        attrs_dates = {
            'SRC_DATE': src_date
            }
        
        for attribute, input_parameter in attrs_dates.items():
            if input_parameter is NULL_DATE:
                setattr(self, attribute, NULL_DATE)
            elif type(input_parameter) is str: 
                setattr(self, attribute, input_parameter)
            else:
                if input_parameter is None or type(input_parameter) is None:
                    setattr(self, attribute, NULL_DATE)
                else:
                    try:
                        setattr(self, attribute, input_parameter.decode(UNICODE_ENCODING))
                    except:
                        setattr(self, attribute, NULL_DATE)
    
        # Set data for attributes that should be numeric
        attrs_numeric = {
            'OGIM_ID': ogim_id,
            'GAS_FLARED_MMCF': gas_flared_mmcf,
            'AVERAGE_FLARE_TEMP_K': avg_temp,
            'DAYS_CLEAR_OBSERVATIONS': days_clear_observs,
            'FLARE_YEAR': flare_year,
            'LATITUDE': fac_latitude,
            'LONGITUDE': fac_longitude
            }
        
        for attribute, input_parameter in attrs_numeric.items():
            if input_parameter is NULL_NUMERIC:
                setattr(self, attribute, NULL_NUMERIC)
            elif type(input_parameter) is float or type(input_parameter) is int:
                    setattr(self, attribute, input_parameter)
            else:
                if type(input_parameter) is None or input_parameter is None:
                    setattr(self, attribute, NULL_NUMERIC)
                else:
                    try:
                        setattr(self, attribute, float(input_parameter))  
                    except:
                        print("Error trying to create facility with parameter {0} for attribute {1}.".format(input_parameter, attribute))

    # OGIM facility representation
        
    def __repr__(self):
        """
        Representation of the O&G facility.
        """
        
        data_ = {
            'OGIM_ID': self.OGIM_ID, 
            'CATEGORY': self.CATEGORY, 
            'COUNTRY': self.COUNTRY, 
            'STATE_PROV': self.STATE_PROV, 
            'SRC_REF_ID': self.SRC_REF_ID, 
            'SRC_DATE': self.SRC_DATE, 
            'ON_OFFSHORE': self.ON_OFFSHORE,
            'FAC_NAME': self.FAC_NAME,
            'FAC_ID': self.FAC_ID, 
            'FAC_TYPE': self.FAC_TYPE, 
            'FAC_STATUS': self.FAC_STATUS, 
            'OPERATOR': self.OPERATOR, 
            'GAS_FLARED_MMCF': self.GAS_FLARED_MMCF,
            'AVERAGE_FLARE_TEMP_K': self.AVERAGE_FLARE_TEMP_K,
            'DAYS_CLEAR_OBSERVATIONS': self.DAYS_CLEAR_OBSERVATIONS,
            'FLARE_YEAR': self.FLARE_YEAR,
            'SEGMENT_TYPE': self.SEGMENT_TYPE,
            'LATITUDE': self.LATITUDE, 
            'LONGITUDE': self.LONGITUDE
            }
        
        return str(data_)
    
# =========================================================

def integrate_flares(
    gdf,
    starting_ids: int=0,
    category: str = None,
    fac_alias: str = "FLARING",
    country: str = None,
    state_prov: str = None,
    src_ref_id: str = None,
    src_date: str = None,
    on_offshore: str = None,
    fac_name: str = None,
    fac_id: str = None,
    fac_type: str = None,
    fac_status: str = None,
    op_name: str = None,
    gas_flared_mmcf: float = None,
    avg_temp: float = None,
    days_clear_observs: int = None,
    flare_year: int = None,
    segment_type: str = None,
    fac_latitude: float = None,
    fac_longitude: float = None
    ):
    
    """Integrate OGIM flaring detections based on VIIRS dataset from EOG
    
    Inputs:
    ---
        starting_ids:       starting OGIM_ID for this dataset
        category:           str, indicates one of the following O&G infra category:
                            (i) Natural gas flaring detections
                            
        fac_alias:          "FLARING", for defining data schema
        country:            str, name of the country
        state_prov:         str, name of state or province
        src_date:           str, date the data was last updated by the data owner
        src_ref_id:         str, a reference ID for the data source. Additional info is contained in a standalone reference table.
        fac_id:             str or float, depends on the specific format used by the data owner. For wells, this represents the American
                            Petroleum Institute number that identifies each unique well or the Universal Well Identifier
                            number, or well ID in the database.
        on_offshore:        str, indicates whether the facility is on or offshore
        op_name:            str, name of the operator
        fac_name:           str, name of the facility
        fac_status:         str, indicates the facility status as of the date the data was last updated
        fac_type:           str, for wells, indicates whether the facility is an oil, gas, or mixed oil and gas type
        gas_flared_mmcf:           Estimated gas flared per year in MMCF
        avg_temp:           Average temperature of the flare in Kelvin
        days_clear_observs:          number of reported clear days of VIIRS observations
        flare_year:          year for which flaring data are reported
        segment_type:        oil and gas industry segment for this detection
        fac_latitude:       float, latitude of the facility location, WGS 1984 (EPSG:4326)
        fac_longitude:      float, longitude of the facility location, WGS 1984 (EPSG: 4326)
        
    Returns:
    --------
      The new geodataframe, properly formatted with the different required attributes.
      
    """
    
    starting_ids = starting_ids # Specify the starting ID for this dataset
    all_facs_ = [] 
    attributes_ = [
        'OGIM_ID',
        'CATEGORY',
        'COUNTRY',
        'STATE_PROV',
        'SRC_REF_ID',
        'SRC_DATE',
        'ON_OFFSHORE',
        'FAC_NAME',
        'FAC_ID',
        'FAC_TYPE',
        'FAC_STATUS',
        'OPERATOR',
        'GAS_FLARED_MMCF',
        'AVERAGE_FLARE_TEMP_K',
        'DAYS_CLEAR_OBSERVATIONS',
        'FLARE_YEAR',
        'SEGMENT_TYPE',
        'LATITUDE',
        'LONGITUDE',
        ]

    # GDF attributes
    # =========================================================
    category = category
    country  = country
    state_prov = state_prov
    src_ref_id = src_ref_id
    src_date = src_date
    on_offshore = on_offshore
    # =========================================================
    fac_name = fac_name
    fac_id = fac_id
    fac_type = fac_type
    fac_status = fac_status
    op_name = op_name
    gas_flared_mmcf = gas_flared_mmcf
    avg_temp = avg_temp
    days_clear_observs = days_clear_observs
    flare_year = flare_year
    segment_type = segment_type
    fac_latitude = fac_latitude
    fac_longitude = fac_longitude

    # =========================================================
    error_logs_, error_log_desc = [], [] # For storing possible errors in data entries
    
    for idx_, row in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        # Specify attributes
        # CATEGORY
        try:
            category2 = row[category]
        except:
            category2 = category
        
        # SR_REF_ID
        try:
            src_ref_id2 = row[src_ref_id]
        except:
            src_ref_id2 = src_ref_id
        # COUNTRY
        try:
            country2 = row[country]
        except:
            country2 = country
        # STATE_PROV
        try:
            state_prov2 = row[state_prov]
        except:
            state_prov2 = state_prov
    
        # ON_OFFSHORE
        try:
            on_offshore2 = row[on_offshore]
        except:
            on_offshore2 = on_offshore
            
        # SOURCE_DATE
        try:
            src_date2 = row[src_date]
        except:
            src_date21 = src_date
            try:
                src_date22 = float(src_date21[0:4]) # If no error, then date entered properly
                src_date2 = src_date21
            except:
                raise KeyError("Invalid source date `src_date` field")
    
        # FACILITY NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_name2 = row[fac_name]
        except:
            if fac_name is not None:
                error_logs_.append(fac_name)
                error_log_desc.append("FAC_NAME")
                fac_name2 = NULL_STRING
            else:
                fac_name2 = NULL_STRING
        
        # FACILITY ID
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_id2 = row[fac_id]
        except:
            if fac_id is not None:
                error_logs_.append(fac_id)
                error_log_desc.append("FAC_ID")
                fac_id2 = NULL_STRING
            else:
                fac_id2 = NULL_STRING
        
        # FACILITY TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_type2 = row[fac_type]
        except:
            if fac_type is not None:
                error_logs_.append(fac_type)
                error_log_desc.append("FAC_TYPE")
                fac_type2 = NULL_STRING
            else:
                fac_type2 = NULL_STRING
        
        # FACILITY STATUS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            fac_status2 = row[fac_status]
        except:
            if fac_status is not None:
                error_logs_.append(fac_status)
                error_log_desc.append("FAC_STATUS")
                fac_status2 = NULL_STRING
            else:
                fac_status2 = NULL_STRING
        
        # OPERATOR NAME
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            op_name2 = row[op_name]
        except:
            if op_name is not None:
                error_logs_.append(op_name)
                error_log_desc.append("OPERATOR")
                op_name2 = NULL_STRING
            else:
                op_name2 = NULL_STRING
            
        # GAS_FLARED_MMCF
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            gas_flared_mmcf2 = row[gas_flared_mmcf]
            gas_flared_mmcf2 = sig_figures(gas_flared_mmcf2)
        except:
            if gas_flared_mmcf is not None:
                error_logs_.append(gas_flared_mmcf)
                error_log_desc.append("GAS_FLARED_MMCF")
                gas_flared_mmcf2 = NULL_NUMERIC
            else:
                gas_flared_mmcf2 = NULL_NUMERIC
                
        # AVERAGE FLARE TEMPERATURE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            avg_temp2 = row[avg_temp]
            avg_temp2 = sig_figures(avg_temp2)
        except:
            if avg_temp is not None:
                error_logs_.append(avg_temp)
                error_log_desc.append("AVERAGE_FLARE_TEMP_K")
                avg_temp2 = NULL_NUMERIC
            else:
                avg_temp2 = NULL_NUMERIC
            
        # DAYS OF CLEAR OBSERVATIONS
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            days_clear_observs2 = row[days_clear_observs]
        except:
            if days_clear_observs is not None:
                error_logs_.append(days_clear_observs)
                error_log_desc.append("DAYS_CLEAR_OBSERVATIONS")
                days_clear_observs2 = NULL_NUMERIC
            else:
                days_clear_observs2 = NULL_NUMERIC
                
        # flare YEAR
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            flare_year2 = row[flare_year]
        except:
            if flare_year is not None:
                error_logs_.append(flare_year)
                error_log_desc.append("FLARE_YEAR")
                flare_year2 = flare_year
            else:
                flare_year2 = NULL_NUMERIC
  
        # SEGMENT TYPE
        # Attribute must come from the dataset, otherwise use NULL VALUE
        try:
            segment_type2 = row[segment_type]
        except:
            if segment_type is not None:
                error_logs_.append(segment_type)
                error_log_desc.append("SEGMENT_TYPE")
                segment_type2 = segment_type
            else:
                segment_type2 = NULL_STRING
        
        # FAC LATITUDE
        try:
            fac_latitude2 = row[fac_latitude]
        except:
            fac_latitude2 = fac_latitude
        
        # FAC LONGITUDE
        try:
            fac_longitude2 = row[fac_longitude]
        except:
            fac_longitude2 = fac_longitude
        
        # =========================================================
        # Generate OBJECT for each facility
    
        id_ = (starting_ids) + (idx_)
        
        ogim_fac = OGIMFlares(
            ogim_id=id_,
            category=category2,
            country=country2,
            state_prov=state_prov2,
            src_ref_id=src_ref_id2,
            src_date=src_date2,
            on_offshore=on_offshore2,
            fac_name=fac_name2,
            fac_id=fac_id2,
            fac_type=fac_type2,
            fac_status=fac_status2,
            op_name=op_name2,
            gas_flared_mmcf=gas_flared_mmcf2,
            days_clear_observs=days_clear_observs2,
            avg_temp=avg_temp2,
            flare_year=flare_year2,
            segment_type=segment_type2,
            fac_latitude=fac_latitude2,
            fac_longitude=fac_longitude2
            )
    
        # Build attributes 
        OGIM_ID = ogim_fac.OGIM_ID                          # int
        
        CATEGORY = ogim_fac.CATEGORY                        # str
        CATEGORY = CATEGORY.upper()
        
        COUNTRY = ogim_fac.COUNTRY                          # str
        COUNTRY = COUNTRY.upper()
        
        STATE_PROV = ogim_fac.STATE_PROV                    # str
        STATE_PROV = STATE_PROV.upper()
        
        SRC_REF_ID = ogim_fac.SRC_REF_ID                    # str
        SRC_REF_ID = SRC_REF_ID.upper()
        
        SRC_DATE = ogim_fac.SRC_DATE                        # str
        
        ON_OFFSHORE = ogim_fac.ON_OFFSHORE                  # str
        ON_OFFSHORE = ON_OFFSHORE.upper()
    
        FAC_NAME = ogim_fac.FAC_NAME                        # str
        # Standardize text styling
        FAC_NAME = FAC_NAME.upper()                         # str
    
        FAC_ID = ogim_fac.FAC_ID                            # str
        FAC_ID = FAC_ID.upper()
    
        FAC_TYPE = ogim_fac.FAC_TYPE                        # str
        # Standardize text styling
        FAC_TYPE = FAC_TYPE.upper()                         # str
        
        FAC_STATUS = ogim_fac.FAC_STATUS                    # str
        FAC_STATUS = FAC_STATUS.upper()
    
        OPERATOR = ogim_fac.OPERATOR                        # str
        OPERATOR = OPERATOR.upper()
        
        SEGMENT_TYPE = ogim_fac.SEGMENT_TYPE                       # str
        SEGMENT_TYPE = SEGMENT_TYPE.upper()
        
        GAS_FLARED_MMCF = ogim_fac.GAS_FLARED_MMCF                          # float
        if np.isnan(GAS_FLARED_MMCF):
            GAS_FLARED_MMCF = NULL_NUMERIC
            
        AVERAGE_FLARE_TEMP_K = ogim_fac.AVERAGE_FLARE_TEMP_K                          # float
        if np.isnan(AVERAGE_FLARE_TEMP_K):
            AVERAGE_FLARE_TEMP_K = NULL_NUMERIC

        DAYS_CLEAR_OBSERVATIONS = ogim_fac.DAYS_CLEAR_OBSERVATIONS                      # int
        if np.isnan(DAYS_CLEAR_OBSERVATIONS):
            DAYS_CLEAR_OBSERVATIONS = NULL_NUMERIC
            
        FLARE_YEAR = ogim_fac.FLARE_YEAR                      # int
        if np.isnan(FLARE_YEAR):
            FLARE_YEAR = NULL_NUMERIC
    
        LATITUDE = ogim_fac.LATITUDE                        # float
        LONGITUDE = ogim_fac.LONGITUDE                      # float
    
        # Format lat and lon to 5 decimal places
        LATITUDE = float(u"{:.5f}".format(LATITUDE))
        LONGITUDE = float(u"{:.5f}".format(LONGITUDE))
    
        # Create DataFrame
        df_ = pd.DataFrame([
            OGIM_ID, CATEGORY, COUNTRY, STATE_PROV, SRC_REF_ID, \
            SRC_DATE, ON_OFFSHORE, FAC_NAME, FAC_ID, FAC_TYPE, \
            FAC_STATUS, OPERATOR, GAS_FLARED_MMCF, AVERAGE_FLARE_TEMP_K, DAYS_CLEAR_OBSERVATIONS, \
            FLARE_YEAR, SEGMENT_TYPE, LATITUDE, LONGITUDE
            ], 
            index=attributes_
            ).T
    
        all_facs_.append(df_)
    # =========================================================
    # Concatenate facility data
    all_facs_df = pd.concat(all_facs_)
    
    # =========================================================
    # Create GeoDataFrame
    all_facs_gdf = gpd.GeoDataFrame(all_facs_df, geometry=gpd.points_from_xy(all_facs_df.LONGITUDE, all_facs_df.LATITUDE), crs="epsg:4326")
    
    # Reset index
    all_facs_gdf2 = all_facs_gdf.reset_index()
    all_facs_gdf3 = all_facs_gdf2.drop(columns=['index'])
    
    # Error logs
    error_logs2 = list(dict.fromkeys(error_logs_))
    error_logs_desc2 = list(dict.fromkeys(error_log_desc))
    
    if len(error_logs2) > 0: # and error_logs2 is not None:
        print ("*** There are possible errors in assigned attribute names! \n Please check error_logs *** \n =========== \n {} for attributes {}".format(error_logs2, error_logs_desc2))
        
    # Preview
    print(all_facs_gdf3.head())
    
    return all_facs_gdf3, error_logs2


# Standardize HIFLD date fields
def standardize_dates_hifld_us(
    gdf: 'GeoDataFrame',
    attrName: 'str',
    newAttrName: 'str'
    ):
    
    """Fix date formats for HIFLD datasets for US
    
    Inputs:
    ---
        gdf: GeoDataFrame
        attrName: attribute name for date field to be standardized
        newAttrName: new attribute name for standardized dates
        
    Returns:
    ---
        gdf: new dataframe with standardized date field in "newAttrName" field
    
    """
    dates2_ = []

    for idx1_, row1_ in tqdm(gdf.iterrows(), total=gdf.shape[0]):
        # Date
        ddates = row1_[attrName]
        try:
            dates1_ = ddates.split("T")[0]
            if dates1_ == "1901-01-01" or dates1_ == "NOT AVAILABLE":
                dates11_ = "1900-01-01"
                dates2_.append(dates11_)
            else:
                dates2_.append(dates1_)
        except:
            dates2_.append("1900-01-01")
            
    # Append results to a new column
    gdf[newAttrName] = dates2_
    
    return gdf

# =========================================================
# Strip unnecessary Z-coordinates from point geometries
# =========================================================

def strip_z_coord(gdf: 'GeoDataFrame'):
    '''
    This function reads each record in a geodataframe and, if a record's 
    geometry is 'POINT Z' type, the Z part of the coordinate is dropped. 
    Use this function to ensure a gdf's geometries do not mix types
    POINT and POINT Z before exporting to a shapefile, geopackage, etc.
    
    *NOTE* that this function has only been tested on POINT type geometries
    TO-DO: extend this function to work with line and polygon geometries.

    Parameters
    ----------
    gdf : Geopandas geodataframe with a geometry column containing at least 
        one POINT Z type geometry.

    Returns
    -------
    new_gdf :  Same as original gdf, but with altered geometry column 
        (all points are reduced to POINT type with only XY values)

    '''
    c = 0  #counter variable
    # Get series of geoms to iterate over
    current_geometry = gdf.geometry
    # Create empty list to append processed geometries to
    new_geometry = []
    # Create temporary empty gdf (will only contain new geometries)
    new_gdf = gpd.GeoDataFrame()

    for geom in current_geometry:
        if geom.has_z:
            c = c+1 
            # remove the Z coordinate
            geom_noz = shapely.ops.transform(lambda x, y, z: (x, y), geom) 
            new_geometry.append(geom_noz)
        else:
            # keep original XY coordinate if no editing is needed
            new_geometry.append(geom)
    
    # Create geometry column in our temp gdf based on our list of processed geoms
    new_gdf.geometry = new_geometry

    # Swap the geometry column in the input gdf for the one created in this function
    output_gdf = gdf.set_geometry(new_gdf.geometry)
    print('Total number of geometries in gdf: '+str(len(gdf)))
    print('Number of z-coordinates dropped: '+str(c))
    return output_gdf


def map_urls_to_ids(dataURLColumn, sourceTableIDColumn, sourceTableURLColumn):
    '''
    This function replaces a column of source URLs in an OGIM database layer 
    with each URL's numeric ID, so that the record can be later matched to 
    its source details in the standalone source table.

    Parameters
    ----------
    dataURLColumn : Column in a OGIM dataframe which contains Source URLs (dtype = string)
    sourceTableIDColumn : Column in the standalone data source table which contains Source Reference IDs (dtype = integer)
    sourceTableURLColumn : Column in the standalone data source table which contains Source URLs (dtype = string)

    Returns
    -------
    SRC_ID_NEW : A data series (whcih can be assigned to a column in a dataframe)
        DESCRIPTION.
        
    Example usage which returns source reference IDs into a new column
    -------
    df['SRC_ID_NEW'] = map_urls_to_ids(df.SRC_URL, sourcetable.SRC_ID, sourcetable.SRC_URL)

    '''
    # TODO as of July 22 2022- strip excess characters from the URL strings that will prevent a perfect match
    # for example, 'https' or trailing '/'

    # create dictionary that relates source URLs to their ID number
    dict_url2id = dict(zip(sourceTableURLColumn, sourceTableIDColumn))
    # create a data series where all dataframe URLs are replaced with their corresponding ID number from the standalone table
    # if a corresponding ID number isn't found, the original URL is retained.
    src_id_new = dataURLColumn.map(dict_url2id).fillna(dataURLColumn)


    failedURLlist = list(src_id_new[dataURLColumn.isin(src_id_new)].unique())
    if failedURLlist:
        print('--------------------------------------------------------------')
        print('The following URLs were NOT successfully mapped to an ID number. Check that the URL strings in the dataframe exactly match the URLS in the source table. \n')
        print(*failedURLlist, sep="\n")
        print('--------------------------------------------------------------')
    else:
        print('--------------------------------------------------------------')
        print('All URLs were successfully mapped to an ID number! \n') 
        print('--------------------------------------------------------------')
    
    return src_id_new


def clean_a_date_field(df, fieldname):
    """Cleans and edits a date column of a dataframe in-place.
    Primarily used during data integration.
    
    Parameters
    ----------
    df : Dataframe or GeoDataFrame
    fieldname: string
        name of `df` column containing date information. 
        
    Returns
    -------
    None
    
    Example usage:
        clean_a_date_field(colorado_wells, 'Spud_Date')
    """
    # First, cut away any HH:MM:SS suffixes present in date fields from the
    # original data, while keeping the values in string format
    df[fieldname] = pd.to_datetime(df[fieldname]).dt.strftime("%Y-%m-%d")

    # Next, find and replace any string-dates with none-like values
    df[fieldname] = df[fieldname].replace({None: "1900-01-01",
                                           np.nan: "1900-01-01",
                                           "1901-01-01": "1900-01-01",
                                           "1800-01-01": "1900-01-01"})

    # Convert entire string-date column to datetime format
    df[fieldname] = pd.to_datetime(df[fieldname], format="%Y-%m-%d")

    # Replace dates in the future (i.e beyond "today") with a NULL value
    today = pd.Timestamp.today().floor('d')
    df.loc[df[fieldname] > today, fieldname] = "1900-01-01"

    # Convert datetime column back to string column format
    df[fieldname] = df[fieldname].astype(str)

    # Print the min and max non-null dates in the column of interest
    df_ = df[df[fieldname] != "1900-01-01"]
    print(f'Column name: {fieldname}')
    print('Minimum and maximum non-null dates, after cleaning')
    print(min(df_[fieldname]))
    print(max(df_[fieldname]))
    print('\n')


def replace_missing_strings_with_na(gdf,
                                    columns2check,
                                    limit_acceptable_columns=False):
    """Replace none-like string values in gdf columns with a standard missing data marker in-place.

    Parameters
    ----------
    gdf : GeoPandas GeoDataFrame
        DESCRIPTION
    columns2check: list of strings
        list of column names in `gdf` that contain string values

    Returns
    -------
    The input `gdf` with none-like string values replaced.

    """
    possible_missing_values = ['CALLE SIN NOMBRE',
                               'NOT AVAILABLE',
                               'NOT APPLICABLE',
                               'NA',
                               'NAN',
                               'UNKNOWN',
                               'UNAVAILABLE',
                               'UNCLASSIFIED',
                               'UNDEFINED',
                               'UNDESIGNATED',
                               'UNNAMED',
                               'UNSPECIFIED',
                               'NO DATA',
                               'NONE',
                               'NONE_SPECIFIED',
                               'SIN DATOS',
                               'SIN NOMBRE',
                               'VIRTUAL/BAD DATA',
                               '?',
                               'UNTITLED PLACEMARK',
                               'UNKNOWN PLATFORM',
                               'UNKNOWN/UN-CODED',
                               '#N/A',
                               '<NULL>',
                               '',
                               ' ',
                               '  ',
                               '   ',
                               None]

    acceptable_columns = ['COUNTRY',
                          'STATE_PROV',
                          'ON_OFFSHORE',
                          'FAC_NAME',
                          'FAC_ID',
                          'FAC_TYPE',
                          'FAC_STATUS',
                          'OGIM_STATUS',
                          'OPERATOR',
                          'COMMODITY',
                          'DRILL_TYPE',
                          'PIPE_MATERIAL'
                          ]

    for column in columns2check:

        if column in gdf.columns:

            if limit_acceptable_columns==True and column not in acceptable_columns:
                continue

            else:
                gdf[column] = gdf[column].astype(str)
                gdf[column] = gdf[column].str.upper()
                gdf[column] = gdf[column].replace(possible_missing_values, NULL_STRING)

    return gdf


def create_concatenated_well_name(df,
                                  wellnamecol,
                                  wellnumbercol,
                                  outputcolname):
    """Create a new dataframe attribute in-place where well names are joined
    with well number to create a more complete well name. Primarily for use
    during data integration.

    Returns
    -------
    None.

    Example usage:
        create_concatenated_well_name(west_virginia_wells_gdf,
                                              'WELL_FARM_NAME',
                                              'WELL_NUMBER',
                                              'well_name_new')
    """
    # Concatenate well name and well number to create a more complete Well Name
    df[wellnamecol] = df[wellnamecol].astype(str)
    df[wellnumbercol] = df[wellnumbercol].astype(str)

    df.loc[(df[wellnamecol] != 'N/A') & (df[wellnumbercol] != 'N/A'), outputcolname] = df[wellnamecol] + ' - ' + df[wellnumbercol]
    df.loc[(df[wellnamecol] == 'N/A') & (df[wellnumbercol] != 'N/A'), outputcolname] = df[wellnumbercol]
    df.loc[(df[wellnamecol] != 'N/A') & (df[wellnumbercol] == 'N/A'), outputcolname] = df[wellnamecol]
    df.loc[(df[wellnamecol] == 'N/A') & (df[wellnumbercol] == 'N/A'), outputcolname] = 'N/A'


def get_duplicate_api_records(df, uniqueidfield):
    """Create a new df containing any records from the input df with a
    duplicate API value. Also, print the number of duplicate records.

    Parameters
    ----------
    df : Pandas GeoDataFrame
    uniqueidfield : string
        name of field containing API values.

    Returns
    -------
    x : Pandas GeoDataFrame
        dataframe containing a subset of records from `df`, specifically those
        records that have a non-unique API value. If no records in `df` have a
        duplicate API value, then x is an empty object.

    Example usage
    -------
        dupes = get_duplicate_api_records(alabama_wells, 'API')
    """
    df = df[df[uniqueidfield].notna()]
    df = df[df[uniqueidfield] != 'N/A']
    x = df[df.duplicated(subset=uniqueidfield, keep=False)]
    print('Number of duplicate records: ' + str(len(x)))
    print('Number of unique values: ' + str(len(x[uniqueidfield].unique())))

    if len(x) == 0:
        return None
    else:
        return x


def deduplicate_with_rounded_geoms(gdf, column_subset, desired_precision=5):
    '''Remove duplicate records in a GDF based on attributes and geometries, with a specified precision.
    * NOTE *
    This function has only been tested on single (not multi) Point geometries.

    Parameters
    ----------
    gdf : GeoDataFrame
        Dataset containing potential duplicate records you want to remove.
    column_subset : list
        List of attribute columns within `gdf` which are used to identify
        duplicate records.
    desired_precision : int, optional
        How many decimal places in the `geometry` column of `gdf` do you want
        to be considered when determining whether two records have the same
        geometry? The default is 5.

    Returns
    -------
    gdf_out : TYPE
        DESCRIPTION.

    Example
    -------
    argentina_facs = gpd.read_file('path//to//shapefile.shp')
    column_subset = ['NPC', 'TIPO', 'EMPRESA_IN', 'DESCPC']
    argentina_facs_deduped = deduplicate_with_rounded_geoms(gdf,
                                                            column_subset,
                                                            desired_precision=5)

    '''

    if 'geometry' in column_subset:
        column_subset.remove('geometry')
    if 'latitude_calc' in column_subset:
        column_subset.remove('latitude_calc')
    if 'longitude_calc' in column_subset:
        column_subset.remove('longitude_calc')

    if not isinstance(desired_precision, int):
        # print('desired precision is not an integer')
        desired_precision = desired_precision.astype(int)
    # else:
        # print('desired precision YES IS an integer')

    gdf["x"] = round(gdf.geometry.x, desired_precision)
    gdf["y"] = round(gdf.geometry.y, desired_precision)

    column_subset.extend(['x', 'y'])

    gdf_out = gdf.drop_duplicates(subset=column_subset, keep='first')

    gdf_out.drop(['x', 'y'], axis=1, inplace=True)

    return gdf_out


def check_df_for_allowed_nans(df):
    '''Check an OGIM dataframe for missing values in attributes that don't allow them.

    This function iterates through each attribute column of a (geo)dataframe,
    and depending on whether that column allows null values under our OGIM
    schema, prints a warning to the user if any null values appear in that
    column. It is incumbent upon the user to fill any erroneously missing
    values with the proper information; this function will not do that for you.

    The dictionary within this function, defining which attribute columns allow
    null values, is based upon the README material for OGIM v2.4 (March 2024).
    As the OGIM schema is updated in the future, the dictionary in this
    function should be updated as well.

    Parameters
    ----------
    df : Dataframe or GeoDataframe
        Should contain all columns present in our OGIM schema that are suitable
        for that infrastructure type (or the Data Catalog)

    Returns
    -------
    None. This function only prints statements.

    Example usage and output:
    -------
    gdf = gpd.read_file('OGIM_v2.4.gpkg', layer='Oil_and_Natural_Gas_Basins')
    check_df_for_allowed_nans(gdf)

    >> ! WARNING ! 2 missing value(s) found in the SRC_REF_ID column
    >>     OGIM_ID                           COUNTRY STATE_PROV SRC_REF_ID
    >>694  2065805  CANADA, UNITED STATES OF AMERICA        N/A        N/A
    >>695  2065806  CANADA, UNITED STATES OF AMERICA        N/A        N/A

    '''
    allow_nulls_dict = {
        'AREA_KM2': False,
        'AVERAGE_FLARE_TEMP_K': False,
        'CATEGORY': False,
        'COMMODITY': True,
        'COMP_DATE': True,
        'COUNTRY': False,  # should not be null in infra layers, ensure catalog has values
        'DAYS_CLEAR_OBSERVATIONS': False,
        'DOWNLOAD_INSTRUCTIONS': True,
        'DRILL_TYPE': True,
        'FAC_ID': True,
        'FAC_NAME': True,
        'FAC_STATUS': True,
        'FAC_TYPE': True,
        'FILE_NAME': True,
        'FLARE_YEAR': False,
        'GAS_CAPACITY_MMCFD': True,
        'GAS_FLARED_MMCF': False,
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
        'ON_OFFSHORE': False,
        'OPERATOR': True,
        'ORIGINAL_CRS': True,
        'PIPE_DIAMETER_MM': True,
        'PIPE_LENGTH_KM': False,
        'PIPE_MATERIAL': True,
        'PROD_DATA': False,
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
        'UPDATE_FREQ': False
    }

    for attr in df.columns:
        # Confirm that the df column is present in our "allow nulls or not" dictionary
        # If it is, then proceed
        if attr not in allow_nulls_dict:
            print(f'{attr} -- error, column name not in dictionary')
        else:
            # If the specified column does not allow null values, then check for them.
            # If any null values are present, print this as a warning to the user
            if not allow_nulls_dict.get(attr):
                if df[attr].isin([np.nan, 'N/A']).any():
                    n = df[attr].isin([np.nan, 'N/A']).value_counts()[True]
                    print(f'! WARNING ! {n} missing value(s) found in the {attr} column')
                    try:
                        print(df[df[attr].isin([np.nan, 'N/A'])][['OGIM_ID',
                                                                  'COUNTRY',
                                                                  'STATE_PROV',
                                                                  'SRC_REF_ID']])
                    except KeyError:
                        print(df[df[attr].isin([np.nan, 'N/A'])][['SRC_ID',
                                                                  'COUNTRY',
                                                                  'STATE_PROV']])


def format_data_catalog(catalog):
    '''Reformat certain raw inputs after importing the OGIM_Data_Catalog.xlsx

    Parameters
    ----------
    catalog : Pandas DataFrame
        OGIM Data Catalog, read in from the master Excel file.

    Returns
    -------
    catalog : Pandas DataFrame
        Same content as input catalog, with some data type alterations and
        capitalization changes.

    Example usage
    -------
    fp = "Public_Data\\data\\OGIM_Data_Catalog.xlsx"
    catalog = pd.read_excel(fp, sheet_name='source_table')
    catalog = format_data_catalog(catalog)

    '''
    # If there are any rows with a null SRC_ID still in the catalog, drop the row
    catalog.drop(catalog[catalog.SRC_ID.isna()].index, inplace=True)

    # Change SRC_ID values to strings with no decimal information
    catalog['SRC_ID'] = catalog['SRC_ID'].map('{:,.0f}'.format)

    # Format date column to strip away timestamps
    catalog['LASTVISIT'] = pd.to_datetime(catalog['LASTVISIT']).dt.strftime("%Y-%m-%d")

    # Specify which (string) columns to capitalize in the catalog, and do so.
    # Afterwards, fill cells with the string 'NAN' with a np.nan value instead.
    catalog_stringcols = ['SRC_NAME',
                          'SRC_ALIAS',
                          'SRC_TYPE',
                          'PUB_PRIV',
                          'UPDATE_FREQ',
                          'LASTVISIT',
                          'REGION',
                          'COUNTRY',
                          'STATE_PROV',
                          'FAC_CATEGORY',
                          'NOTES'
                          ]
    for attr in catalog_stringcols:
        catalog[attr] = [str(catalog[attr].iloc[x]).upper() for x in range(catalog.shape[0])]
    catalog = catalog.replace('NAN', np.nan)

    return catalog


def _get_src_date_from_single_ref_id(src_ref_id, catalog_ix):
    '''Use a (singular) SRC_REF_ID value to look up and get a SRC_DATE from the Data Catalog

    Parameters
    ----------
    src_ref_id : str
        A valid SRC_REF_ID value that is present in the Data Catalog. Must be
        in the format of a single number/source, e.g., '82' or '114'.

    catalog_ix : Pandas DataFrame
        The up-to-date OGIM Data Catalog table, with the 'SRC_ID' column
        specified as the index of the dataframe.

    Returns
    -------
    out_src_date : str
        The SRC_DATE (in 'YYYY-MM-DD' format) that corresponds with the
        SRC_REF_ID provided by the user, from the Data Catalog table.

    '''
    # Check that user-provided SRC_REF_ID is a string; if not, cast it.
    if type(src_ref_id) != str:
        src_ref_id = src_ref_id.astype(str)

    # Confirm that the 'SRC_ID' column is being used as the Data Catalog's
    # index. If not, set it.
    if catalog_ix.index.name != 'SRC_ID':
        catalog_ix = catalog_ix.copy().set_index('SRC_ID')

    # Error handling if the SRC_ID input isn't present in data catalog
    if src_ref_id not in catalog_ix.index:
        print(f'ERROR: user-provided SRC_REF_ID "{src_ref_id}" is not present in this Data Catalog')
        return

    year = catalog_ix.loc[src_ref_id].SRC_YEAR
    month = catalog_ix.loc[src_ref_id].SRC_MNTH
    day = catalog_ix.loc[src_ref_id].SRC_DAY

    # If Month or Day is missing, fill with our default value of "1"
    if pd.isna(month):
        month = 1.0  # January
    if pd.isna(day):
        day = 1.0  # first of the month
    # Convert floats to integers before combining them into a date object
    year_, month_, day_ = [np.int64(x) for x in [year, month, day]]
    out_src_date = datetime.date(year_, month_, day_).strftime("%Y-%m-%d")

    return out_src_date


def get_src_date_from_ref_id(src_ref_id, catalog_):
    '''Wrapper for `_get_src_date_from_single_ref_id` that handles multi-source SRC_REF_IDs.

    Parameters
    ----------
    src_ref_id : str
        A valid SRC_REF_ID value that is present in the Data Catalog.
        SRC_REF_ID may be in the format of a single number/source ('82'), or
        multiple numbers/sources separated by commas ('40,41').

    catalog_ : Pandas DataFrame
        The up-to-date OGIM Data Catalog table.

    Returns
    -------
    out_src_date : str
        The SRC_DATE (in 'YYYY-MM-DD' format) that corresponds with the
        SRC_REF_ID provided by the user, from the Data Catalog table.
        If a SRC_REF_ID refers to multiple sources / entries in the Data
        Catalog table with different dates, only the *most recent date* is returned.

    Example usage(s)
    -------
    get_src_date_from_ref_id('3', data_catalog_df)
    >> '2024-04-18'

    get_src_date_from_ref_id('26, 27', data_catalog_df)
    >> '2024-04-19'

    # Apply to a column of SRC_REF_ID values
    df['SRC_DATE_NEW'] = df.SRC_REF_ID.apply(lambda x: get_src_date_from_ref_id(x, catalog))

    '''
    # Check that user-provided SRC_REF_ID is a string; if not, cast it.
    if type(src_ref_id) != str:
        src_ref_id = src_ref_id.astype(str)

    # Check if the 'SRC_ID' column is being used as the Data Catalog's
    # index. If not, set it.
    if catalog_.index.name == 'SRC_ID':
        catalog_ix = catalog_.copy()
    else:
        catalog_ix = catalog_.copy().set_index('SRC_ID')

    # Handle SRC_REF_IDs with comma-separated multiple SRC_IDs
    if ',' in src_ref_id:

        date_list = []

        ids = src_ref_id.split(',')
        ids = [s.strip(' ') for s in ids]

        for src in ids:
            src_date = _get_src_date_from_single_ref_id(src, catalog_ix)
            date_list.append(src_date)

        # Once date_list is populated, convert string dates to datetime.date() objects
        date_list_dt = [datetime.datetime.strptime(date, '%Y-%m-%d').date() for date in date_list]
        # Get most recent date, then convert the result back to a string
        most_recent_date = max(date_list_dt)
        out_src_date = most_recent_date.strftime("%Y-%m-%d")

        return out_src_date

    #  Handle SRC_REF_IDs with a single SRC_ID
    else:
        out_src_date = _get_src_date_from_single_ref_id(src_ref_id, catalog_ix)
        return out_src_date
