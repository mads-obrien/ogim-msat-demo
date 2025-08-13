# -*- coding: utf-8 -*-
"""
Created on Thu Jul 25 14:01:23 2024

Explore Oklahoma production data.

Most of the data from the Oklahoma Tax Commission are .DAT files, with fixed
character widths specifying where one column ends and another begins. The .DAT
files must be read in with the correct field lengths specified, otherwise the
data will seem like gibberish.

@author: maobrien
"""
import os
import pandas as pd
import numpy as np
import geopandas as gpd
from tqdm import tqdm
from datetime import datetime
# import time

os.chdir(r'C:\Users\maobrien\Documents\GitHub\ogim-msat\functions')
from ogimlib import integrate_production, save_spatial_data, schema_OIL_GAS_PROD

os.chdir(r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Production_v0\data\oklahoma')

results_folder = "C:\\Users\\maobrien\\Environmental Defense Fund - edf.org\\Mark Omara - Infrastructure_Mapping_Project\\Bottom-Up-Infra-Inventory\\Public_Production_v0\\integrated_results\\"


# =============================================================================
# %% Define custom functions
# =============================================================================


def create_pun_number_column(df, county_num, lease_num, sub_num, merge_num):
    df = df[[county_num, lease_num, sub_num, merge_num]].copy()
    df[county_num] = df[county_num].astype(str).str.zfill(3)
    df[lease_num] = df[lease_num].astype(str).str.zfill(6)
    df[sub_num] = df[sub_num].astype(str)  # only one character, no zfill needed
    df[merge_num] = df[merge_num].astype(str).str.zfill(4)
    output_column = df[[county_num, lease_num, sub_num, merge_num]].agg('-'.join, axis=1)
    return output_column


def create_hash_from_columns(df, col_list):
    df_for_hashing = df[col_list]
    return df_for_hashing.apply(lambda x: hash(tuple(x)), axis=1)


def get_40acre_polys_from_LLD(q160, q40):

    # Create a dictionary that maps a given "quarter (160 acre)" value to the
    # quarter-quarter(s), or 40-acre plot(s), that would reside within it
    mydict = {
        'NW': ['NW'],
        'NE': ['NE'],
        'SW': ['SW'],
        'SE': ['SE'],
        'EH': ['NE', 'SE'],  # eastern half
        'WH': ['NW', 'SW'],  # western half
        'SH': ['SW', 'SE'],  # southern half
        'NH': ['NW', 'NE'],  # northern half
        'AL': ['NW', 'NE', 'SW', 'SE'],  # all
        np.nan: ['']
    }

    sufQ = mydict[q160]
    if q40 in ['', None]:
        preQ = mydict['AL']
    elif pd.isna(q40):
        preQ = mydict['AL']
    else:
        preQ = mydict[q40]

    # print(sufQ)
    # print(preQ)

    allQ = [""] * (len(preQ) * len(sufQ))
    # print(len(allQ))
    idx = 0
    for p in preQ:
        for s in sufQ:
            # print(p, s)
            allQ[idx] = p + s
            idx += 1
    # print(allQ)
    return allQ


def subset_40acre_polygons(poly_df, t, s, r, p):
    # start = time.time()

    # conduct comparisons for int-type values first, which are faster than str
    df_0 = poly_df[poly_df.section == s]
    df_1 = df_0[df_0.panhandle == p]

    # t1 = time.time() - start

    # next, conduct comparisons for string-type values, which are a bit slower
    df_2 = df_1[df_1.range == r]
    df_3 = df_2[df_2.township == t]

    # print(t1, time.time() - start - t1)
    # mask = (poly_df['township'] == t) & (poly_df['section'] == s) & (poly_df['range'] == r) & (poly_df['panhandle'] == m)
    # print('subsetting by LLD complete')
    df_3 = df_3.reset_index(drop=True)
    return df_3


def select_geometries_of_qqs(t, s, r, p, lease_qqs):
    # start = time.time()
    # Create a subset of polygons that match the township, section, range and
    # panhandle of the given lease. Then, reduce that polygon gdf further to
    # ONLY those quarter-quarter geometries that the lease resides in.
    subset_df = subset_40acre_polygons(polys_40acre, t, s, r, p)
    # t1 = time.time() - start
    # print('returning geometries...')
    ret = subset_df[subset_df.ALIQUOT.isin(lease_qqs)].geometry
    ret = ret.reset_index(drop=True)
    # print(t1, time.time() - start - t1)
    return ret


# =============================================================================
# %% Read the Excel sheets that defines all attribute col names and their fixed widths
# Use the values in this "metadata" object to populate the `widths` and
# `names` parameters in subsequent `pd.read_fwf()` calls
# =============================================================================
# Create empty dictionary to hold metadata info from each Excel tab
metadata = {}

excel_sheet_names = ['GHP12,36,GTR36',
                     'QUALIFY TAX RATE',
                     'EXEMPTION',
                     'LEASE LEGAL',
                     'OPERATOR']

for sheet in excel_sheet_names:
    # Read the Excel sheet as a (temp) df
    df = pd.read_excel('Layout for Outside Entities.xlsx',
                       sheet_name=sheet)

    # Save the set of col names and widths as LISTS, to preserve their order
    metadata[sheet] = {}
    metadata[sheet]['widths'] = list(df['FIELD SIZE'])
    metadata[sheet]['names'] = list(df['FIELD NAME'])
    # strip trailing and leading spaces from each field name, just in case
    metadata[sheet]['names'] = [x.strip() for x in metadata[sheet]['names']]

# =============================================================================
# %% Read in 100 lines from the Operator and Tax Rate .DAT files, to see what they look like
# =============================================================================

# oper = pd.read_fwf('exp_gpoper20240711.dat',
#                    widths=metadata['OPERATOR']['widths'],
#                    names=metadata['OPERATOR']['names'],
#                    nrows=100,
#                    header=None)

# taxrate = pd.read_fwf('exp_gpqtrat20240711.dat',
#                       widths=metadata['QUALIFY TAX RATE']['widths'],
#                       names=metadata['QUALIFY TAX RATE']['names'],
#                       nrows=100,
#                       header=None)


# =============================================================================
# %% Read production tables with my adjusted width parameters
# =============================================================================
test_widths = [255, 20, 2, 4, 2, 2, 2, 3, 6, 1, 4, 255, 20, 1, 20, 2, 20, 1, 20, 1, 20]
test_colnames = ['company name ??',
                 'company_number',
                 'reporting_month',
                 'reporting_year',
                 'tax_type_code',
                 'product_code',
                 'report_type_code',
                 'pun_county_num',
                 'pun_lease_num',
                 'pun_sub_num',
                 'pun_merge_num',
                 'producer name ??',
                 'producer_purchaser',
                 'gross_volume_sign',
                 'gross_volume',
                 'exempt_code',
                 'decimal_equiv',
                 'exempt_volume_sign',
                 'exempt_volume',
                 'taxable_volume_sign',
                 'taxable_volume']

# gtr36 = pd.read_fwf('exp_gph_reports_gtr3620240111.dat',  # 5 GB
#                     # widths=metadata['GHP12,36,GTR36']['widths'],
#                     widths=test_widths,
#                     names=test_colnames,
#                     # nrows=500,
#                     index_col=False,
#                     header=None)

# last12 = pd.read_fwf('exp_gph_reports_1220240910.dat',  # 0.5 GB
#                      # widths=metadata['GHP12,36,GTR36']['widths'],
#                      widths=test_widths,
#                      names=test_colnames,
#                      # nrows=100,
#                      index_col=False,
#                      header=None)

last36 = pd.read_fwf('exp_gph_reports_3620240910.dat',  # 2 GB
                     # widths=metadata['GHP12,36,GTR36']['widths'],
                     widths=test_widths,
                     names=test_colnames,
                     # nrows=10000,
                     index_col=False,
                     header=None)

# =============================================================================
# %% Investigate the reporting year/month values in each table
# =============================================================================
last36 = last36.rename(columns={"reporting_year": "year",
                                "reporting_month": "month"}, errors="raise")
last36.columns
last36['yearmonth'] = pd.to_datetime(last36[['year', 'month']].assign(day=1))
last36.yearmonth.describe(datetime_is_numeric=True)

# For now, filter the production records to just one year you want
ok_prod_2022 = last36[last36.year == 2022].copy()
ok_prod_2022.yearmonth.describe(datetime_is_numeric=True)

# =============================================================================
# %% Clean the production numbers
# =============================================================================
# Use the gross_volume_sign field to properly negate production numbers
ok_prod_2022.loc[ok_prod_2022.gross_volume_sign == '-', 'gross_volume'] = ok_prod_2022.gross_volume * -1

# Convert product type codes to product type names
prod_codes = {1: 'OIL',
              3: 'RECLAIMED OIL',
              5: 'NATURAL GAS',
              6: 'NATURAL GAS LIQUIDS'}
ok_prod_2022['product_name'] = ok_prod_2022.product_code.map(prod_codes)

# Create separate columns for oil vs gas production values.
ok_prod_2022.loc[ok_prod_2022.product_code == 5, 'gas_mcf'] = ok_prod_2022.gross_volume
ok_prod_2022.loc[ok_prod_2022.product_code.isin([1, 3]), 'oil_barrels'] = ok_prod_2022.gross_volume

# Check: are there any natural gas liquids reported produced?
ok_prod_2022.query("product_code == 6").gross_volume.sum()
# TODO - ask Mark, should we include reclaimed oil as part of the produced oil total?
if ok_prod_2022.query("product_code == 6").gross_volume.sum() == 0:
    ok_prod_2022 = ok_prod_2022[ok_prod_2022.product_code != 6].reset_index(drop=True)
    print('No natural gas liquids records with any production; NGL rows dropped')

# Record the total oil and gas produced in 2022 before any other data cleaning
before_after_table.at['OKLAHOMA', 'oil_original'] = ok_prod_2022.oil_barrels.sum()
before_after_table.at['OKLAHOMA', 'gas_original'] = ok_prod_2022.gas_mcf.sum()


# =============================================================================
# %% Aggregate the production records so there is one per lease
# =============================================================================
# Create a 14-digit PUN number
ok_prod_2022['PUN_NUM'] = create_pun_number_column(ok_prod_2022,
                                                   'pun_county_num',
                                                   'pun_lease_num',
                                                   'pun_sub_num',
                                                   'pun_merge_num')

ok_agg_funcs = {
    'pun_county_num': 'first',
    'pun_lease_num': 'first',
    'pun_sub_num': 'first',
    'pun_merge_num': 'first',
    'company name ??': 'first',
    'producer name ??': 'first',
    'product_name': pd.Series.mode,
    'oil_barrels': 'sum',
    'gas_mcf': 'sum'
}

ok_prod_2022_agg = ok_prod_2022.groupby(by='PUN_NUM', as_index=False).agg(ok_agg_funcs)
ok_prod_2022_agg['product_name'] = ok_prod_2022_agg.product_name.astype(str)

# Record the total oil and gas produced in 2022 AFTER aggregation
before_after_table.at['OKLAHOMA', 'oil_agg'] = ok_prod_2022_agg.oil_barrels.sum()
before_after_table.at['OKLAHOMA', 'gas_agg'] = ok_prod_2022_agg.gas_mcf.sum()

# =============================================================================
# %% Read in Lease table and tidy up
# =============================================================================
lease_ = pd.read_fwf('exp_gplease20240610.dat',
                     widths=metadata['LEASE LEGAL']['widths'],
                     names=metadata['LEASE LEGAL']['names'],
                     # nrows=100,
                     header=None)

# Filter lease entries to JUST those that are legal land descriptions, not
# surface or bottom specific
lease_ = lease_.query("legal_description_type == 'Legal'").reset_index(drop=True)
# Drop cols I don't need
lease_.drop(['quarter2p5', 'quarter10', 'formation_names'], axis=1, inplace=True)

# Modify the format of township, range, section columns so that
# they may be joined with the PLSS polygons
# For townships without a leading zero, add one so they match the PLSS format
lease_['township'] = lease_.township.str.zfill(3)
lease_['section'] = lease_['section']  # leave as-is, already int
lease_['section_str'] = lease_['section'].astype(str)  # create string copy
# Add padding zeroes so they match the PLSS polygon section format
lease_['section_str'] = lease_['section_str'].str.zfill(2)
# Strip away the "CM" suffix when it appears
lease_['range'] = lease_.range.str.rstrip('CM')
# Add padding zeroes to range so they match PLSS
lease_['range'] = lease_.range.str.zfill(3)
# TODO - clean up the 'range' field so it matches the PLSS range field

# Add a column to flag whether a given lease_ resides in a county that was surveyed
# with the Cimarron Meridian or the Indian Meridian
lease_['panhandle'] = 0
lease_.loc[lease_.name.isin(['BEAVER', 'CIMARRON', 'TEXAS']), 'panhandle'] = 1
lease_['panhandle_str'] = 'i'
lease_.loc[lease_.name.isin(['BEAVER', 'CIMARRON', 'TEXAS']), 'panhandle_str'] = 'c'

# REDUCE LEASE TABLE TO ONLY LEASES THAT MIGHT INTERSECT PRODUCTION
lease_['PUN_NUM'] = create_pun_number_column(lease_,
                                             'pun_county_num',
                                             'pun_lease_num',
                                             'pun_sub_num',
                                             'pun_merge_num')
lease = lease_[lease_.PUN_NUM.isin(list(ok_prod_2022_agg.PUN_NUM))].reset_index(drop=True)

# For reducing the PLSS dataset later, create a Section-Township-Range value
lease['STR_M'] = lease[['section_str',
                        'township',
                        'range',
                        'panhandle_str']].apply(lambda x: '-'.join(x), axis=1)

# =============================================================================
# %% Load and clean PLSS polygons -- takes ~5min (0.8 GB)
# These polygons, in most cases, represent a quarter-quarter, or a 40-acre plot
# =============================================================================
print('Reading in PLSS begun')
print(datetime.now())
plss_fp = r'C:\Users\maobrien\OneDrive - MethaneSAT, LLC\raw_data\oklahoma_plss_OWRB_subdivisions.geojson'
polys_40acre = gpd.read_file(plss_fp)
print(polys_40acre.columns)
# Only keep columns I need
polys_40acre = polys_40acre[['OBJECTID',
                             'TOWNSHIP',
                             'RANGE',
                             'SECT',
                             'ALIQUOT',
                             'geometry']]
# Manually set the correct CRS, since the geojson says the file is epsg:4326 but
# I know the coordinates use epsg:26914
polys_40acre = polys_40acre.set_crs('epsg:26914', allow_override=True)

# -----------------------------------------------------------------------------
# All geometries are multi-polygon type but only contain a single polygon;
# recast them all as single-part polygons.

# # Confirm that all records are multipolygon type
# print(polys_40acre.geometry.type.value_counts())
# # How many polygon parts are present in every MultiPolygon record?
# polys_40acre['multipolycount'] = polys_40acre.geometry.apply(lambda x: len(x.geoms))
# polys_40acre.multipolycount.value_counts()

# Explode multi-part geometries into a single polygon geometry per record
polys_40acre = polys_40acre.explode(ignore_index=True).reset_index(drop=True)
print(polys_40acre.geometry.type.value_counts())  # all "Polygon"

print('Reading and processing of PLSS done')
print(datetime.now())

# =============================================================================
# %% Modify the format of TOWNSHIP, RANGE, SECT, and ALIQUOT columns so that
# they may be joined with the legal land description of the leases
# =============================================================================
polys_40acre['township'] = polys_40acre['TOWNSHIP']  # no change needed
polys_40acre['section'] = polys_40acre['SECT'].astype(int)
polys_40acre['section_str'] = polys_40acre['SECT']  # no change, already string
polys_40acre['range'] = polys_40acre['RANGE'].str[:-1]  # remove last character
polys_40acre['quarter160'] = polys_40acre['ALIQUOT'].str[-2:]  # use last 2 characters
polys_40acre['quarter40'] = polys_40acre['ALIQUOT'].str[:2]  # use first 2 characters

# There are TWO different meridians used in PLSS in Oklahoma!!
# https://www.berlinroyalties.com/sell-oklahoma-mineral-rights-the-oil-scout/2015/5/17/the-public-land-survey-system-plss-in-oklahoma
# That means there can be a Range 01 East in TWO different places in the state.
# Add another column to flag whether the given Range is based on the Cimmaron
# Meridian or the Indian Meridian
polys_40acre['panhandle'] = 0
polys_40acre.loc[polys_40acre.RANGE.str.endswith('I'), 'panhandle'] = 0
polys_40acre.loc[polys_40acre.RANGE.str.endswith('C'), 'panhandle'] = 1
polys_40acre['panhandle_str'] = 'i'
polys_40acre.loc[polys_40acre.RANGE.str.endswith('I'), 'panhandle_str'] = 'i'
polys_40acre.loc[polys_40acre.RANGE.str.endswith('C'), 'panhandle_str'] = 'c'

# To reduce future computation time on this massive gdf, filter the gdf so that
# only polygons that might possibly be associated with a lease are left.
# (Use a concatenated section township range and meridian number to match with leases)
polys_40acre['STR_M'] = polys_40acre[['section_str',
                                      'township',
                                      'range',
                                      'panhandle_str']].apply(lambda x: '-'.join(x), axis=1)

print(f'Original number of 40-acre polygons: {len(polys_40acre):,.0f}')
polys_40acre = polys_40acre[polys_40acre['STR_M'].isin(list(lease['STR_M']))].reset_index(drop=True)
print(f'Count of 40-acre polygons possibly related to a lease: {len(polys_40acre):,.0f}')


# =============================================================================
# %% Join quarter section centroids with lease numbers based on legal land descrip

# KNOWN ISSUE - there are some leases that claim to be in section 36 of range
# 28E & township 5N, but section 36 doesn't exist in that township-range.
# just ignore this for now.... those producing records will not have a spatial
# location associated with them, and will be dropped from the total.
# =============================================================================
# TEST WITH JUST 500 LEASES
# lease = lease.loc[1:500].copy().reset_index(drop=True)

# Create a new column in the lease df that takes a lease legal land description
# and returns a list of the quarter-quarter polygon(s) this lease occupies
lease['qqs'] = [get_40acre_polys_from_LLD(x, y) for x, y in zip(lease.quarter160,
                                                                lease.quarter40)]

# METHOD 1 List comprehension -- THIS TAKES 20 MINUTES (but took 1 hr at a coffee shop for some reason)
# Create a list of Geoseries, with each Geoseries containing the
# quarter-quarter polygon(s) associated with one lease/row
print(datetime.now())
qq_geom_array = [
    select_geometries_of_qqs(
        t, s, r, p, q
    ) for t, s, r, p, q
    in tqdm(lease[[
        "township",  # str, in the format 07N or 12S
        "section",  # int
        "range",  # str, in the format 09W or 24E
        "panhandle",  # int, 1 for yes, 0 for no
        "qqs"  # str
    ]].to_numpy())
]
print(datetime.now())


# Create a dictionary that will contain the quarter-quarter polygon(s) that
# belong to each row in the lease table. Indexed on lease table
if len(lease) == len(qq_geom_array):
    qq_polygon_dict = dict(zip(lease.index, qq_geom_array))
else:
    print('Double check that the number of items in `qq_geom_array` matches the length of the lease table')


# %% Check the results on a map
lease_index_to_check = 32
# 2713 = empty
# 123
# 32

my_lease = lease.iloc[lease_index_to_check]

base = qq_polygon_dict[lease_index_to_check].boundary.plot()
# qq_polygon_dict[lease_index_to_check].plot(ax=base, color='red', markersize=13)
mytitle = f'{my_lease.quarter40} of {my_lease.quarter160}'
base.set_title(mytitle)


# %% Create a GeoSeries in the 'lease' table which will hold the polygons
# pertaining to that lease
# Must do a unary union so that all of the separate sets of polygons affiliated
# with each lease/row are instead represented by a single polygon
lease['geom_single_poly'] = gpd.GeoSeries([x.unary_union for x in qq_polygon_dict.values()])

# Turn df into gdf -- drop rows with empty geometries as well
lease_gdf = gpd.GeoDataFrame(lease, geometry='geom_single_poly', crs='epsg:26914')
lease_gdf = lease_gdf[~lease_gdf.geom_single_poly.is_empty].reset_index(drop=True)
lease_gdf = lease_gdf[lease_gdf.geom_single_poly.notna()].reset_index(drop=True)


# Since a production reporting unit lease can be made up of lands with a few
# different LLDs, dissolve the lease gdf based on the numbers that uniquely
# identify the lease / PRU. After this, we'll have one row per PUN.
# !!! After this step, some of the attribute values might be incorrect, since the
# "first" value is taken by default during the dissolve
lease_gdf_diss = lease_gdf.dissolve(by=['PUN_NUM'])
# Reset index so that PUN_ID remains a column for joining with later
lease_gdf_diss = lease_gdf_diss.reset_index(drop=False)
# Plot to see how some leases/PUNs are made up of multiple, contiguous LLD areas.
# lease_gdf_diss.geom_single_poly.iloc[4]
# lease_gdf_diss.iloc[0:400].plot(column='well_name')

# Create new geom column for these leases/PUNs that contain the CENTROID of
# the area the lease covers
lease_gdf_diss['geom_centroid'] = lease_gdf_diss.geom_single_poly.centroid
# Create new gdf with centroid geometries representing each lease
lease_gdf_centroids = lease_gdf_diss.set_geometry('geom_centroid')
lease_gdf_centroids.iloc[0:400].plot(column='well_name')

# =============================================================================
# %%
# =============================================================================
lease_gdf_diss['qqs'] = lease_gdf_diss['qqs'].astype(str)
lease_gdf_diss = lease_gdf_diss.drop('geom_centroid', axis=1)
lease_gdf_diss.to_file(results_folder + 'ok_lease_diss_TEST2.shp')

lease_gdf_centroids = lease_gdf_centroids.drop('geom_single_poly', axis=1)
lease_gdf_centroids['qqs'] = lease_gdf_centroids['qqs'].astype(str)
lease_gdf_centroids.to_file(results_folder + 'ok_lease_gdf_centroids_TEST2.shp')

# =============================================================================
# %% Join production records with lease table records and clean
# =============================================================================
ok_prod_2022_pt = pd.merge(
    left=ok_prod_2022_agg,   # aggregated to annual already
    right=lease_gdf_centroids,
    how='left',
    on=['PUN_NUM']
)
# gdf turned into df during the merge
ok_prod_2022_pt = gpd.GeoDataFrame(ok_prod_2022_pt,
                                   geometry='geom_centroid',
                                   crs='epsg:26914')

# NOT ALL PRODUCTION RECORDS will be successfully joined to a geometry
# drop the ones that aren't.
ok_prod_2022_pt = ok_prod_2022_pt[ok_prod_2022_pt.geom_centroid.notna()].reset_index(drop=True)
# convert to wgs84
ok_prod_2022_pt = ok_prod_2022_pt.to_crs(4326)
# create lat long
ok_prod_2022_pt['lat'] = ok_prod_2022_pt.geom_centroid.y
ok_prod_2022_pt['long'] = ok_prod_2022_pt.geom_centroid.x

# =============================================================================
# %% OKLAHOMA - Integration
# =============================================================================
ok_integrated, ok_prod_err = integrate_production(
    ok_prod_2022_pt,
    starting_ids=1,
    category="Oil and natural gas production",
    fac_alias="OIL_GAS_PROD",
    country="United States",
    state_prov="OKLAHOMA",
    src_ref_id="254",  # DONE
    src_date="2024-09-11",
    on_offshore="Onshore",
    fac_name='well_name',
    fac_id="PUN_NUM",
    fac_type='product_name',
    # spud_date=None,
    # comp_date=None,
    # drill_type=None,
    # fac_status=None,
    op_name="company name ??",
    oil_bbl='oil_barrels',
    gas_mcf='gas_mcf',
    # water_bbl=None,
    # condensate_bbl=None,
    # prod_days=None,
    prod_year="2022",
    entity_type="LEASE",  # FIXME
    fac_latitude="lat",
    fac_longitude="long"
)


# Record the total oil and gas produced in 2022 AFTER integration, to compare
# against the raw production data
populate_before_after_table_post_integration('OKLAHOMA',
                                             before_after_table,
                                             ok_integrated)

save_spatial_data(
    ok_integrated,
    "oklahoma_oil_natural_gas_production_2022",
    schema_def=True,
    schema=schema_OIL_GAS_PROD,
    file_type="GeoJSON",
    out_path=results_folder
)
