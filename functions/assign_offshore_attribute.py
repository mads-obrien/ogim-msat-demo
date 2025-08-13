# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 10:14:08 2022

@author: maobrien
"""
import pandas as pd
import geopandas as gpd
from datetime import datetime
import numpy as np


def assign_offshore_attribute(
        gdf,
        boundary_geoms=None,
        overwrite_onoff_field=True):
    '''Determine whether a feature falls onshore or offshore, and assign that attribute.

    When `overwrite_onoff_field` is True, this function WILL OVERWRITE any existing ON_OFFSHORE attribute
    information!!

    # TODO - write docstring

    Parameters
    ----------
    gdf : GeoDataFrame object
        Point geometries.
    boundary_geoms : GeoDataFrame object, optional
        DESCRIPTION. The default is None.
    overwrite_onoff_field : bool
        DESCRIPTION.

    Returns
    -------
    gdf_joined : GeoDataFrame object
        identical to input `gdf` but with added (or overwritten) ON_OFFSHORE attribute.

    Example
    -------
    infra = gpd.read_file(r'path\to\brazil_terminals_.shp')
    path_to_boundary_geoms = r'path\to\marine_and_land_boundaries_seamless.shp'
    my_boundary_geoms = gpd.read_file(path_to_boundary_geoms)
    infra = infra.to_crs(my_boundary_geoms.crs)
    infra = assign_offshore_attribute(infra, my_boundary_geoms, overwrite_onoff_field = True)

    '''

    # TODO - remove this hard-coded path
    if boundary_geoms.empty:
        print('No land-ocean boundary geometries provided; reading in marine_and_land_boundaries_seamless.shp...')
        path_to_boundary_geoms = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\data\International_data_sets\National_Maritime_Boundaries\marine_and_land_boundaries_seamless.shp'
        print(str(datetime.now()) + '  Reading in onshore and offshore boundary geometries...')
        boundary_geoms = gpd.read_file(path_to_boundary_geoms)
        print(str(datetime.now()) + '  Onshore and offshore boundary geometries loaded successfully!')

    # Reduce boundary_geoms to just on/offshore status and geometry
    boundary_geoms_onoff = boundary_geoms.filter(['ON_OFF', 'geometry'])

    # =============================================================================
    # Data checks before spatial join
    # =============================================================================
    if gdf.crs != boundary_geoms_onoff.crs:
        print("CRS don't match! \n Please ensure both datasets use the same CRS and try again.")
        return
        # Terminate function if the CRS don't match, to not waste computation time
        # run gdf = gdf.to_crs(boundary_geoms_onoff.crs)
    else:
        print("CRS of both GDFs match.")

    # Check that index is unique
    if gdf.index.is_unique is False:
        print("GeoDataFrame indices are not unique! \n Results may not be accurate!")

    # Check if ON_OFFSHORE column already exists.
    if 'ON_OFFSHORE' in gdf.columns:
        print('Column called "ON_OFFSHORE" already exists in the input geodataframe.')

    # =============================================================================
    # Run spatial join - method differs depending on geometry
    # =============================================================================
    print(str(datetime.now()) + '  Spatially joining infrastructure with offshore boundaries...')
    print('Infrastructure geometry type: ' + str(gdf.geom_type.unique()))
    print('Note: This step may take some time for Polygon assets like basins or fields')

    # -------------------------------------------------------------------------
    if 'Point' in gdf.geom_type.unique() or 'MultiPoint' in gdf.geom_type.unique():
        # The 'within' parameter ensures that points are joined with the land
        # or ocean polygon that they fall completely within.
        # Only one match is possible.
        gdf_joined = gpd.sjoin(gdf,
                               boundary_geoms_onoff,
                               how='left',
                               op='within')
        print(str(datetime.now()) + '  Spatial join successful!')

        # In our output schema, the column name is "ON_OFFSHORE", so ensure that the final result has this name
        # Afterwards, drop the un-needed columns which came from the land-ocean boundaries
        # gdf_joined['ON_OFFSHORE'] = gdf_joined['ON_OFF']
        # gdf_joined = gdf_joined.drop(['ON_OFF', 'index_right'], axis=1)

        # If a point intersects with more than one on/offshore polygon (this
        # can happen in Joint Regime areas between two countries), drop the
        # duplicate of the well record that appears in `gdf_joined`
        gdf_joined = gdf_joined.drop_duplicates(subset=['OGIM_ID',
                                                        'SRC_REF_ID',
                                                        'ON_OFFSHORE',
                                                        'LATITUDE',
                                                        'LONGITUDE'],
                                                keep='first')

    # -------------------------------------------------------------------------
    if 'LineString' in gdf.geom_type.unique() or 'MultiLineString' in gdf.geom_type.unique() or 'Polygon' in gdf.geom_type.unique() or 'MultiPolygon' in gdf.geom_type.unique():
        # The 'intersects' parameter joins the input pipe/basin to ALL polygon(s)
        # they intersect with. One OR MORE matches are possible
        # If a pipe/basin in 'gdf' intersects more than one polygon, more than one record
        # for that pipe/basin will appear in gdf_joined_lines
        gdf_joined_shapes = gpd.sjoin(boundary_geoms_onoff,
                                      gdf,
                                      how='right',
                                      op='intersects')

        # Group the dataframe so there's one row per unique pipe/basin feature
        # All possible onshore or offshore values are concatenated together by sum()
        # Replace any concatenations (like ONSHOREOFFSHORE, OFFSHOREONSHOREONSHOREOFFSHORE)
        # with my standard value to indicate "both on and offshore"
        shape_ids_grouped = pd.DataFrame(gdf_joined_shapes.groupby('OGIM_ID')['ON_OFF'].sum())
        acceptable_values = ['ONSHORE', 'OFFSHORE']
        shape_ids_grouped.loc[~ shape_ids_grouped.ON_OFF.isin(acceptable_values), 'ON_OFF'] = 'ONSHORE, OFFSHORE'

        gdf_joined = gdf.merge(right=shape_ids_grouped,
                               how='left',
                               on='OGIM_ID',
                               right_index=False)

    # -------------------------------------------------------------------------

    # If overwrite OK, then overwrite the entire ON_OFFSHORE column with the
    # results of the spatial join.
    if overwrite_onoff_field is True:
        print('! Existing "ON_OFFSHORE" column values will be overwritten !')
        gdf_joined['ON_OFFSHORE'] = gdf_joined['ON_OFF']

    # If overwrite NOT OK, keep whatever non-null value is already in the
    # ON_OFFSHORE column, even if the spatial join associated the feature
    # with a different value. Only fill a cell in the ON_OFFSHORE
    # column with its spatial join result if the cell is null.
    if overwrite_onoff_field is False:
        mask_null_vals = gdf_joined['ON_OFFSHORE'].isin([np.nan, 'N/A', 'NAN', None])
        gdf_joined.loc[mask_null_vals, 'ON_OFFSHORE'] = gdf_joined['ON_OFF']

    # Finally, drop columns which came from the land-ocean boundaries join that
    # I do not want in my output GDF (because they're not part of OGIM schema)
    if 'ON_OFF' in gdf_joined.columns:
        gdf_joined = gdf_joined.drop(['ON_OFF'], axis=1)
    if 'index_right' in gdf_joined.columns:
        gdf_joined = gdf_joined.drop(['index_right'], axis=1)

    # TODO - move position of ON_OFFSHORE field within dataframe

    return gdf_joined
