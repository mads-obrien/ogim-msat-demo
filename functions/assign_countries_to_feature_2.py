# -*- coding: utf-8 -*-
"""
Created on Fri Jan  6 10:53:31 2023

@author: maobrien
"""

import pandas as pd
import geopandas as gpd
from datetime import datetime
import numpy as np


def assign_countries_to_feature(
        gdf,
        gdf_country_colname='COUNTRY',
        gdf_uniqueid_field='OGIM_ID',
        boundary_geoms=None,
        overwrite_country_field=True):
    """Add an attribute to a geodataframe identifying the country(s) to which each feature belongs.

    This function uses a spatial join to identify what national boundary
    (or boundaries) each feature in a gdf intersects, using both country land
    masses and Exclusive Economic Zones (i.e. maritime boundaries)
    to attribute country names.

    When `overwrite_country_field` is True, this function WILL OVERWRITE any
    existing COUNTRY attribute information!!

    Parameters
    ----------
    gdf : GeoDataFrame object
        Can be Point, Line, or Polygon geometry type.
    gdf_country_colname : str, optional
        The column in `gdf` that contains the name of each country, if one
        already exists. The default value is 'COUNTRY'.
    gdf_uniqueid_field : str, optional
        The column in `gdf` that serves as a unique ID or unique index for each
        feature. The default value is 'OGIM_ID'.
    boundary_geoms : GeoDataFrame, optional
        Shapefile containing both land and maritime boundaries, and the
        sovereign power that governs that area. The default is None.
        *NOTE* that `boundary_geoms` must use the same CRS as `gdf` for the
        spatial join to work properly.
    overwrite_country_field : bool, optional
        DESCRIPTION. The default is True.

    Returns
    -------
    gdf_joined : GeoDataFrame
        identical to input `gdf` but with added (or overwritten) COUNTRY attribute.

    Example
    -------
    infra = gpd.read_file(r'path\to\brazil_pipelines_.shp')
    path_to_boundary_geoms = r'path\\to\\marine_and_land_boundaries_seamless.shp'
    my_boundary_geoms = gpd.read_file(path_to_boundary_geoms)
    infra = infra.to_crs(my_boundary_geoms.crs)
    infra = assign_countries_to_feature(infra,
                                        gdf_country_colname = 'COUNTRY',
                                        gdf_uniqueid_field = 'OGIM_ID',
                                        boundary_geoms = my_boundary_geoms,
                                        overwrite_country_field = True)

    """

    # TODO - remove this hard-coded path
    if boundary_geoms.empty:
        print('No land-ocean boundary geometries provided; reading in marine_and_land_boundaries_seamless.shp...')
        path_to_boundary_geoms = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\data\International_data_sets\National_Maritime_Boundaries\marine_and_land_boundaries_seamless.shp'
        print(str(datetime.now()) + '  Reading in onshore and offshore boundary geometries...')
        boundary_geoms = gpd.read_file(path_to_boundary_geoms)
        print(str(datetime.now()) + '  Onshore and offshore boundary geometries loaded successfully!')

    # =============================================================================
    # Data checks before spatial join
    # =============================================================================
    if gdf.crs != boundary_geoms.crs:
        print("CRS don't match! \n Please ensure both datasets use the same CRS and try again.")
        return
        # Terminate function if the CRS don't match, to not waste computation time
    else:
        print("CRS of both GDFs match.")

    # Check that index is unique
    if gdf.index.is_unique is False:
        print("GeoDataFrame indices are not unique! \n Results may not be accurate!")

    # Check if COUNTRY column already exists.
    if 'COUNTRY' in gdf.columns:
        print('Column called "COUNTRY" already exists in the input geodataframe.')
        if overwrite_country_field is True:
            print('! Existing "COUNTRY" column values will be overwritten !')
        else:
            print('Ability to overwrite was set to False. \nPlease remove existing "COUNTRY" column and try again, or set `overwrite_country_field` to True.')
            return
            # end function

    # =============================================================================
    # Run spatial join - method differs depending on geometry
    # =============================================================================
    print(str(datetime.now()) + '  Spatially joining infrastructure with land and marine boundaries...')
    print('Infrastructure geometry type: ' + str(gdf.geom_type.unique()))
    print('Note: This step may take some time for Polygon assets like basins or fields')

    # Reduce boundary_geoms to just the 'SOVEREIGN1' field (our country name)
    # plus geometry because that's all we need, and rename 'SOVEREIGN1' to
    # something more explicit
    boundary_geoms_sov = boundary_geoms.filter(['SOVEREIGN1', 'geometry'])
    boundary_geoms_sov = boundary_geoms_sov.rename(columns={'SOVEREIGN1': 'COUNTRYNAME'})

    # Check if geometry is Point or Multipoint
    if 'Point' in gdf.geom_type.unique() or 'MultiPoint' in gdf.geom_type.unique():
        # The 'within' parameter ensures that points are joined with the land
        # or ocean polygon that they fall completely within.
        # Only one match is possible.
        gdf_joined = gpd.sjoin(gdf,
                               boundary_geoms_sov,
                               how='left',
                               op='within')
        print(str(datetime.now()) + '  Spatial join successful!')

        # If there are any cases where the join failed, default to keeping the
        # gdf's existing country information
        if gdf_joined['COUNTRYNAME'].isnull().values.any():
            if gdf_country_colname:
                gdf_joined['COUNTRYNAME'] = gdf_joined['COUNTRYNAME'].fillna(gdf_joined[gdf_country_colname])

        # The OGIM schema uses the column name "COUNTRY",
        # so ensure that the GDF returned by the function uses "COUNTRY".
        # Afterwards, drop the un-needed columns which came from the land-ocean boundaries join
        gdf_joined['COUNTRY'] = gdf_joined['COUNTRYNAME']
        gdf_joined = gdf_joined.drop(['COUNTRYNAME', 'index_right'], axis=1)

    if 'LineString' in gdf.geom_type.unique() or 'MultiLineString' in gdf.geom_type.unique() or 'Polygon' in gdf.geom_type.unique() or 'MultiPolygon' in gdf.geom_type.unique():
        # The 'intersects' parameter joins the input pipe/basin to ALL polygon(s)
        # they intersect with. One OR MORE matches are possible.
        # If a pipe/basin in 'gdf' intersects more than one polygon, more than one record
        # for that pipe/basin will appear in gdf_joined_lines.
        gdf_joined_shapes = gpd.sjoin(boundary_geoms_sov,
                                      gdf,
                                      how='right',
                                      op='intersects')
        print(str(datetime.now()) + '  Spatial join successful!')

        # If there are any cases where the join failed, default to keeping the
        # gdf's existing country information
        if gdf_joined_shapes['COUNTRYNAME'].isnull().values.any():
            if gdf_country_colname:
                gdf_joined_shapes['COUNTRYNAME'] = gdf_joined_shapes['COUNTRYNAME'].fillna(gdf_joined_shapes[gdf_country_colname])

        # Group the dataframe so there's one row per unique pipe/basin feature.
        # and each row lists the set of country or countries the pipe/basin intersects.
        shape_ids_grouped = pd.DataFrame(gdf_joined_shapes.groupby(gdf_uniqueid_field)['COUNTRYNAME'].apply(set))
        # Sort the countries associated with each asset alphabetically
        # and then convert the list to a string (countries separated by commas)
        shape_ids_grouped['COUNTRYNAME'] = shape_ids_grouped['COUNTRYNAME'].apply(list).apply(lambda x: sorted(x))
        shape_ids_grouped['COUNTRYNAME'] = shape_ids_grouped['COUNTRYNAME'].apply(lambda x: ', '.join(x))

        gdf_joined = gdf.merge(right=shape_ids_grouped,
                               how='left',
                               on=gdf_uniqueid_field,
                               right_index=False)
        gdf_joined['COUNTRY'] = gdf_joined['COUNTRYNAME']
        gdf_joined = gdf_joined.drop(['COUNTRYNAME'], axis=1)

    # TODO - move position of COUNTRY field within dataframe

    return gdf_joined


def assign_stateprov_to_feature(
        gdf,
        gdf_stateprov_colname='STATE_PROV',
        gdf_uniqueid_field='OGIM_ID',
        boundary_geoms=None,
        limit_assignment_to_usa_can=False,
        overwrite_stateprov_field=True):
    """Add an attribute to a geodataframe identifying the state/province(s) to which each feature belongs.

    This function uses a spatial join to identify what boundary (or boundaries)
    each feature in a gdf intersects, using -------------
    # TODO - finish writing this section

    Parameters
    ----------
    gdf : GeoDataFrame object
        Can be Point, Line, or Polygon geometry type.
    gdf_stateprov_colname : str, optional
        The column in `gdf` that contains the name of each state/province, if one
        already exists. The default value is 'STATE_PROV'.
    gdf_uniqueid_field : str, optional
        The column in `gdf` that serves as a unique ID or unique index for each
        feature. The default value is 'OGIM_ID'.
    boundary_geoms : GeoDataFrame, optional
        Shapefile containing state and provincial boundaries. The default is None.
        *NOTE* that `boundary_geoms` must use the same CRS as `gdf` for the
        spatial join to work properly.
    limit_assignment_to_usa_can : bool, optional
        If this parameter is True, then non-point features (a.k.a., linestrings
        and polygons) will only be assigned a new STATE_PROV value IF the
        record's existing COUNTRY value in the input `gdf` indicates the
        record lies in the USA or Canada. The default is False.
    overwrite_stateprov_field : bool, optional
        When `overwrite_stateprov_field` is True, this function WILL OVERWRITE
        any existing STATE_PROV attribute information. The default is True.

    Returns
    -------
    gdf_joined : GeoDataFrame
        identical to input `gdf` but with added (or overwritten) STATE_PROV attribute.

    Example
    -------
    infra = gpd.read_file(r'path\to\brazil_pipelines_.shp')
    path_to_boundary_geoms = r'path\to\10m_states_provinces.shp'
    my_boundary_geoms = gpd.read_file(path_to_boundary_geoms)
    infra = infra.to_crs(my_boundary_geoms.crs)
    infra = assign_stateprov_to_feature(infra,
                                        gdf_stateprov_colname = 'STATE_PROV',
                                        gdf_uniqueid_field = 'OGIM_ID',
                                        boundary_geoms = my_boundary_geoms,
                                        overwrite_stateprov_field = True)

    """

    # TODO - remove this hard-coded path
    if boundary_geoms.empty:
        print('No provincial boundary geometries provided; reading in ne_10m_admin_1_states_provinces.shp...')
        path_to_boundary_geoms = r'C:\Users\maobrien\Environmental Defense Fund - edf.org\Mark Omara - Infrastructure_Mapping_Project\Bottom-Up-Infra-Inventory\Public_Data\NaturalEarth\ne_10m_admin_1_states_provinces.shp'
        print(str(datetime.now()) + '  Reading in provincial boundary geometries...')
        boundary_geoms = gpd.read_file(path_to_boundary_geoms)
        print(str(datetime.now()) + '  Provincial boundary geometries loaded successfully!')

    # =============================================================================
    # Data checks before spatial join
    # =============================================================================
    if gdf.crs != boundary_geoms.crs:
        print("CRS don't match! \n Please ensure both datasets use the same CRS and try again.")
        return
        # Terminate function if the CRS don't match, to not waste computation time
    else:
        print("CRS of both GDFs match.")

    # Check that index is unique
    if gdf.index.is_unique is False:
        print("GeoDataFrame indices are not unique! \n Results may not be accurate!")

    # Check if STATE_PROV column already exists.
    if 'STATE_PROV' in gdf.columns:
        print('Column called "STATE_PROV" already exists in the input geodataframe.')

    # =============================================================================
    # Run spatial join - method differs depending on geometry
    # =============================================================================
    print(str(datetime.now()) + '  Spatially joining infrastructure with provincial boundaries...')
    print('Infrastructure geometry type: ' + str(gdf.geom_type.unique()))
    print('Note: This step may take some time for Polygon assets like basins or fields')

    # Reduce boundary_geoms to just the 'name' field (our province name) + geom,
    # because that's all we need, and rename 'name' to something more explicit
    boundary_geoms_names = boundary_geoms.filter(['name', 'geometry'])
    boundary_geoms_names = boundary_geoms_names.rename(columns={'name': 'STATENAME'})

    # -------------------------------------------------------------------------
    if 'Point' in gdf.geom_type.unique() or 'MultiPoint' in gdf.geom_type.unique():
        # The 'within' parameter ensures that points are joined with the land
        # or ocean polygon that they fall completely within.
        # Only one match is possible.
        gdf_joined = gpd.sjoin(gdf,
                               boundary_geoms_names,
                               how='left',
                               op='within')
        print(str(datetime.now()) + '  Spatial join successful!')

        # If there are any cases where the join failed (and therefore the
        # STATENAME field is empty), default to keeping the gdf's existing
        # STATE_PROV information
        if gdf_joined['STATENAME'].isnull().values.any():
            if gdf_stateprov_colname:
                gdf_joined['STATENAME'] = gdf_joined['STATENAME'].fillna(gdf_joined[gdf_stateprov_colname])

    # -------------------------------------------------------------------------
    if 'LineString' in gdf.geom_type.unique() or 'MultiLineString' in gdf.geom_type.unique() or 'Polygon' in gdf.geom_type.unique() or 'MultiPolygon' in gdf.geom_type.unique():
        # The 'intersects' parameter joins the input pipe/basin to ALL region
        # polygon(s) they intersect with. One OR MORE matches are possible.
        # If a pipe/basin in 'gdf' intersects more than one polygon, more than one record
        # for that pipe/basin will appear in gdf_joined_shapes.
        gdf_joined_shapes = gpd.sjoin(boundary_geoms_names,
                                      gdf,
                                      how='right',
                                      op='intersects')

        # If there are any cases where the join failed (and therefore the
        # STATENAME field is empty), default to keeping the gdf's existing
        # STATE_PROV information
        if gdf_joined_shapes['STATENAME'].isnull().values.any():
            if gdf_stateprov_colname:
                gdf_joined_shapes['STATENAME'] = gdf_joined_shapes['STATENAME'].fillna(gdf_joined_shapes[gdf_stateprov_colname])

        # Group the dataframe so there's one row per unique pipe/basin feature
        # and each row lists the set of provinces the pipe/basin intersects.
        shape_ids_grouped = pd.DataFrame(gdf_joined_shapes.groupby(gdf_uniqueid_field)['STATENAME'].apply(set))
        # Sort the provinces associated with each asset alphabetically
        # and then convert the list to a string (provinces separated by commas)
        shape_ids_grouped['STATENAME'] = shape_ids_grouped['STATENAME'].apply(list).apply(lambda x: sorted(x))
        shape_ids_grouped['STATENAME'] = shape_ids_grouped['STATENAME'].apply(lambda x: ', '.join(x))

        gdf_joined = gdf.merge(right=shape_ids_grouped,
                               how='left',
                               on=gdf_uniqueid_field,
                               right_index=False)
    # -------------------------------------------------------------------------

    # If overwrite OK, then overwrite the entire STATE_PROV column with the
    # results of the spatial join.
    if overwrite_stateprov_field is True:
        print('! Existing "STATE_PROV" column values will be overwritten !')
        if limit_assignment_to_usa_can:  # Logic to only overwrite USA and CAN rows
            usa_can_substrings = ['canada', 'united states']
            mask_usa_can_only = gdf_joined['COUNTRY'].str.contains('|'.join(usa_can_substrings), case=False)
            gdf_joined.loc[mask_usa_can_only, gdf_stateprov_colname] = gdf_joined['STATENAME']
        else:
            gdf_joined['STATE_PROV'] = gdf_joined['STATENAME']

    # If overwrite NOT OK, keep whatever non-null value is already in the
    # STATE_PROV column, even if the spatial join associated the feature
    # with a different state/province. Only fill a cell in the STATE_PROV
    # column with its spatial join result if the cell is null.
    if overwrite_stateprov_field is False:
        mask_null_vals = gdf_joined[gdf_stateprov_colname].isin([np.nan, 'N/A', 'NAN', None])
        if limit_assignment_to_usa_can:  # Logic to only populate null USA and CAN rows
            usa_can_substrings = ['canada', 'united states']
            mask_usa_can_only = gdf_joined['COUNTRY'].str.contains('|'.join(usa_can_substrings), case=False)
            gdf_joined.loc[(mask_null_vals & mask_usa_can_only), gdf_stateprov_colname] = gdf_joined['STATENAME']
        else:
            gdf_joined.loc[mask_null_vals, gdf_stateprov_colname] = gdf_joined['STATENAME']

    # The OGIM schema uses the column name "STATE_PROV", so ensure that the
    # GDF returned by the function uses "STATE_PROV".
    if gdf_stateprov_colname != 'STATE_PROV':
        gdf_joined['STATE_PROV'] = gdf_joined[gdf_stateprov_colname]
    # Finally, drop the columns which came from the land-ocean boundaries join
    # that I do not want in my output GDF (because they're not part of OGIM schema)
    if 'STATENAME' in gdf_joined.columns:
        gdf_joined = gdf_joined.drop(['STATENAME'], axis=1)
    if 'STATENAME' in gdf_joined.columns:
        gdf_joined = gdf_joined.drop(['index_right'], axis=1)

    return gdf_joined
