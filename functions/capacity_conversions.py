# -*- coding: utf-8 -*-
"""
Created on Tue Nov 22 10:30:52 2022

Functions for converting capacity units

Conversion factors mostly drawn from this resource:
    https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2022-approximate-conversion-factors.pdf

@author: maobrien
"""


def convert_metric_tons_to_barrels_crudeoil(value):
    return value * 7.33


def convert_barrels_to_metric_tons_crudeoil(value):
    return value * 0.1364


def convert_million_metric_tons_lng_to_mmcf_natgas(value):
    '''
    From 1 million tonnes LNG to billion cubic feet natural gas, multiply by 48.028
    https://www.bp.com/content/dam/bp/business-sites/en/global/corporate/pdfs/energy-economics/statistical-review/bp-stats-review-2022-approximate-conversion-factors.pdf

    '''
    return (value * 48.028) * 1000  # Multiply by 1000 because 1 BCF = 1000 MMCF


def convert_MMm3d_to_mmcfd(number):
    # 1 million cubic metre = 35.31467 million cubic feet
    # This is backed up by the BP document: they use a 1-to-35.315 conversion
    return number * 35.31467


def convert_m3d_to_bbld(number):
    # 1 cubic meter = 6.2898107704 bbl (US)
    return number * 6.2898107704
