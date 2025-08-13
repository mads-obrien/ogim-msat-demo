# -*- coding: utf-8 -*-
"""
Created on Tue Apr  5 14:31:43 2022

@author: maobrien
"""

can_province_to_abbrev = {
  'Alberta': 'AB',
  'British Columbia': 'BC',
  'Manitoba': 'MB',
  'New Brunswick': 'NB',
  'Newfoundland and Labrador': 'NL',
  'Northwest Territories': 'NT',
  'Nova Scotia': 'NS',
  'Nunavut': 'NU',
  'Ontario': 'ON',
  'Prince Edward Island': 'PE',
  'Quebec': 'QC',
  'Saskatchewan': 'SK',
  'Yukon': 'YT'
}

us_state_to_abbrev = {
    "Alabama": "AL",
    "Alaska": "AK",
    "Arizona": "AZ",
    "Arkansas": "AR",
    "California": "CA",
    "Colorado": "CO",
    "Connecticut": "CT",
    "Delaware": "DE",
    "Florida": "FL",
    "Georgia": "GA",
    "Hawaii": "HI",
    "Idaho": "ID",
    "Illinois": "IL",
    "Indiana": "IN",
    "Iowa": "IA",
    "Kansas": "KS",
    "Kentucky": "KY",
    "Louisiana": "LA",
    "Maine": "ME",
    "Maryland": "MD",
    "Massachusetts": "MA",
    "Michigan": "MI",
    "Minnesota": "MN",
    "Mississippi": "MS",
    "Missouri": "MO",
    "Montana": "MT",
    "Nebraska": "NE",
    "Nevada": "NV",
    "New Hampshire": "NH",
    "New Jersey": "NJ",
    "New Mexico": "NM",
    "New York": "NY",
    "North Carolina": "NC",
    "North Dakota": "ND",
    "Ohio": "OH",
    "Oklahoma": "OK",
    "Oregon": "OR",
    "Pennsylvania": "PA",
    "Rhode Island": "RI",
    "South Carolina": "SC",
    "South Dakota": "SD",
    "Tennessee": "TN",
    "Texas": "TX",
    "Utah": "UT",
    "Vermont": "VT",
    "Virginia": "VA",
    "Washington": "WA",
    "West Virginia": "WV",
    "Wisconsin": "WI",
    "Wyoming": "WY",
    "District of Columbia": "DC",
    "American Samoa": "AS",
    "Guam": "GU",
    "Northern Mariana Islands": "MP",
    "Puerto Rico": "PR",
    "United States Minor Outlying Islands": "UM",
    "U.S. Virgin Islands": "VI",
}

can_abbrev_to_province = {v:k  for k,v in can_province_to_abbrev.items()}

us_abbrev_to_state = {v:k  for k,v in us_state_to_abbrev.items()}

def abbrev2name(df, colname, usa=False, can=False):
    if usa==True:
        df[colname] = df[colname].map(us_abbrev_to_state).fillna(df[colname])
    if can==True:
        df[colname] = df[colname].map(can_abbrev_to_province).fillna(df[colname])
    if (usa==False) & (can==False):
        print('WARNING: Please set at least one of the flags `usa` and/or `can` to TRUE.')
    return df[colname]
    
   
def name2abbrev(df, colname, usa=False, can=False):
    if usa==True:
        df[colname] = df[colname].map(us_state_to_abbrev).fillna(df[colname])
    if can==True:
        df[colname] = df[colname].map(can_province_to_abbrev).fillna(df[colname])
    if (usa==False) & (can==False):
        print('WARNING: Please set at least one of the flags `usa` and/or `can` to TRUE.')
    return df[colname]



##### EXAMPLE USAGE
# wells_env = gpd.read_file(r'raw_data\Proprietary_Data\Enverus_Drillinginfo\Domestic\Enverus_Wells_2020_USA_CAN\_enverus_usa_canada_wells_2020.shp')
# print(wells_env.State.unique())  # column contains abbreviations of both USA and CAN states
# # Replace the abbreviations with name; EDITS DATAFRAME IN PLACE
# wells_env['State'] = abbrev2name(wells_env, 'State', usa=True, can=True)
# print(wells_env.State.unique())
# # Change the Province names only back into abbreviations
# wells_env['State'] = name2abbrev(wells_env, 'State', usa=False, can=True)
# print(wells_env.State.unique())


