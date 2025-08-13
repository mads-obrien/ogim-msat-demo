# -*- coding: utf-8 -*-
#====================================================================================================================

def create_translation_dict(df, vars2trans, api_key, target, source = None):
    """Translates string phrases from within a dataframe into a target language 
    using Google Translate API (Basic Edition), and returns a dictionary of 
    translated phrase pairs.
    @author: maobrien
    
    Requires'google-cloud-translation' Python client library to be installed. 
    See https://cloud.google.com/translate/docs/reference/libraries/v2/python
    
    PARAMETERS:
        df = dataframe, in original langauge
        vars2trans = list of variable names in the specified dataframe that contain values to translate
        target = (string) target language code; "en" for English
        api_key = filepath to location of Google API key on user's machine
        source = *optional* (string) source language code; "es" for Spanish, "fr" for French, etc.
            If no value is provided, language will be auto-detected by the API. 
            (The detected language is printed to the console.)
            *NOTE that it is possible for different languages to be detected for each phrase,
            so use auto-detection with caution.*
        
        'target' and 'source' parameters must be an ISO 639-1 language code.
        For codes, see https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
        https://g.co/cloud/translate/v2/translate-reference#supported_languages
    
    OUTPUT:
        Dictionary object, where keys contain strings from original 
        dataset/language, and values contain translated version
        
    TO DO as of March 25 2022:
    (1) Add better error handling / error messages
    (2) Skip the attempted translation of NoneType objects (DONE)
    (3) Error handling of attempted translation of NaN cells
    """
    import six
    import os
    from datetime import datetime
    from google.cloud import translate_v2 as translate
    import pandas as pd
    
    starttime = datetime.now()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = api_key
    # "C:\\Users\\maobrien\\OneDrive - MethaneSAT, LLC\\assets\\inlaid-fx-323500-c652a9545194.json"
    # instantiates a google cloud client
    translate_client = translate.Client()
    
    # create an empty dictionary, to fill with keys to translate
    outdict = {}
    
    # if vars2trans is only one attribute (string), convert the string to a one-item list for looping
    if type(vars2trans) != list:
        vars2trans = [vars2trans]
    
    # record all unique string values within desired attribute columns
    for v in vars2trans: # for each column...
        for u in df[v].unique(): #unique() returns a numpy array
            if pd.isnull(u) == False: 
                outdict[u] = None  #adds a new key to the dictionary, with value pair 'None'
            else:
                continue
    
    # Use Google Translate API to add a value (translated text) 
    #  to each key (original language text) in dictionary
    for key in outdict.keys():
        # Converts key text to Unicode (if it's not already), assuming utf-8 input
        if isinstance(key, six.binary_type): 
            key = key.decode("utf-8")
        # **THE TRANSLATION STEP... This is the one that costs money **
        result = translate_client.translate(key, target_language=target, source_language=source)
        outdict[key] = result["translatedText"]
    
    print("Dictionary created")
    if source==None:
        print(u'Detected source language: {}'.format(result['detectedSourceLanguage']))
    else:
        print("Specified source language: "+source)
    print("Runtime H:M:S")
    print(datetime.now() - starttime)
    return outdict

#====================================================================================================================

def translate_dataset(df, vars2trans, mydict):
    """Replaces each occurrence of specific string phrases in a dataframe with 
    a new string phrase, based on a dictionary of phrase + translation pairs.
    @author: maobrien
    
    PARAMETERS:
        df = dataframe, in original language
        vars2trans = list of variable names in the specified dataframe that contain values to translate
        mydict = dictionary of phrase + translation pairs, i.e. old + new values
        
        'target' and 'source' must be an ISO 639-1 language code.
        For codes, see https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
        https://g.co/cloud/translate/v2/translate-reference#supported_languages
    
    OUTPUT:
        Copy of original dataframe where cell values in the specified columns 
        have been replaced with translations.
        Attributes not specified in 'vars2trans' will not be altered.
        
    TO DO as of March 25 2022:
    (1) Add better error handling / error messages
    """
    # Create copy of dataset; the copy will be overwritten, preserving the original
    df_trans = df.copy()
    
    # if vars2trans is only one attribute (string), convert the string to a one-item list for looping
    if type(vars2trans) != list:
        vars2trans = [vars2trans]
    
    # Map the "new" translated strings to replace "old" original strings in a given column
    for v in vars2trans:
        df_trans[v] = df_trans[v].map(mydict)
    
    return df_trans
    

def translate_argentina_installations_fac_types(gdf,
                                                fac_type_col_spanish='TIPO'):
    '''Replace exact matches in a Spanish facility type field with English translations.

    Parameters
    ----------
    gdf : GeoPandas GeoDataFrame
        GeoDataFrame containing Argentina O&G infrastructure data from EITHER
        1) instalaciones-hidrocarburos-instalaciones-res-318-shp.shp, OR
        2) instalaciones-hidrocarburos-instalaciones-res-319-93-p-caracteristicos--shp.shp
    fac_type_col_spanish : str
        Name of the attribute column that contains the Spanish values for
        facility type that you wish to translate. In both datasets (1) and (2)
        above, the name of this field is 'TIPO'.

    Returns
    -------
    output_series : Pandas Series
        Series that can be assigned to a new column in the original GeoDataFrame.

    Example Usage
    -------
    filepath = r"path\\to\\instalaciones-hidrocarburos-instalaciones-res-318-shp.shp"
    facilities_318 = read_spatial_data(filepath, specify_encoding=True, data_encoding="utf-8")
    facilities_318['new_fac_type'] = translate_argentina_installations_fac_types(facilities_318,
                                                                                 fac_type_col_spanish='TIPO')
    '''
    type_dict_argentina = {
        'BATERIA DE GAS (ALTA PROPORCIÓN DE GAS)': 'GAS BATTERY',
        'BATERIA DE PETRÓLEO (ALTA PROPORCIÓN PETRÓLEO)': 'OIL BATTERY',
        'BATERIA DE PETRÓLEO/GAS (SIMILAR PROPORCIÓN PETRÓLEO/GAS)': 'OIL BATTERY',
        'Baterías': 'TANK BATTERY',
        'Cargadero de camiones': 'TRUCK LOADING DOCK',
        'COLECTOR DE AGUA (ALTA PROPORCIÓN DE AGUA)': 'WATER COLLECTOR',
        'COLECTOR DE GAS (ALTA PROPORCIÓN DE GAS)': 'GAS MANIFOLD',
        'COLECTOR DE PETRÓLEO (ALTA PROPORCIÓN PETRÓLEO)': 'CRUDE OIL COLLECTOR',
        'COLECTOR DE PETRÓLEO/GAS (SIMILAR PROPORCIÓN PETRÓLEO/GAS)': 'OIL AND GAS COLLECTOR',
        'ESTACION DE BOMBEO': 'FIRE STATION',
        'Evacuación Fuera de Concesión/Provincia': 'EVACUATION OUTSIDE CONCESSION/PROVINCE',
        'Gasoducto': 'PIPELINE',
        'INTERCONEXION': 'INTERCONNECTION',
        'MONOBOYA - OFFSHORE': 'Monoboya - Offshore',
        'Oleoducto': 'PIPELINE',
        'OTROS': 'N/A',
        'PLANTA ACONDICIONADORA DE GAS': 'Gas conditioning plant',
        'Planta Acondicionamiento': 'CONDITIONING PLANT',
        'PLANTA CAPTACION DE AGUA': 'Water Catchment Plant',
        'PLANTA COMPRESORA': 'COMPRESSOR PLANT',
        'Planta de Almacenaje de Líquidos del Gas': 'GAS LIQUIDS STORAGE PLANT',
        'Planta de Almacenaje de Petróleo': 'OIL STORAGE PLANT',
        'PLANTA DE EMPAQUE': 'Packaging plant',
        'Planta de Gas': 'GAS PLANT',
        'PLANTA DE PROCESAMIENTO': 'PROCESSING PLANT',
        'PLANTA DE TRATAMIENTO DE AGUA': 'WATER TREATMENT PLANT',
        'PLANTA DESHIDRATADORA': 'Dehydrating plant',
        'PLANTA EFLUENTES': 'Effluent plant',
        'PLANTA ENDULZAMIENTO DE GAS': 'Gas sweetening plant',
        'PLANTA GLP': 'LPG plant',
        'PLANTA INYECCION AGUA DULCE': 'FRESH WATER INJECTION PLANT',
        'PLANTA INYECCION AGUA SALADA': 'SALT WATER INJECTION PLANT',
        'PLANTA INYECTORA DE GAS': 'Gas injection plant',
        'PLANTA RECUPERADORA': 'Recovery plant',
        'PLANTA REGULADORA': 'Regulatory plant',
        'Planta Separación': 'GAS SEPARATION PLANT',
        'PLANTA SEPARADORA DE GAS': 'GAS SEPARATION PLANT',
        'PLANTA TRATAMIENTO DE CRUDO': 'OIL TREATMENT PLANT',
        'PLANTA TRATAMIENTO DE GAS': 'GAS TREATMENT PLANT',
        'PLATAFORMA': 'PLATFORM',
        'Plataformas Off Shore': 'OFFSHORE PLATFORM',
        'Poliducto': 'PIPELINE',
        'Pozos Gas Plus': 'GAS WELL',
        'PTC': 'PTC',
        'PUNTO DE CARGA': 'CHARGING POINT',
        'PUNTO DE MEDICION  DE GASOLINA Y CONDENSADO': 'Gasoline and condensate measurement point',
        'PUNTO DE MEDICION  DE GLP': 'LPG measurement point',
        'PUNTO DE MEDICION FISCAL DE CRUDO (UNIDAD LACT)': 'CRUDE FISCAL MEASUREMENT POINT (LACT)',
        'PUNTO DE MEDICION FISCAL DE GAS (PIST)': 'GAS FISCAL MEASUREMENT POINT (PIST)',
        'Punto de Medición Fiscal de Gas': 'GAS FISCAL MEASUREMENT POINT',
        'SATÉLITE INYECTOR DE AGUA': 'SATELLITE WATER INJECTOR',
        'SATÉLITE INYECTOR DE GAS': 'SATELLITE GAS INJECTOR',
        'SCADA': 'SCADA',
        'SCRAPPER': 'SCRAPPER',
        'SRYTD': 'SRYTD',
        'TANQUE': 'TANK',
        'TERMINAL MARITIMA': 'NAVY TERMINAL',
        'Unidad LACT': 'LEASE AUTOMATIC CUSTODY TRANSFER UNIT',
        'VALVULA DE BLOQUEO': 'BLOCK VALVE',
        'VALVULA DE PURGA': 'PURGE VALVE',
        'VALVULA': 'VALVE',
        'Venteos': 'VENTS'
    }

    print(f'\nOriginal contents of {fac_type_col_spanish} field:\n-------')
    print(gdf[fac_type_col_spanish].value_counts())

    output_series = gdf[fac_type_col_spanish].replace(type_dict_argentina)

    print('\nContents of new, translated facility type field:\n-------')
    print(output_series.value_counts())

    return output_series


def replace_special_chars_in_column_argentina(df, col_name):

    replace_special_chars = {'Ã©': 'é',
                             'Ã¡': 'á',
                             'Ã³': 'ó',
                             'Ã±': 'ñ',
                             'Â°': 'ð',
                             # 'Ã¼': ???????  # FIXME
                             'Ã\x81': 'Á',  # valvula  # FIXME
                             'Ã\x89': 'É',
                             # 'Ã\x91': ???????,  # n tilde?  # FIXME
                             'Ã\x93': 'Ó',
                             'Ã\x8d': 'Í',
                             'Ã\xad': 'í',
                             'lÃ­quido': 'lí­quido',
                             'LÃ­quidos': 'Lí­quidos',
                             'BaterÃ­a': 'Baterí­a',
                             'MarÃ­tima': 'Marí­tima',
                             ' RÃ­o ': ' Río ',
                             'LogÃ­stica': 'Logí­stica',
                             ' LÃ­q. ': ' Líq. ',
                             'EnergÃ­a': 'Energí­a',
                             'PolÃ­meros': 'Polí­meros',
                             'RefinerÃ­a': 'Refinerí­a'
                             # 'VÃ­as': ??????  # FIXME
                             }

    series = df[col_name].replace(replace_special_chars, regex=True)
    return series
