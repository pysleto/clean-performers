import re

import pandas as pd
import numpy as np
import unidecode


def fill_col(value, alternative):
    """
    This function replaces an unavailable value by an alternative
    """

    if value == '':
        value = alternative

    return value


def clean(chunk, legal_status_std, country_2did_std):
    """
    This function standardizes the format of a company_name in order to enable matching
    """

    # Normalize unicode to ascii
    chunk['name_out'] = chunk['input_name'].str.normalize('NFD')
    chunk['name_out'] = chunk['name_out'].str.encode('ASCII', 'ignore')
    chunk['name_out'] = chunk['name_out'].str.decode("utf-8")

    # Format in lower case
    chunk['name_out'] = chunk['name_out'].str.upper()

    # Add a space at the beginning and at the end
    chunk['name_out'] = ' ' + chunk['name_out'] + ' '

    # Add a space after opening parenthesis and before closing ones
    chunk['name_out'] = chunk['name_out'].str.replace('[(]+', '( ', regex=True)
    chunk['name_out'] = chunk['name_out'].str.replace('[)]+', ' )', regex=True)

    # remove special characters
    chunk['name_out'] = chunk['name_out'].str.replace('[^A-Za-z0-9() ]+', ' ', regex=True)

    # remove multiple spaces
    chunk['name_out'] = chunk['name_out'].str.replace(' +', ' ', regex=True)

    # print(chunk)

    # Remove abbreviation (u.)
    # chunk['abbreviation'] = chunk['name_out'].str.findall('\s([\S]+)\.')
    # chunk['name_out'] = chunk['name_out'].str.replace('\s([\S]+)\.', '', regex=True)

    # TODO: Remove and, und, ... when in between two spaces and replace by a single space

    # Standardize company legal status (SA, LTD, GMBH ...)
    # TODO: Isolate in separate field, Remove if multiples, Place at the end

    chunk['legal_status'] = chunk['name_out'].str.extract(
        '({})'.format('|'.join(legal_status_std.keys())),
        expand=True
    )

    chunk['name_out'] = chunk['name_out'].str.replace(
        '({})'.format('|'.join(legal_status_std.keys())),
        ' ',
        regex=True
    )

    chunk['name_out'] = np.where(chunk['legal_status'].isna(),
                                 chunk['name_out'],
                                 chunk['name_out'] + chunk['legal_status'].map(legal_status_std)
                                 )

    # Standardize country_2did
    for country_2did, country_2did_iso in country_2did_std.items():
        chunk['country_out'] = chunk['input_country'].str.replace(country_2did, country_2did_iso, regex=False)

    # DO NOT Remove company legal status (SA, LTD, GMBH ...) / only for phonetic matching

    # Remove every sequence that is inside two parenthesis
    chunk['parenthesis'] = chunk['name_out'].str.findall('\(([^)]+)\)')
    chunk['name_out'] = chunk['name_out'].str.replace('\(([^)]+)\)', ' ', regex=True)

    # remove multiple spaces
    chunk['name_out'] = chunk['name_out'].str.replace(' +', ' ', regex=True)

    # print(chunk)

    return chunk

# 2. SGML and HTML codes substituted by the ASCII/ANSI equivalent, such as for example �&OACUTE;� replaced by �O� etc.
# 6. The conjunction �and� and its translations into other languages are standardized as �&�.
# 7. Umlaut harmonization by reducing variations such as �ue�, �ae�, and �oe� to respectively �u�, �a�, and �o�.

