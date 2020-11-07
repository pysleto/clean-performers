import re
import unicodedata


def fill_up(value, alternative):
    """
    This function replaces an unavailable value by an alternative
    """

    if value == '':
        value = alternative

    return value


def clean_name(name):
    """
    This function standardizes the format of a company_name in order to enable matching
    """
    # normalize unicode to ascii
    normal = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore')

    cleaned_name = normal.decode("utf-8")

    # formats in lower case
    cleaned_name = cleaned_name.lower()

    # TODO: Remove every sequence that is inside two paranthesis

    # TODO: remove abbreviation (u.)

    # TODO: Remove and, und, ... when in between two spaces and replace by a single space

    # remove special characters
    cleaned_name = re.sub('[^A-Za-z0-9 ]+', ' ', cleaned_name)

    # DO NOT Remove company legal status (SA, LTD, GMBH ...) / only for phonetic matching

    # remove multiple spaces
    cleaned_name = re.sub(' +', ' ', cleaned_name)

    return cleaned_name

