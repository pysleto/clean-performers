"""
This program loads company identification from PATSTAT csv file extract and prepares data set for matching.
"""

import os

from pathlib import Path

import pandas as pd

from database.scripts.standardize import clean

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from tabulate import tabulate

from config import cfg

folder = Path(r"V:\JRC.B.1 - Green patents")

# Load accented_char dictionary
with open(os.path.join(folder, "input", r"legal_status.csv"), 'rb') as src:
    df = pd.read_csv(src, encoding='UTF-8', usecols=['ext_full_clean', 'ext_short_clean'], index_col='ext_full_clean')
    df.index = df.index.str.normalize('NFKD')
    legal_status_std = df.to_dict(orient='dict')['ext_short_clean']

with open(os.path.join(folder, "input", r"country_2DID.csv"), 'rb') as src:
    df = pd.read_csv(src, encoding='UTF-8', usecols=['country_2DID', 'country_2DID_iso'], index_col='country_2DID')
    country_2DID_std = df.to_dict(orient='dict')['country_2DID_iso']

if not Path(os.path.join(folder, "Patstat2019b", "cleaned_chunk_0.csv")).exists():

    with open(os.path.join(folder, "input", r"Patstat2019b_List of unique companies with patents.csv"), 'rb') as src:
        df = pd.read_csv(src, chunksize=50000, encoding='utf_8')

        for i, chunk in enumerate(df):

            # chunk_copy = chunk[chunk['input_name'] == 'Société SYLEA (Société Anonyme de droit français)'].copy()
            # chunk_copy = chunk[chunk['JRC_name'] == 'VORTEX GMBH  CO SYSTEMTECHNIK GMBH'].copy()

            cleaned_chunk = clean(chunk, legal_status_std, country_2DID_std)

            cleaned_chunk.to_csv(
                os.path.join(folder, "Patstat2019b", "cleaned_chunk_" + str(i) + ".csv"), index=False
            )

if not Path(os.path.join(folder, "B1_Orbis", "cleaned_company_list.csv")).exists():

    with open(os.path.join(folder, "input", r"B1_ORBIS_List of companies to match.csv"), 'rb') as src:
        df = pd.read_csv(
            src,
            dtype=str,
            encoding='utf_8')

        cleaned_df = clean(df, legal_status_std, country_2DID_std)

        cleaned_df.to_csv(os.path.join(folder, "B1_Orbis", "cleaned_company_list.csv"), index=False)


# Load current match
current_match = pd.DataFrame()

# Load cleaned B1_ORBIS list
orbis_ids = pd.read_csv(
    os.path.join(folder, "B1_Orbis", "cleaned_company_list.csv"),
    na_values='#N/A',
    dtype=str,
    encoding='UTF-8'
)

to_search_in_count = len(os.listdir(os.path.join(folder, "Patstat2019b"))) - 1

for file_count, chunk_name in enumerate(os.listdir(os.path.join(folder, "Patstat2019b")), start=1):

    # Load cleaned Patstat chunk
    patstat_ids = pd.read_csv(
        os.path.join(folder, "Patstat2019b", chunk_name),
        na_values='#N/A',
        # nrows=10,
        dtype=str,
        encoding='UTF-8'
    )

    patstat_ids.set_index('name_out', inplace=True)

    to_search_for_count = patstat_ids.index.value_counts().sum()

    for match_count, name_to_match in enumerate(patstat_ids.index.values[1:], start=1):

        match_ids = patstat_ids[patstat_ids.index == name_to_match].copy()

        # is_match = False

        print(
            tabulate([[
                # str(i_match) + ' matches',
                str(file_count) + ' / ' + str(to_search_in_count) + ' files & ' + str(match_count) + ' / ' + str(to_search_for_count) + ' steps',
                # 'Is a match: ' + str(is_match),
                'Input: ' + str(name_to_match),
                # 'Outputs:' + match[0],
                # '(' + str(match[1]) + ')'
            ]], tablefmt="plain")
        )

        # print('... fuzz.ratio')

        ratio_match = process.extractOne(
            name_to_match, orbis_ids['name_out'], scorer=fuzz.ratio
        )

        # print('... fuzz.partial_ratio')
        #
        # partial_ratio_match = process.extractOne(
        #     name_to_match, orbis_ids['name_out'], scorer=fuzz.partial_ratio
        # )
        #
        # print('... fuzz.token_sort_ratio')
        #
        # token_sort_ratio_match = process.extractOne(
        #     name_to_match, orbis_ids['name_out'], scorer=fuzz.token_sort_ratio
        # )
        #
        # print('... fuzz.token_set_ratio')
        #
        # token_set_ratio_match = process.extractOne(
        #     name_to_match, orbis_ids['name_out'], scorer=fuzz.token_set_ratio
        # )

        # fuzz.ratio
        # fuzz.partial_ratio
        # fuzz.token_sort_ratio
        # fuzz.token_set_ratio

        match_ids['ratio_name'] = ratio_match[0]
        match_ids['ratio_rate'] = ratio_match[1]
        # patstat_ids.loc[name_to_match, 'partial_ratio_name'] = partial_ratio_match[0]
        # patstat_ids.loc[name_to_match, 'partial_ratio_rate'] = partial_ratio_match[1]
        # patstat_ids.loc[name_to_match, 'token_sort_ratio_name'] = token_sort_ratio_match[0]
        # patstat_ids.loc[name_to_match, 'token_sort_ratio_rate'] = token_sort_ratio_match[1]
        # patstat_ids.loc[name_to_match, 'token_set_ratio_name'] = token_set_ratio_match[0]
        # patstat_ids.loc[name_to_match, 'token_set_ratio_rate'] = token_set_ratio_match[1]

        match_ids.reset_index(inplace=True)

        match_ids = pd.merge(
            match_ids,
            orbis_ids,
            left_on='ratio_name', right_on='name_out',
            how='left',
            # suffixes=(False, False)
            suffixes=('_patstat', '_orbis')
        )

        match_ids['country_check'] = match_ids['country_out_patstat'] == match_ids['country_out_orbis']
        match_ids['chunk_name'] = chunk_name

        current_match = current_match.append(match_ids[
                                                 (match_ids['ratio_rate'] >= 95) & (match_ids['country_check'] == True)
                                             ])

    current_match.to_csv(
        os.path.join(folder, "current_match.csv"),
        float_format='%.10f',
        index=False,
        na_rep='#N/A'
    )

