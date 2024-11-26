import pandas as pd
import os
# pull senate election data
# data source citations (BibTeX, website)
pd.set_option('display.max_colwidth', None)
#pd.set_option('display.max_rows', None)
'''
@data{DVN/PEJ5QU_2017,
author = {MIT Election Data and Science Lab},
publisher = {Harvard Dataverse},
title = {{U.S. Senate statewide 1976-2020}},
UNF = {UNF:6:NFZ83YH7C/fCm6x0stmMwA==},
year = {2017},
version = {V7},
doi = {10.7910/DVN/PEJ5QU},
url = {https://doi.org/10.7910/DVN/PEJ5QU}
}
'''
"""MIT Election Data and Science Lab, 2017, "U.S. Senate statewide 1976-2020", 
https://doi.org/10.7910/DVN/PEJ5QU, Harvard Dataverse, V7, UNF:6:NFZ83YH7C/fCm6x0stmMwA== [fileUNF]"""

# function that calculates the margins and number of ballots (2000-2020)
def calculate_margins_and_num_ballots_from_2000_to_2020():
    # years we are looking at (2000-2024). We also don't care about independent parties and write-in
    sen_elec_df = pd.read_csv("dataverse_files/1976-2020-senate.csv")

    sen_elec_df = sen_elec_df[sen_elec_df["year"] >= 2000]
    sen_elec_df['writein'] = sen_elec_df['writein'].astype(dtype=bool)
    sen_elec_df = sen_elec_df[sen_elec_df["party_simplified"].isin(['DEMOCRAT', 'REPUBLICAN']) & ~sen_elec_df['writein']]

    # get rid of some columns we don't need
    sen_elec_df = sen_elec_df.drop(columns=['notes', 'version', 'party_detailed', 'office', 'state_ic', 'state_cen', 'state_fips', 'candidate'])

    '''# for each election year from 2000-2024, and state combination, find the margin
    republican = sen_elec_df[sen_elec_df['party_simplified'].str.startswith("R")].drop(columns=['party_simplified'])
    democrat = sen_elec_df[sen_elec_df['party_simplified'].str.startswith("D")].drop(columns=['party_simplified'])
    margin = (republican['candidatevotes'].reset_index(drop=True)-democrat['candidatevotes'].reset_index(drop=True)).abs()/republican['totalvotes'].reset_index(drop=True)
    five_pct_rla_ballots = ((7/margin)+1).astype(int).reset_index(drop=True)
    FINAL = pd.concat([republican['year'].reset_index(drop=True), republican['state'].reset_index(drop=True), republican['state_po'].reset_index(drop=True),
                    margin, five_pct_rla_ballots], ignore_index=True, axis=1).reset_index(drop=True)
    FINAL.rename(mapper={0: "year", 1: "state", 2: "state_abbr", 3: "margin", 4:"num_ballots"}, inplace=True, axis=1)
    return FINAL'''
    pass