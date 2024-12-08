import pandas as pd
import os
from functools import reduce
import re
import numpy as np
# pull senate election data
# data source citations (BibTeX, website)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)
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

# 2024 senate data: https://www.nbcnews.com/politics/2024-elections/senate-results
# 2022 senate data: https://ballotpedia.org/United_States_Senate_elections,_2022 
# state abbr. table:  https://www.scouting.org/resources/los/states/ 

# function to prepare 2024 senate data
def prepare_2024_senate_data():
    # processing of data
    datafile = open("dataverse_files/2024_senate.txt")
    data = datafile.read()
    data = np.array(data.split("\n"))
    datafile.close()

    # split apart states and percents
    is_state = np.array([type(re.search(pattern=r"\d+\.\d+%", string=dp))!=re.Match for dp in data])
    states = np.char.upper(data[is_state])
    pcts = np.char.replace(data[~is_state], "%", "").astype(float)/100
    margins = np.abs((pcts[::3]-pcts[1::3]))
    num_ballots = np.ceil(7/margins).astype(int)

    # add state abbreviations
    state_abbr = pd.read_csv("dataverse_files/state_abbr.tsv", sep="\t")
    state_abbr["State"] = state_abbr["State"].str.upper()
    sabbr = list(map(lambda st: state_abbr[state_abbr['State']==st]['Postal'].item(), states))

    # create dataFrame
    final_df_data = {"year": [2024]*len(margins), "state": states, "state_po": sabbr, 
                             "margin": margins, "num_ballots": num_ballots}
    final_df = pd.DataFrame(final_df_data)
    return final_df

# function that calculates the margins and number of ballots (2000-2020)
def calculate_margins_and_num_ballots_from_2000_to_2020():
    # years we are looking at (2000-2024). We also don't care about independent parties and write-in
    sen_elec_df = pd.read_csv("dataverse_files/1976-2020-senate.csv")

    sen_elec_df = sen_elec_df[sen_elec_df["year"].between(2000,2020)]
    sen_elec_df['writein'] = sen_elec_df['writein'].astype(dtype=bool)
    sen_elec_df = sen_elec_df[(sen_elec_df["party_simplified"] != "OTHER") & ~sen_elec_df['writein']]

    # drop some unnecessary columns
    sen_elec_df.drop(columns=['mode', 'writein', 'district', 'special', 'unofficial', 'stage', 'version', 'party_detailed', 'office', 'state_ic', 'state_cen', 'state_fips', 'candidate'], inplace=True)
    
    # for each election year from 2000-2020, and state combination, find the margin
    republican = sen_elec_df[sen_elec_df['party_simplified'].str.startswith("R")].groupby(by=['year','state','state_po']).max()
    democrat = sen_elec_df[sen_elec_df['party_simplified'].str.startswith("D")].groupby(by=['year','state','state_po']).max()
    libertaraian = sen_elec_df[sen_elec_df['party_simplified'].str.startswith("L")].groupby(by=['year','state','state_po']).max()
    all3 = reduce(lambda L,R: pd.merge(L,R,on=['year','state','state_po'],how='outer'),[republican,democrat,libertaraian])
    all3 = all3.rename(mapper=dict(zip(['writein_x', 'candidatevotes_x', 'totalvotes_x',
                                        'party_simplified_x', 'writein_y', 'candidatevotes_y',
                                        'totalvotes_y', 'party_simplified_y', 'writein',
                                        'candidatevotes', 'totalvotes', 'party_simplified'],
                                        ['writein_r', 'candidatevotes_r', 'totalvotes_r',
                                        'party_simplified_r', 'writein_d', 'candidatevotes_d',
                                        'totalvotes_d', 'party_simplified_d', 'writein_l',
                                        'candidatevotes_l', 'totalvotes_l', 'party_simplified_l'])), axis=1)
    for pt in "rdl":
        all3[f"candidatevotes_{pt}"] = all3[f"candidatevotes_{pt}"].replace(float("nan"), 0)
        all3[f"totalvotes_{pt}"] = all3[f"totalvotes_{pt}"].replace(float("nan"), 0)
        all3[f"{pt}pct"] = all3[f"candidatevotes_{pt}"]/all3[f"totalvotes_{pt}"]
        all3[f"{pt}pct"] = all3[f"{pt}pct"].replace(float("nan"), 0)
    all3["margin"] = (all3['rpct']-all3['dpct']-all3['lpct']).abs()
    all3["num_ballots"] = np.ceil(7/all3["margin"]).astype(int)
    for col in all3.columns.to_list():
        if col not in ["margin", "num_ballots"]:
            all3.drop(columns=[col], inplace=True)
    all3.to_csv("dataverse_files/senate_margins_0020.csv")
    return    

# function to prepare 2022 senate data
def prepare_2022_senate_data():
    # read in csv, and remove irrelevant rows
    data = pd.read_csv("dataverse_files/2022_senate.tsv", sep="\t")
    data = data[~data["State"].str.contains(pat="special", case=False)]
    
    # the only columns we need are state and margin
    cols = data.columns.tolist()
    for col in cols:
        if col not in ['State', 'Margin(%)']:
            data.drop(columns=[col], inplace=True)

    # transfrom the data appropriately
    data['State'] = data['State'].str.replace(pat="U.S. Senate, ", repl="").str.upper()
    data['year'] = [2022]*len(data)
    data['Margin(%)'] = data['Margin(%)'].str.replace(pat="%", repl="").astype(float)/100
    data['num_ballots'] = np.ceil(7/data['Margin(%)']).astype(int)

    # add state abbreviations
    state_abbr = pd.read_csv("dataverse_files/state_abbr.tsv", sep="\t")
    state_abbr["State"] = state_abbr["State"].str.upper()
    data = pd.merge(data, state_abbr, how='inner', on=['State'])

    # rename columns appropriately, and drop unnecessary columns
    data.drop(columns=["Standard"], inplace=True)
    data = data.rename(mapper={'State':'state', 'Margin(%)':'margin', "year": "year", 
                               "num_ballots": "num_ballots", "Postal": "state_po"}, axis=1)
    # final data
    return data

# function that applies the procedural cost model
# Procedural Total: (# of ballots needed for RLA *  "minutely" wage of county clerk * time per ballot)
def procedural_cost(nbals: int):
    minutesWage = 0.35
    minutes_balTime = 2.
    return nbals*minutesWage*minutes_balTime

def join_data_and_add_procedural_cost(df22: pd.DataFrame, df24: pd.DataFrame):
    # re-read 2000-20
    _00_to_20 = pd.read_csv("dataverse_files/senate_margins_0020.csv")
    # simply concatenate the other 2 as is
    final = pd.concat([_00_to_20, df22, df24], axis=0).reset_index(drop=True)
    # add the procdural cost according to our model
    final["procedural_cost"] = final["num_ballots"].apply(lambda nb: procedural_cost(nbals=nb)).round(2)
    return final

def write_results(allData: pd.DataFrame):
    allData.to_csv("senate_margins.csv")
    if not os.path.exists("state-by-state"):
        os.mkdir("state-by-state")
    state_abbrs = set(allData['state_po'].to_list())
    for sa in state_abbrs:
        state_specific = allData[allData['state_po']==sa].reset_index(drop=True)
        state_specific.to_csv(f"state-by-state/senate_margins_{sa}.csv")
    return

# main process
if __name__ == "__main__":
    # 2000-2020 senate data
    calculate_margins_and_num_ballots_from_2000_to_2020()
    # obtain 2024 data
    df24 = prepare_2024_senate_data()
    # obtain 2022 senate 
    df22 = prepare_2022_senate_data()
    # add the two dataframs to the 2000-20 df and calculate procedural cost
    full_data = join_data_and_add_procedural_cost(df22=df22,df24=df24)
    # add the total df to the files directory
    # additionally calculate state specific margins
    write_results(allData=full_data)