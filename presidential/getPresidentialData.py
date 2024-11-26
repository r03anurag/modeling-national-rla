import pandas as pd
import os
# pull presidential election data
# data source citations (BibTeX, website)
pd.set_option('display.max_colwidth', None)
#pd.set_option('display.max_rows', None)
'''
@data{DVN/42MVDX_2017,
author = {MIT Election Data and Science Lab},
publisher = {Harvard Dataverse},
title = {{U.S. President 1976-2020}},
UNF = {UNF:6:F0opd1IRbeYI9QyVfzglUw==},
year = {2017},
version = {V8},
doi = {10.7910/DVN/42MVDX},
url = {https://doi.org/10.7910/DVN/42MVDX}
}
'''
"""MIT Election Data and Science Lab, 2017, "U.S. President 1976-2020", 
https://doi.org/10.7910/DVN/42MVDX, Harvard Dataverse, V8, UNF:6:F0opd1IRbeYI9QyVfzglUw== [fileUNF]"""
# for 2024: https://apps.npr.org/2024-election-results/ (data), https://www.scouting.org/resources/los/states/ (state abbr.)

# function that calculates the margins and number of ballots (2000-2020)
def calculate_margins_and_num_ballots_from_2000_to_2020():
    # years we are looking at (2000-2024). We also don't care about independent parties and write-in
    pres_elec_df = pd.read_csv("dataverse_files/1976-2020-president.csv")

    pres_elec_df = pres_elec_df[pres_elec_df["year"] >= 2000]
    pres_elec_df['writein'] = pres_elec_df['writein'].astype(dtype=bool)
    pres_elec_df = pres_elec_df[pres_elec_df["party_simplified"].isin(['DEMOCRAT', 'REPUBLICAN']) & ~pres_elec_df['writein']]

    # get rid of some columns we don't need
    pres_elec_df = pres_elec_df.drop(columns=['notes', 'version', 'party_detailed', 'office', 'state_ic', 'state_cen', 'state_fips', 'candidate'])

    # for each election year from 2000-2024, and state combination, find the margin
    republican = pres_elec_df[pres_elec_df['party_simplified'].str.startswith("R")].drop(columns=['party_simplified'])
    democrat = pres_elec_df[pres_elec_df['party_simplified'].str.startswith("D")].drop(columns=['party_simplified'])
    margin = (republican['candidatevotes'].reset_index(drop=True)-democrat['candidatevotes'].reset_index(drop=True)).abs()/republican['totalvotes'].reset_index(drop=True)
    five_pct_rla_ballots = ((7/margin)+1).astype(int).reset_index(drop=True)
    FINAL = pd.concat([republican['year'].reset_index(drop=True), republican['state'].reset_index(drop=True), republican['state_po'].reset_index(drop=True),
                    margin, five_pct_rla_ballots], ignore_index=True, axis=1).reset_index(drop=True)
    FINAL.rename(mapper={0: "year", 1: "state", 2: "state_abbr", 3: "margin", 4:"num_ballots"}, inplace=True, axis=1)
    return FINAL

# function to extract the data from the text file containing 2024 election data
def extract_textfile_data(state_shorts: pd.DataFrame):
    preselec24 = {"year": [], "state": [], "state_abbr": [], "margin": [], "num_ballots": []}
    lines = []
    with open("dataverse_files/2024_US_President.txt") as datafile:
        for line in datafile:
            lines.append(line.strip().replace("\n",""))
    # Remove any lines that say "Flip", as well as the title lines
    lines = lines[5:]
    flipCount = lines.count("Flip")
    for _ in range(flipCount):
        lines.remove("Flip")
    # we know how many states there are, so assign that many 2024s
    preselec24['year'] = [2024]*51
    # now begins main process
    # 1st line is state (short form)
    # 2nd line is electoral vote [ignore]
    # 3rd line is Harris %
    # 4th line is Trump %
    # 5th line is percent in [ignore]
    for i in range(0, len(lines), 5):
        # get the batch of 5
        sstate, _, dpct, rpct, _ = lines[i:i+5]
        # process 1st line
        sstate_row = state_shorts[state_shorts.Standard == sstate].reset_index()
        # for Maine and Nebrasks, only consider them as whole states, and skip individual districts
        if len(sstate_row) == 0:
            continue
        long, abbr = sstate_row.iloc[0].State, sstate_row.iloc[0].Postal
        preselec24['state'].append(long)
        preselec24['state_abbr'].append(abbr)
        # process 3rd and 4th lines
        thisMargin = eval(f"{dpct.replace("%", "")} - {rpct.replace("%", "")}")
        thisMargin /= 100
        preselec24['margin'].append(abs(thisMargin))
    # we can calculate number of ballots from the margin
    preselec24['num_ballots'] = list(map(lambda marg: int(7/marg)+1, preselec24['margin']))
    preselec24_df = pd.DataFrame(preselec24)
    # for consistency, all state names are uppercase
    preselec24_df['state'] = preselec24_df['state'].str.upper()
    return preselec24_df
        

# 2024 data
def add_margins_and_num_ballots_from_2024(_2000_to_2020: pd.DataFrame):
    # state abbreviations and short forms
    stateShortForms = pd.read_csv("dataverse_files/state_abbr.tsv", sep="\t")
    for col in list(stateShortForms.columns):
        stateShortForms[col] = stateShortForms[col].str.strip()
    # get the resultant dataFrame for 2024 data
    _24_data = extract_textfile_data(state_shorts=stateShortForms)
    _00_to_24_ALL = pd.concat([_2000_to_2020.reset_index(drop=True), _24_data.reset_index(drop=True)], axis=0)
    _00_to_24_ALL.reset_index(inplace=True, drop=True)
    return _00_to_24_ALL

# calculate time needed for each state, as well
# write final results all together, as well as state-by-state
def write_results(FINAL: pd.DataFrame):
    FINAL.to_csv("presidential_margins.csv")
    FINAL_desc = FINAL['num_ballots'].describe()
    FINAL_desc['total'] = FINAL['num_ballots'].sum()
    FINAL_desc.to_csv("presidential_margins_stats.csv")
    if not os.path.exists("state-by-state"):
        os.makedirs("state-by-state")
    for state_abbr in set(FINAL['state_abbr']):
        state_data = FINAL[FINAL['state_abbr'] == state_abbr].reset_index(drop=True)
        state_data.to_csv(f"state-by-state/presidential_margins_{state_abbr}.csv")
        state_data_desc = state_data['num_ballots'].describe()
        state_data_desc['total'] = state_data['num_ballots'].sum()
        state_data_desc.to_csv(f"state-by-state/presidential_margins_stats_{state_abbr}.csv")

if __name__ == '__main__':
    fres = calculate_margins_and_num_ballots_from_2000_to_2020()
    fres = add_margins_and_num_ballots_from_2024(fres)
    write_results(fres)
