import pandas as pd
from functools import reduce
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

# data sources: 
'''
MIT Election Data and Science Lab, 2017, "U.S. House 1976-2022", 
https://doi.org/10.7910/DVN/IG0UN2, Harvard Dataverse, V13, 
UNF:6:Ky5FkettbvohjTSN/IVldA== [fileUNF]
'''

# function that obtains total number of votes cast per year per state
def get_total_votes_cast():
    # read the data, and get relevant years.
    house_data = pd.read_csv("house/dataverse_files/1976-2022-house.csv")
    house_data = house_data[house_data['year'].between(2000,2022)]
    # drop any unnecessary columns
    house_data_cols = house_data.columns.tolist()
    for hccol in house_data_cols:
        if hccol not in ['year', 'state', 'state_po', 'district', 'totalvotes']:
            house_data.drop(columns=[hccol], inplace=True)
    # drop duplicate rows
    house_data.drop_duplicates(inplace=True)
    house_data.drop(columns=['district'], inplace=True)
    # get the total number of votes
    totals = house_data.groupby(by=['year','state','state_po']).sum()
    return totals

# get all the 3 dataframes
def compute_totals():
    house = pd.read_csv("house/house_margins.csv")
    senate = pd.read_csv("senate/senate_margins.csv")
    president = pd.read_csv("presidential/presidential_margins.csv")
    # perform a full-outer join on them
    allHSP = reduce(lambda L,R: pd.merge(L,R,on=['year','state','state_po'],
                                        how='outer'),[house,senate,president])
    allHSP_cols = allHSP.columns.tolist()
    # drop irrelevant columns
    for acol in allHSP_cols:
        if acol.startswith("margin") or acol.startswith("Unnamed"):
            allHSP.drop(columns=[acol], inplace=True)
    # rename the columns appropriately {'_x': '_house', '_y':'_senate'}
    mapper = {'num_ballots_x':'num_ballots_house',
            'num_ballots_y':'num_ballots_senate',
            'num_ballots':'num_ballots_president',
            'procedural_cost_x':'procedural_cost_house',
            'procedural_cost_y':'procedural_cost_senate',
            'procedural_cost':'procedural_cost_president'}
    allHSP.rename(mapper=mapper, axis=1, inplace=True)
    # any NAs are indicative of an election not existing 
    # for that year in that state, so replace with 0
    allHSP.fillna(value=0.0,inplace=True)
    # compute totals
    allHSP["num_ballots_total"] = allHSP['num_ballots_house'] + \
                                    allHSP['num_ballots_senate'] + \
                                    allHSP['num_ballots_president']
    allHSP["procedural_cost_total"] = allHSP['procedural_cost_house'] + \
                                        allHSP['procedural_cost_senate'] + \
                                        allHSP['procedural_cost_president']
    # typecast columns to the proper type
    allHSP_cols = allHSP.columns.to_list()
    for col in allHSP_cols:
        if col.startswith("num_ballots"):
            allHSP[col] = allHSP[col].astype(int)
    allHSP["procedural_cost_total"] = allHSP["procedural_cost_total"].apply(lambda pc: f"{pc:.2f}")
    # attach total votes data
    totHSP = get_total_votes_cast()
    allHSP = pd.merge(left=allHSP, right=totHSP, how='inner', on=['year','state','state_po'])
    return allHSP

# function that writes results
def write_results(all_data: pd.DataFrame):
    all_data.to_csv("all_data_by_state_yr.csv")
    if not os.path.exists("state-by-state"):
        os.mkdir("state-by-state")
    for st in set(all_data['state_po']):
        ssp = all_data[all_data.state_po == st].reset_index(drop=True)
        ssp.to_csv(f"state-by-state/all_data_{st}.csv")

### main procedure
if __name__ == '__main__':
    # as a precautionary check, generate all data first
    for dt in ['presidential/getPresidentialData.py', 
               'house/calculateHouseCosts.py', 
               'senate/getSenateData.py']:
        dir, script = dt.split('/')
        os.chdir(f"{dir}")
        os.system(f"python3 {script}")
        os.chdir("..")
    totals_data = compute_totals()
    write_results(totals_data)