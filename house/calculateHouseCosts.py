import pandas as pd
import math
import os
import re

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

# read all dataframes from 2022-2000
def read_data():
    data = pd.read_excel("house_election_chart.xlsx", sheet_name="2022")
    data.rename(mapper={"Unnamed: 3":'num_ballots'}, axis=1, inplace=True)
    cols = data.columns.to_list()
    data["year"] = [2022]*len(data)
    for yr in range(2024,1999,-2):
        if yr != 2022:
            hec = pd.read_excel("house_election_chart.xlsx", 
                                sheet_name=str(yr), names=cols)
            hec["year"] = [yr]*len(hec)
            data = pd.concat([data, hec], axis=0)
    return data

# transform the data
def transform_data(data: pd.DataFrame):
    # function to resolve any values that are unusual
    data["num_ballots"] = data["num_ballots"].fillna(0.0)
    data["num_ballots"] = data["num_ballots"].apply(func=math.ceil)
    data["Winner (Percentage of Votes)"] = data["Winner (Percentage of Votes)"].fillna(0.0)
    data["1st Runner-Up (Percentage of Votes)"] = data["1st Runner-Up (Percentage of Votes)"].fillna(0.0)
    # function to extract the state from State and District
    def get_state(snd: str):
        pat = r".(\d+|at-large)\s*"
        e = re.search(pattern=pat, string=snd)
        if type(e) == re.Match:
            info = snd.split(e.group())
            return info[0].strip()
        else:
            return snd.strip()
    data["State"] = data["State and District"].apply(get_state)
    # apply our procedural cost model: 
    # num_ballots * 1.5min/ballot * 0.35USD/min
    data["procedural_cost"] = data["num_ballots"]*(1.5*0.35)
    # add state abbr. data
    abbr = pd.read_csv("state_abbr.tsv", sep="\t")
    data = pd.merge(left=data, right=abbr, on=['State'], how='inner')
    data.drop(columns=['Standard'], inplace=True)
    data.rename(mapper={"State": "state","Postal": "state_po"}, inplace=True, axis=1)
    data['state'] = data['state'].str.upper()
    # we ignore the districts and simply look at states and years
    for col in data.columns.tolist():
        if col not in ["year", "state", "procedural_cost", "state_po", "num_ballots"]:
            data.drop(columns=[col], inplace=True)
    data = data.groupby(by=['year', 'state', 'state_po']).sum()
    data["margin"] = data["num_ballots"].apply(lambda nb: 7/nb if nb != 0.0 else 0.0)
    data["procedural_cost"] = data["procedural_cost"].apply(lambda cs: f"{cs:.2f}")
    return data

# write the final data to csv
def write_results(data: pd.DataFrame):
    data.to_csv("house_margins.csv")
    new_data = pd.read_csv('house_margins.csv')
    new_data["procedural_cost"] = new_data["procedural_cost"].apply(lambda cs: f"{cs:.2f}")
    states = set(new_data.state_po)
    if not os.path.exists("state-by-state"):
        os.mkdir("state-by-state")
    for st in states:
        stsp = new_data[new_data.state_po==st].reset_index(drop=True)
        stsp.to_csv(f"state-by-state/house_margins_{st}.csv")
    return 

if __name__ == "__main__":
    # main procedure
    hdata = read_data()
    hdata = transform_data(hdata)
    write_results(hdata)