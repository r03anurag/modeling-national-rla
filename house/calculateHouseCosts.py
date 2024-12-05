import pandas as pd
import math
import os

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

# read all dataframes from 2022-2000
data = pd.read_excel("house_election_chart.xlsx", sheet_name="2022")
data.rename(mapper={"Unnamed: 3":'num_ballots'}, axis=1, inplace=True)
cols = data.columns.to_list()
data["year"] = [2022]*len(data)
for yr in range(2020,1999,-2):
    hec = pd.read_excel("house_election_chart.xlsx", 
                        sheet_name=str(yr), names=cols)
    hec["year"] = [yr]*len(hec)
    data = pd.concat([data, hec], axis=0)

# transform the data
# function to resolve any values that are unusual
data["num_ballots"] = data["num_ballots"].fillna(0.0)
data["num_ballots"] = data["num_ballots"].apply(func=math.ceil)
data["Winner (Percentage of Votes)"] = data["Winner (Percentage of Votes)"].fillna(0.0)
data["1st Runner-Up (Percentage of Votes)"] = data["1st Runner-Up (Percentage of Votes)"].fillna(0.0)
data["State"] = data["State and District"].apply(func=lambda sd: sd.split()[0])
# apply our procedural cost model: 
# num_ballots * 1.5min/ballot * 0.35USD/min
data["procedural_cost"] = (data["num_ballots"]*(1.5*0.35)).apply(lambda cs: f"{cs:.2f}")

# add state abbr. data
abbr = pd.read_csv("state_abbr.tsv", sep="\t")
data = pd.merge(left=data, right=abbr, on=['State'], how='inner')
data.drop(columns=['Standard'], inplace=True)
data.rename(mapper={"State": "state", "Postal": "state_po"}, inplace=True, axis=1)
data["state"] = data["state"].str.upper()

# write the final data to csv
data.to_csv("house.csv")
states = set(data.state_po)
yrs = set(data.year)
if not os.path.exists("state-by-state-by-year"):
    os.mkdir("state-by-state-by-year")
for yr in yrs:
    if not os.path.exists(f"state-by-state-by-year/{yr}"):
        os.mkdir(f"state-by-state-by-year/{yr}")
    for st in states:
        stsp = data[(data.state_po==st) & (data.year==yr)]
        stsp.to_csv(f"state-by-state-by-year/{yr}/house_data_{st}.csv")