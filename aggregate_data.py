import pandas as pd
from functools import reduce
import os
import matplotlib.pyplot as plt

pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

# data sources: 
'''
MIT Election Data and Science Lab, 2017, "U.S. House 1976-2022", 
https://doi.org/10.7910/DVN/IG0UN2, Harvard Dataverse, V13, 
UNF:6:Ky5FkettbvohjTSN/IVldA== [fileUNF]
'''
# 2024 US election totals
# https://www.bbc.com/news/articles/cvglg3klrpzo

# House percentages:
# https://www.nbcnews.com/politics/{year}-elections/house-results 

# number of counties by state:
# https://wisevoter.com/state-rankings/states-with-the-most-counties/ 

# function that transforms raw 2024 results
def transform_2024_results():
    # 2024 House totals - based on presidential totals. Transform appropriately
    house24 = pd.read_csv("presidential/dataverse_files/2024_votes.tsv", sep="\t")
    house24['State'] = house24['State'].str.upper()
    house24 = house24[(house24["Party"].str.contains("Democrat")) | (house24["Party"].str.contains("Republican"))]
    house24["Votes"] = house24["Votes"].str.replace(",","").astype(int)
    house24['year'] = [2024]*len(house24)
    house24.rename(mapper={'State':'state','Votes':'totalvotes'}, inplace=True, axis=1)
    h24cols = house24.columns.to_list()
    for hcl in h24cols:
        if hcl not in ['state', 'totalvotes','year']:
            house24.drop(columns=[hcl], inplace=True)    
    house24 = house24.groupby(by=['state','year']).sum()
    house24.to_csv("intermediate_data/2024_votes_transformed.csv")
    return 

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
    # obtain 2024 data, and add state abbreviation data
    transform_2024_results()
    h24 = pd.read_csv("intermediate_data/2024_votes_transformed.csv")
    h24['state_po'] = h24['state'].apply(lambda st: set(house_data[house_data.state==st].state_po).pop())
    # stack with current data
    house_data = pd.concat(objs=[house_data,h24],axis=0)
    # get the total number of votes
    totals = house_data.groupby(by=['year','state','state_po']).sum()
    totals.to_csv("intermediate_data/totals.csv")
    return

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
    allHSP["procedural_cost_excl_pres"] = allHSP['procedural_cost_house'] + allHSP['procedural_cost_senate']
    # typecast columns to the proper type
    allHSP_cols = allHSP.columns.to_list()
    for col in allHSP_cols:
        if col.startswith("num_ballots"):
            allHSP[col] = allHSP[col].astype(int)
    # attach total votes data
    get_total_votes_cast()
    totHSP = pd.read_csv("intermediate_data/totals.csv")
    allHSP = pd.merge(left=allHSP, right=totHSP, how='outer', on=['year','state','state_po'])
    allHSP.fillna(0.0, inplace=True)
    return allHSP

# function that will calculate total prep costs
# Prep Total (for each state): 8 * clerk_wage/hr * num_counties + num_ballots_cast/500 * time_to_index
def add_prep_costs(tot: pd.DataFrame):
    # number of counties by state, transform appropriately.
    counties = pd.read_csv("counties_by_state.tsv", sep="\t")
    counties['State'] = counties['State'].str.upper()
    counties.drop(columns=["#"], inplace=True)
    # apply number of counties data to each states
    tot["num_counties"] = tot["state"].apply(lambda st: counties[counties.State==st]["Total Number of Counties"].item())
    clerk_wage_hr = 21.23
    hrs_to_idx = 2
    # add prep total
    tot["prep_cost_total"] = (8*clerk_wage_hr*tot["num_counties"]) + ((tot['totalvotes']/500)*hrs_to_idx)
    return tot

# function that will add central costs
# constant value of 33580
def add_central_costs(tot: pd.DataFrame):
    centrals = [33580]*len(tot)
    tot["central_cost_total"] = centrals
    return tot

# function that adds all 3 costs together
def add_all3_costs():
    totals_data = compute_totals()
    totals_data = add_prep_costs(tot=totals_data)
    totals_data = add_central_costs(tot=totals_data)
    totals_data["cost_total"] = totals_data['central_cost_total'] + \
                                totals_data['prep_cost_total'] + \
                                totals_data['procedural_cost_total']
    totals_data["cost_total_excl_pres"] = totals_data['central_cost_total'] + \
                                        totals_data['prep_cost_total'] + \
                                        totals_data['procedural_cost_excl_pres']
    return totals_data

# function that would give the total cost of the RLA (national) by year, as well as average
def calculate_national_rla_cost(totData: pd.DataFrame):
    natl_rla_cost_wpres = pd.concat([totData["year"], totData["cost_total"]], axis=1)
    natl_rla_cost_nopres = pd.concat([totData["year"], totData["cost_total_excl_pres"]], axis=1)
    natl_rla_cost_wpres = natl_rla_cost_wpres[natl_rla_cost_wpres['year']%4==0]
    natl_rla_cost_wpres = natl_rla_cost_wpres.groupby(by=['year']).sum()
    natl_rla_cost_nopres = natl_rla_cost_nopres.groupby(by=['year']).sum()
    if not os.path.exists("important-data"):
        os.mkdir("important-data")
    # 1. cost for elections that include president
    natl_rla_cost_wpres['cost_total'] = natl_rla_cost_wpres['cost_total'].round(2)
    # 2. cost for elections that exclude president
    natl_rla_cost_nopres['cost_total_excl_pres'] = natl_rla_cost_nopres['cost_total_excl_pres'].round(2)
    natl_rla_cost_wpres.to_csv("important-data/total_yearly_cost_(president).csv")
    natl_rla_cost_nopres.to_csv("important-data/total_yearly_cost_(no_president).csv")
    print("National Avg. RLA cost (president):", natl_rla_cost_wpres['cost_total'].mean())
    print("National Avg. RLA cost (no-president):", natl_rla_cost_nopres['cost_total_excl_pres'].mean())
    return

# function that will calculate state-by-state averages
def calculate_state_by_state_rla_cost(totData: pd.DataFrame):
    state_rla_cost_wpres = pd.concat([totData["state"], totData["year"],totData["state_po"],totData["cost_total"]], axis=1)
    state_rla_cost_nopres = pd.concat([totData["state"], totData["state_po"],totData["cost_total_excl_pres"]], axis=1)
    state_rla_cost_wpres = state_rla_cost_wpres[state_rla_cost_wpres['year']%4==0]
    state_rla_cost_wpres.drop(columns=['year'], inplace=True)
    state_rla_cost_wpres = state_rla_cost_wpres.groupby(by=['state','state_po']).mean()
    state_rla_cost_nopres = state_rla_cost_nopres.groupby(by=['state','state_po']).mean()
    if not os.path.exists("important-data"):
        os.mkdir("important-data")
    # 1. cost for elections that include president
    state_rla_cost_wpres['cost_total'] = state_rla_cost_wpres['cost_total'].round(2)
    # 2. cost for elections that exclude president
    state_rla_cost_nopres['cost_total_excl_pres'] = state_rla_cost_nopres['cost_total_excl_pres'].round(2)
    state_rla_cost_wpres.to_csv("important-data/avg_state_cost_(president).csv")
    state_rla_cost_nopres.to_csv("important-data/avg_state_cost_(no_president).csv")    
    return 

# function that writes results
def write_results(all_data: pd.DataFrame):
    if not os.path.exists("important-data"):
        os.mkdir("important-data")
    all_data['cost_total'] = all_data['cost_total'].round(2)
    all_data.to_csv("important-data/all_data_by_state_yr.csv")
    if not os.path.exists("important-data/state-by-state"):
        os.mkdir("important-data/state-by-state")
    for st in set(all_data['state_po']):
        ssp = all_data[all_data.state_po == st].reset_index(drop=True)
        ssp.to_csv(f"important-data/state-by-state/all_data_{st}.csv")

# function that generates a line plot showing the combined total cost of an RLA
# for presidential years
def graph_total_cost():
    # get the correct data
    data = pd.read_csv("important-data/total_yearly_cost_(president).csv")
    years = data.year
    cost = data.cost_total
    plt.plot(years, cost, color='orange')
    # create directory
    if not os.path.exists("plots"):
        os.mkdir('plots')
    plt.xticks(ticks=years)
    plt.xlabel("Year")
    plt.ylabel("Cost (Millions of $)")
    for yr in range(2000, 2025, 4):
        y,c = data[data.year==yr].year.item(), data[data.year==yr].cost_total
        dispc = (c/1e6).round(2).item()
        plt.annotate(text=f"${dispc}M", xy=(y,c.item()))
    plt.title("Combined Total Cost for Presidential Election Years")
    plt.savefig('plots/total_presidential_plot_1.png')


# function that generates a line plot showing the combined total cost of an RLA
# for presidential years
def graph_total_cost_non_presidential():
    plt.clf()
    # get the correct data
    data = pd.read_csv("important-data/total_yearly_cost_(no_president).csv")
    years = data.year
    cost = data.cost_total_excl_pres
    plt.plot(years, cost, color='lightblue')
    # create directory
    if not os.path.exists("plots"):
        os.mkdir('plots')
    plt.xticks(ticks=years)
    plt.xlabel("Year")
    plt.ylabel("Cost (Millions of $)")
    for yr in range(2000, 2025, 2):
        y,c = data[data.year==yr].year.item(), data[data.year==yr].cost_total_excl_pres
        dispc = (c/1e6).round(2).item()
        plt.annotate(text=f"${dispc}M", xy=(y,c.item()))
    plt.title("Combined Total Cost for Election Years")
    plt.savefig('plots/total_allyr_plot_2.png')

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
    totals_data = add_all3_costs()
    calculate_national_rla_cost(totData=totals_data)
    calculate_state_by_state_rla_cost(totData=totals_data)
    write_results(totals_data)
    # generate plots
    graph_total_cost()
    graph_total_cost_non_presidential()