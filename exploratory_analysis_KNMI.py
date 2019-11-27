#%% Loading data
from knmy import knmy
import knmi
#%% Showing essential attributes and structure of the data
#list stations:
knmi.stations
knmi.variables

#Hoogeveen is closest station, stationnumber 615
#In current API, station number of Hoogeveen is 279

#%% Get data through API
disclaimer, stations, variables, knmi_df = knmy.get_hourly_data(stations=[279], start=2017010112, end=2017010113, parse=True)

knmi_df.drop(knmi_df.index[0])

knmi_df