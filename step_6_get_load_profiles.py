import pandas as pd
import numpy as np
from pathlib import Path
import os
from datetime import timedelta
from datetime import datetime, time
import math
import sys


# define a function to calculate power demand for the load profile
def load_profile(charge_events, charger_power):
    
    # Round to minute precision
    charge_events['start_time'] = charge_events['start_time'].dt.floor('min')
    charge_events['end_time'] = charge_events['end_time'].dt.floor('min')

    # Mark +1 at start and -1 at end
    event_changes = pd.concat([
        charge_events['start_time'].value_counts().sort_index().rename("change"),
        charge_events['end_time'].value_counts().sort_index().rename("change") * -1
    ]).groupby(level=0).sum()
    
    # Compute cumulative sum to get concurrent events
    active_events = event_changes.cumsum()
    
    # Fill in missing minutes for continuity
    active_events = active_events.asfreq('T', method='pad').fillna(0)
    full_range = pd.date_range(start="2023-01-01 00:00:00", end="2023-12-31 23:59:00", freq='T')
    active_events = active_events.reindex(full_range, fill_value=0)

    
    active_df = active_events.rename("num_active_events").reset_index()
    active_df.columns = ["time_stamp", "count"]
    active_df['power'] = active_df['count']*charger_power
    
    return active_df

## Define a function to run each airport in parallel

def single_airport_load_profile_S1(file_path):
    all_events = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    all_events['start_time']= all_events['start_time'].dt.tz_localize(None)
    all_events['end_time']= all_events['end_time'].dt.tz_localize(None)

    charge_events = all_events[all_events.Task_type == 'Charge']
    power_demand = load_profile(charge_events, 40)
    airport = os.path.basename(file_path).split('_')[0]
    power_demand.to_csv('{}/all_flight_load_profiles/S1_SOC_insufficient_40/{}_load_profiles.csv'.format(folder_father,airport))

def single_airport_load_profile_S2(file_path):
    all_events = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    all_events['start_time']= all_events['start_time'].dt.tz_localize(None)
    all_events['end_time']= all_events['end_time'].dt.tz_localize(None)

    charge_events = all_events[all_events.Task_type == 'Charge']
    power_demand = load_profile(charge_events, 20)
    airport = os.path.basename(file_path).split('_')[0]
    power_demand.to_csv('{}/all_flight_load_profiles/S2_SOC_insufficient_20/{}_load_profiles.csv'.format(folder_father,airport))

def single_airport_load_profile_S3(file_path):
    all_events = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    all_events['start_time']= all_events['start_time'].dt.tz_localize(None)
    all_events['end_time']= all_events['end_time'].dt.tz_localize(None)

    charge_events = all_events[all_events.Task_type == 'Charge']
    power_demand = load_profile(charge_events, 40)
    airport = os.path.basename(file_path).split('_')[0]
    power_demand.to_csv('{}/all_flight_load_profiles/S3_charge_during_service_gaps_40/{}_load_profiles.csv'.format(folder_father,airport))

def single_airport_load_profile_S4(file_path):
    all_events = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    all_events['start_time']= all_events['start_time'].dt.tz_localize(None)
    all_events['end_time']= all_events['end_time'].dt.tz_localize(None)

    charge_events = all_events[all_events.Task_type == 'Charge']
    power_demand = load_profile(charge_events, 20)
    airport = os.path.basename(file_path).split('_')[0]
    power_demand.to_csv('{}/all_flight_load_profiles/S4_charge_during_service_gaps_20/{}_load_profiles.csv'.format(folder_father,airport))

def single_airport_load_profile_S5(file_path):
    all_events = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    all_events['start_time']= all_events['start_time'].dt.tz_localize(None)
    all_events['end_time']= all_events['end_time'].dt.tz_localize(None)

    charge_events = all_events[all_events.Task_type == 'Charge']
    power_demand = load_profile(charge_events, 40)
    airport = os.path.basename(file_path).split('_')[0]
    power_demand.to_csv('{}/all_flight_load_profiles/S5_charge_overnight_40/{}_load_profiles.csv'.format(folder_father,airport))

def single_airport_load_profile_S6(file_path):
    all_events = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    all_events['start_time']= all_events['start_time'].dt.tz_localize(None)
    all_events['end_time']= all_events['end_time'].dt.tz_localize(None)

    charge_events = all_events[all_events.Task_type == 'Charge']
    power_demand = load_profile(charge_events, 20)
    airport = os.path.basename(file_path).split('_')[0]
    power_demand.to_csv('{}/all_flight_load_profiles/S6_charge_overnight_20/{}_load_profiles.csv'.format(folder_father,airport))

if __name__ == '__main__':
    # Parallelizing using Pool.map()
    import multiprocessing as mp
    import time

    scenario = sys.argv[1]
    folder_father = sys.argv[2]
    
    ts = time.time()
    ## All raw data
    folder_path = Path('{}/all_flight_GSE_events/{}'.format(folder_father,scenario))
    all_files_New = folder_path.glob('*.csv')
    
    pool = mp.Pool(mp.cpu_count())

    ## Scenario index based function call
    scen_idx = scenario.split('_')[0]


    if scen_idx == 'S1': 
        results = pool.map(single_airport_load_profile_S1, list(all_files_New))
    elif scen_idx == 'S2': 
        results = pool.map(single_airport_load_profile_S2, list(all_files_New))
    elif scen_idx == 'S3': 
        results = pool.map(single_airport_load_profile_S3, list(all_files_New))
    elif scen_idx == 'S4': 
        results = pool.map(single_airport_load_profile_S4, list(all_files_New))
    elif scen_idx == 'S5': 
        results = pool.map(single_airport_load_profile_S5, list(all_files_New))
    elif scen_idx == 'S6': 
        results = pool.map(single_airport_load_profile_S6, list(all_files_New))
        
    pool.close()
    te = time.time()
    print('Time Used: ', te-ts)
    
