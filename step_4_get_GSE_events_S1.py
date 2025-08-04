import pandas as pd
import numpy as np
import os
from pathlib import Path
import sys

import get_GSE_events_1_SOC_insufficient as EVENT1

## Define a function to run each airport in parallel
def single_airport_add_event(file_path):
    GSE_tasks = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
    tasks_per_airline = [group for _,group in GSE_tasks.groupby('airline')] # task df of each airline
    GSE_events_list = []
    airport = os.path.basename(file_path).split('_')[0]
    for j in tasks_per_airline:
        list_of_gse = [group for _,group in j.groupby('GSE_type')]
        airline = j['airline'].iloc[0]
        for df_tmp in list_of_gse:
            if df_tmp['GSE_type'].iloc[0] in ['baggage tractor', 'catering truck']:
                df_tmp['task_idx'] = ["service_" + str(m) for m in range(df_tmp.shape[0])]
                df_assign_result_tmp, veh_num_init = EVENT1.GSE_assign(df_tmp, 0.1, 0.9, 40, 1)
                df_assign_result_tmp['airline'] = airline
                GSE_events_list.append(df_assign_result_tmp)
            else:
                task_per_gse_per_aircraft_type = [group for _,group in df_tmp.groupby('aircraft_type')]
                for df_temp_temp in task_per_gse_per_aircraft_type:
                    df_temp_temp['task_idx'] = ["service_" + str(m) for m in range(df_temp_temp.shape[0])]
                    df_assign_result_tmp, veh_num_init = EVENT1.GSE_assign(df_temp_temp, 0.1, 0.9, 40, 1)
                    df_assign_result_tmp['airline'] = airline
                    GSE_events_list.append(df_assign_result_tmp) 
                    
    GSE_events = pd.concat(GSE_events_list)
    GSE_events.to_csv('{}/all_flight_GSE_events/S1_SOC_insufficient_40/{}_GSE_events.csv'.format(folder_father,airport))

def single_GSE_airline_add_event(df_tmp):
    if df_tmp['GSE_type'].iloc[0] in ['baggage tractor', 'catering truck']:

        df_assign_result_tmp, veh_num_init = EVENT1.GSE_assign(df_tmp, 0.1, 0.9, 40, 1)
        airline = df_tmp['airline'].iloc[0]
        df_assign_result_tmp['airline'] = airline
        return df_assign_result_tmp
    else:
        task_per_gse_per_aircraft_type = [group for _,group in df_tmp.groupby('aircraft_type')]
        for df_temp_temp in task_per_gse_per_aircraft_type:
            df_temp_temp['task_idx'] = ["service_" + str(m) for m in range(df_temp_temp.shape[0])]
            df_assign_result_tmp, veh_num_init = EVENT1.GSE_assign(df_temp_temp, 0.1, 0.9, 40, 1)
            airline = df_tmp['airline'].iloc[0]
            df_assign_result_tmp['airline'] = airline     
            return df_assign_result_tmp   


if __name__ =='__main__':
    # Parallelizing using Pool.map()
    import multiprocessing as mp
    import time
    
    ts = time.time()
    ## All raw data

    flag = int(sys.argv[1]) ## If flag == 1, means single file run for large airports, otherwise, multiprocessing for small airports
    file = sys.argv[2]
    folder_father = sys.argv[3]
    
    if flag == 1: ## Parallel run for each airport
        file_path = file
        GSE_tasks = pd.read_csv(file_path, parse_dates=['start_time', 'end_time'])
        tasks_per_airline = [group for _,group in GSE_tasks.groupby('airline')] # task df of each airline
        GSE_events_list = []
        airport = os.path.basename(file_path).split('_')[0]
        list_all_gse = []
        for j in tasks_per_airline:
            j['task_idx'] = ["service_" + str(m) for m in range(j.shape[0])]
            list_of_gse = [group for _,group in j.groupby('GSE_type')]
            airline = j['airline'].iloc[0]
            list_all_gse += list_of_gse
        
        # Parallelizing using Pool.map()
        import multiprocessing as mp
        
        pool = mp.Pool(mp.cpu_count())
         
        GSE_events_list = pool.map(single_GSE_airline_add_event, list_all_gse)
         
        pool.close()
        GSE_events = pd.concat(GSE_events_list) 
        GSE_events.to_csv('{}/all_flight_GSE_events/S1_SOC_insufficient_40/{}_GSE_events.csv'.format(folder_father,airport))
    else:
        folder_path = Path('{}/all_flight_GSE_tasks'.format(folder_father))
        all_files_New = folder_path.glob('*.csv')
    
        ###---------------------------------------------------------------###
        ## Check the size of each file
        def read_csv(file_path):
            GSE_tasks = pd.read_csv(file_path, usecols=['start_time'])
            return (file_path,GSE_tasks.shape[0])
    
        # Parallelizing using Pool.map()
        import multiprocessing as mp
        
        pool = mp.Pool(mp.cpu_count())
         
        results = pool.map(read_csv, list(all_files_New))
         
        pool.close()
        
        file_list = [i[0] for i in results]
        row_list = [i[1] for i in results]
        
        df_file_size = pd.DataFrame({'File':file_list,'Row_Count':row_list})
        df_file_size = df_file_size.sort_values(by = 'Row_Count')
        ###---------------------------------------------------------------###
        
        
            
        pool = mp.Pool(mp.cpu_count())
         
        results = pool.map(single_airport_add_event, df_file_size[0:300].File.to_list())
         
        pool.close()
    te = time.time()
    print('Time Used: ', te-ts)