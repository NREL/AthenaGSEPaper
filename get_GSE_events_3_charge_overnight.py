import pandas as pd
import numpy as np
from datetime import timedelta
from datetime import datetime, date, time

def GSE_assign_3(GSE_tasks_input, SOC_lower, SOC_upper, charger_power,veh_num_init):
    df = GSE_tasks_input.copy().sort_values(by=['start_time','end_time']).reset_index(drop=True)
    df = df[df['end_time']>= pd.to_datetime('1/1/2023 08:00:00 AM')] # start from 8 am of the first day
    df['assigned'] = False

    aircraft_type = df['aircraft_type'].iloc[0]
    
    GSE_type = df['GSE_type'].iloc[0]
    batt_cap = df['batt_cap'].iloc[0]
    charging_power = charger_power * 0.94

    veh_id = veh_num_init
    all_vehicles = []
    charge_event_idx = 0
    task_idx = 0
    num_tasks = len(df)

    assigned_array = df['assigned'].values
    start_times = df['start_time'].values
    end_times = df['end_time'].values
    energy_consumptions = df['energy_consumption'].values
    task_indices = df['task_idx'].values

    while not all(assigned_array):
        SOC = SOC_upper
        current_time = None
        event_log = []

        # Start from the first unassigned task
        while task_idx < num_tasks and assigned_array[task_idx]:
            task_idx += 1
        if task_idx >= num_tasks:
            break

        # Assign first task
        i = task_idx
        task_start = start_times[i]
        task_end = end_times[i]
        energy = energy_consumptions[i]
        SOC_end = SOC - energy / batt_cap

        if SOC_end < SOC_lower:
            raise ValueError("Cannot even complete the first task with full battery.")

        event_log.append(pd.DataFrame({
            'Task_type': ['Service'],
            'Idx': [task_indices[i]],
            'SOC_start': [SOC],
            'SOC_end': [SOC_end],
            'start_time': [task_start],
            'end_time': [task_end]
        }))
        assigned_array[i] = True
        current_time = task_end
        SOC = SOC_end

        # Move forward
        pointer = i + 1
        if pointer >= num_tasks:
            ## Add a final charge event to charge to full
            if SOC < SOC_upper:
                # Charge to full
                charge_time = (SOC_upper - SOC) * batt_cap / charging_power * 60
                charge_td = np.timedelta64(int(charge_time), 'm')

                current_time = current_time + np.timedelta64(5, 'm')
                current_time_pd = pd.Timestamp(current_time)
                # charge if the time is between 10 pm to 8 am
                if (current_time_pd.time() >= pd.Timestamp("22:00:00").time()) | (current_time_pd.time() <= pd.Timestamp("08:00:00").time()):
                    start_charge_time = current_time
                    end_charge_time = start_charge_time + charge_td
                    event_log.append(pd.DataFrame({
                        'Task_type': ['Charge'],
                        'Idx': [f'charge_{charge_event_idx}'],
                        'SOC_start': [SOC],
                        'SOC_end': [SOC_upper],
                        'start_time': [start_charge_time],
                        'end_time': [end_charge_time]
                    }))
                # otherwise wait until 10 am to charge
                else:
                    # Truncate to date
                    date_only = current_time.astype('datetime64[D]')
                    # Add 22 hours (10:00 PM)
                    start_charge_time = date_only + np.timedelta64(22, 'h')
                    end_charge_time = start_charge_time + charge_td
                    event_log.append(pd.DataFrame({
                        'Task_type': ['Charge'],
                        'Idx': [f'charge_{charge_event_idx}'],
                        'SOC_start': [SOC],
                        'SOC_end': [SOC_upper],
                        'start_time': [start_charge_time],
                        'end_time': [end_charge_time]
                    }))                    
                SOC = SOC_upper
                current_time = end_charge_time
                charge_event_idx += 1 
        while pointer < num_tasks:
            min_gap = current_time + np.timedelta64(5, 'm')
            while pointer < num_tasks and (assigned_array[pointer] or start_times[pointer] < min_gap):
                pointer += 1
            if pointer >= num_tasks:
                ## Add a final charge event to charge to full
                if SOC < SOC_upper:
                    # Charge to full
                    charge_time = (SOC_upper - SOC) * batt_cap / charging_power * 60
                    charge_td = np.timedelta64(int(charge_time), 'm')

                    current_time = current_time + np.timedelta64(5, 'm')
                    current_time_pd = pd.Timestamp(current_time)
                    # charge if the time is between 10 pm to 8 am
                    if (current_time_pd.time() >= pd.Timestamp("22:00:00").time()) | (current_time_pd.time() <= pd.Timestamp("08:00:00").time()):
                        start_charge_time = current_time
                        end_charge_time = start_charge_time + charge_td
                        event_log.append(pd.DataFrame({
                            'Task_type': ['Charge'],
                            'Idx': [f'charge_{charge_event_idx}'],
                            'SOC_start': [SOC],
                            'SOC_end': [SOC_upper],
                            'start_time': [start_charge_time],
                            'end_time': [end_charge_time]
                        }))
                    # otherwise wait until 10 am to charge
                    else:
                        # Truncate to date
                        date_only = current_time.astype('datetime64[D]')
                        # Add 22 hours (10:00 PM)
                        start_charge_time = date_only + np.timedelta64(22, 'h')
                        end_charge_time = start_charge_time + charge_td
                        event_log.append(pd.DataFrame({
                            'Task_type': ['Charge'],
                            'Idx': [f'charge_{charge_event_idx}'],
                            'SOC_start': [SOC],
                            'SOC_end': [SOC_upper],
                            'start_time': [start_charge_time],
                            'end_time': [end_charge_time]
                        }))                    
                    SOC = SOC_upper
                    current_time = end_charge_time
                    charge_event_idx += 1                
                break

            next_start = start_times[pointer]
            next_energy = energy_consumptions[pointer]
            energy_need = next_energy / batt_cap

            # Check if current SOC can handle next task
            if SOC - energy_need >= SOC_lower:
                # Assign the task
                task_start = start_times[pointer]
                task_end = end_times[pointer]
                SOC_end = SOC - energy_need
                event_log.append(pd.DataFrame({
                    'Task_type': ['Service'],
                    'Idx': [task_indices[pointer]],
                    'SOC_start': [SOC],
                    'SOC_end': [SOC_end],
                    'start_time': [task_start],
                    'end_time': [task_end]
                }))
                assigned_array[pointer] = True
                current_time = task_end
                SOC = SOC_end
                pointer += 1
            else:
                # Charge to full
                charge_time = (SOC_upper - SOC) * batt_cap / charging_power * 60
                charge_td = np.timedelta64(int(charge_time), 'm')

                current_time = current_time + np.timedelta64(5, 'm')
                current_time_pd = pd.Timestamp(current_time)
                if (current_time_pd.time() >= pd.Timestamp("22:00:00").time()) | (current_time_pd.time() <= pd.Timestamp("08:00:00").time()):
                    start_charge_time = current_time
                    end_charge_time = start_charge_time + charge_td
                    event_log.append(pd.DataFrame({
                        'Task_type': ['Charge'],
                        'Idx': [f'charge_{charge_event_idx}'],
                        'SOC_start': [SOC],
                        'SOC_end': [SOC_upper],
                        'start_time': [start_charge_time],
                        'end_time': [end_charge_time]
                    }))
                    current_time = end_charge_time
                else:
                    # Truncate to date
                    date_only = current_time.astype('datetime64[D]')
                    # Add 22 hours (10:00 PM)
                    start_charge_time = date_only + np.timedelta64(22, 'h')
                    end_charge_time = start_charge_time + charge_td
                    event_log.append(pd.DataFrame({
                        'Task_type': ['Charge'],
                        'Idx': [f'charge_{charge_event_idx}'],
                        'SOC_start': [SOC],
                        'SOC_end': [SOC_upper],
                        'start_time': [start_charge_time],
                        'end_time': [end_charge_time]
                    }))    
                    # Get the next day's datetime
                    next_day = date_only + np.timedelta64(1, 'D') + np.timedelta64(8, 'h') - np.timedelta64(5, 'm')
                    current_time = max(end_charge_time,next_day)
                
                SOC = SOC_upper
                charge_event_idx += 1
            
            if pointer >= num_tasks:
                ## Add a final charge event to charge to full
                if SOC < SOC_upper:
                    # Charge to full
                    charge_time = (SOC_upper - SOC) * batt_cap / charging_power * 60
                    charge_td = np.timedelta64(int(charge_time), 'm')

                    current_time = current_time + np.timedelta64(5, 'm')
                    current_time_pd = pd.Timestamp(current_time)
                    # charge if the time is between 10 pm to 8 am
                    if (current_time_pd.time() >= pd.Timestamp("22:00:00").time()) | (current_time_pd.time() <= pd.Timestamp("08:00:00").time()):
                        start_charge_time = current_time
                        end_charge_time = start_charge_time + charge_td
                        event_log.append(pd.DataFrame({
                            'Task_type': ['Charge'],
                            'Idx': [f'charge_{charge_event_idx}'],
                            'SOC_start': [SOC],
                            'SOC_end': [SOC_upper],
                            'start_time': [start_charge_time],
                            'end_time': [end_charge_time]
                        }))
                    # otherwise wait until 10 am to charge
                    else:
                        # Truncate to date
                        date_only = current_time.astype('datetime64[D]')
                        # Add 22 hours (10:00 PM)
                        start_charge_time = date_only + np.timedelta64(22, 'h')
                        end_charge_time = start_charge_time + charge_td
                        event_log.append(pd.DataFrame({
                            'Task_type': ['Charge'],
                            'Idx': [f'charge_{charge_event_idx}'],
                            'SOC_start': [SOC],
                            'SOC_end': [SOC_upper],
                            'start_time': [start_charge_time],
                            'end_time': [end_charge_time]
                        }))                    
                    SOC = SOC_upper
                    current_time = end_charge_time
                    charge_event_idx += 1 
        df_vehicle = pd.concat(event_log, ignore_index=True)
        df_vehicle['veh_id'] = veh_id
        all_vehicles.append(df_vehicle)
        veh_id += 1

    df_result = pd.concat(all_vehicles, ignore_index=True)
    df_result['GSE_type'] = GSE_type
    df_result['aircraft_type'] = aircraft_type
    return df_result, veh_id


