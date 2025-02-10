# Import modules
import datetime
import pandas as pd
import meteostat as met
import numpy as np
import pickle
from collections import Counter



# Read processed EXCEL
COMED_device_5days = pd.read_excel('./ComEd/Aug-8-2021 to Aug-12-2021.xlsx')    # Aug-10 and Aug-11 high outages
COMED_device_5days['DEVICE'] = COMED_device_5days['DEVICE'].astype(str)
'''
total_outage_count = len(COMED_device_5days['DEVICE'])
total_unique_device_count = len(np.unique(COMED_device_5days['DEVICE']))
'''
# process the outage duration and impacted customers
COMED_device_5days['START_DATETIME'] = COMED_device_5days['START_DATETIME'].astype(str)
COMED_device_5days['RESTORE_DATETIME'] = COMED_device_5days['RESTORE_DATETIME'].astype(str)

COMED_device_out_status = np.zeros((len(COMED_device_5days['DEVICE']), 24 * 5))

zero_hour_dt = datetime.datetime.strptime("2021-08-08 00:00:00", "%Y-%m-%d %H:%M:%S")    # Considering date is in mm/dd/yyyy format

for it_out in range(len(COMED_device_5days['DEVICE'])):
    outstarthour_round_down = datetime.datetime.strptime(COMED_device_5days.loc[it_out, 'START_DATETIME'], '%Y-%m-%d %H:%M:%S').replace(microsecond=0, second=0, minute=0)
    if it_out in ([4000]): # specific row with inconsistent format
        outendhour_round_up = datetime.datetime.strptime(COMED_device_5days.loc[it_out, 'RESTORE_DATETIME'], '%Y-%m-%d %H:%M:%S').replace(microsecond=0, second=0, minute=0) + datetime.timedelta(hours=1)
    else:
        outendhour_round_up = datetime.datetime.strptime(COMED_device_5days.loc[it_out, 'RESTORE_DATETIME'], '%Y-%m-%d %H:%M:%S').replace(microsecond=0, second=0, minute=0) + datetime.timedelta(hours=1)
    flag_one_start_col = (outstarthour_round_down - zero_hour_dt).days*24 + (outstarthour_round_down - zero_hour_dt).seconds / (60 * 60)
    flag_one_end_col = (outendhour_round_up - zero_hour_dt).days*24 + (outendhour_round_up - zero_hour_dt).seconds / (60 * 60)
    if flag_one_end_col > 24*5:
        flag_one_end_col = 24*5
    COMED_device_out_status[it_out, int(flag_one_start_col):int(flag_one_end_col)] = 1

COMED_device_out_status_120h = pd.DataFrame(COMED_device_out_status)
COMED_device_out_status_120h.to_excel(r'COMED_device_out_status_120h_2021Aug_replicate.xlsx', index=False)

# check device_type, unique ones give color
devicetype_count = dict(Counter(COMED_device_5days['DEVICE_TYPE']))
devicetype_count = pd.DataFrame.from_dict(devicetype_count, orient='index').reset_index()
devicetype_count = devicetype_count.rename(columns={'index': 'Device Type', 0: 'Count of Outages'})

devicetype_count = devicetype_count.sort_values(by=['Count of Outages'], ascending=False).reset_index(drop=True)
devicetype_count_name_list = devicetype_count['Device Type'].tolist()


devicetype_duration_average = dict.fromkeys(devicetype_count_name_list, 0)
devicetype_customer_average = dict.fromkeys(devicetype_count_name_list, 0)
devicetype_outageimpact_average = dict.fromkeys(devicetype_count_name_list, 0)
devicetype_lightning_average = dict.fromkeys(devicetype_count_name_list, 0)
# for it in range(1):
for it in range(len(COMED_device_5days['DEVICE_TYPE'])):
    it_devicetype = COMED_device_5days['DEVICE_TYPE'][it]
    devicetype_duration_average [it_devicetype] += COMED_device_5days['DURATION_MINUTES'][it]
    devicetype_customer_average[it_devicetype] += COMED_device_5days['NUM_CUST_AFFCTD'][it]
    devicetype_outageimpact_average[it_devicetype] += COMED_device_5days['NUM_CUST_AFFCTD'][it] * COMED_device_5days['DURATION_MINUTES'][it]
    if COMED_device_5days['HAS_LIGHTNING'][it] == 1:
        devicetype_lightning_average[it_devicetype] += 1

devicetype_duration_average_df = pd.DataFrame.from_dict(devicetype_duration_average, orient='index').reset_index()
devicetype_duration_average_df = devicetype_duration_average_df.rename(columns={'index': 'Device Type', 0: 'Total Outage Duration (Mins)'})
devicetype_duration_average_df['Average Outage Duration (Hours)'] = devicetype_duration_average_df['Total Outage Duration (Mins)'] / devicetype_count['Count of Outages'] / 60

devicetype_customer_average_df = pd.DataFrame.from_dict(devicetype_customer_average, orient='index').reset_index()
devicetype_customer_average_df = devicetype_customer_average_df.rename(columns={'index': 'Device Type', 0: 'Total Affected Customers'})
devicetype_customer_average_df['Average Affected Customers'] = devicetype_customer_average_df['Total Affected Customers'] / devicetype_count['Count of Outages']

devicetype_outageimpact_average_df = pd.DataFrame.from_dict(devicetype_outageimpact_average, orient='index').reset_index()
devicetype_outageimpact_average_df = devicetype_outageimpact_average_df.rename(columns={'index': 'Device Type', 0: 'Total Outage Impact (Customer*Mins)'})
devicetype_outageimpact_average_df['Average Outage Impact (Customer*Hours)'] = devicetype_outageimpact_average_df['Total Outage Impact (Customer*Mins)'] / devicetype_count['Count of Outages'] / 60

devicetype_lightning_average_df = pd.DataFrame.from_dict(devicetype_lightning_average, orient='index').reset_index()
devicetype_lightning_average_df = devicetype_lightning_average_df.rename(columns={'index': 'Device Type', 0: 'Percentage of Outages that has lightning (%)'})
devicetype_lightning_average_df['Percentage of Outages that has lightning (%)'] = devicetype_lightning_average_df['Percentage of Outages that has lightning (%)'] *100 / devicetype_count['Count of Outages']

devicetype_duration_average_df.to_excel(r'device_type_duration.xlsx', index=False)
devicetype_customer_average_df.to_excel(r'device_type_customer.xlsx', index=False)
devicetype_outageimpact_average_df.to_excel(r'device_type_outageimpact.xlsx', index=False)
devicetype_lightning_average_df.to_excel(r'device_type_lightning.xlsx', index=False)

