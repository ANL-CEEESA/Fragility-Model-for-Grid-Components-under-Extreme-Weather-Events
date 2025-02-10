# Import modules
import datetime
import pandas as pd
import numpy as np
import pickle
from tqdm import tqdm



##### Read processed EXCEL
# ComEd device information
COMED_device_5days = pd.read_excel('./ComEd/Aug-8-2021 to Aug-12-2021.xlsx')    # Aug-10 and Aug-11 high outages
# weather metrics
start = datetime.datetime(2020, 8, 8)
end = datetime.datetime(2020, 8, 13)
COMED_device_weather = pickle.load( open( "weather_metrics_at_device_Hourly.pickle", "rb" ) )
'''
COMED_device_weather = pd.read_excel(r'weather_metrics_at_device_Hourly.xlsx')
'''
# outage status
COMED_device_out_status = pd.read_excel(r'COMED_device_out_status_120h_2021Aug_replicate.xlsx')

###  process - INDEX from outage records to device
# Find out the replicated ones (not unique ones)
device_value_counts = COMED_device_5days['DEVICE'].value_counts()
device_multipleoutage = device_value_counts.index[device_value_counts.gt(1)].tolist()
COMED_device_5days_multipleoutage =COMED_device_5days[COMED_device_5days['DEVICE'].isin(device_multipleoutage)]
device_list = device_value_counts.index.tolist()
# Find out the non-unique ones - corresponding index in outage records
multiple_outage_device_dict = {}
for multiple_outage_device in device_multipleoutage:
    multiple_outage_device_dict[multiple_outage_device] = COMED_device_5days[COMED_device_5days['DEVICE'] == multiple_outage_device].index.tolist()
# This dict is to create skip list when compiling for 4015 records
    # Entries after first number index will be skip list
outagerecord_skip_list = []
for multiple_outage_device in multiple_outage_device_dict:
    outagerecord_skip_list.extend(multiple_outage_device_dict[multiple_outage_device][1:])
# This dict can also merge the outage status by summing together the 1 as in outage, and replace values >0 with 1
for multiple_outage_device in multiple_outage_device_dict:
    tempseries = COMED_device_out_status.iloc[multiple_outage_device_dict[multiple_outage_device]].sum()
    tempseries[tempseries > 0] = 1
    for number_index in multiple_outage_device_dict[multiple_outage_device]:    # although the number_index after the first one will be skipped
        COMED_device_out_status.iloc[number_index] = tempseries


# CHECK NAN VALUES
testdf = COMED_device_5days
print("Count total NaN at each column in a DataFrame : \n\n", testdf.isnull().sum())



COMED_device_lightning_status = pd.read_excel(r'COMED_device_lightning_status_120h_1km.xlsx')
'''
COMED_device_lightning_status = pd.read_excel(r'COMED_device_lightning_status_72h_0.5km.xlsx')
COMED_device_lightning_status = pd.read_excel(r'COMED_device_lightning_status_72h_0.1km.xlsx')
'''



##### Create output by merging EXCELs
# COMED_HILP_Derecho = pd.DataFrame(columns =['Outage_Device_Index', 'Device_Type', 'LAT_Isolation', 'LON_Isolation',  'TimeYear', 'TimeMonth', 'TimeDay', 'TimeHour', 'Outage_status'] + COMED_device_weather.columns.tolist())
COMED_HILP_Derecho_columns =['Outage_Device_Index', 'Device_Type', 'LAT_Isolation', 'LON_Isolation',  'TimeYear', 'TimeMonth', 'TimeDay', 'TimeHour', 'Outage_status'] + COMED_device_weather.columns.tolist() + ['Lightning']
COMED_HILP_Derecho_list = []
total_outage_count = len(COMED_device_5days['DEVICE'])
total_unique_outage_count = len(device_list)
# for it_out_record in range(3):
for it_out_record in tqdm(range(len(COMED_device_5days['DEVICE']))):
    if it_out_record in outagerecord_skip_list:
        continue
    else:
        for itday in range((end - start).days):
            for ithour in range(24):
                temp_list = [COMED_device_5days['DEVICE'][it_out_record],
                             COMED_device_5days['DEVICE_TYPE'][it_out_record],
                             COMED_device_5days['GIS_ISOLATE_LATITUDE'][it_out_record],
                             COMED_device_5days['GIS_ISOLATE_LONGITUDE'][it_out_record],
                             (start + datetime.timedelta(days=itday)).year,
                             (start + datetime.timedelta(days=itday)).month,
                             (start + datetime.timedelta(days=itday)).day,
                             ithour,
                             COMED_device_out_status.iloc[it_out_record, itday * 24 + ithour]]
                temp_list = temp_list + COMED_device_weather.iloc[it_out_record * 24*5 + itday * 24 + ithour, :].tolist()
                temp_list = temp_list + [COMED_device_lightning_status.iloc[it_out_record, itday * 24 + ithour]]
                COMED_HILP_Derecho_list.append(temp_list)
                # temp_series = pd.Series(temp_list, copy=False, index = COMED_HILP_Derecho.columns)
                # COMED_HILP_Derecho = COMED_HILP_Derecho.append(temp_series, ignore_index = True)
        # np.disp(f'Compiling format for total {total_outage_count} records ({total_unique_outage_count} unique device), completed {it_out_record}')


COMED_HILP_Derecho = pd.DataFrame(COMED_HILP_Derecho_list, columns=COMED_HILP_Derecho_columns)


with open('COMED_HILP_Derecho_Hourly_0808-0812_2021_replicate.pickle', 'wb') as handle:
    pickle.dump(COMED_HILP_Derecho, handle, protocol=pickle.HIGHEST_PROTOCOL)
'''
COMED_HILP_Derecho.to_excel(r'COMED_HILP_Derecho_Hourly_0808-0812_2021_replicate.xlsx', index = False)

'''


'''
# Read from EXCEL - haven't processed on the NAN values
COMED_HILP_Derecho = pd.read_excel(r'COMED_HILP_Derecho_Hourly_0808-0812_2021_replicate.xlsx')
COMED_HILP_Derecho = pickle.load( open( "COMED_HILP_Derecho_Hourly_0808-0812_2021_replicate.pickle", "rb" ) )
'''



## clear df
COMED_HILP_Derecho_clean = COMED_HILP_Derecho.copy()
COMED_HILP_Derecho_clean = COMED_HILP_Derecho_clean.dropna(subset=['Device_Type'])
COMED_HILP_Derecho_clean = COMED_HILP_Derecho_clean.dropna(subset=['LAT_Isolation'])
COMED_HILP_Derecho_clean['prcp'] = COMED_HILP_Derecho_clean['prcp'].fillna(0)
COMED_HILP_Derecho_clean['pres'] = COMED_HILP_Derecho_clean['pres'].fillna(method="ffill")
COMED_HILP_Derecho_clean['pres'] = COMED_HILP_Derecho_clean['pres'].fillna(method="bfill")
COMED_HILP_Derecho_clean = COMED_HILP_Derecho_clean.drop(columns=['snow', 'wpgt', 'tsun', 'coco'])

with open('COMED_HILP_Derecho_clean.pickle', 'wb') as handle:
    pickle.dump(COMED_HILP_Derecho_clean, handle, protocol=pickle.HIGHEST_PROTOCOL)
np.disp('completed')

COMED_HILP_Derecho_clean.to_excel(r'COMED_HILP_Derecho_Hourly_0808-0812_2021_replicate_clean.xlsx', index = False)
np.disp(f'EXCEL output completed')

'''
# Read from EXCEL - clean version
COMED_HILP_Derecho_clean = pd.read_excel(r'COMED_HILP_Derecho_Hourly_0808-0812_2021_replicate_clean.xlsx')
COMED_HILP_Derecho_clean = pickle.load( open( "COMED_HILP_Derecho_clean.pickle", "rb" ) )
'''


'''
# CHECK NAN VALUES
testdf = COMED_HILP_Derecho_clean
testdf = COMED_HILP_Derecho
print("Count total NaN at each column in a DataFrame : \n\n", testdf.isnull().sum())
'''