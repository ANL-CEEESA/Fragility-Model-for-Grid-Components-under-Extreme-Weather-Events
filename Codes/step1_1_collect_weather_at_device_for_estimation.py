# Import modules
import datetime
import pandas as pd
import meteostat as met
import numpy as np
import pickle


COMED_device_5days = pd.read_excel('./ComEd/Aug-8-2021 to Aug-12-2021.xlsx')    # Aug-10 and Aug-11 high outages
list_cor = []
for it in range(len(COMED_device_5days['GIS_ISOLATE_LATITUDE'])):
    list_cor.append([COMED_device_5days['GIS_ISOLATE_LATITUDE'][it], COMED_device_5days['GIS_ISOLATE_LONGITUDE'][it]])
list_cor_temp, uniq_cnt = np.unique(list_cor, axis=0, return_counts=True)
list_cor_unique = list_cor_temp[uniq_cnt==1]

weather_resolution = 'Hourly'
start = datetime.datetime(2021, 8, 8)   # same as outage start date
end = datetime.datetime(2021, 8, 13)    # outage end date + 1 day


location_points = {}

for i in range(len(list_cor)):
    location_points[i] = list_cor[i]

weather_data = dict.fromkeys(location_points.keys())
weather_empty_list = []

empty_count = 0
total_outage_count = len(COMED_device_5days['DEVICE'])
# for key in range(2):
for key in weather_data:
    point = met.Point(location_points[key][0], location_points[key][1])
    point.radius = 111000  # 1 degree in lat/lon equals 111km
    # data = met.Hourly(point, start, end)

    data = met.Hourly(point, start, end, "US/Central")        # timezone!!!
    # data = met.Hourly(point, start, end, "US/Eastern")

    # data = data.convert(units.imperial)
    temp = data.fetch()
    weather_data[key] = temp
    if (len(temp) < 24*(end-start).days):  # Detect the empty entry (if radius=50000, 14 will not have values)
        weather_empty_list.append(key)
        empty_count += 1

    np.disp(f'Getting weather metrics for total {total_outage_count} outage records, completed {key} with counts of empty as {empty_count}')

### Note: 377 empty in total 3015 outages, ~2600 unique locations

weather_data_keys_list = list(weather_data.keys())
for itkey in weather_empty_list:
    tempindex = weather_data_keys_list.index(itkey)
    itkey_update = weather_data_keys_list[tempindex - 1]
    weather_data[itkey] = weather_data[itkey_update]


with open('weather_data_hourly_0808-0812_2021.pickle', 'wb') as handle:
    pickle.dump(weather_data, handle, protocol=pickle.HIGHEST_PROTOCOL)
'''
weather_data = pickle.load( open( "weather_data_hourly_0808-0812_2021.pickle", "rb" ) )
'''
COMED_device_weather = pd.DataFrame(columns=weather_data[0].columns)
for i in range(len(weather_data_keys_list)):
    itkey = weather_data_keys_list[i]
    temp_df = weather_data[itkey].iloc[0:120, :]  # only take the 72 rows, original three days fetch has 73 rows
    # https://dev.meteostat.net/python/hourly.html#example
    COMED_device_weather = COMED_device_weather.append(temp_df, ignore_index=True)
    np.disp(f'Output weather metrics to total {total_outage_count} records, completed {i}')
# This is 605 gen * 72 hours = 43560 rows, for the weather impact analysis

'''
COMED_device_weather.to_excel(r'weather_metrics_at_device_Hourly.xlsx', index=False)
'''
with open('weather_metrics_at_device_Hourly.pickle', 'wb') as handle:
    pickle.dump(COMED_device_weather, handle, protocol=pickle.HIGHEST_PROTOCOL)
'''
COMED_device_weather = pickle.load( open( "weather_metrics_at_device_Hourly.pickle", "rb" ) )
'''



