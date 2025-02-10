# Import modules
import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
import haversine as hs
import pickle

##### Read processed EXCEL
# ComEd device information
COMED_device_5days = pd.read_excel('./ComEd/Aug-8-2021 to Aug-12-2021.xlsx')    # Aug-10 and Aug-11 high outages
# weather metrics
start = datetime.datetime(2021, 8, 8)
end = datetime.datetime(2021, 8, 13)
COMED_device_weather = pickle.load( open( "weather_metrics_at_device_Hourly.pickle", "rb" ) )
'''
COMED_device_weather = pd.read_excel(r'weather_metrics_at_generator_Hourly.xlsx')
'''
# outage status
COMED_device_out_status = pd.read_excel(r'COMED_device_out_status_120h_2021Aug_replicate.xlsx')


### Infer lightning status of intact device
# The size of the region within a thunderstorm cloud where lightning originates can vary greatly, but typically the diameter of a lightning strike is on the order of tens to hundreds of meters.
# Convert to lat/lon
# 0.000009 degrees/meter x 1000 meters = 0.009 degrees = 0.01 degree in lat/lon
# each lightning region has 1km-radius circle size [assumption]
## - To create a status list of lightning

target_radius = 1   # hs default is in km
'''
target_radius = 0.5   # hs default is in km
target_radius = 0.1   # hs default is in km
'''

# make sure this is before outage-list is updated later
COMED_device_lightning_status = COMED_device_out_status.copy()
for col in COMED_device_lightning_status.columns:
    COMED_device_lightning_status[col].values[:] = 0


# for it_out_record in tqdm(range(9)):
for it_out_record in tqdm(range(len(COMED_device_5days['DEVICE']))):
    if COMED_device_5days['HAS_LIGHTNING'][it_out_record] == 1:
        COMED_device_lightning_status.iloc[it_out_record, COMED_device_out_status.iloc[it_out_record].gt(0).idxmax()] = 1
        # currently we only take 1st hour as lightning - to put into lightning status df.
        center_lat = COMED_device_5days['GIS_ISOLATE_LATITUDE'][it_out_record]
        center_lon = COMED_device_5days['GIS_ISOLATE_LONGITUDE'][it_out_record]
        for it_out_record_2 in range(len(COMED_device_5days['DEVICE'])):
            target_lat = COMED_device_5days['GIS_ISOLATE_LATITUDE'][it_out_record_2]
            target_lon = COMED_device_5days['GIS_ISOLATE_LONGITUDE'][it_out_record_2]
            temp_distance = hs.haversine((center_lat, center_lon),(target_lat, target_lon))
            if temp_distance <= target_radius:
                tempseries = COMED_device_lightning_status.iloc[[it_out_record, it_out_record_2]].sum()
                tempseries[tempseries > 0] = 1
                COMED_device_lightning_status.iloc[it_out_record_2] = tempseries

COMED_device_lightning_status.to_excel(r'COMED_device_lightning_status_120h_1km.xlsx', index=False)
'''
COMED_device_lightning_status.to_excel(r'COMED_device_lightning_status_120h_0.5km.xlsx', index=False)
COMED_device_lightning_status.to_excel(r'COMED_device_lightning_status_120h_0.1km.xlsx', index=False)
'''

