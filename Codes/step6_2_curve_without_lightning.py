# Import modules
import datetime
import pandas as pd
import numpy as np
import pickle
import matplotlib;
import matplotlib.pyplot as plt
from scipy import stats
import operator
matplotlib.use('Qt5Agg')
from tqdm import tqdm


# Read from pickle - clean version
COMED_HILP_Derecho_clean = pickle.load( open( "COMED_HILP_Derecho_clean.pickle", "rb" ) )
'''
# Read from EXCEL - clean version
COMED_HILP_Derecho_clean = pd.read_excel(r'COMED_HILP_Derecho_Hourly_0809-0811_replicate_clean.xlsx')

with open('COMED_HILP_Derecho_clean.pickle', 'wb') as handle:
    pickle.dump(COMED_HILP_Derecho_clean, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
COMED_HILP_Derecho_clean = pickle.load( open( "COMED_HILP_Derecho_clean.pickle", "rb" ) )
'''

'''
# CHECK NAN VALUES
testdf = COMED_HILP_Derecho_clean
testdf = COMED_HILP_Derecho
print("Count total NaN at each column in a DataFrame : \n\n", testdf.isnull().sum())
'''


### Use lightning == 1 or 0 to separate the df, and build separate lines
### Only show difference at the plot stage [same figure, different .py code to plot on it]
COMED_HILP_Derecho_clean = COMED_HILP_Derecho_clean[COMED_HILP_Derecho_clean['Lightning'] == 0]




### outage - 3points-max


COMED_HILP_Derecho_clean_outage = COMED_HILP_Derecho_clean[COMED_HILP_Derecho_clean['Outage_status'] == 1]
list_index_outage = COMED_HILP_Derecho_clean_outage.index.tolist()
list_index_outage_not_start = []
for it_clean in range(1, len(list_index_outage)):
    if list_index_outage[it_clean - 1] == list_index_outage [it_clean] - 1:
        list_index_outage_not_start.append(list_index_outage[it_clean])

list_index_outage_start = list(set(list_index_outage) - set(list_index_outage_not_start))
list_index_outage_start.sort()

#### Note - do not include other as intact, because we do not know if device under outage before repair will fail or not under those weather metrics
# list_index_intact = list(set(range(len(COMED_HILP_Derecho_clean))) - set(list_index_outage_start))
# list_index_intact.sort()

COMED_HILP_Derecho_clean_outage = COMED_HILP_Derecho_clean.loc[list_index_outage_start]
# COMED_HILP_Derecho_clean_intact = COMED_HILP_Derecho_clean.loc[list_index_intact]
COMED_HILP_Derecho_clean_intact = COMED_HILP_Derecho_clean[COMED_HILP_Derecho_clean['Outage_status'] == 0]

### to pick the maximum in neighboring 3 points, this step could be omitted in later temp - that has two extremes

for it_out_seq in tqdm(range(len(list_index_outage_start))):
    it_out = list_index_outage_start[it_out_seq]
    temp_p3 = COMED_HILP_Derecho_clean.loc[it_out-1 : it_out+1]
    COMED_HILP_Derecho_clean_outage.loc[it_out, 'temp'] = max(temp_p3['temp'])
    COMED_HILP_Derecho_clean_outage.loc[it_out, 'prcp'] = max(temp_p3['prcp'])
    COMED_HILP_Derecho_clean_outage.loc[it_out, 'wspd'] = max(temp_p3['wspd'])

'''
with open('COMED_HILP_Derecho_clean_outage.pickle', 'wb') as handle:
    pickle.dump(COMED_HILP_Derecho_clean_outage, handle, protocol=pickle.HIGHEST_PROTOCOL)
with open('COMED_HILP_Derecho_clean_intact.pickle', 'wb') as handle:
    pickle.dump(COMED_HILP_Derecho_clean_intact, handle, protocol=pickle.HIGHEST_PROTOCOL)
    
COMED_HILP_Derecho_clean_outage = pickle.load( open( "COMED_HILP_Derecho_clean_outage.pickle", "rb" ) )
COMED_HILP_Derecho_clean_intact = pickle.load( open( "COMED_HILP_Derecho_clean_intact.pickle", "rb" ) )
'''
Dict_All = {'outage': COMED_HILP_Derecho_clean_outage, 'intact': COMED_HILP_Derecho_clean_intact}

'''
## Device Type NAME
FUSE
CUSTOMER
TRANSFORMER
LINE RECLOSER
SUBSTATION BREAKER
SWITCH
'''
Dict_fuse = {'outage': COMED_HILP_Derecho_clean_outage[COMED_HILP_Derecho_clean_outage['Device_Type'] == 'FUSE'], 'intact': COMED_HILP_Derecho_clean_intact[COMED_HILP_Derecho_clean_intact['Device_Type'] == 'FUSE']}
Dict_customer = {'outage': COMED_HILP_Derecho_clean_outage[COMED_HILP_Derecho_clean_outage['Device_Type'] == 'CUSTOMER'], 'intact': COMED_HILP_Derecho_clean_intact[COMED_HILP_Derecho_clean_intact['Device_Type'] == 'CUSTOMER']}
Dict_transformer = {'outage': COMED_HILP_Derecho_clean_outage[COMED_HILP_Derecho_clean_outage['Device_Type'] == 'TRANSFORMER'], 'intact': COMED_HILP_Derecho_clean_intact[COMED_HILP_Derecho_clean_intact['Device_Type'] == 'TRANSFORMER']}
Dict_linerecloser = {'outage': COMED_HILP_Derecho_clean_outage[COMED_HILP_Derecho_clean_outage['Device_Type'] == 'LINE RECLOSER'], 'intact': COMED_HILP_Derecho_clean_intact[COMED_HILP_Derecho_clean_intact['Device_Type'] == 'LINE RECLOSER']}
Dict_subbreaker = {'outage': COMED_HILP_Derecho_clean_outage[COMED_HILP_Derecho_clean_outage['Device_Type'] == 'SUBSTATION BREAKER'], 'intact': COMED_HILP_Derecho_clean_intact[COMED_HILP_Derecho_clean_intact['Device_Type'] == 'SUBSTATION BREAKER']}
Dict_switch = {'outage': COMED_HILP_Derecho_clean_outage[COMED_HILP_Derecho_clean_outage['Device_Type'] == 'SWITCH'], 'intact': COMED_HILP_Derecho_clean_intact[COMED_HILP_Derecho_clean_intact['Device_Type'] == 'SWITCH']}




wm_name = 'wspd'  # hourly data Meteostat - The average wind speed in km/h
binnum = 80

wm_name = 'prcp'  # hourly data Meteostat - The one hour precipitation total in mm
binnum = 50

wm_name = 'temp'     # The air temperature in °C
binnum = 35



def figstwo_record_outage_intact_trends (plot_dict, wm_name, device_name):
    SMALL_SIZE = 18
    MEDIUM_SIZE = 20
    BIGGER_SIZE = 22
    plt.rc('font', size=SMALL_SIZE)  # controls default text sizes
    plt.rc('axes', titlesize=SMALL_SIZE)  # fontsize of the axes title
    plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
    plt.rc('xtick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
    plt.rc('legend', fontsize=SMALL_SIZE)  # legend fontsize
    plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

    plot_df1 = plot_dict['outage']
    plot_df2 = plot_dict['intact']

    fig, (ax1, ax2) = plt.subplots(1, 2)
    fig.suptitle(f'Comparison of Outage Records and Intact Records for {device_name}')
    if wm_name == 'wspd':
        fig.supxlabel(f'Hourly Average Wind Speed (unit: km/h)')
    elif wm_name == 'prcp':
        fig.supxlabel(f'Hourly Total Precipitation (unit: mm)')
    elif wm_name == 'temp':
        fig.supxlabel(f'Hourly Average Temperature (unit: degree C)')
    fig.supylabel ('Number of records')

    ax1.hist(plot_df1[wm_name], bins = binnum, stacked=True, label = 'Outage Record Only')
    ax1.legend(['Outage Records Only'], loc="upper left")
    test = ax2.hist([plot_df1[wm_name],plot_df2[wm_name]], bins = binnum, stacked=True)
    ax2.legend(['Outage Records', 'Intact Records'], loc="upper left")

    frq = test[0]
    edges = test[1]
    normfrq = frq.copy()
    # frq_weight = frq[0, :].copy()
    for it in range(np.shape(frq)[1]):
        normfrq[0, it] = frq[0, it] / frq[1, it]
        normfrq[1, it] = (frq[1, it] - frq[0, it]) / frq[1, it]
        # frq_weight[it] = frq[0, it] + frq[0, it]
    normfrq[np.isnan(normfrq)] = 0

    fig, ax = plt.subplots()
    fig.suptitle(f'Normalized Failure Trends for {device_name}')
    if wm_name == 'wspd':
        fig.supxlabel(f'Hourly Average Wind Speed (unit: km/h)')
    elif wm_name == 'prcp':
        fig.supxlabel(f'Hourly Total Precipitation (unit: mm)')
    elif wm_name == 'temp':
        fig.supxlabel(f'Hourly Average Temperature (unit: degree C)')
    fig.supylabel ('Normalized Records Ratio')

    ax.bar(edges[:-1], normfrq[0], width=np.diff(edges), edgecolor="black", color='red', align="edge", label='Outage')
    ax.bar(edges[:-1], normfrq[1], width=np.diff(edges), edgecolor="black", color='green', align="edge",
           bottom=normfrq[0], label='Intact')
    ax.legend(['Outage Records', 'Intact Records'], loc="upper left")

    return test

test = figstwo_record_outage_intact_trends(Dict_All, wm_name, 'All Device - without lightning')
test = figstwo_record_outage_intact_trends(Dict_fuse, wm_name, 'Fuse')
test = figstwo_record_outage_intact_trends(Dict_customer, wm_name, 'Customer')
test = figstwo_record_outage_intact_trends(Dict_transformer, wm_name, 'Transformer')
test = figstwo_record_outage_intact_trends(Dict_linerecloser, wm_name, 'Line Recloser')
test = figstwo_record_outage_intact_trends(Dict_subbreaker, wm_name, 'Substation Breaker')
test = figstwo_record_outage_intact_trends(Dict_switch, wm_name, 'Switch')


def trends_to_fpcum (test):
    # build failure trends data from hist_results
    frq = test[0]
    # frq[0] is the outage records
    # frq[1] is outage + intact records - based on output of python-hist function
    edges = test[1]
    normfrq = frq.copy()
    frq_weight = frq[0,:].copy()
    for it in range(np.shape(frq)[1]):
        normfrq[0, it] = frq[0, it] / frq[1, it]
        # normfrq[0] is the outage percentage
        normfrq[1, it] = (frq[1, it] - frq[0, it]) / frq[1, it]
        # normfrq[1] is the intact percentage
        frq_weight [it] = frq[0, it]
    normfrq[np.isnan(normfrq)] = 0
    # build cumulative probability
    fp_cum_weight = list(map(operator.mul, normfrq[0],frq_weight))
    fp_cum_sum = sum(fp_cum_weight)
    fp_cum_step = [i/fp_cum_sum for i in fp_cum_weight]
    fp_cum = np.cumsum(fp_cum_step)
    fp_cum = np.insert(fp_cum, 0, 0)
    return [edges, fp_cum]
[edges, fp_cum] = trends_to_fpcum(test)


'''
plt.close (2)
plt.close (3)

plt.show()
'''

def cdf_oneline_colordefault(inputdata1, inputdata2, device_name, binnum, wm_name):
    # extend_coef = 1
    x1 = inputdata1
    # get the cdf values of y
    N1 = len(inputdata1)
    y1 = inputdata2
    # fontsize
    SMALL_SIZE = 18
    MEDIUM_SIZE = 20
    BIGGER_SIZE = 22

    plt.rc('font', size=SMALL_SIZE)  # controls default text sizes
    plt.rc('axes', titlesize=SMALL_SIZE)  # fontsize of the axes title
    plt.rc('axes', labelsize=MEDIUM_SIZE)  # fontsize of the x and y labels
    plt.rc('xtick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
    plt.rc('ytick', labelsize=SMALL_SIZE)  # fontsize of the tick labels
    plt.rc('legend', fontsize=SMALL_SIZE)  # legend fontsize
    plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

    # plotting
    plt.xlim(0-2.5, binnum+2.5)
    if wm_name == 'wspd':
        plt.xlabel(f'Hourly Average Wind Speed (unit: km/h)')
    elif wm_name == 'prcp':
        plt.xlabel(f'Hourly Total Precipitation (unit: mm)')
    elif wm_name == 'temp':
        plt.xlabel(f'Hourly Average Temperature (unit: degree C)')
    plt.ylabel('Failure probability based on records from Derecho')
    plt.title('Fragility curves based on records from Derecho')
    # plt.plot(x1, y1, color = 'b', marker = 'o', markerfacecolor = 'b',  label = device_name)
    plt.plot(x1, y1, marker='o', label=device_name)
    plt.legend(loc="upper left")


cdf_oneline_colordefault(edges, fp_cum, 'All Devices - without lightning', binnum, wm_name)
cdf_oneline_colordefault(edges, fp_cum, 'Fuse', binnum, wm_name)
cdf_oneline_colordefault(edges, fp_cum, 'Customer', binnum, wm_name)
cdf_oneline_colordefault(edges, fp_cum, 'Transformer', binnum, wm_name)
cdf_oneline_colordefault(edges, fp_cum, 'Line Recloser', binnum, wm_name)
cdf_oneline_colordefault(edges, fp_cum, 'Substation Breaker', binnum, wm_name)
cdf_oneline_colordefault(edges, fp_cum, 'Switch', binnum, wm_name)




