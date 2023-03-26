
# importing libraries
import pandas as pd
import numpy as np
import os
import sys
import warnings
import xlsxwriter

from datetime import datetime
from datetime import timedelta
from datetime import date

import calendar 

import plotly
from plotly.subplots import make_subplots
import plotly.graph_objs as go

# Ignoring Warnings
warnings.filterwarnings("ignore")

# loading database
from arctic import Arctic, CHUNK_STORE
conn = Arctic('localhost')
lib = conn['entsoe']

# function to change timezone from UTC to local time
def changing_timezone(x):
    ts = x.index.tz_localize('utc').tz_convert('Europe/Brussels')
    y = x.set_index(ts)
    return y.tz_localize(None)


# parameters
list_countries = ['DE','FR','ES','BE','PL']

df_hist = pd.read_pickle(os.path.join(os.getcwd() + '/hist_data', 'df_hist_cap_price_DE.p'))

#-------------------------------------------------------------------------------------
#ref_date = datetime(year=2016, month=1, day=1).date()
#start_date = ref_date + timedelta(days = - 1)
#-------------------------------------------------------------------------------------
start_date = df_hist.index[-1] + timedelta(days = - 1)
#-------------------------------------------------------------------------------------
end_date = date.today().replace(day=1)

# function to create plots

def create_plot_monthly(df=None,
                country=None,
                list_countries=None):
    
    from datetime import datetime
    from plotly.subplots import make_subplots
    from datetime import timedelta
    
    #-----------------------------------------------------------------------------
    l  = list_countries.index(country)
    if ((l+1) % 2) == 0:
            k = 2
    else:
            k = 1    

    trace = go.Scatter(x=df.index, 
               y=df['cap_price_Solar'],
               name = 'Solar PV',
               line_color='yellow',
               showlegend=False if l>0 else True,
               legendgroup='g1',
               hovertemplate='%{x},%{y:.1f}',
                      )
    fig.append_trace(trace, -(-(l+1)//2), k)
    #-----------------------------------------------------------------------------
    trace = go.Scatter(x=df.index, 
               y=df['cap_price_Wind Onshore'],
               name = 'Onshore Wind',
               line_color='royalblue',
               showlegend=False if l>0 else True,
               legendgroup='g2',
               hovertemplate='%{x},%{y:.1f}',
                         )
    fig.append_trace(trace, -(-(l+1)//2), k)
    #-----------------------------------------------------------------------------
    try:
        trace = go.Scatter(x=df.index, 
               y=df['cap_price_Wind Offshore'],
               name = 'Offshore Wind',
               line_color='green',
               showlegend=False if l>0 else True,
               legendgroup='g3',
               hovertemplate='%{x},%{y:.1f}',
            )
        fig.append_trace(trace, -(-(l+1)//2), k) 
    except KeyError:
        pass
    #-----------------------------------------------------------------------------          
    trace = go.Scatter(
               x = df.index, 
               y = df['DayAheadPrices'+'_'+country], 
               name = 'Baseload',
               line = dict(color='indianred'),
               showlegend= False if l>0 else True,
               legendgroup='g4',
               hovertemplate='%{x},%{y:.1f}',
           )
    fig.add_trace(trace, -(-(l+1)//2), k)
 #----------------------------------------------------------------------------
    return fig

def create_plot_yearly(df=None,
                country=None,
                list_countries=None):

#-----------------------------------------------------------------------------
    l  = list_countries.index(country)
    if ((l+1) % 2) == 0:
            k = 2
    else:
            k = 1
            
    trace = go.Scatter(x=df.index, 
            y=df['cap_price_Solar'],
            name = 'Solar PV',
            line_color='yellow',
            showlegend=False if l>0 else True,
            legendgroup='g1',
            hovertemplate='%{x},%{y:.1f}')
    fig.append_trace(trace, -(-(l+1)//2), k)
#-----------------------------------------------------------------------------
    trace = go.Scatter(x=df.index, 
            y=df['cap_price_Wind Onshore'],
            name = 'Onshore Wind',
            line_color='royalblue',
            showlegend=False if l>0 else True,
            legendgroup='g2',
            hovertemplate='%{x},%{y:.1f}')
    fig.append_trace(trace, -(-(l+1)//2), k)
#-----------------------------------------------------------------------------
    try:
        trace = go.Scatter(x=df.index, 
            y=df['cap_price_Wind Offshore'],
            name = 'Offshore Wind',
            line_color='green',
            showlegend=False if l>0 else True,
            legendgroup='g3',
            hovertemplate='%{x},%{y:.1f}')
        fig.append_trace(trace, -(-(l+1)//2), k) 
    except KeyError:
        pass
#-----------------------------------------------------------------------------          
    trace = go.Scatter(
            x = df.index, 
            y = df['DayAheadPrices'+'_'+country], 
            name = 'Baseload',
            line = dict(color='indianred'),
            showlegend= False if l>0 else True,
            legendgroup='g4',
            hovertemplate='%{x},%{y:.1f}'
        )
    fig.add_trace(trace, -(-(l+1)//2), k)
 #-----------------------------------------------------------------------------
    return fig

writer_1 = pd.ExcelWriter('hist_cap_price_monthly.xlsx', engine='xlsxwriter')
writer_2 = pd.ExcelWriter('hist_cap_price_yearly.xlsx', engine='xlsxwriter')
workbook_1 = writer_1.book
workbook_2 = writer_2.book
    
#-----------------------------------------------------------------------------
fig = plotly.subplots.make_subplots(
            rows=3, cols=2, 
            subplot_titles = list_countries,
            shared_xaxes=False,
            vertical_spacing=0.1)

for i in list_countries:
    
    df_hist_cap_price = pd.read_pickle(os.path.join(os.getcwd()+'/hist_data','df_hist_cap_price_'+i+'.p'))

    # Read Spot price
    var = 'DayAheadPrices_12.1.D'
    prefix = var + '_' + i 
    
    df_DA_price = lib.read(prefix, chunk_range=pd.date_range(start_date, end_date))
    
    # Read power generation data
    var = 'AggregatedGenerationPerType_16.1.B_C'
    prefix = var + '_' + i 
    
    df_gen = lib.read(prefix,chunk_range=pd.date_range(start_date, end_date))
    
    # Extract Solar and Wind data
    
    var = 'ActualGenerationOutput'
    res_tech = ['Solar','Wind Onshore', 'Wind Offshore']
    
    df_res_gen = pd.DataFrame()
    prefix = var + ' ' + i 
    for j in res_tech:
        try:
            df_res_gen = pd.concat([df_res_gen,df_gen[prefix+' '+j]],axis=1)
        except KeyError:
            pass 
        
    df_res_gen.index = pd.to_datetime(df_res_gen.index)
    
    df_DA_price =df_DA_price[~df_DA_price.index.duplicated()]
    df_res_gen =df_res_gen[~df_res_gen.index.duplicated()]
    
   # merging data to a single dataframe

    var = [df_DA_price,df_res_gen]     
    df_merge = pd.DataFrame(columns=[])
    
    for j in var:
        df_merge = pd.merge(df_merge, j,how='outer',right_index=True, left_index=True)
    
    for j in res_tech:
        try:
            df_merge['t_cap'+' '+j] = df_merge['DayAheadPrices_'+i]*df_merge[prefix+' '+j]
        except KeyError:
            pass

    # convert 15 min data to hourly data
    df_merge = df_merge.resample('H').mean()
    
    # changing timezones 
    df_merge = changing_timezone(df_merge)
    
    #df_h= df_merge.iloc[(df_merge.index.year >=2016)&(df_merge.index.date<end_date)]
    
    df_merge = df_merge.iloc[(df_merge.index.date<end_date)]
    
    df_h = pd.concat([df_hist_cap_price,df_merge])
    
    df_h = df_h[~df_h.index.duplicated(keep='last')]
    
    #df_d = df_h.groupby(df_h.index.date).mean()
    
    df_m = df_h.groupby([(df_h.index.year),(df_h.index.month)]).mean()
    
    df_y = df_h.groupby([(df_h.index.year)]).mean()
    
    for j in res_tech:
        try:
            df_m['cap_price' +'_'+j] = df_m['t_cap' +' '+ j]/df_m[prefix + ' ' + j]
            df_y['cap_price' +'_'+j] = df_y['t_cap' +' '+ j]/df_y[prefix + ' ' + j]
        except KeyError:
            pass
        
    df_m['qf_Solar'] = df_m['cap_price_Solar']/df_m['DayAheadPrices_'+i]
    df_m['qf_Wind Onshore'] = df_m['cap_price_Wind Onshore']/df_m['DayAheadPrices_'+i]
    
    df_y['qf_Solar'] = df_y['cap_price_Solar']/df_y['DayAheadPrices_'+i]
    df_y['qf_Wind Onshore'] = df_y['cap_price_Wind Onshore']/df_y['DayAheadPrices_'+i]
    try:
        df_m['qf_Wind Offshore'] = df_m['cap_price_Wind Offshore']/df_m['DayAheadPrices_'+i]
        df_y['qf_Wind Offshore'] = df_y['cap_price_Wind Offshore']/df_y['DayAheadPrices_'+i]
    except KeyError:
        pass
        
    df = df_m
    
    #Use calendar library for abbreviations and order
    dd=dict((enumerate(calendar.month_abbr)))
    
    #rename level zero of multiindex
    df = df.rename(index=dd,level=1)
    
    #Create calendar month data type with order for sorting
    cal_dtype = pd.CategoricalDtype(list(calendar.month_abbr), ordered=True)
    
    #Change the dtype of the level zero index
    df.index = df.index.set_levels(df.index.levels[1].astype(cal_dtype), level=1)
    
    try:
        df_m_final = df[['DayAheadPrices_'+i,'cap_price_Solar','qf_Solar', 
                       'cap_price_Wind Onshore','qf_Wind Onshore',
                       'cap_price_Wind Offshore', 'qf_Wind Offshore']]
        df_y_final = df_y[['DayAheadPrices_'+i,'cap_price_Solar','qf_Solar', 
                       'cap_price_Wind Onshore','qf_Wind Onshore',
                       'cap_price_Wind Offshore', 'qf_Wind Offshore']]
    except KeyError:
        df_m_final = df[['DayAheadPrices_'+i,'cap_price_Solar','qf_Solar', 
                       'cap_price_Wind Onshore','qf_Wind Onshore']]
        df_y_final = df_y[['DayAheadPrices_'+i,'cap_price_Solar','qf_Solar', 
                       'cap_price_Wind Onshore','qf_Wind Onshore']]

    df_m_final.to_excel(writer_1, sheet_name=i)
    df_y_final.to_excel(writer_2, sheet_name=i)
    
    dx = df_m
    y = dx.index.get_level_values(0)
    z = dx.index.get_level_values(1)

    dx['date'] = pd.to_datetime(y * 10000 + z * 100 + 1, format="%Y%m%d")
    dx.set_index('date', append=True, inplace=True)
    dx = dx.reset_index(level=[0,1])
         
    #fig = create_plot_yearly(df = df_y,
    #                  country = i,
    #                  list_countries = list_countries)
    
    fig = create_plot_monthly(df = dx,
                    country = i,
                    list_countries = list_countries)
        
    df_h.to_pickle(os.path.join(os.getcwd() +'/hist_data', 'df_hist_cap_price_'+i+'.p'))
    
writer_1.save()
workbook_1.close()
writer_2.save()
workbook_2.close()

# Add figure title
fig.update_layout(
        title_text="RES Capture Price in EU",
        plot_bgcolor="#FFF",  # Sets background color to white,
    )
fig.update_yaxes(title_text="€/MWh", title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 1, col = 1)
fig.update_yaxes(title_text="€/MWh", title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 1, col = 2)
fig.update_yaxes(title_text="€/MWh", title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 2, col = 1)
fig.update_yaxes(title_text="€/MWh", title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 2, col = 2)
fig.update_yaxes(title_text="€/MWh", title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 3, col = 1)

fig.update_xaxes(title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 1, col = 1)
fig.update_xaxes(title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 1, col = 2)
fig.update_xaxes(title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 2, col = 1)
fig.update_xaxes(title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 2, col = 2)
fig.update_xaxes(title_font = dict(size = 12), linecolor = "#BCCCDC", showgrid=False, row = 3, col = 1)

outfile = 'Capture Price - '+ (end_date-timedelta(days=1)).strftime('%B-%Y') + '.html'
filename = os.path.join(os.getcwd() + '/plots', outfile)

f = open(filename,"w")  # append mode 
f.write(fig.to_html(full_html=False))
f.close()
