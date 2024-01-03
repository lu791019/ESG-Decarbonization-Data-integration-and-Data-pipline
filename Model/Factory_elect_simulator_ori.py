#!/usr/bin/python
import time
import os
import json
# from azureml.core import Workspace, Dataset
# from azureml.core.authentication import ServicePrincipalAuthentication
# from inference_schema.schema_decorators import input_schema, output_schema
# from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
# from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
# from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
import psycopg2
import pandas as pd
from datetime import datetime
import math
import numpy as np
from sqlalchemy import create_engine
import urllib.request
import json
import requests
import ssl
from time import strptime
import numpy_financial as npf
import urllib.parse
from services.mail_service import MailService
from models import engine
from utils.indicator_queue import IndicatorQueue

from app.celery import app

def baseline_data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)

    # close the communication with the PostgreSQL
    # cur.close()

    data_result_history = data_result[data_result.columns[np.where([x.find('id')==-1 for x in data_result.columns])]]
    data_result_history.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                                   'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                                   '營業額（十億NTD）','外氣平均溫度（℃）','plant','site','last_update_time']
    return data_result, data_result_history

def data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql,con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result

def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null',np.nan)
    for x in dataset.columns:
        if x in ['datetime','plant','bo','日期','site','year','month']:
            dataset[x] = dataset[x].astype('string')
        # elif x in ['year','month']:
        #     dataset[x] = dataset[x].astype('int')
        else:
            dataset[x] = dataset[x].astype('float')
    return dataset

def data_uploader(data, db_name, table_name):
    # Truncate table
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name,conn,index= False, if_exists = 'append',schema=db_name, chunksize = 10000)
    return 0

def data_uploader_append(data, db_name, table_name):
    # Truncate table
    # connect_string = engine.get_connect_string()
    # conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    # conn.execute(f'TRUNCATE TABLE '+db_name+'.'+table_name+';')

    # Connect to DB to upload data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name,conn,index= False, if_exists = 'append',schema=db_name, chunksize = 10000)
    return 0

def real_power_computer(power_real_data):
    # modify column names
    power_real_data = power_real_data[power_real_data.columns[np.where([x.find('id')==-1 for x in power_real_data.columns])]]
    power_real_data.columns = ['日期','工廠用電（kwh）','空調用電（kwh）','空壓用電（kwh）','生產用電（kwh）','基礎用電（kwh）','宿舍用電（kwh）',
                               'PCBA產量（pcs)','FA產量（pcs)','人數（人）','PCBA平均開線數（條）','FA平均開線數量（條）',
                               '營業額（十億NTD）','外氣平均溫度（℃）','plant','site','last_update_time']
    power_ten_month_total = power_real_data.loc[[power_real_data['日期'].astype(str)[x][5:7]<'11' for x in range(power_real_data.shape[0])],:].reset_index(drop=True)
    power_ten_month_total['year'] = [power_ten_month_total['日期'].astype(str)[x][0:4] for x in range(power_ten_month_total.shape[0])]
    power_ten_month_total = power_ten_month_total.groupby(['plant','site','year'], group_keys=True).agg({'工廠用電（kwh）':'sum','宿舍用電（kwh）':'sum','日期':'size'}).reset_index().rename(columns={'日期':'month_count'})
    # Compute total power
    power_ten_month_total['ten_month_real'] = power_ten_month_total['工廠用電（kwh）']+12*power_ten_month_total['宿舍用電（kwh）']/power_ten_month_total['month_count']
    power_ten_month_total_final = power_ten_month_total.loc[power_ten_month_total.month_count==10,:]
    return power_ten_month_total_final

def irr_func(x):
    if len(x)>1:
        x = np.array(x)
        x_new = np.append(-1*x[0],np.diff(x)[0:(len(x)-2)])
        x_new = np.append(x_new,x[len(x)-1])
        return npf.irr(x_new)
    else:
        x_new = 0
        return x_new
def cagr_func(x):
    if len(x)>1:
        x = np.array(x)
        x_new = (x[len(x)-1]/x[0])**(1/len(x))-1
    else:
        x_new = 0
    return x_new

def trend_rate_generator(dataset,power_usage,method):
    # data_result_trend_revenue_full = dataset.loc[(dataset.month_cnt==12) & (dataset.plant!='WKS-1') & (dataset.year!='2022'),:]
    data_result_trend_revenue_full = dataset.loc[(dataset.plant!='WKS-1'),:]
    data_result_trend_revenue_full['year_'+power_usage]=data_result_trend_revenue_full['year_'+power_usage]/data_result_trend_revenue_full.month_cnt
    if method=='cagr':
        data_result_trend_revenue_min = data_result_trend_revenue_full.groupby(['plant','bo']).agg({'year_'+power_usage:cagr_func}).reset_index().rename(columns={'year_'+power_usage:power_usage+'_rate'})
        data_result_trend_revenue_min[power_usage+'_rate']=[np.quantile(data_result_trend_revenue_min.loc[data_result_trend_revenue_min[power_usage+'_rate']>0,[power_usage+'_rate']],0.25) if x<0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
    elif method=='irr':
        data_result_trend_revenue_min = data_result_trend_revenue_full.groupby(['plant','bo']).agg({'year_'+power_usage:irr_func}).reset_index().rename(columns={'year_'+power_usage:power_usage+'_rate'})
        data_result_trend_revenue_min[power_usage+'_rate']=[np.quantile(data_result_trend_revenue_min.loc[data_result_trend_revenue_min[power_usage+'_rate']>0,[power_usage+'_rate']],0.25) if x<0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
    else:
        data_result_trend_revenue_max = pd.merge(data_result_trend_revenue_full,
                                                 data_result_trend_revenue_full.groupby(['plant','bo']).agg({'year':'max'}).reset_index(),
                                                 on=['plant','bo','year'],how='inner').rename(columns={'year':'year_max','month_cnt':'month_cnt_max','year_'+power_usage:'year_'+power_usage+'_max'})
        data_result_trend_revenue_min = pd.merge(data_result_trend_revenue_full,
                                                 data_result_trend_revenue_max,
                                                 on=['plant','bo'],how='left')
        data_result_trend_revenue_min = data_result_trend_revenue_min.loc[data_result_trend_revenue_min.year_max.astype(int)-data_result_trend_revenue_min.year.astype(int)==1,:].reset_index(drop=True)
        data_result_trend_revenue_min[power_usage+'_rate']=(data_result_trend_revenue_min['year_'+power_usage+'_max']-data_result_trend_revenue_min['year_'+power_usage])/data_result_trend_revenue_min['year_'+power_usage]
        data_result_trend_revenue_min[power_usage+'_rate']=[np.quantile(data_result_trend_revenue_min.loc[data_result_trend_revenue_min[power_usage+'_rate']>0,[power_usage+'_rate']],0.25) if x<0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
        # data_result_trend_revenue_min[power_usage+'_rate']=[np.mean(data_result_trend_revenue_min.loc[:,[power_usage+'_rate']])[0] if x<0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]

    
    return data_result_trend_revenue_min[['plant','bo',power_usage+'_rate']]

def history_data_fixer(data_result, data_result_history):
    data_result_wok = data_result.loc[(data_result.plant=='WOK') & (data_result.datetime==pd.to_datetime('2022-11-01')),:]
    data_result_wok['datetime'] = datetime.date(pd.to_datetime('2022-12-01'))
    data_result = (data_result.loc[~((data_result.plant=='WOK') & (data_result.datetime==pd.to_datetime('2022-12-01'))),:]).append(data_result_wok).reset_index(drop=True)
    data_result_history_wok = data_result_history.loc[(data_result_history.plant=='WOK') & (data_result_history['日期']==pd.to_datetime('2022-11-01')),:]
    data_result_history_wok['日期'] = datetime.date(pd.to_datetime('2022-12-01'))
    data_result_history = (data_result_history.loc[~((data_result_history.plant=='WOK') & (data_result_history['日期']==pd.to_datetime('2022-12-01'))),:]).append(data_result_history_wok).reset_index(drop=True)
    #--------
    # fix wcd pcba, fa qty
    data_result_wcd = data_result.loc[(data_result.plant=='WCD') & (data_result.datetime==pd.to_datetime('2022-11-01')),:]
    data_result.loc[(data_result.plant=='WCD') & (data_result.datetime==pd.to_datetime('2022-12-01')),['pcba_qty']] = data_result_wcd.pcba_qty.iloc[0]
    data_result.loc[(data_result.plant=='WCD') & (data_result.datetime==pd.to_datetime('2022-12-01')),['fa_qty']] = data_result_wcd.fa_qty.iloc[0]
    # fix wzs-8 pcba, fa qty
    data_result_wzs8 = data_result.loc[(data_result.plant=='WZS-8') & (data_result.datetime==pd.to_datetime('2022-11-01')),:]
    data_result.loc[(data_result.plant=='WZS-8') & (data_result.datetime==pd.to_datetime('2022-12-01')),['pcba_qty']] = data_result_wzs8.pcba_qty.iloc[0]
    data_result.loc[(data_result.plant=='WZS-8') & (data_result.datetime==pd.to_datetime('2022-12-01')),['fa_qty']] = data_result_wzs8.fa_qty.iloc[0]

    data_result = pd.merge(data_result,
                           data_result.loc[(~data_result.ac_electricity.isna()) & (~data_result.revenue.isna()) & (~data_result.pcba_qty.isna()),:].groupby(['plant','bo']).agg({'datetime':'max'}).reset_index().rename(columns={'datetime':'datetime_max'}),
                           on=['plant','bo'], how='left')
    return data_result, data_result_history

def variable_data_generator(data_result):
    data_result_trend_raw = data_result.loc[(data_result.datetime.astype(str)>='2020-01-01') & ((data_result.datetime.astype(str)<='2023-04-01')),:].reset_index(drop=True) # fix me!!!!!!
    data_result = data_result.loc[((data_result['plant'].isin(['WIH','WCQ'])) & (data_result['datetime'].astype(str)>'2020-12-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ'])) & (data_result['datetime'].astype(str)>'2020-09-01') & (data_result['datetime']<=data_result['datetime_max'])) | ((~data_result['plant'].isin(['WKS-5','WKS-6','WOK','WTZ','WIH','WCQ'])) & (data_result['datetime']<=data_result['datetime_max'])),:].reset_index(drop=True)
    # print(data_result)
    # Compute 2022 power usage total
    # data_result_2022 = data_result.loc[(data_result.datetime.astype(str)>='2022-01-01') & ((data_result.datetime.astype(str)<='2022-12-01')),:].reset_index(drop=True)
    # power_usage_2022_real = sum((data_result_2022.loc[data_result_2022.plant.isin(['WZS-1','WZS-3','WZS-6','WZS-8','WKS-5','WKS-6','WCD','WCQ','WIH','WOK','WTZ'])]).factory_electricity.astype('float'))
    # Compute 2022 item trend rate
    data_result_trend = pd.DataFrame(data_result_trend_raw) # fix me
    data_result_trend['year'] = [str(x)[0:4] for x in data_result_trend.datetime]
    data_result_trend['month'] = [str(x)[5:7] for x in data_result_trend.datetime]
    data_result_trend_revenue = data_result_trend.loc[~data_result_trend.revenue.isna(),:].groupby(['plant','bo','year']).agg({'revenue':'sum','datetime':'size'}).reset_index().rename(columns={'revenue':'year_revenue','datetime':'month_cnt'})
    data_result_trend_pcba_qty = data_result_trend.loc[~data_result_trend.pcba_qty.isna(),:].groupby(['plant','bo','year']).agg({'pcba_qty':'sum','datetime':'size'}).reset_index().rename(columns={'pcba_qty':'year_pcba_qty','datetime':'month_cnt'})
    data_result_trend_fa_qty = data_result_trend.loc[~data_result_trend.fa_qty.isna(),:].groupby(['plant','bo','year']).agg({'fa_qty':'sum','datetime':'size'}).reset_index().rename(columns={'fa_qty':'year_fa_qty','datetime':'month_cnt'})
    data_result_trend_member_counts = data_result_trend.loc[~data_result_trend.member_counts.isna(),:].groupby(['plant','bo','year']).agg({'member_counts':'sum','datetime':'size'}).reset_index().rename(columns={'member_counts':'year_member_counts','datetime':'month_cnt'})
    data_result_trend_ac_electricity = data_result_trend.loc[~data_result_trend.ac_electricity.isna(),:].groupby(['plant','bo','year']).agg({'ac_electricity':'sum','datetime':'size'}).reset_index().rename(columns={'ac_electricity':'year_ac_electricity','datetime':'month_cnt'})
    data_result_trend_ap_electricity = data_result_trend.loc[~data_result_trend.ap_electricity.isna(),:].groupby(['plant','bo','year']).agg({'ap_electricity':'sum','datetime':'size'}).reset_index().rename(columns={'ap_electricity':'year_ap_electricity','datetime':'month_cnt'})
    # Compute trend rate
    data_result_trend_revenue_min = trend_rate_generator(data_result_trend_revenue,'revenue','mape')
    data_result_trend_pcba_qty_min = trend_rate_generator(data_result_trend_pcba_qty,'pcba_qty','mape')
    data_result_trend_fa_qty_min = trend_rate_generator(data_result_trend_fa_qty,'fa_qty','mape')
    data_result_trend_member_counts_min = trend_rate_generator(data_result_trend_member_counts,'member_counts','mape')
    data_result_trend_ac_electricity_min = trend_rate_generator(data_result_trend_ac_electricity,'ac_electricity','mape')
    data_result_trend_ap_electricity_min = trend_rate_generator(data_result_trend_ap_electricity,'ap_electricity','mape')
    data_result_trend_final = pd.merge(data_result_trend_revenue_min,
                                         pd.merge(data_result_trend_member_counts_min,
                                                  pd.merge(data_result_trend_fa_qty_min,
                                                           pd.merge(data_result_trend_pcba_qty_min,
                                                                    pd.merge(data_result_trend_ac_electricity_min,
                                                                             data_result_trend_ap_electricity_min,
                                                                             on=['plant','bo'], how='outer'),
                                                                    on=['plant','bo'], how='outer'),
                                                           on=['plant','bo'], how='outer'), 
                                                  on=['plant','bo'], how='left'), 
                                         on=['plant','bo'], how='left')
    data_result_trend_final.loc[data_result_trend_final.fa_qty_rate.isna(),['fa_qty_rate']] = np.median(data_result_trend_final.loc[~data_result_trend_final.fa_qty_rate.isna(),['fa_qty_rate']])
    data_result_trend_final.loc[data_result_trend_final.pcba_qty_rate.isna(),['pcba_qty_rate']] = np.median(data_result_trend_final.loc[~data_result_trend_final.pcba_qty_rate.isna(),['pcba_qty_rate']])
    return data_result_trend_final

def simulation_data_generator(conn, data_result, data_result_history, data_result_trend_final, baseline_year, target_year):
    data_result_simulation_all = data_result.loc[(data_result.datetime>='2020-11-01') & ((data_result.datetime<=str(baseline_year+'-12-01'))),:].reset_index(drop=True)
    data_result_history_simulation_all = data_result_history.loc[(data_result_history['日期']>='2020-11-01') & (data_result_history['日期']<=str(baseline_year+'-12-01')),:].reset_index(drop=True)

    # Add WKS, WZS electricity seperate rate
    data_result_simulation_all_plant_rate = data_result_simulation_all.loc[(data_result_simulation_all.plant.isin(['WKS-5','WKS-6','WZS-1','WZS-3','WZS-6','WZS-8'])) & (data_result_simulation_all.datetime.astype(str)>='2022-01-01'),:]
    data_result_simulation_all_plant_rate['Month'] = [int(str(x)[5:7]) for x in data_result_simulation_all_plant_rate['datetime']]
    data_result_simulation_all_plant_rate['Site'] = [x[0:3] for x in data_result_simulation_all_plant_rate['plant']]
    data_result_simulation_all_plant_rate['factory_electricity'] = data_result_simulation_all_plant_rate['factory_electricity'].astype(float)
    data_result_simulation_all_plant_rate = pd.merge(data_result_simulation_all_plant_rate,
                                                     data_result_simulation_all_plant_rate.groupby(['Month','Site']).agg({'factory_electricity':'sum'}).rename(columns = {'factory_electricity':'factory_electricity_total'}).reset_index(),
                                                    on=['Month','Site'],how='left').reset_index(drop=True)
    data_result_simulation_all_plant_rate['factory_rate'] = data_result_simulation_all_plant_rate.factory_electricity/data_result_simulation_all_plant_rate.factory_electricity_total
    data_result_simulation_all_plant_rate = data_result_simulation_all_plant_rate[['plant','Site','Month','factory_rate']]

    # Y23 factory electory result
    # plant_predict_energy = pd.read_excel('D:/ESG/脫碳減碳方案/用電量資料/Annual Capacity  power consumption Survey Y23(Y22年底預估版).xlsx',sheet_name='工作表1')
    # plant_predict_energy_sub = plant_predict_energy.iloc[3:23,1:15].reset_index(drop=True)
    # plant_predict_energy_sub.columns = ['Site']+[x for x in plant_predict_energy.iloc[1,2:15]]
    # plant_predict_energy = pd.read_excel('D:/ESG/脫碳減碳方案/用電量資料/Annual Capacity  power consumption Survey Y23(Y22年底預估版) (1).xlsx',sheet_name='工作表1')
    # plant_predict_energy_sub = plant_predict_energy.iloc[3:45,1:15].reset_index(drop=True)
    # plant_predict_energy_sub.columns = ['Site']+[x for x in plant_predict_energy.iloc[1,2:15]]
    # # if type_name=='real_predict':
    # for y in ['Site','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']:
    #     if y!='Site':
    #         plant_predict_energy_sub[y] = [plant_predict_energy_sub[y][0]]+[float(np.where(pd.isna(plant_predict_energy_sub[y][x]),plant_predict_energy_sub[y][x-1],plant_predict_energy_sub[y][x])) for x in range(1,len(plant_predict_energy_sub[y]))]
    #     else:
    #         plant_predict_energy_sub[y] = [plant_predict_energy_sub[y][0]]+[str(np.where(pd.isna(plant_predict_energy_sub[y][x]),plant_predict_energy_sub[y][x-1],plant_predict_energy_sub[y][x])) for x in range(1,len(plant_predict_energy_sub[y]))]
    # plant_predict_energy_sub = plant_predict_energy_sub.loc[plant_predict_energy_sub['類別']=='實際',:].reset_index(drop=True)
    # plant_predict_energy_sub.columns = [x for x in plant_predict_energy_sub.columns[0:2]]+['M'+str(strptime(x,'%b').tm_mon) for x in plant_predict_energy_sub.columns[2:15]]
    # plant_predict_energy_sub = pd.wide_to_long(plant_predict_energy_sub, ['M'], i="Site", j="Month").reset_index().rename(columns = {'M':'factory_electricity'})
    load_decarb_data_query = "SELECT * FROM app.decarb_elect_summary;"
    decarb_data = data_loader(conn, load_decarb_data_query)
    max_version = max(decarb_data['version'])
    plant_predict_energy_sub = "SELECT * FROM app.decarb_elect_summary where (version='"+max_version+"') and (last_update_time is not null);"
    plant_predict_energy_sub = data_loader(conn, plant_predict_energy_sub)
    plant_predict_energy_sub['amount'] = plant_predict_energy_sub['amount']*1000
    plant_predict_energy_sub = plant_predict_energy_sub.rename(columns={'site':'Site','amount':'factory_electricity','month':'Month'})
    plant_predict_energy_sub['datetime'] = [str(pd.to_datetime(str(plant_predict_energy_sub['year'][x])+'-'+str(plant_predict_energy_sub['Month'][x])+'-01'))[0:10] for x in range(len(plant_predict_energy_sub.Month))]
    plant_predict_energy_sub1 = plant_predict_energy_sub.loc[~plant_predict_energy_sub.Site.isin(['WKS','WZS']),:].reset_index(drop=True)
    plant_predict_energy_sub1['plant'] = plant_predict_energy_sub1['Site']
    plant_predict_energy_sub2 = pd.merge(data_result_simulation_all_plant_rate,plant_predict_energy_sub,on=['Site','Month'],how='left').reset_index(drop=True)
    plant_predict_energy_sub2['factory_electricity'] = plant_predict_energy_sub2['factory_electricity']*plant_predict_energy_sub2['factory_rate']
    plant_predict_energy_all = plant_predict_energy_sub1.append(plant_predict_energy_sub2[plant_predict_energy_sub1.columns]).reset_index(drop=True)

    for x in range(1,(int(target_year)-int(baseline_year)+1)):
        # data 
        data_result_simulation = data_result.loc[(data_result.datetime>=str(baseline_year+'-01-01')) & ((data_result.datetime<=str(baseline_year+'-12-01'))),:]
        data_result_simulation = pd.merge(data_result_simulation, 
                                      data_result_trend_final, 
                                      on=['plant','bo'], how='left')
        data_result_simulation.average_temperature = data_result_simulation.average_temperature.astype('float')+0.02*x
        for factor in ['revenue','fa_qty','pcba_qty','member_counts','ac_electricity','ap_electricity']: #
            if factor=='member_counts':
                data_result_simulation[factor] = data_result_simulation[factor].astype('float')*(1+data_result_simulation[factor+'_rate'].astype('float')/2*x)
            else:
                data_result_simulation[factor] = data_result_simulation[factor].astype('float')*(1+data_result_simulation[factor+'_rate'].astype('float')*x)
        # data_result_simulation.revenue = data_result_simulation.revenue.astype('float')*(1+0.05*x)
        data_result_simulation['datetime'] = (pd.to_datetime(data_result_simulation['datetime'])+pd.DateOffset(years=x)).astype('string')
        data_result_simulation_all = data_result_simulation_all.append(data_result_simulation).reset_index(drop=True)
        # data history
        data_result_history_simulation = data_result_history.loc[(data_result_history['日期']>=str(baseline_year+'-01-01')) & (data_result_history['日期']<=str(baseline_year+'-12-01')),:]
        data_result_history_simulation = pd.merge(data_result_history_simulation, 
                                                  data_result_trend_final, 
                                                  on=['plant'], how='left')
        data_result_history_simulation['外氣平均溫度（℃）'] = data_result_history_simulation['外氣平均溫度（℃）'].astype('float')+0.02*x
        for factor_ori in ['營業額（十億NTD）','FA產量（pcs)','PCBA產量（pcs)','人數（人）','空調用電（kwh）','空壓用電（kwh）']: #
            factor = str(np.where(factor_ori=='營業額（十億NTD）','revenue',
                               np.where(factor_ori=='FA產量（pcs)','fa_qty',
                                       np.where(factor_ori=='PCBA產量（pcs)','pcba_qty',
                                                np.where(factor_ori=='人數（人）','member_counts',
                                                        np.where(factor_ori=='空調用電（kwh）','ac_electricity','ap_electricity'))))))
            print(factor)
            if factor=='member_counts':
                data_result_history_simulation[factor_ori] = data_result_history_simulation[factor_ori].astype('float')*(1+data_result_history_simulation[factor+'_rate'].astype('float')/2*x)
            else:
                data_result_history_simulation[factor_ori] = data_result_history_simulation[factor_ori].astype('float')*(1+data_result_history_simulation[factor+'_rate'].astype('float')*x)
        # data_result_history_simulation['營業額（十億NTD）'] = data_result_history_simulation['營業額（十億NTD）'].astype('float')*(1+0.05*x)
        data_result_history_simulation['日期'] = (pd.to_datetime(data_result_history_simulation['日期'])+pd.DateOffset(years=x)).astype('string')
        data_result_history_simulation_all = data_result_history_simulation_all.append(data_result_history_simulation).reset_index(drop=True)
    data_result_simulation_all = data_result_simulation_all.sort_values(by=['plant','datetime'], ascending=True).reset_index(drop=True)
    data_result_history_simulation_all = data_result_history_simulation_all.sort_values(by=['plant','日期'], ascending=True).reset_index(drop=True)

    # replace Y23 factory electricity to Gene version 
    plant_predict_energy_all['datetime'] = [str(pd.to_datetime('2023-'+str(x)+'-01'))[0:10] for x in plant_predict_energy_all['Month']]
    plant_predict_energy_all = plant_predict_energy_all.rename(columns={'factory_electricity':'factory_electricity_new'})
    data_result_simulation_all = pd.merge(data_result_simulation_all,
                                          plant_predict_energy_all[['plant','datetime','factory_electricity_new']],
                                          on=['plant','datetime'],how='left').reset_index(drop=True)
    data_result_simulation_all['factory_electricity'] = np.where(data_result_simulation_all['factory_electricity_new'].isna(),
                                                                data_result_simulation_all['factory_electricity'],
                                                                data_result_simulation_all['factory_electricity_new'])
    data_result_history_simulation_all = pd.merge(data_result_history_simulation_all,
                                          plant_predict_energy_all[['plant','datetime','factory_electricity_new']].rename(columns={'datetime':'日期'}),
                                          on=['plant','日期'],how='left').reset_index(drop=True)
    data_result_history_simulation_all['工廠用電（kwh）'] = np.where(data_result_history_simulation_all['factory_electricity_new'].isna(),
                                                                data_result_history_simulation_all['工廠用電（kwh）'],
                                                                data_result_history_simulation_all['factory_electricity_new'])
    return plant_predict_energy_all, data_result_simulation_all, data_result_history_simulation_all

def real_base_generator(conn, data_result_simulation_all, plant_predict_energy_all, base_start_month, base_end_month, predict_site):
    # Add Y22 real factory electricity
    # plant_predict_energy_Y22 = pd.read_excel('D:/ESG/脫碳減碳方案/用電量資料/Annual Capacity  power consumption Survey Y21-Y22.xlsx',sheet_name='用電量')
    # plant_predict_energy_sub_Y22 = plant_predict_energy_Y22.iloc[3:45,2:16].reset_index(drop=True)
    # plant_predict_energy_sub_Y22.columns = ['Site']+[x for x in plant_predict_energy_Y22.iloc[1,3:16]]
    # # if type_name=='real_predict':
    # for y in ['Site','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']:
    #     if y!='Site':
    #         plant_predict_energy_sub_Y22[y] = [plant_predict_energy_sub_Y22[y][0]]+[float(np.where(pd.isna(plant_predict_energy_sub_Y22[y][x]),plant_predict_energy_sub_Y22[y][x-1],plant_predict_energy_sub_Y22[y][x])) for x in range(1,len(plant_predict_energy_sub_Y22[y]))]
    #     else:
    #         plant_predict_energy_sub_Y22[y] = [plant_predict_energy_sub_Y22[y][0]]+[str(np.where(pd.isna(plant_predict_energy_sub_Y22[y][x]),plant_predict_energy_sub_Y22[y][x-1],plant_predict_energy_sub_Y22[y][x])) for x in range(1,len(plant_predict_energy_sub_Y22[y]))]
    #         plant_predict_energy_sub_Y22[y] = [plant_predict_energy_sub_Y22[y][0]]+[str(np.where(plant_predict_energy_sub_Y22[y][x]=='nan',plant_predict_energy_sub_Y22[y][x-1],plant_predict_energy_sub_Y22[y][x])) for x in range(1,(len(plant_predict_energy_sub_Y22[y])))]
    # plant_predict_energy_sub_Y22 = plant_predict_energy_sub_Y22.loc[plant_predict_energy_sub_Y22['類別'].isin(['實際用電量（市政）','總用電量']),:].reset_index(drop=True)
    # plant_predict_energy_sub_Y22 = plant_predict_energy_sub_Y22.groupby(['Site']).agg({'Jan':'max','Feb':'max','Mar':'max','Apr':'max','May':'max','Jun':'max','Jul':'max','Aug':'max','Sep':'max','Oct':'max','Nov':'max','Dec':'max'}).reset_index()
    # plant_predict_energy_sub_Y22.columns = [x for x in plant_predict_energy_sub_Y22.columns[0:1]]+['M'+str(strptime(x,'%b').tm_mon) for x in plant_predict_energy_sub_Y22.columns[1:15]]
    # plant_predict_energy_sub_Y22 = pd.wide_to_long(plant_predict_energy_sub_Y22, ['M'], i="Site", j="Month").reset_index().rename(columns = {'M':'factory_electricity'})
    # plant_predict_energy_sub_Y22['datetime'] = [str(pd.to_datetime('2022-'+str(x)+'-01'))[0:10] for x in plant_predict_energy_sub_Y22.Month]
    plant_predict_energy_Y22_query = "SELECT * FROM app.elect_target_month where (category='actual') and (year='2022') and (last_update_time is not null);"
    plant_predict_energy_sub_Y22 = data_loader(conn, plant_predict_energy_Y22_query)
    plant_predict_energy_sub_Y22 = plant_predict_energy_sub_Y22.rename(columns={'site':'Site','amount':'factory_electricity'})
    plant_predict_energy_sub_Y22['datetime'] = [str(pd.to_datetime(str(plant_predict_energy_sub_Y22['year'][x])+'-'+str(plant_predict_energy_sub_Y22['month'][x])+'-01'))[0:10] for x in range(len(plant_predict_energy_sub_Y22.month))]
    plant_predict_energy_sub_Y22['plant'] = plant_predict_energy_sub_Y22['Site']

    ##----------generate real factory electricity -------------##
    ##----------new method(best)-------------##
    ##----- consider Y22-07~Y23-06-----##
    # base_end_month = '2023-06-01'
    # base_start_month = str(np.datetime64(base_end_month[0:7]) - np.timedelta64(11, 'M'))+'-01'
    # Y22 by month factory real electricity
    # predict_site = ['XTRKS','KOE','WGKS','WGTX','WVN','WTN'] # no Y22 data
    data_upload_final_Y22real = plant_predict_energy_sub_Y22.loc[(~plant_predict_energy_sub_Y22.plant.isin(predict_site)) & (plant_predict_energy_sub_Y22.datetime>=base_start_month) & (plant_predict_energy_sub_Y22.datetime<='2022-12-01'),['plant','datetime','factory_electricity']].rename(columns={'factory_electricity':'factory_electricity_base'})
    data_upload_final_Y22real['month'] = [x[5:7] for x in data_upload_final_Y22real['datetime']]
    # Y22 by month factory real electricity in WKS-5,6
    data_upload_final_Y22real_wks = data_result_simulation_all.loc[(data_result_simulation_all.plant.isin(['WKS-5','WKS-6'])) & (data_result_simulation_all.datetime>=base_start_month) & (data_result_simulation_all.datetime<='2022-12-01'),['plant','datetime','factory_electricity']].rename(columns={'factory_electricity':'factory_electricity_base'})
    data_upload_final_Y22real_wks['month'] = [x[5:7] for x in data_upload_final_Y22real_wks['datetime']]
    # Y23 by month factory real electricity
    data_upload_final_Y23real = plant_predict_energy_all.loc[(((~plant_predict_energy_all.plant.isin(predict_site)) & (plant_predict_energy_all.datetime<=base_end_month)) | ((plant_predict_energy_all.plant.isin(predict_site)) & (plant_predict_energy_all.datetime<='2023-12-01'))) & (plant_predict_energy_all.datetime>='2023-01-01'),['plant','datetime','factory_electricity_new']].rename(columns={'factory_electricity_new':'factory_electricity_base'}).reset_index(drop=True)
    data_upload_final_Y23real['month'] = [x[5:7] for x in data_upload_final_Y23real['datetime']]
    data_upload_final_Y23real = data_upload_final_Y23real.append(data_upload_final_Y22real).reset_index(drop=True)
    data_upload_final_Y23real = data_upload_final_Y23real.append(data_upload_final_Y22real_wks).reset_index(drop=True)
    return data_upload_final_Y23real, plant_predict_energy_sub_Y22

def site_electricity_generator(data_upload_final, data_upload_final_Y23real, base_year, base_start_month, base_end_month, model_site):
    ##----------model predict electricity increase rate-------------##
    # New version
    # base_year = 2023
    data_upload_final_Y23base = data_upload_final.loc[data_upload_final.datetime.astype(str)>=base_start_month,:]
    data_upload_final_Y23base = pd.merge(data_upload_final_Y23base.loc[data_upload_final_Y23base.year.astype(int)>base_year-1,:],
                                         data_upload_final_Y23base.loc[(data_upload_final_Y23base.datetime>=base_start_month) & (data_upload_final_Y23base.datetime<=base_end_month),['month','plant','predict_electricity']].rename(columns = {'predict_electricity':'predict_electricity_base'}),
                                        on=['month','plant'],how='left')
    data_upload_final_Y23base['month_grouth_rate'] = (data_upload_final_Y23base['predict_electricity']-data_upload_final_Y23base['predict_electricity_base'])/data_upload_final_Y23base['predict_electricity_base']
    data_upload_final_Y23base['month_grouth_rate'] = np.where(data_upload_final_Y23base['month_grouth_rate']<0,(data_upload_final_Y23base['year'].astype(int)-base_year)*0.05,data_upload_final_Y23base['month_grouth_rate']) # fix me !!! 0.01
    data_upload_final_Y23base = pd.merge(data_upload_final_Y23base,
                                         data_upload_final_Y23real[['plant','month','factory_electricity_base']],
                                         on=['plant','month'],how='left').reset_index(drop=True)
    data_upload_final_Y23base['predict_electricity_v2'] = np.where(data_upload_final_Y23base.month_grouth_rate!=0,(1+data_upload_final_Y23base.month_grouth_rate.astype(float))*data_upload_final_Y23base.factory_electricity_base.astype(float),data_upload_final_Y23base.factory_electricity_base.astype(float))
    data_upload_final_Y23base_year = data_upload_final_Y23base.groupby(['year'], group_keys=True).agg({'predict_electricity_v2':'sum'}).reset_index().rename(columns={'predict_electricity_v2':'predict_electricity_v2_total'})

    # Compute rate = (build model sites)/(total sites) 
    total_energy = data_upload_final_Y23real.copy()#.loc[(~total_energy.factory_electricity_base.isna()),:]
    total_energy['Site_top'] = [str(np.where(real_id.find('-')!=-1,str(real_id[0:(real_id.find('-'))]),real_id)) for real_id in total_energy.plant]
    total_energy['factory_electricity_base'] = (total_energy['factory_electricity_base'].astype(str)).astype(float)
    total_energy_site = total_energy.loc[(total_energy.plant!='WKS-Zara') & (total_energy.plant!='WCQ-HP'),:].groupby(['Site_top','month'], group_keys=True).agg({'factory_electricity_base':'sum'}).reset_index().rename(columns={'factory_electricity_base':'site_total'})
    total_energy_site['site_rate']=total_energy_site.site_total/sum(total_energy_site.site_total)
    total_sit_rate = sum(total_energy_site.loc[total_energy_site.Site_top.isin(model_site),'site_rate'])
    data_upload_final_Y23base_year['predict_electricity_v2_total'] = data_upload_final_Y23base_year['predict_electricity_v2_total']/total_sit_rate

    # Compute other not build model site by month factory electricity
    data_upload_final_Y23base_other=pd.DataFrame({})
    for x in range(data_upload_final_Y23base_year.shape[0]):
        total_energy_site_new = total_energy_site.loc[~total_energy_site.Site_top.isin(model_site),:].reset_index(drop=True)
        total_energy_site_new['year'] = data_upload_final_Y23base_year['year'][x]
        total_energy_site_new['predict_electricity_v2']= total_energy_site_new['site_rate']*data_upload_final_Y23base_year['predict_electricity_v2_total'][x]
        data_upload_final_Y23base_other = data_upload_final_Y23base_other.append(total_energy_site_new).reset_index(drop=True)
    data_upload_final_Y23base_other['plant'] = data_upload_final_Y23base_other['Site_top']
    data_upload_final_Y23base_other['factory_electricity_base'] = data_upload_final_Y23base_other['site_total']
    data_upload_final_Y23base_other['datetime'] = [data_upload_final_Y23base_other['year'][x]+'-'+data_upload_final_Y23base_other['month'][x]+'-01' for x in range(data_upload_final_Y23base_other.shape[0])]
    # Combine build and un-build model site by month factory electricity
    column_name = ['datetime','year','month','plant','factory_electricity_base','predict_electricity_v2']
    data_upload_final_Y23base_final = data_upload_final_Y23base[column_name].append(data_upload_final_Y23base_other[column_name]).reset_index(drop=True)
    return data_upload_final_Y23base_final,total_energy_site_new

def model_api_caller(data, url, api_key):
    payload = json.dumps(data)
    headers = {
    'Authorization': 'Bearer ' + api_key,
    'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers, data=payload, verify=False)
        result = response.text
        # print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))
    return result

@app.task
async def factory_elct_main_fn():
    print('factory_elct_main_fn start')
    try:
        print('Upload data is start!')
        ############### Generate Inputs #####################
        connect_string = engine.get_connect_string()
        conn = create_engine(connect_string, echo=True)
        url0 = 'http://10.30.80.134:80/api/v1/service/factory-elec-simulate-prd/score'
        api_key0 = '8CqJdO3gaXeujgGiyI0HcXLrWqY0b65H'
        baseline_year = '2022'
        target_year = '2030'
        plant_batch = [['WOK'],['WZS-1'],['WZS-3'],['WZS-6'],['WZS-8'],['WTZ'],['WCD'],['WKS-5'],['WKS-6'],['WCQ'],['WIH']]
        base_end_month = (datetime.now() + pd.DateOffset(months=2)).strftime('%Y-%m-%d')
        base_start_month = str(np.datetime64(base_end_month[0:7]) - np.timedelta64(11, 'M'))+'-01'
        
        # Connect to DB to download data
        print('Download machine info from DB.')
        # conn = psycopg2.connect(host=host0, port=port0, database=database0, 
        #                     user=user0, password=password0)
        load_baseline_data_query = "SELECT * FROM app.baseline_data_overview;"
        load_predict_data_query = "SELECT * FROM app.predict_baseline_data;"
        data_result, data_result_history = baseline_data_loader(conn, load_baseline_data_query)
        ##----------fix history electricity data-------------##
        data_result, data_result_history = history_data_fixer(data_result, data_result_history)
        ##----------generate variable trend by site-------------##
        data_result_trend_final = variable_data_generator(data_result)

        data_result = data_result.astype('string')
        # data_result_history['日期'] = data_result_history['日期'].astype('string')
        data_result_history = data_result_history.astype('string')
        data_result = data_result.drop(columns={'last_update_time','datetime_max'})
        data_result_history = data_result_history.drop(columns={'last_update_time'})

        ##----------generate simulation data by site, month-------------##
        plant_predict_energy_all, data_result_simulation_all, data_result_history_simulation_all = simulation_data_generator(conn, data_result, data_result_history, data_result_trend_final, baseline_year, target_year)

        ##----------get simulation data forecast electricity by site, month-------------##
        # plant_batch = [['WTZ']]
        data_upload_final = pd.DataFrame({})
        for x in plant_batch:
            print(x)
            data_result_json =  json.loads(data_result_simulation_all.fillna('null').loc[data_result_simulation_all['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))
            data_result_history_json = json.loads(data_result_history_simulation_all.fillna('null').loc[data_result_history_simulation_all['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))
            # predict_data_result_json = json.loads(predict_data_result.fillna('null').loc[predict_data_result['plant'].isin(x),:].reset_index(drop=True).to_json(orient="records"))

            input_json = {
                "data_result": data_result_json,
                "data_result_history":data_result_history_json#,
                # "predict_data_result":predict_data_result_json
            }
            ############### Call API #####################
            outputs = model_api_caller(input_json, url0, api_key0)
            outputs
            request = json.loads(outputs)
            data_upload = data_type_checker(request['data_upload_final'])
            print('data_upload is successful')
            data_upload_final = data_upload_final.append(data_upload).reset_index(drop=True)
        # Adjust plant name
        data_upload_final.plant = np.where(data_upload_final.plant=='WCQ-1','WCQ',data_upload_final.plant)
        data_upload_final = data_upload_final.loc[data_upload_final.year>=baseline_year,:]
        # Add carbon emission
        data_upload_final['emission_factor'] = np.where(data_upload_final.plant=='WIH',0.509,
                                                      np.where(data_upload_final.plant=='WCZ',0.39,
                                                               np.where(data_upload_final.plant=='WMX',0.423,0.581)))
        data_upload_final['carb_emission'] = data_upload_final.emission_factor*data_upload_final.predict_electricity/1000
        ##----------generate real factory electricity -------------##
        ##----- consider Y22-07~Y23-06-----##
        # Y22 by month factory real electricity
        predict_site = ['XTRKS','KOE','WGKS','WGTX','WVN','WTN'] # no Y22 data
        data_upload_final_Y23real, plant_predict_energy_sub_Y22 = real_base_generator(conn, data_result_simulation_all, plant_predict_energy_all, base_start_month, base_end_month, predict_site)
        ##----------model predict electricity increase rate-------------##
        base_year = int(datetime.now().strftime('%Y'))
        model_site = ['WZS','WKS','WCD','WCQ','WIH','WOK','WTZ']
        data_upload_final_Y23base_final, total_energy_site_new = site_electricity_generator(data_upload_final, data_upload_final_Y23real, base_year, base_start_month, base_end_month, model_site)
        ##----------merge Y23-Y30 factory electricity by month or year-------------##
        # By site & by month
        data_upload_final_Y23base_final_bymonth = data_upload_final_Y23base_final[['datetime','year','month','plant','predict_electricity_v2']].rename(columns={'predict_electricity_v2':'factory_electricity_predict'})
        data_upload_final_Y23base_final_bymonth['Site'] = [str(np.where(real_id.find('-')!=-1,str(real_id[0:(real_id.find('-'))]),real_id)) for real_id in data_upload_final_Y23base_final_bymonth.plant]
        data_upload_final_Y23base_final_bymonth_bysite = data_upload_final_Y23base_final_bymonth.loc[data_upload_final_Y23base_final_bymonth.year>='2023'].groupby(['datetime','year','month','Site'], group_keys=True).agg({'factory_electricity_predict':'sum'}).reset_index()
        # data_upload_final_Y23base_final_bymonth_bysite_wide = pd.pivot(data_upload_final_Y23base_final_bymonth_bysite.loc[data_upload_final_Y23base_final_bymonth_bysite.year.isin(['2023','2024'])], index=['Site','year'], columns = 'month',values = 'factory_electricity_predict').rename_axis(None, axis=1).reset_index() 
        # By site & by year
        data_upload_final_Y23base_final_byyear = data_upload_final_Y23base_final_bymonth.loc[data_upload_final_Y23base_final_bymonth.year>='2023'].groupby(['Site','year'], group_keys=True).agg({'factory_electricity_predict':'sum'}).reset_index()
        # data_upload_final_Y23base_final_byyear_wide = pd.pivot(data_upload_final_Y23base_final_byyear.loc[data_upload_final_Y23base_final_byyear.year<='2030'], index=['Site'], columns = 'year',values = 'factory_electricity_predict').rename_axis(None, axis=1).reset_index() 
        ##----------merge Y23-Y30 5% factory electricity by month or year-------------##
        # Compute by site, month rate in year
        data_upload_final_Y23real['site_rate'] = data_upload_final_Y23real['factory_electricity_base'].astype(float)/sum(data_upload_final_Y23real['factory_electricity_base'].astype(float))
        # Generate 5% factory electricity by year
        original_year_elec = pd.DataFrame({'factory_electricity_original':[x*100000000 for x in [4.6,4.8,5.1,5.3,5.6,5.9,6.2,6.5,6.8,7.2]],'year':[x+2021 for x in range(10)]})
        original_month_elec = pd.DataFrame({})
        # By site & by month
        for x in range(original_year_elec.shape[0]):
            data_upload_final_Y23real['factory_electricity_original'] = [original_year_elec['factory_electricity_original'][x] for y in range(data_upload_final_Y23real.shape[0])]
            data_upload_final_Y23real['year'] = [original_year_elec['year'][x] for y in range(data_upload_final_Y23real.shape[0])]
            original_month_elec = original_month_elec.append(data_upload_final_Y23real.loc[~data_upload_final_Y23real.plant.isin(['WZS','WMY'])]).reset_index(drop=True)
        original_month_elec['factory_electricity_original'] = original_month_elec['factory_electricity_base'].astype(float)*(1+0.05*(original_month_elec['year']-min(original_month_elec['year'])))
        # original_month_elec['factory_electricity_original'] = original_month_elec['factory_electricity_original']*original_month_elec['site_rate']
        original_month_elec['datetime'] = [str(original_month_elec['year'][x])+'-'+str(original_month_elec['month'][x])+'-01' for x in range(len(original_month_elec['datetime']))]
        original_month_elec['Site'] = [str(np.where(real_id.find('-')!=-1,str(real_id[0:(real_id.find('-'))]),real_id)) for real_id in original_month_elec.plant]
        original_month_elec_bymonth_bysite = original_month_elec.loc[original_month_elec.year>=base_year].groupby(['datetime','year','month','Site'], group_keys=True).agg({'factory_electricity_original':'sum'}).reset_index()
        # By site & by year
        original_month_elec_byyear = original_month_elec_bymonth_bysite.loc[original_month_elec_bymonth_bysite.year>=base_year].groupby(['Site','year'], group_keys=True).agg({'factory_electricity_original':'sum'}).reset_index()
        ##----------upload data--------------##
        load_decarb_data_query = "SELECT * FROM app.decarb_elect_summary;"
        decarb_data = data_loader(conn, load_decarb_data_query)
        max_version = max(decarb_data['version'])
        max_version_year = np.unique(decarb_data.loc[decarb_data.version==max_version,['year']])[0]
        new_version = data_upload_final_Y23base_final_byyear.rename(columns={'Site':'site','factory_electricity_predict':'amount'})
        new_version['version'] = max_version
        original_version = original_month_elec_byyear.rename(columns={'Site':'site','factory_electricity_original':'amount'})
        original_version['version'] = 'base'
        decarb_elect_simulate = new_version.append(original_version).reset_index(drop=True)
        decarb_elect_simulate['last_update_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        decarb_elect_simulate['amount'] = decarb_elect_simulate['amount']/1000
        data_uploader_append(decarb_elect_simulate,'app','decarb_elect_simulate')
        print('Upload simulate electricity data is successful!')
        ##-------generate elect_target_month--------##
        # actual factory electricity in base year bymonth
        actual_version_bymonth = data_upload_final_Y23real.copy().rename(columns={'factory_electricity_base':'amount'})
        actual_version_bymonth = actual_version_bymonth.loc[(actual_version_bymonth.datetime<(datetime.now() - pd.DateOffset(months=1)).strftime('%Y-%m-%d')) & (actual_version_bymonth.datetime>=str(base_year)+'-01-01')]
        actual_version_bymonth['site'] = [str(np.where(real_id.find('-')!=-1,str(real_id[0:(real_id.find('-'))]),real_id)) for real_id in actual_version_bymonth.plant]
        actual_version_bymonth['year'] = str(base_year)
        actual_version_bymonth = actual_version_bymonth.groupby(['site','year','month'], group_keys=True).agg({'amount':'sum'}).reset_index()
        actual_version_bymonth['last_update_time'] = np.unique(decarb_elect_simulate['last_update_time'])[0]
        actual_version_bymonth['category'] = 'actual'
        actual_version_byyear = actual_version_bymonth.groupby(['site','year'], group_keys=True).agg({'amount':'sum'}).reset_index()
        actual_version_byyear['last_update_time'] = np.unique(decarb_elect_simulate['last_update_time'])[0]
        actual_version_byyear['category'] = 'actual'
        actual_version_byyear['amount'] = actual_version_byyear['amount']/1000
        decarb_elect_simulate['version_year'] = max_version_year
        # new factory electricity in base year bymonth
        new_version_bymonth = data_upload_final_Y23base_final_bymonth_bysite.copy().rename(columns={'Site':'site','factory_electricity_predict':'amount'})
        # new_version_bymonth = new_version_bymonth.loc[new_version_bymonth.datetime<(datetime.now() - pd.DateOffset(months=1)).strftime('%Y-%m-%d')]
        new_version_bymonth['last_update_time'] = np.unique(decarb_elect_simulate['last_update_time'])[0]
        new_version_bymonth['category'] = 'predict'
        new_version_bymonth = new_version_bymonth.loc[new_version_bymonth.year.astype(int)==base_year,:].reset_index(drop=True)
        elect_target_month = actual_version_bymonth.append(new_version_bymonth[actual_version_bymonth.columns])
        elect_target_month['amount'] = elect_target_month['amount']/1000
        ##-------generate elect_target_year--------##
        plant_predict_energy_sub_Y22['year'] = [x[0:4] for x in plant_predict_energy_sub_Y22.datetime]
        plant_predict_energy_sub_Y22['Site'] = [str(np.where(real_id.find('-')!=-1,str(real_id[0:(real_id.find('-'))]),real_id)) for real_id in plant_predict_energy_sub_Y22.plant]
        plant_predict_energy_sub_Y22_byyear = plant_predict_energy_sub_Y22.groupby(['year','Site'], group_keys=True).agg({'factory_electricity':'sum'}).reset_index().rename(columns={'Site':'site','factory_electricity':'amount'})
        plant_predict_energy_sub_Y22_byyear['category'] = max_version
        plant_predict_energy_sub_Y22_byyear['last_update_time'] = np.unique(decarb_elect_simulate['last_update_time'])[0]
        plant_predict_energy_sub_Y22_byyear['amount'] = plant_predict_energy_sub_Y22_byyear['amount']/1000
        elect_target_year = decarb_elect_simulate.copy().drop(columns={'version_year'}).rename(columns={'version':'category'})
        elect_target_year = elect_target_year.append(plant_predict_energy_sub_Y22_byyear[elect_target_year.columns]).reset_index(drop=True)
        elect_target_year = elect_target_year.append(decarb_elect_simulate.copy().drop(columns={'version_year'}).loc[decarb_elect_simulate.version=='base',:].rename(columns={'version':'category'})).reset_index(drop=True)
        elect_target_year.loc[(elect_target_year.year.astype(int)>=base_year) & (elect_target_year.category!='base'),['category']]='predict'
        elect_target_year.loc[(elect_target_year.year.astype(int)<base_year) & (elect_target_year.category!='base'),['category']]='actual'
        elect_target_year = elect_target_year.append(actual_version_byyear[elect_target_year.columns]).reset_index(drop=True)
        data_uploader_append(elect_target_month,'app','elect_target_month')
        data_uploader(elect_target_year,'app','elect_target_year')
        print('Upload target electricity data is successful!')
        print('Upload data is end!')
        
        return 0
    except Exception as e:
        error = str(e)
        # mail = MailService('[failed][{}] simulate factory electricity cron job report'.format(stage))
        # mail.send('failed: {}'.format(error))
        
        return error
    print('factory_elct_main_fn end')
    