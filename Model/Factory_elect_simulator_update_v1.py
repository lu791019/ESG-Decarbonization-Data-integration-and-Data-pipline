#!/usr/bin/python
import json
import math
import os
import ssl
import time
import urllib.parse
import urllib.request
from datetime import datetime
from time import strptime

import numpy as np
import numpy_financial as npf
import pandas as pd
# from azureml.core import Workspace, Dataset
# from azureml.core.authentication import ServicePrincipalAuthentication
# from inference_schema.schema_decorators import input_schema, output_schema
# from inference_schema.parameter_types.numpy_parameter_type import NumpyParameterType
# from inference_schema.parameter_types.pandas_parameter_type import PandasParameterType
# from inference_schema.parameter_types.standard_py_parameter_type import StandardPythonParameterType
import psycopg2
import requests
from sqlalchemy import create_engine

from app.celery import app
# from services.mail_service import MailService
from models import engine
from utils.indicator_queue import IndicatorQueue


def variable_forecast_generator(conn, plant_mapping):
    # Budget variable
    sql = 'SELECT * FROM raw."V_BUDGETMOH_ESG";'
    variable_predict_Y23 = pd.read_sql(sql, con=conn)
    variable_predict_Y23 = variable_predict_Y23.drop(
        columns={'site', 'last_update_time'}).rename(columns={'plant': 'plant_code'})
    variable_predict_Y23 = pd.merge(variable_predict_Y23,
                                    plant_mapping, on=['plant_code'], how='left').reset_index(drop=True)
    variable_predict_Y23 = variable_predict_Y23.loc[~variable_predict_Y23.bo.isna(
    ), :].reset_index(drop=True)
    variable_predict_Y23['account_name'] = [str(np.where(x.find('Headcount') != -1, 'member_counts',
                                                         np.where(x.find('Production') != -1, 'product_qty', 'shipment_qty'))) for x in variable_predict_Y23['accountnm']]
    variable_predict_Y23_summary = variable_predict_Y23.groupby(['bo', 'site', 'plant_name', 'account_name', 'date_key'], group_keys=True).agg(
        {'value': 'sum'}).reset_index().rename(columns={'value': 'value_total', 'date_key': 'datetime'})
    variable_predict_Y23_summary = variable_predict_Y23_summary.loc[variable_predict_Y23_summary.datetime.astype(
        str) >= str(max(variable_predict_Y23_summary.datetime))[0:4]+'-01-01', :]
    variable_predict_Y23_summary_wide = pd.pivot(variable_predict_Y23_summary, index=[
                                                 'bo', 'site', 'plant_name', 'datetime'], columns='account_name', values='value_total').rename_axis(None, axis=1).reset_index()
    variable_predict_Y23_summary_wide['plant_name'] = np.where(
        variable_predict_Y23_summary_wide['plant_name'] == 'WCD-1', 'WCD', variable_predict_Y23_summary_wide['plant_name'])

    # Actual variable
    sql = 'SELECT * FROM raw."V_ACTUALMOH_ESG";'
    variable_actual_Y23 = pd.read_sql(sql, con=conn)
    variable_actual_Y23 = variable_actual_Y23.drop(
        columns={'site', 'last_update_time'}).rename(columns={'plant': 'plant_code'})
    variable_actual_Y23 = pd.merge(variable_actual_Y23,
                                   plant_mapping, on=['plant_code'], how='left').reset_index(drop=True)
    variable_actual_Y23 = variable_actual_Y23.loc[~variable_actual_Y23.bo.isna(
    ), :].reset_index(drop=True)
    variable_actual_Y23['account_name'] = [str(np.where(x.find('Headcount') != -1, 'member_counts',
                                                        np.where(x.find('Production') != -1, 'product_qty', 'shipment_qty'))) for x in variable_actual_Y23['accountnm']]
    variable_actual_Y23_summary = variable_actual_Y23.groupby(['bo', 'site', 'plant_name', 'account_name', 'date_key'], group_keys=True).agg(
        {'value': 'sum'}).reset_index().rename(columns={'value': 'value_total', 'date_key': 'datetime'})
    variable_actual_Y23_summary_wide = pd.pivot(variable_actual_Y23_summary, index=[
                                                'bo', 'site', 'plant_name', 'datetime'], columns='account_name', values='value_total').rename_axis(None, axis=1).reset_index()
    variable_actual_Y23_summary_wide['plant_name'] = np.where(
        variable_actual_Y23_summary_wide['plant_name'] == 'WCD-1', 'WCD', variable_actual_Y23_summary_wide['plant_name'])
    return variable_predict_Y23_summary_wide, variable_actual_Y23_summary_wide


def baseline_data_loader(conn, query, plant_mapping):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql, con=conn)
    data_result['plant'] = np.where(
        data_result['plant'] == 'WCD-1', 'WCD', data_result['plant'])

    # Merge actual variable and
    variable_predict_Y23_summary_wide, variable_actual_Y23_summary_wide = variable_forecast_generator(
        conn, plant_mapping)
    # variable_actual_Y23_summary_wide = variable_actual_Y23_summary_wide.loc[variable_actual_Y23_summary_wide.plant_name==plant,:].reset_index(drop=True)
    variable_actual_Y23_summary_wide['datetime'] = variable_actual_Y23_summary_wide['datetime'].astype(
        str)
    data_result['datetime'] = data_result['datetime'].astype(str)

    data_result = pd.merge(variable_actual_Y23_summary_wide.rename(columns={'plant_name': 'plant'}),
                           data_result[['datetime', 'factory_electricity', 'ac_electricity', 'ap_electricity', 'production_electricity', 'base_electricity', 'dorm_electricity',
                                        'average_temperature', 'plant', 'last_update_time']], on=['plant', 'datetime'], how='left').reset_index(drop=True)
    data_result = data_result.loc[(~data_result['average_temperature'].isna()) & (
        ~data_result['factory_electricity'].isna()), :].reset_index(drop=True)

    # close the communication with the PostgreSQL
    # cur.close()

    data_result_history = data_result[data_result.columns[np.where(
        [x.find('id') == -1 for x in data_result.columns])]]
    data_result_history.columns = ['bo', 'site', 'plant', '日期', '人數（人）', '產量（pcs)', '出貨量（pcs)', '工廠用電（kwh）',
                                   '空調用電（kwh）', '空壓用電（kwh）', '生產用電（kwh）', '基礎用電（kwh）', '宿舍用電（kwh）', '外氣平均溫度（℃）', 'last_update_time']
    return data_result, data_result_history


def data_loader(conn, query):
    # create a cursor
    # cur = conn.cursor()
    # select one table
    sql = query
    data_result = pd.read_sql(sql, con=conn)
    # close the communication with the PostgreSQL
    # cur.close()
    return data_result


def data_type_checker(dataset_json):
    dataset = pd.DataFrame(dataset_json)
    dataset = dataset.replace('null', np.nan)
    for x in dataset.columns:
        if x in ['datetime', 'plant', 'bo', '日期', 'site', 'year', 'month']:
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
    data.to_sql(table_name, conn, index=False, if_exists='append',
                schema=db_name, chunksize=10000)
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
    data.to_sql(table_name, conn, index=False, if_exists='append',
                schema=db_name, chunksize=10000)
    return 0


def data_uploader_delete(data, db_name, table_name, condition):
    # Connect to DB to upload data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # Delete table
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'DELETE from '+db_name+'.' +
                 table_name+' WHERE '+condition+';')
    return 0


def real_power_computer(power_real_data):
    # modify column names
    power_real_data = power_real_data[power_real_data.columns[np.where(
        [x.find('id') == -1 for x in power_real_data.columns])]]
    power_real_data.columns = ['日期', '工廠用電（kwh）', '空調用電（kwh）', '空壓用電（kwh）', '生產用電（kwh）', '基礎用電（kwh）', '宿舍用電（kwh）',
                               'PCBA產量（pcs)', 'FA產量（pcs)', '人數（人）', 'PCBA平均開線數（條）', 'FA平均開線數量（條）',
                               '營業額（十億NTD）', '外氣平均溫度（℃）', 'plant', 'site', 'last_update_time']
    power_ten_month_total = power_real_data.loc[[power_real_data['日期'].astype(
        str)[x][5:7] < '11' for x in range(power_real_data.shape[0])], :].reset_index(drop=True)
    power_ten_month_total['year'] = [power_ten_month_total['日期'].astype(
        str)[x][0:4] for x in range(power_ten_month_total.shape[0])]
    power_ten_month_total = power_ten_month_total.groupby(['plant', 'site', 'year'], group_keys=True).agg(
        {'工廠用電（kwh）': 'sum', '宿舍用電（kwh）': 'sum', '日期': 'size'}).reset_index().rename(columns={'日期': 'month_count'})
    # Compute total power
    power_ten_month_total['ten_month_real'] = power_ten_month_total['工廠用電（kwh）'] + \
        12*power_ten_month_total['宿舍用電（kwh）'] / \
        power_ten_month_total['month_count']
    power_ten_month_total_final = power_ten_month_total.loc[
        power_ten_month_total.month_count == 10, :]
    return power_ten_month_total_final


def irr_func(x):
    if len(x) > 1:
        x = np.array(x)
        x_new = np.append(-1*x[0], np.diff(x)[0:(len(x)-2)])
        x_new = np.append(x_new, x[len(x)-1])
        return npf.irr(x_new)
    else:
        x_new = 0
        return x_new


def cagr_func(x):
    if len(x) > 1:
        x = np.array(x)
        x_new = (x[len(x)-1]/x[0])**(1/len(x))-1
    else:
        x_new = 0
    return x_new


def trend_rate_generator(dataset, power_usage, method):
    # data_result_trend_revenue_full = dataset.loc[(dataset.month_cnt==12) & (dataset.plant!='WKS-1') & (dataset.year!='2022'),:]
    data_result_trend_revenue_full = dataset.loc[(dataset.plant != 'WKS-1'), :]
    data_result_trend_revenue_full['year_'+power_usage] = data_result_trend_revenue_full['year_' +
                                                                                         power_usage]/data_result_trend_revenue_full.month_cnt
    if method == 'cagr':
        data_result_trend_revenue_min = data_result_trend_revenue_full.groupby(['plant', 'bo']).agg(
            {'year_'+power_usage: cagr_func}).reset_index().rename(columns={'year_'+power_usage: power_usage+'_rate'})
        data_result_trend_revenue_min[power_usage+'_rate'] = [np.quantile(data_result_trend_revenue_min.loc[data_result_trend_revenue_min[power_usage+'_rate'] > 0, [
                                                                          power_usage+'_rate']], 0.25) if x < 0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
    elif method == 'irr':
        data_result_trend_revenue_min = data_result_trend_revenue_full.groupby(['plant', 'bo']).agg(
            {'year_'+power_usage: irr_func}).reset_index().rename(columns={'year_'+power_usage: power_usage+'_rate'})
        # data_result_trend_revenue_min[power_usage+'_rate']=[np.quantile(data_result_trend_revenue_min.loc[data_result_trend_revenue_min[power_usage+'_rate']>0,[power_usage+'_rate']],0.25) if x<0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
    else:
        data_result_trend_revenue_max = pd.merge(data_result_trend_revenue_full,
                                                 data_result_trend_revenue_full.groupby(
                                                     ['plant', 'bo']).agg({'year': 'max'}).reset_index(),
                                                 on=['plant', 'bo', 'year'], how='inner').rename(columns={'year': 'year_max', 'month_cnt': 'month_cnt_max', 'year_'+power_usage: 'year_'+power_usage+'_max'})
        data_result_trend_revenue_min = pd.merge(data_result_trend_revenue_full,
                                                 data_result_trend_revenue_max,
                                                 on=['plant', 'bo'], how='left')
        data_result_trend_revenue_min = data_result_trend_revenue_min.loc[data_result_trend_revenue_min.year_max.astype(
            int)-data_result_trend_revenue_min.year.astype(int) == 1, :].reset_index(drop=True)
        data_result_trend_revenue_min[power_usage+'_rate'] = (data_result_trend_revenue_min['year_'+power_usage+'_max'] -
                                                              data_result_trend_revenue_min['year_'+power_usage])/data_result_trend_revenue_min['year_'+power_usage]
        data_result_trend_revenue_min[power_usage+'_rate'] = [np.quantile(data_result_trend_revenue_min.loc[data_result_trend_revenue_min[power_usage+'_rate'] > 0, [
                                                                          power_usage+'_rate']], 0.25) if x < 0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
        # data_result_trend_revenue_min[power_usage+'_rate']=[np.mean(data_result_trend_revenue_min.loc[:,[power_usage+'_rate']])[0] if x<0 else x for x in data_result_trend_revenue_min[power_usage+'_rate']]
    return data_result_trend_revenue_min[['plant', 'bo', power_usage+'_rate']]


def history_data_fixer(data_result, data_result_history):
    data_result_wok = data_result.loc[(data_result.plant == 'WOK') & (
        data_result.datetime == pd.to_datetime('2022-11-01')), :]
    data_result_wok['datetime'] = datetime.date(pd.to_datetime('2022-12-01'))
    data_result = (data_result.loc[~((data_result.plant == 'WOK') & (
        data_result.datetime == pd.to_datetime('2022-12-01'))), :]).append(data_result_wok).reset_index(drop=True)
    data_result_history_wok = data_result_history.loc[(data_result_history.plant == 'WOK') & (
        data_result_history['日期'] == pd.to_datetime('2022-11-01')), :]
    data_result_history_wok['日期'] = datetime.date(pd.to_datetime('2022-12-01'))
    data_result_history = (data_result_history.loc[~((data_result_history.plant == 'WOK') & (
        data_result_history['日期'] == pd.to_datetime('2022-12-01'))), :]).append(data_result_history_wok).reset_index(drop=True)
    # --------
    # fix wcd pcba, fa qty
    # data_result_wcd = data_result.loc[(data_result.plant=='WCD') & (data_result.datetime==pd.to_datetime('2022-11-01')),:]
    # data_result.loc[(data_result.plant=='WCD') & (data_result.datetime==pd.to_datetime('2022-12-01')),['pcba_qty']] = data_result_wcd.pcba_qty.iloc[0]
    # data_result.loc[(data_result.plant=='WCD') & (data_result.datetime==pd.to_datetime('2022-12-01')),['fa_qty']] = data_result_wcd.fa_qty.iloc[0]
    # fix wzs-8 pcba, fa qty
    # data_result_wzs8 = data_result.loc[(data_result.plant=='WZS-8') & (data_result.datetime==pd.to_datetime('2022-11-01')),:]
    # data_result.loc[(data_result.plant=='WZS-8') & (data_result.datetime==pd.to_datetime('2022-12-01')),['pcba_qty']] = data_result_wzs8.pcba_qty.iloc[0]
    # data_result.loc[(data_result.plant=='WZS-8') & (data_result.datetime==pd.to_datetime('2022-12-01')),['fa_qty']] = data_result_wzs8.fa_qty.iloc[0]

    data_result = pd.merge(data_result,
                           data_result.loc[(~data_result.ac_electricity.isna()) & (~data_result.shipment_qty.isna()) & (~data_result.product_qty.isna(
                           )), :].groupby(['plant', 'bo']).agg({'datetime': 'max'}).reset_index().rename(columns={'datetime': 'datetime_max'}),
                           on=['plant', 'bo'], how='left')
    return data_result, data_result_history


def variable_data_generator(data_result):
    data_result_trend_raw = data_result.loc[(data_result.datetime.astype(str) >= '2020-01-01') & (
        (data_result.datetime.astype(str) <= '2023-04-01')), :].reset_index(drop=True)  # fix me!!!!!!
    data_result = data_result.loc[((data_result['plant'].isin(['WIH', 'WCQ'])) & (data_result['datetime'].astype(str) > '2020-12-01') & (data_result['datetime'] <= data_result['datetime_max'])) | ((data_result['plant'].isin(['WKS-5', 'WKS-6', 'WOK', 'WTZ'])) & (
        data_result['datetime'].astype(str) > '2020-09-01') & (data_result['datetime'] <= data_result['datetime_max'])) | ((~data_result['plant'].isin(['WKS-5', 'WKS-6', 'WOK', 'WTZ', 'WIH', 'WCQ'])) & (data_result['datetime'] <= data_result['datetime_max'])), :].reset_index(drop=True)
    # print(data_result)
    # Compute 2022 power usage total
    # data_result_2022 = data_result.loc[(data_result.datetime.astype(str)>='2022-01-01') & ((data_result.datetime.astype(str)<='2022-12-01')),:].reset_index(drop=True)
    # power_usage_2022_real = sum((data_result_2022.loc[data_result_2022.plant.isin(['WZS-1','WZS-3','WZS-6','WZS-8','WKS-5','WKS-6','WCD','WCQ','WIH','WOK','WTZ'])]).factory_electricity.astype('float'))
    # Compute 2022 item trend rate
    data_result_trend = pd.DataFrame(data_result_trend_raw)  # fix me
    data_result_trend['year'] = [str(x)[0:4]
                                 for x in data_result_trend.datetime]
    data_result_trend['month'] = [str(x)[5:7]
                                  for x in data_result_trend.datetime]
    data_result_trend_shipment_qty = data_result_trend.loc[~data_result_trend.shipment_qty.isna(), :].groupby(['plant', 'bo', 'year']).agg(
        {'shipment_qty': 'sum', 'datetime': 'size'}).reset_index().rename(columns={'shipment_qty': 'year_shipment_qty', 'datetime': 'month_cnt'})
    data_result_trend_product_qty = data_result_trend.loc[~data_result_trend.product_qty.isna(), :].groupby(['plant', 'bo', 'year']).agg(
        {'product_qty': 'sum', 'datetime': 'size'}).reset_index().rename(columns={'product_qty': 'year_product_qty', 'datetime': 'month_cnt'})
    # data_result_trend_fa_qty = data_result_trend.loc[~data_result_trend.fa_qty.isna(),:].groupby(['plant','bo','year']).agg({'fa_qty':'sum','datetime':'size'}).reset_index().rename(columns={'fa_qty':'year_fa_qty','datetime':'month_cnt'})
    data_result_trend_member_counts = data_result_trend.loc[~data_result_trend.member_counts.isna(), :].groupby(['plant', 'bo', 'year']).agg(
        {'member_counts': 'sum', 'datetime': 'size'}).reset_index().rename(columns={'member_counts': 'year_member_counts', 'datetime': 'month_cnt'})
    # data_result_trend_ac_electricity = data_result_trend.loc[~data_result_trend.ac_electricity.isna(),:].groupby(['plant','bo','year']).agg({'ac_electricity':'sum','datetime':'size'}).reset_index().rename(columns={'ac_electricity':'year_ac_electricity','datetime':'month_cnt'})
    # data_result_trend_ap_electricity = data_result_trend.loc[~data_result_trend.ap_electricity.isna(),:].groupby(['plant','bo','year']).agg({'ap_electricity':'sum','datetime':'size'}).reset_index().rename(columns={'ap_electricity':'year_ap_electricity','datetime':'month_cnt'})
    # Compute trend rate
    data_result_trend_shipment_qty_min = trend_rate_generator(
        data_result_trend_shipment_qty, 'shipment_qty', 'irr')
    data_result_trend_product_qty_min = trend_rate_generator(
        data_result_trend_product_qty, 'product_qty', 'irr')
    # data_result_trend_fa_qty_min = trend_rate_generator(data_result_trend_fa_qty,'fa_qty','mape')
    data_result_trend_member_counts_min = trend_rate_generator(
        data_result_trend_member_counts, 'member_counts', 'irr')
    # data_result_trend_ac_electricity_min = trend_rate_generator(data_result_trend_ac_electricity,'ac_electricity','mape')
    # data_result_trend_ap_electricity_min = trend_rate_generator(data_result_trend_ap_electricity,'ap_electricity','mape')
    data_result_trend_final = pd.merge(data_result_trend_shipment_qty_min,
                                       pd.merge(data_result_trend_member_counts_min,
                                                data_result_trend_product_qty_min,
                                                on=['plant', 'bo'], how='left'),
                                       on=['plant', 'bo'], how='left')
    data_result_trend_final.loc[data_result_trend_final.product_qty_rate.isna(), ['product_qty_rate']] = np.median(
        data_result_trend_final.loc[~data_result_trend_final.product_qty_rate.isna(), ['product_qty_rate']])
    # data_result_trend_final.loc[data_result_trend_final.pcba_qty_rate.isna(),['pcba_qty_rate']] = np.median(data_result_trend_final.loc[~data_result_trend_final.pcba_qty_rate.isna(),['pcba_qty_rate']])
    return data_result_trend_final


def simulation_data_generator(conn, data_result, data_result_history, data_result_trend_final, baseline_year, target_year):
    data_result_simulation_all = data_result.loc[(data_result.datetime >= '2020-11-01') & (
        (data_result.datetime <= str(baseline_year+'-12-01'))), :].reset_index(drop=True)
    data_result_history_simulation_all = data_result_history.loc[(data_result_history['日期'] >= '2020-11-01') & (
        data_result_history['日期'] <= str(baseline_year+'-12-01')), :].reset_index(drop=True)

    # Add WKS, WZS electricity seperate rate
    data_result_simulation_all_plant_rate = data_result_simulation_all.loc[(data_result_simulation_all.plant.isin(
        ['WKS-5', 'WKS-6', 'WZS-1', 'WZS-3', 'WZS-6', 'WZS-8', 'WIHK-1', 'WIHK-2'])) & (data_result_simulation_all.datetime.astype(str) >= '2022-01-01'), :]
    data_result_simulation_all_plant_rate['Month'] = [
        int(str(x)[5:7]) for x in data_result_simulation_all_plant_rate['datetime']]
    data_result_simulation_all_plant_rate['Site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in data_result_simulation_all_plant_rate.plant]
    data_result_simulation_all_plant_rate['factory_electricity'] = data_result_simulation_all_plant_rate['factory_electricity'].astype(
        float)
    data_result_simulation_all_plant_rate = pd.merge(data_result_simulation_all_plant_rate,
                                                     data_result_simulation_all_plant_rate.groupby(['Month', 'Site']).agg({'factory_electricity': 'sum'}).rename(
                                                         columns={'factory_electricity': 'factory_electricity_total'}).reset_index(),
                                                     on=['Month', 'Site'], how='left').reset_index(drop=True)
    data_result_simulation_all_plant_rate['factory_rate'] = data_result_simulation_all_plant_rate.factory_electricity / \
        data_result_simulation_all_plant_rate.factory_electricity_total
    data_result_simulation_all_plant_rate = data_result_simulation_all_plant_rate[[
        'plant', 'Site', 'Month', 'factory_rate']]

    # Y23 factory electory result
    load_decarb_data_query = "SELECT max(last_update_time) as last_update_time FROM app.elect_target_month;"
    decarb_data = data_loader(conn, load_decarb_data_query)
    max_version = str(decarb_data['last_update_time'][0])
    plant_predict_energy_sub = "SELECT * FROM app.elect_target_month where (last_update_time='" + \
        max_version+"') and (category='predict');"
    plant_predict_energy_sub = data_loader(conn, plant_predict_energy_sub)
    # plant_predict_energy_sub['amount'] = plant_predict_energy_sub['amount']*1000
    plant_predict_energy_sub = plant_predict_energy_sub.rename(
        columns={'site': 'Site', 'amount': 'factory_electricity', 'month': 'Month'})
    plant_predict_energy_sub['datetime'] = [str(pd.to_datetime(str(plant_predict_energy_sub['year'][x])+'-'+str(
        plant_predict_energy_sub['Month'][x])+'-01'))[0:10] for x in range(len(plant_predict_energy_sub.Month))]
    plant_predict_energy_sub1 = plant_predict_energy_sub.loc[~plant_predict_energy_sub.Site.isin(
        ['WKS', 'WZS']), :].reset_index(drop=True)
    plant_predict_energy_sub1['plant'] = plant_predict_energy_sub1['Site']
    plant_predict_energy_sub2 = pd.merge(data_result_simulation_all_plant_rate, plant_predict_energy_sub, on=[
                                         'Site', 'Month'], how='left').reset_index(drop=True)
    plant_predict_energy_sub2['factory_electricity'] = plant_predict_energy_sub2['factory_electricity'] * \
        plant_predict_energy_sub2['factory_rate']
    plant_predict_energy_all = plant_predict_energy_sub1.append(
        plant_predict_energy_sub2[plant_predict_energy_sub1.columns]).reset_index(drop=True)

    for x in range(1, (int(target_year)-int(baseline_year)+1)):
        # data
        data_result_simulation = data_result.loc[(data_result.datetime >= str(
            baseline_year+'-01-01')) & ((data_result.datetime <= str(baseline_year+'-12-01'))), :]
        data_result_simulation = pd.merge(data_result_simulation,
                                          data_result_trend_final,
                                          on=['plant', 'bo'], how='left')
        data_result_simulation.average_temperature = data_result_simulation.average_temperature.astype(
            'float')+0.02*x
        # ,'ac_electricity','ap_electricity'
        for factor in ['shipment_qty', 'product_qty', 'member_counts']:
            if factor == 'member_counts':
                data_result_simulation[factor] = data_result_simulation[factor].astype(
                    'float')*(1+data_result_simulation[factor+'_rate'].astype('float')/2*x)
            else:
                data_result_simulation[factor] = data_result_simulation[factor].astype(
                    'float')*(1+data_result_simulation[factor+'_rate'].astype('float')*x)
        # data_result_simulation.revenue = data_result_simulation.revenue.astype('float')*(1+0.05*x)
        data_result_simulation['datetime'] = (pd.to_datetime(
            data_result_simulation['datetime'])+pd.DateOffset(years=x)).astype('string')
        data_result_simulation_all = data_result_simulation_all.append(
            data_result_simulation).reset_index(drop=True)
        # data history
        data_result_history_simulation = data_result_history.loc[(data_result_history['日期'] >= str(
            baseline_year+'-01-01')) & (data_result_history['日期'] <= str(baseline_year+'-12-01')), :]
        data_result_history_simulation = pd.merge(data_result_history_simulation,
                                                  data_result_trend_final,
                                                  on=['plant', 'bo'], how='left')
        data_result_history_simulation['外氣平均溫度（℃）'] = data_result_history_simulation['外氣平均溫度（℃）'].astype(
            'float')+0.02*x
        for factor_ori in ['出貨量（pcs)', '產量（pcs)', '人數（人）']:  # ,'空調用電（kwh）','空壓用電（kwh）'
            factor = str(np.where(factor_ori == '出貨量（pcs)', 'shipment_qty',
                                  np.where(factor_ori == '產量（pcs)', 'product_qty',
                                                np.where(factor_ori == '人數（人）', 'member_counts',
                                                         np.where(factor_ori == '空調用電（kwh）', 'ac_electricity', 'ap_electricity')))))
            print(factor)
            if factor == 'member_counts':
                data_result_history_simulation[factor_ori] = data_result_history_simulation[factor_ori].astype(
                    'float')*(1+data_result_history_simulation[factor+'_rate'].astype('float')/2*x)
            else:
                data_result_history_simulation[factor_ori] = data_result_history_simulation[factor_ori].astype(
                    'float')*(1+data_result_history_simulation[factor+'_rate'].astype('float')*x)
        # data_result_history_simulation['營業額（十億NTD）'] = data_result_history_simulation['營業額（十億NTD）'].astype('float')*(1+0.05*x)
        data_result_history_simulation['日期'] = (pd.to_datetime(
            data_result_history_simulation['日期'])+pd.DateOffset(years=x)).astype('string')
        data_result_history_simulation_all = data_result_history_simulation_all.append(
            data_result_history_simulation).reset_index(drop=True)
    data_result_simulation_all = data_result_simulation_all.sort_values(
        by=['plant', 'datetime'], ascending=True).reset_index(drop=True)
    data_result_history_simulation_all = data_result_history_simulation_all.sort_values(
        by=['plant', '日期'], ascending=True).reset_index(drop=True)

    # replace Y23 factory electricity to Gene version
    plant_predict_energy_all['datetime'] = [str(pd.to_datetime(
        '2023-'+str(x)+'-01'))[0:10] for x in plant_predict_energy_all['Month']]
    plant_predict_energy_all = plant_predict_energy_all.rename(
        columns={'factory_electricity': 'factory_electricity_new'})
    data_result_simulation_all = pd.merge(data_result_simulation_all,
                                          plant_predict_energy_all[[
                                              'plant', 'datetime', 'factory_electricity_new']],
                                          on=['plant', 'datetime'], how='left').reset_index(drop=True)
    data_result_simulation_all['factory_electricity'] = np.where(data_result_simulation_all['factory_electricity_new'].isna(),
                                                                 data_result_simulation_all['factory_electricity'],
                                                                 data_result_simulation_all['factory_electricity_new'])
    data_result_history_simulation_all = pd.merge(data_result_history_simulation_all,
                                                  plant_predict_energy_all[['plant', 'datetime', 'factory_electricity_new']].rename(
                                                      columns={'datetime': '日期'}),
                                                  on=['plant', '日期'], how='left').reset_index(drop=True)
    data_result_history_simulation_all['工廠用電（kwh）'] = np.where(data_result_history_simulation_all['factory_electricity_new'].isna(),
                                                               data_result_history_simulation_all['工廠用電（kwh）'],
                                                               data_result_history_simulation_all['factory_electricity_new'])
    return plant_predict_energy_all, data_result_simulation_all, data_result_history_simulation_all


def mfg_workspace_elec_generator(conn, workspace_site, target_year, base_year):
    # MFG actual factory electricity
    data_result_maxdatetime_query = "SELECT max(last_update_time) as max_datetime FROM app.elect_target_month;"
    max_datetime = data_loader(conn, data_result_maxdatetime_query)
    data_result_site_query = "SELECT * FROM app.elect_target_month where category='predict' and last_update_time='" + \
        str(max_datetime.max_datetime[0])+"';"
    data_result_site = data_loader(conn, data_result_site_query)
    # data_result_site_sub = data_result_site.loc[~data_result_site.factory_electricity.isna()].reset_index(drop=True)
    # data_result_site_sub['Site'] = [str(np.where(real_id.find('-')!=-1,str(real_id[0:(real_id.find('-'))]),real_id)) for real_id in data_result_site_sub.plant]
    # data_result_site_sub['Site'] = np.where(data_result_site_sub.Site=='WIHK',data_result_site_sub.plant,data_result_site_sub.Site)
    # data_result_site_sub = data_result_site_sub.groupby(['Site','datetime'], group_keys=True).agg({'factory_electricity':'sum'}).reset_index().rename(columns={'factory_electricity':'amount_actual'})
    data_result_site = data_result_site[['site', 'month', 'year', 'amount']].rename(
        columns={'site': 'Site', 'amount': 'amount_actual'})
    data_result_site['datetime'] = [datetime.date(datetime.strptime(str(data_result_site.year[x])+'-'+str(
        data_result_site.month[x])+'-01', '%Y-%m-%d')) for x in range(len(data_result_site.year))]
    data_result_site['year'] = [str(x)[0:4] for x in data_result_site.datetime]
    data_result_site['month'] = [str(x)[5:7]
                                 for x in data_result_site.datetime]
    data_result_site_sub = data_result_site.loc[~data_result_site.Site.isin(
        workspace_site)]
    # workspace actual factory electricity
    data_result_workspace_query = "SELECT * FROM app.baseline_data_overview where (datetime>='2022-01-01') and (datetime<='2022-12-01') and plant in ('"+"','".join(
        workspace_site)+"');"
    data_result_workspace = data_loader(conn, data_result_workspace_query)
    data_result_workspace_sub = data_result_workspace.loc[~data_result_workspace.factory_electricity.isna(
    )].reset_index(drop=True)
    data_result_workspace_sub['Site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in data_result_workspace_sub.plant]
    data_result_workspace_sub['Site'] = np.where(
        data_result_workspace_sub.Site == 'WIHK', data_result_workspace_sub.plant, data_result_workspace_sub.Site)
    data_result_workspace_sub = data_result_workspace_sub.groupby(['Site', 'datetime'], group_keys=True).agg(
        {'factory_electricity': 'sum'}).reset_index().rename(columns={'factory_electricity': 'amount_actual'})
    data_result_workspace_sub['year'] = [
        str(x)[0:4] for x in data_result_workspace_sub.datetime]
    data_result_workspace_sub['month'] = [
        str(x)[5:7] for x in data_result_workspace_sub.datetime]
    data_result_workspace_sub_forecst = data_result_site.loc[data_result_site.Site.isin(
        workspace_site)]
    data_result_workspace = data_result_workspace_sub.append(data_result_workspace_sub_forecst.rename(
        columns={'factory_electricity_predict': 'amount_actual'})).reset_index(drop=True)
    workspace_max_year = max(data_result_workspace.year)
    # compute previous two year mean factory electricity
    for year_index in range(1, int(target_year)-int(workspace_max_year)+1, 1):
        data_result_workspace_prev = data_result_workspace.loc[(data_result_workspace.year < str(int(
            workspace_max_year)+year_index)) & (data_result_workspace.year >= str(int(workspace_max_year)+year_index-2))]
        data_result_workspace_prev = data_result_workspace_prev.loc[data_result_workspace_prev.amount_actual > 0].groupby(
            ['Site', 'month'], group_keys=True).agg({'amount_actual': 'mean'}).reset_index()
        data_result_workspace_prev['year'] = str(
            int(workspace_max_year)+year_index)
        data_result_workspace_prev['datetime'] = [datetime.date(datetime.strptime(str(data_result_workspace_prev.year[x])+'-'+str(
            data_result_workspace_prev.month[x])+'-01', '%Y-%m-%d')) for x in range(len(data_result_workspace_prev.year))]
        data_result_workspace = data_result_workspace.append(
            data_result_workspace_prev).reset_index(drop=True)
    data_result_site_sub_y23 = data_result_site_sub.append(
        data_result_workspace.loc[data_result_workspace.year >= str(base_year)]).reset_index(drop=True)
    return data_result_site_sub_y23


def variable_3y_forecast_generator(conn, plant_mapping, target_year):
    # Connect to DB to download data
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    # agg shipment data
    sql = 'SELECT max(last_update_time) as last_update_time FROM app."decarb_est_shipments";'
    shipment_predict_Y23 = pd.read_sql(sql, con=conn)
    max_datetime = str(shipment_predict_Y23['last_update_time'][0])
    sql = "SELECT * FROM app.decarb_est_shipments where last_update_time='"+max_datetime+"';"
    shipment_predict_Y23 = pd.read_sql(sql, con=conn)
    # variable_predict_Y23 = variable_predict_Y23.drop(columns={'site','last_update_time'}).rename(columns={'plant':'plant_code'})
    y23_plant_predict_shipment_month = shipment_predict_Y23.loc[:, ['site', 'year', 'month', 'amount']].reset_index(
        drop=True).rename(columns={'site': 'Site', 'amount': 'shipment_qty'})
    y23_plant_predict_shipment_month['shipment_qty'] = y23_plant_predict_shipment_month.shipment_qty*1000
    y23_plant_predict_shipment_month.loc[y23_plant_predict_shipment_month.Site ==
                                         'WIHK', 'Site'] = 'WIHK1'
    y23_plant_predict_shipment_month['datetime'] = [str(datetime.strptime(str(y23_plant_predict_shipment_month['year'][x])+'-'+str(
        y23_plant_predict_shipment_month['month'][x])+'-01', '%Y-%m-%d'))[0:10] for x in range(len(y23_plant_predict_shipment_month['year']))]
    y23_plant_predict_shipment_month['month'] = [
        str(x)[5:7] for x in y23_plant_predict_shipment_month['datetime']]

    sql = 'SELECT * FROM raw."V_BUDGETMOH_ESG";'
    variable_predict_Y23 = pd.read_sql(sql, con=conn)
    variable_predict_Y23 = variable_predict_Y23.drop(
        columns={'site', 'last_update_time'}).rename(columns={'plant': 'plant_code'})
    variable_predict_Y23 = pd.merge(variable_predict_Y23,
                                    plant_mapping, on=['plant_code'], how='left').reset_index(drop=True)
    variable_predict_Y23 = variable_predict_Y23.loc[~variable_predict_Y23.bo.isna(
    ), :].reset_index(drop=True)
    variable_predict_Y23['account_name'] = [str(np.where(x.find('Headcount') != -1, 'member_counts',
                                                         np.where(x.find('Production') != -1, 'product_qty', 'shipment_qty'))) for x in variable_predict_Y23['accountnm']]
    variable_predict_Y23_summary = variable_predict_Y23.groupby(['bo', 'site', 'plant_name', 'account_name', 'date_key'], group_keys=True).agg(
        {'value': 'sum'}).reset_index().rename(columns={'value': 'value_total', 'date_key': 'datetime'})
    variable_predict_Y23_summary = variable_predict_Y23_summary.loc[variable_predict_Y23_summary.datetime.astype(
        str) >= str(max(variable_predict_Y23_summary.datetime))[0:4]+'-01-01', :]
    variable_predict_Y23_summary_wide = pd.pivot(variable_predict_Y23_summary, index=[
                                                 'bo', 'site', 'plant_name', 'datetime'], columns='account_name', values='value_total').rename_axis(None, axis=1).reset_index()
    variable_predict_Y23_summary_wide['plant_name'] = np.where(
        variable_predict_Y23_summary_wide['plant_name'] == 'WCD-1', 'WCD', variable_predict_Y23_summary_wide['plant_name'])

    variable_predict_Y23_summary_wide['site'] = np.where(
        variable_predict_Y23_summary_wide.site == 'WIHK', variable_predict_Y23_summary_wide.plant_name, variable_predict_Y23_summary_wide.site)
    variable_predict_Y23_summary_wide_month_rate = variable_predict_Y23_summary_wide.copy()
    variable_predict_Y23_summary_wide_month_rate['year'] = [
        str(x)[0:4] for x in variable_predict_Y23_summary_wide_month_rate['datetime']]
    variable_predict_Y23_summary_wide_month_rate['month'] = [
        str(x)[5:7] for x in variable_predict_Y23_summary_wide_month_rate['datetime']]
    #######################################################################
    variable_predict_Y23_summary_month_plant_rate = pd.merge(variable_predict_Y23_summary_wide_month_rate.loc[variable_predict_Y23_summary_wide_month_rate.site.isin(['WZS', 'WKS'])],
                                                             variable_predict_Y23_summary_wide_month_rate.groupby(['site', 'year', 'month']).agg(
                                                                 {'shipment_qty': 'sum'}).reset_index().rename(columns={'shipment_qty': 'shipment_qty_total'}),
                                                             on=['site', 'year', 'month'], how='left')
    variable_predict_Y23_summary_month_plant_rate['plant_month_rate'] = variable_predict_Y23_summary_month_plant_rate.shipment_qty / \
        variable_predict_Y23_summary_month_plant_rate.shipment_qty_total
    # merge plant month rate
    y23_plant_predict_shipment_bymonth = pd.merge(y23_plant_predict_shipment_month.rename(columns={'Site': 'site'}),
                                                  variable_predict_Y23_summary_month_plant_rate[[
                                                      'site', 'plant_name', 'month', 'plant_month_rate']],
                                                  on=['site', 'month'], how='left')
    y23_plant_predict_shipment_bymonth['plant_month_rate'] = np.where(
        y23_plant_predict_shipment_bymonth['plant_month_rate'].isna(), 1, y23_plant_predict_shipment_bymonth['plant_month_rate'])
    y23_plant_predict_shipment_bymonth['shipment_qty'] = y23_plant_predict_shipment_bymonth.plant_month_rate * \
        y23_plant_predict_shipment_bymonth.shipment_qty
    y23_plant_predict_shipment_bymonth['plant_name'] = np.where(y23_plant_predict_shipment_bymonth['plant_name'].isna(
    ), y23_plant_predict_shipment_bymonth['site'], y23_plant_predict_shipment_bymonth['plant_name'])
    y23_plant_predict_shipment_bymonth['site'] = [str(np.where(x.find('WIHK1') != -1, 'WIHK-1',
                                                               np.where(x.find('WIHK2') != -1, 'WIHK-2', x))) for x in y23_plant_predict_shipment_bymonth.site]
    y23_plant_predict_shipment_bymonth['datetime'] = [datetime.date(datetime.strptime(
        str(x), '%Y-%m-%d')) for x in y23_plant_predict_shipment_bymonth.datetime]
    #######################################################################
    variable_predict_Y23_summary_wide_month_rate = pd.merge(variable_predict_Y23_summary_wide_month_rate,
                                                            variable_predict_Y23_summary_wide_month_rate.groupby(['site', 'year']).agg(
                                                                {'shipment_qty': 'sum'}).reset_index().rename(columns={'shipment_qty': 'shipment_qty_total'}),
                                                            on=['site', 'year'], how='left')
    variable_predict_Y23_summary_wide_month_rate['plant_month_rate'] = variable_predict_Y23_summary_wide_month_rate.shipment_qty / \
        variable_predict_Y23_summary_wide_month_rate.shipment_qty_total
    # agg shipment data
    sql = 'SELECT max(last_update_time) as last_update_time FROM app."decarb_est_shipments";'
    shipment_predict_Y23 = pd.read_sql(sql, con=conn)
    max_datetime = str(shipment_predict_Y23['last_update_time'][0])
    sql = "SELECT * FROM app.decarb_est_shipments where last_update_time='"+max_datetime+"';"
    shipment_predict_Y23 = pd.read_sql(sql, con=conn)
    # variable_predict_Y23 = variable_predict_Y23.drop(columns={'site','last_update_time'}).rename(columns={'plant':'plant_code'})
    y23_plant_predict_shipment_month = shipment_predict_Y23.loc[:, ['site', 'year', 'month', 'amount']].reset_index(
        drop=True).rename(columns={'site': 'Site', 'amount': 'shipment_qty'})
    y23_plant_predict_shipment_month['shipment_qty'] = y23_plant_predict_shipment_month.shipment_qty*1000
    y23_plant_predict_shipment_sub = y23_plant_predict_shipment_month.groupby(['Site', 'year'], group_keys=True).agg(
        {'shipment_qty': 'sum'}).reset_index().rename(columns={'shipment_qty': 'shipment_qty_total'})
    y23_plant_predict_shipment_sub.loc[y23_plant_predict_shipment_sub.Site ==
                                       'WIHK', 'Site'] = 'WIHK1'
    # GR grouth data
    plant_predict_shipment = pd.DataFrame({})
    for x in range(1, 3, 1):
        plant_predict_shipment_sub = y23_plant_predict_shipment_sub.loc[y23_plant_predict_shipment_sub.year == min(
            y23_plant_predict_shipment_sub.year)+(x-1)].rename(columns={'year': 'base_year', 'shipment_qty_total': 'base_shipment_qty_total'})
        plant_predict_shipment_gr = pd.merge(plant_predict_shipment_sub,
                                             y23_plant_predict_shipment_sub.loc[y23_plant_predict_shipment_sub.year == min(
                                                 y23_plant_predict_shipment_sub.year)+x],
                                             on=['Site'], how='left').reset_index(drop=True)
        plant_predict_shipment_gr['GR'] = (plant_predict_shipment_gr.shipment_qty_total -
                                           plant_predict_shipment_gr.base_shipment_qty_total)/plant_predict_shipment_gr.base_shipment_qty_total
        plant_predict_shipment = plant_predict_shipment.append(
            plant_predict_shipment_gr[['Site', 'year', 'GR']]).reset_index(drop=True)
    # plant_predict_shipment = plant_predict_shipment.loc[~plant_predict_shipment.year.isna()].reset_index(drop=True)
    plant_predict_shipment.loc[plant_predict_shipment.GR > 1000, 'GR'] = 0.5
    # Process shipment contain na data
    plant_predict_shipment_na = y23_plant_predict_shipment_sub.loc[(y23_plant_predict_shipment_sub.year == max(
        y23_plant_predict_shipment_sub.year)) & (~y23_plant_predict_shipment_sub.Site.isin(plant_predict_shipment.Site))]
    plant_predict_shipment_na = pd.merge(plant_predict_shipment_na,
                                         y23_plant_predict_shipment_sub.loc[(y23_plant_predict_shipment_sub.year == min(y23_plant_predict_shipment_sub.year)) & (
                                             y23_plant_predict_shipment_sub.Site.isin(plant_predict_shipment_na.Site))].rename(columns={'year': 'base_year', 'shipment_qty_total': 'base_shipment_qty_total'}),
                                         on=['Site'], how='left').reset_index(drop=True)
    plant_predict_shipment_na['GR'] = (plant_predict_shipment_na.shipment_qty_total -
                                       plant_predict_shipment_na.base_shipment_qty_total)/plant_predict_shipment_na.base_shipment_qty_total/2
    plant_predict_shipment_na_previous = plant_predict_shipment_na.copy()
    plant_predict_shipment_na_previous['year'] = plant_predict_shipment_na_previous['year']-1
    plant_predict_shipment_na = plant_predict_shipment_na.append(
        plant_predict_shipment_na_previous).reset_index(drop=True)
    plant_predict_shipment = plant_predict_shipment.append(
        plant_predict_shipment_na[plant_predict_shipment.columns]).reset_index(drop=True)
    plant_predict_shipment['year'] = plant_predict_shipment.year.astype(int)

    # plant_predict_shipment_sub['Site'] = [str(np.where(x.find('WIHK')!=-1, 'WIHK', x)) for x in plant_predict_shipment_sub.Site]
    y23_plant_predict_shipment_sub['Site'] = [str(np.where(x.find('WIHK1') != -1, 'WIHK-1',
                                                           np.where(x.find('WIHK2') != -1, 'WIHK-2', x))) for x in y23_plant_predict_shipment_sub.Site]
    y23_plant_predict_shipment_sub = pd.merge(y23_plant_predict_shipment_sub.rename(columns={'Site': 'site'}),
                                              variable_predict_Y23_summary_wide_month_rate[[
                                                  'site', 'plant_name', 'month', 'plant_month_rate']],
                                              on=['site'], how='left').reset_index(drop=True)
    y23_plant_predict_shipment_sub['shipment_qty'] = y23_plant_predict_shipment_sub.shipment_qty_total * \
        y23_plant_predict_shipment_sub.plant_month_rate
    y23_plant_predict_shipment_sub['datetime'] = [datetime.date(datetime.strptime(str(y23_plant_predict_shipment_sub.year[x])+'-'+str(
        y23_plant_predict_shipment_sub.month[x])+'-01', '%Y-%m-%d')) for x in range(y23_plant_predict_shipment_sub.shape[0])]
    y23_plant_predict_shipment_sub = pd.merge(y23_plant_predict_shipment_sub,
                                              y23_plant_predict_shipment_bymonth[['plant_name', 'datetime', 'shipment_qty']].rename(
                                                  columns={'shipment_qty': 'shipment_qty_new'}),
                                              on=['plant_name', 'datetime'], how='left')
    y23_plant_predict_shipment_sub['shipment_qty_new'] = np.where(y23_plant_predict_shipment_sub.shipment_qty_new.isna(),
                                                                  y23_plant_predict_shipment_sub.shipment_qty,
                                                                  y23_plant_predict_shipment_sub.shipment_qty_new)
    y23_plant_predict_shipment_sub = y23_plant_predict_shipment_sub.drop(
        columns={'shipment_qty'}).rename(columns={'shipment_qty_new': 'shipment_qty'})
    # add merge new shipment
    variable_predict_Y23_summary_wide = pd.merge(variable_predict_Y23_summary_wide,
                                                 y23_plant_predict_shipment_bymonth[['plant_name', 'datetime', 'shipment_qty']].rename(
                                                     columns={'shipment_qty': 'shipment_qty_new'}),
                                                 on=['plant_name', 'datetime'], how='left')
    variable_predict_Y23_summary_wide['shipment_qty_new'] = np.where(variable_predict_Y23_summary_wide.shipment_qty_new.isna(),
                                                                     variable_predict_Y23_summary_wide.shipment_qty,
                                                                     variable_predict_Y23_summary_wide.shipment_qty_new)
    variable_predict_Y23_summary_wide = variable_predict_Y23_summary_wide.drop(
        columns={'shipment_qty'}).rename(columns={'shipment_qty_new': 'shipment_qty'})
    return variable_predict_Y23_summary_wide, y23_plant_predict_shipment_sub, plant_predict_shipment


def real_base_generator(conn, data_result_simulation_all, plant_predict_energy_all, base_start_month, base_end_month, predict_site):
    # Add Y22 real factory electricity
    plant_predict_energy_Y22_query = "SELECT * FROM app.elect_target_month where (category='actual') and (year='2022') and (last_update_time is not null);"
    plant_predict_energy_sub_Y22 = data_loader(
        conn, plant_predict_energy_Y22_query)
    plant_predict_energy_sub_Y22 = plant_predict_energy_sub_Y22.rename(
        columns={'site': 'Site', 'amount': 'factory_electricity'})
    plant_predict_energy_sub_Y22['datetime'] = [str(pd.to_datetime(str(plant_predict_energy_sub_Y22['year'][x])+'-'+str(
        plant_predict_energy_sub_Y22['month'][x])+'-01'))[0:10] for x in range(len(plant_predict_energy_sub_Y22.month))]
    plant_predict_energy_sub_Y22['plant'] = plant_predict_energy_sub_Y22['Site']

    ## ----------generate real factory electricity -------------##
    ## ----------new method(best)-------------##
    ## ----- consider Y22-07~Y23-06-----##
    # base_end_month = '2023-06-01'
    # base_start_month = str(np.datetime64(base_end_month[0:7]) - np.timedelta64(11, 'M'))+'-01'
    # Y22 by month factory real electricity
    # predict_site = ['XTRKS','KOE','WGKS','WGTX','WVN','WTN'] # no Y22 data
    data_upload_final_Y22real = plant_predict_energy_sub_Y22.loc[(~plant_predict_energy_sub_Y22.plant.isin(predict_site)) & (plant_predict_energy_sub_Y22.datetime >= base_start_month) & (
        plant_predict_energy_sub_Y22.datetime <= '2022-12-01'), ['plant', 'datetime', 'factory_electricity']].rename(columns={'factory_electricity': 'factory_electricity_base'})
    data_upload_final_Y22real['month'] = [x[5:7]
                                          for x in data_upload_final_Y22real['datetime']]
    # Y22 by month factory real electricity in WKS-5,6
    data_upload_final_Y22real_wks = data_result_simulation_all.loc[(data_result_simulation_all.plant.isin(['WKS-5', 'WKS-6'])) & (data_result_simulation_all.datetime >= base_start_month) & (
        data_result_simulation_all.datetime <= '2022-12-01'), ['plant', 'datetime', 'factory_electricity']].rename(columns={'factory_electricity': 'factory_electricity_base'})
    data_upload_final_Y22real_wks['month'] = [x[5:7]
                                              for x in data_upload_final_Y22real_wks['datetime']]
    # Y23 by month factory real electricity
    data_upload_final_Y23real = plant_predict_energy_all.loc[(((~plant_predict_energy_all.plant.isin(predict_site)) & (plant_predict_energy_all.datetime <= base_end_month)) | ((plant_predict_energy_all.plant.isin(predict_site)) & (
        plant_predict_energy_all.datetime <= '2023-12-01'))) & (plant_predict_energy_all.datetime >= '2023-01-01'), ['plant', 'datetime', 'factory_electricity_new']].rename(columns={'factory_electricity_new': 'factory_electricity_base'}).reset_index(drop=True)
    data_upload_final_Y23real['month'] = [x[5:7]
                                          for x in data_upload_final_Y23real['datetime']]
    data_upload_final_Y23real = data_upload_final_Y23real.append(
        data_upload_final_Y22real).reset_index(drop=True)
    data_upload_final_Y23real = data_upload_final_Y23real.append(
        data_upload_final_Y22real_wks).reset_index(drop=True)
    # Y25 by month factory real electricity
    data_upload_final_Y25pred = data_result_simulation_all.loc[(data_result_simulation_all.datetime >= base_start_month) & (data_result_simulation_all.datetime <= base_end_month), [
        'plant', 'datetime', 'factory_electricity']].rename(columns={'factory_electricity': 'factory_electricity_base'}).reset_index(drop=True)
    data_upload_final_Y25pred['month'] = [x[5:7]
                                          for x in data_upload_final_Y25pred['datetime']]
    return data_upload_final_Y23real, data_upload_final_Y25pred, plant_predict_energy_sub_Y22
    return data_upload_final_Y23real, plant_predict_energy_sub_Y22


def site_electricity_generator(data_upload_final, data_upload_final_Y23real, data_upload_final_Y25pred, base_year, base_start_month, base_end_month, model_site, plant_predict_shipment):
    ## ----------model predict electricity increase rate-------------##
    # New version
    # base_year = 2023
    base_year = int(base_year)
    data_upload_final_Y23base = data_upload_final.loc[data_upload_final.datetime.astype(
        str) >= base_start_month, :]
    data_upload_final_Y23base = pd.merge(data_upload_final_Y23base.loc[data_upload_final_Y23base.year.astype(int) > base_year-1, :],
                                         data_upload_final_Y23base.loc[(data_upload_final_Y23base.datetime >= base_start_month) & (data_upload_final_Y23base.datetime <= base_end_month), [
                                             'month', 'plant', 'predict_electricity']].rename(columns={'predict_electricity': 'predict_electricity_base'}),
                                         on=['month', 'plant'], how='left')
    data_upload_final_Y23base['month_grouth_rate'] = (data_upload_final_Y23base['predict_electricity'] -
                                                      data_upload_final_Y23base['predict_electricity_base'])/data_upload_final_Y23base['predict_electricity_base']
    # Adjust GR parameters-------
    data_upload_final_Y23base['Site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in data_upload_final_Y23base.plant]
    data_upload_final_Y23base['Site'] = np.where(
        data_upload_final_Y23base.Site == 'WIHK', data_upload_final_Y23base.plant, data_upload_final_Y23base.Site)
    plant_predict_shipment_rate = plant_predict_shipment.loc[plant_predict_shipment.year.astype(
        int) == base_year+1]
    plant_predict_shipment_rate['GR'] = np.where(plant_predict_shipment_rate['GR'] >= 0.99, 0.7,
                                                 np.where(plant_predict_shipment_rate['GR'] < -0.99, -1,
                                                          plant_predict_shipment_rate['GR']*0.5))
    data_upload_final_Y23base = pd.merge(data_upload_final_Y23base, plant_predict_shipment_rate[['Site', 'GR']],
                                         on=['Site'], how='left').reset_index(drop=True)
    data_upload_final_Y23base['month_grouth_rate'] = np.where((data_upload_final_Y23base['month_grouth_rate'] < 0) & (data_upload_final_Y23base['year'].astype(
        int) >= base_year+1), (data_upload_final_Y23base['year'].astype(int)-base_year)*data_upload_final_Y23base['GR'], data_upload_final_Y23base['month_grouth_rate'])  # fix me !!! 0.01
    data_upload_final_Y23base = data_upload_final_Y23base.drop(columns={
                                                               'Site'})
    # Adjust GR parameters-------
    data_upload_final_Y23base['month_grouth_rate'] = np.where((data_upload_final_Y23base['year'].astype(int) == base_year+1) & (data_upload_final_Y23base['plant'].isin(
        ['KOE', 'WCQ', 'WKS-5', 'WKS-6'])), (data_upload_final_Y23base['year'].astype(int)-base_year)*(-0.20), data_upload_final_Y23base['month_grouth_rate'])  # fix me !!! 0.01
    data_upload_final_Y23base['month_grouth_rate'] = np.where((data_upload_final_Y23base['month_grouth_rate'] < 0) & (data_upload_final_Y23base['year'].astype(
        int) >= base_year+1) & (data_upload_final_Y23base['plant'].isin(['WOK', 'WIHK', 'WTZ'])), data_upload_final_Y23base['GR'], data_upload_final_Y23base['month_grouth_rate'])  # fix me !!! 0.01
    data_upload_final_Y23base['month_grouth_rate'] = np.where((data_upload_final_Y23base['year'].astype(int) > base_year+1) & ~(data_upload_final_Y23base['plant'].isin(
        ['WOK', 'WIHK', 'WTZ'])), (data_upload_final_Y23base['year'].astype(int)-base_year)*0.06, data_upload_final_Y23base['month_grouth_rate'])  # fix me !!! 0.01
    data_upload_final_Y23base['month_grouth_rate'] = np.where((data_upload_final_Y23base['month_grouth_rate']/(data_upload_final_Y23base['year'].astype(
        int)-base_year) >= 0.30), (data_upload_final_Y23base['year'].astype(int)-base_year)*0.30, data_upload_final_Y23base['month_grouth_rate'])  # fix me !!! 0.01

    data_upload_final_Y23base = pd.merge(data_upload_final_Y23base,
                                         # data_upload_final_Y23real[['plant','month','factory_electricity_base']],
                                         data_upload_final_Y25pred[[
                                             'plant', 'month', 'factory_electricity_base']],
                                         on=['plant', 'month'], how='left').reset_index(drop=True)
    data_upload_final_Y23base['predict_electricity_v2'] = np.where(data_upload_final_Y23base.month_grouth_rate != 0, (1+data_upload_final_Y23base.month_grouth_rate.astype(
        float))*data_upload_final_Y23base.factory_electricity_base.astype(float), data_upload_final_Y23base.factory_electricity_base.astype(float))
    data_upload_final_Y23base_year = data_upload_final_Y23base.groupby(['year'], group_keys=True).agg(
        {'predict_electricity_v2': 'sum'}).reset_index().rename(columns={'predict_electricity_v2': 'predict_electricity_v2_total'})

    # Compute rate = (build model sites)/(total sites)
    # .loc[(~total_energy.factory_electricity_base.isna()),:]
    total_energy = data_upload_final_Y23real.copy()
    total_energy = total_energy.loc[total_energy.plant != 'WIHK', :].reset_index(
        drop=True)
    total_energy['Site_top'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in total_energy.plant]
    total_energy['Site_top'] = np.where(
        total_energy.Site_top == 'WIHK', total_energy.plant, total_energy.Site_top)
    total_energy['factory_electricity_base'] = (
        total_energy['factory_electricity_base'].astype(str)).astype(float)
    total_energy.loc[total_energy.Site_top == 'WVN', ['factory_electricity_base']] = [
        80354325/12 for x in range(0, 12)]  # fix me
    # total_energy.loc[total_energy.plant.isin(['WIHK-1','WIHK-2']),['plant']] = 'WIHK'
    total_energy = total_energy.groupby(['plant', 'datetime', 'month', 'Site_top']).agg({
        'factory_electricity_base': 'sum'}).reset_index()
    # total_energy = total_energy.loc[(total_energy.datetime>=base_start_month) & (total_energy.datetime<=base_end_month)].reset_index(drop=True)
    total_energy = total_energy.loc[((total_energy.datetime >= base_start_month) & (total_energy.datetime <= base_end_month) & (~total_energy.Site_top.isin(['WVN', 'WTN']))) | (
        (total_energy.datetime >= str(base_year)+'-01-01') & (total_energy.datetime <= str(base_year)+'-12-01') & (total_energy.Site_top.isin(['WVN', 'WTN'])))].reset_index(drop=True)
    total_energy_site = total_energy.loc[(total_energy.plant != 'WKS-Zara') & (total_energy.plant != 'WCQ-HP'), :].groupby(
        ['Site_top', 'month'], group_keys=True).agg({'factory_electricity_base': 'sum'}).reset_index().rename(columns={'factory_electricity_base': 'site_total'})
    total_energy_site['site_rate'] = total_energy_site.site_total / \
        sum(total_energy_site.site_total)
    total_sit_rate = sum(
        total_energy_site.loc[total_energy_site.Site_top.isin(model_site), 'site_rate'])
    data_upload_final_Y23base_year['predict_electricity_v2_total'] = data_upload_final_Y23base_year['predict_electricity_v2_total']/total_sit_rate

    # Compute other not build model site by month factory electricity
    data_upload_final_Y23base_other = pd.DataFrame({})
    for x in range(data_upload_final_Y23base_year.shape[0]):
        total_energy_site_new = total_energy_site.loc[~total_energy_site.Site_top.isin(
            model_site), :].reset_index(drop=True)
        total_energy_site_new['year'] = data_upload_final_Y23base_year['year'][x]
        total_energy_site_new['predict_electricity_v2'] = total_energy_site_new['site_rate'] * \
            data_upload_final_Y23base_year['predict_electricity_v2_total'][x]
        data_upload_final_Y23base_other = data_upload_final_Y23base_other.append(
            total_energy_site_new).reset_index(drop=True)
    data_upload_final_Y23base_other['plant'] = data_upload_final_Y23base_other['Site_top']
    data_upload_final_Y23base_other['factory_electricity_base'] = data_upload_final_Y23base_other['site_total']
    data_upload_final_Y23base_other['datetime'] = [data_upload_final_Y23base_other['year'][x]+'-' +
                                                   data_upload_final_Y23base_other['month'][x]+'-01' for x in range(data_upload_final_Y23base_other.shape[0])]
    data_upload_final_Y23base_other.loc[(data_upload_final_Y23base_other['datetime'].astype(str) >= str(base_year+1)+'-01-01') & (data_upload_final_Y23base_other['plant'].isin(['WVN'])), 'predict_electricity_v2'] = data_upload_final_Y23base_other.loc[(
        data_upload_final_Y23base_other['datetime'].astype(str) >= str(base_year+1)+'-01-01') & (data_upload_final_Y23base_other['plant'].isin(['WVN'])), 'predict_electricity_v2']*(1+0.25)
    data_upload_final_Y23base_other.loc[(data_upload_final_Y23base_other['datetime'].astype(str) >= str(base_year+1)+'-01-01') & (data_upload_final_Y23base_other['plant'].isin(['WMX'])), 'predict_electricity_v2'] = data_upload_final_Y23base_other.loc[(
        data_upload_final_Y23base_other['datetime'].astype(str) >= str(base_year+1)+'-01-01') & (data_upload_final_Y23base_other['plant'].isin(['WMX'])), 'predict_electricity_v2']*(1+0.4)
    # Combine build and un-build model site by month factory electricity
    column_name = ['datetime', 'year', 'month', 'plant',
                   'factory_electricity_base', 'predict_electricity_v2']
    data_upload_final_Y23base_final = data_upload_final_Y23base[column_name].append(
        data_upload_final_Y23base_other[column_name]).reset_index(drop=True)
    return data_upload_final_Y23base_final, total_energy_site_new


def model_api_caller(data, url, api_key):
    payload = json.dumps(data)
    headers = {
        'Authorization': 'Bearer ' + api_key,
        'Content-Type': 'application/json'
    }
    try:
        response = requests.post(url, headers=headers,
                                 data=payload, verify=False)
        result = response.text
        # print(result)
    except urllib.error.HTTPError as error:
        print("The request failed with status code: " + str(error.code))

        # Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
        print(error.info())
        print(error.read().decode("utf8", 'ignore'))
    return result


@app.task(name='electricity-ai-simulator')
def factory_elct_main_fn():
    print('factory_elct_main_fn start')
    # if ((datetime.now()).strftime('%Y-%m-%d')[5:10]=='09-15') or ((datetime.now()).strftime('%Y-%m-%d')[5:10]=='09-30'):
    print('Upload data is start!')
    ############### Generate Inputs #####################
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    url0 = 'http://10.30.80.134:80/api/v1/service/factory-elec-simulator-prd/score'
    api_key0 = 'JQMiF0CFkn1hQSL8Lcy3DG9hYXdamhzY'
    baseline_year = '2022'
    target_year = '2030'
    base_end_month = (datetime.now() - pd.DateOffset(months=1)
                      ).strftime('%Y-%m-%d')
    base_year = int(base_end_month[0:4])
    base_elect_year = str(base_year)
    base_end_month = base_elect_year+'-08-01'
    real_date = base_end_month
    base_start_month = str(np.datetime64(
        base_end_month[0:7]) - np.timedelta64(11, 'M'))+'-01'
    plant_batch = [['WOK'], ['WZS-1'], ['WZS-3'], ['WZS-6'], ['WZS-8'], ['WTZ'], ['WCD'],
                   ['WKS-5'], ['WKS-6'], ['WCQ'], ['WIH'], ['KOE'], ['WGKS'], ['XTRKS'], ['WIHK-2']]
    base_end_month = (datetime.now() - pd.DateOffset(months=1)
                      ).strftime('%Y-%m-%d')
    base_start_month = str(np.datetime64(
        base_end_month[0:7]) - np.timedelta64(11, 'M'))+'-01'

    # Connect to DB to download data
    print('Download machine info from DB.')
    # conn = psycopg2.connect(host=host0, port=port0, database=database0,
    #                     user=user0, password=password0)
    # Load plant mapping table
    load_plant_data_query = "SELECT * FROM raw.plant_mapping;"
    plant_mapping = data_loader(conn, load_plant_data_query)
    load_baseline_data_query = "SELECT * FROM app.baseline_data_overview;"
    load_predict_data_query = "SELECT * FROM app.predict_baseline_data;"
    data_result, data_result_history = baseline_data_loader(
        conn, load_baseline_data_query, plant_mapping)
    ## ----------------1.fix history electricity data-------------------##
    data_result, data_result_history = history_data_fixer(
        data_result, data_result_history)
    ## ----------------2.generate variable trend by site----------------##
    data_result_trend_final = variable_data_generator(data_result)
    data_result = data_result.astype('string')
    # data_result_history['日期'] = data_result_history['日期'].astype('string')
    data_result_history = data_result_history.astype('string')
    data_result = data_result.drop(
        columns={'last_update_time', 'datetime_max'})
    data_result_history = data_result_history.drop(
        columns={'last_update_time'})

    ## ----------3.generate simulation data by site, month-------------##
    plant_predict_energy_all, data_result_simulation_all, data_result_history_simulation_all = simulation_data_generator(
        conn, data_result, data_result_history, data_result_trend_final, baseline_year, target_year)
    # generate 3 year variable forecast
    variable_predict_Y23_summary_wide, y23_plant_predict_shipment_sub, plant_predict_shipment = variable_3y_forecast_generator(
        conn, plant_mapping, target_year)
    variable_predict_Y23_summary_wide['datetime'] = variable_predict_Y23_summary_wide.datetime.astype(
        str)
    y23_plant_predict_shipment_sub['datetime'] = y23_plant_predict_shipment_sub.datetime.astype(
        str)
    # merge 3 year forecast
    data_result_simulation_all = pd.merge(data_result_simulation_all,
                                          variable_predict_Y23_summary_wide[['plant_name', 'datetime', 'shipment_qty', 'member_counts', 'product_qty']].rename(
                                              columns={'plant_name': 'plant', 'shipment_qty': 'shipment_qty_forecast', 'member_counts': 'member_counts_forecast', 'product_qty': 'product_qty_forecast'}),  # fix me
                                          on=['plant', 'datetime'], how='left').reset_index(drop=True)
    data_result_simulation_all = pd.merge(data_result_simulation_all,
                                          y23_plant_predict_shipment_sub.loc[y23_plant_predict_shipment_sub.datetime.astype(
                                              str) >= '2024-01-01', ['plant_name', 'datetime', 'shipment_qty']].rename(columns={'plant_name': 'plant', 'shipment_qty': 'shipment_qty_forecast_v2'}),  # fix me
                                          on=['plant', 'datetime'], how='left').reset_index(drop=True)
    for x in ['shipment_qty', 'member_counts', 'product_qty']:
        data_result_simulation_all[x] = np.where(data_result_simulation_all[x+'_forecast'].isna(),  # fixme
                                                 data_result_simulation_all[x],
                                                 data_result_simulation_all[x+'_forecast'])
    data_result_simulation_all['shipment_qty'] = np.where(data_result_simulation_all['shipment_qty_forecast_v2'].isna(),  # fixme
                                                          data_result_simulation_all['shipment_qty'],
                                                          data_result_simulation_all['shipment_qty_forecast_v2'])
    ## --------------4.shipment and product align---------------------##
    data_result_simulation_all.loc[data_result_simulation_all.datetime <= real_date,
                                   'shipment_qty'] = data_result_simulation_all.loc[data_result_simulation_all.datetime <= real_date, 'product_qty']
    data_result_simulation_all.loc[data_result_simulation_all.datetime > real_date,
                                   'product_qty'] = data_result_simulation_all.loc[data_result_simulation_all.datetime > real_date, 'shipment_qty']
    data_result_history_simulation_all = pd.merge(data_result_history_simulation_all,
                                                  variable_predict_Y23_summary_wide[['plant_name', 'datetime', 'shipment_qty', 'member_counts', 'product_qty']].rename(
                                                      columns={'plant_name': 'plant', 'shipment_qty': 'shipment_qty_forecast', 'member_counts': 'member_counts_forecast', 'product_qty': 'product_qty_forecast', 'datetime': '日期'}),  # fix me
                                                  on=['plant', '日期'], how='left').reset_index(drop=True)
    data_result_history_simulation_all = pd.merge(data_result_history_simulation_all,
                                                  y23_plant_predict_shipment_sub.loc[y23_plant_predict_shipment_sub.datetime.astype(str) >= '2024-01-01', ['plant_name', 'datetime', 'shipment_qty']].rename(
                                                      columns={'plant_name': 'plant', 'shipment_qty': 'shipment_qty_forecast_v2', 'datetime': '日期'}),  # fix me
                                                  on=['plant', '日期'], how='left').reset_index(drop=True)

    ## ---5.get simulation data forecast electricity by site, month---##
    data_upload_final = pd.DataFrame({})
    for x in plant_batch:
        print(x)
        data_result_json = json.loads(data_result_simulation_all.fillna('null').loc[data_result_simulation_all['plant'].isin(x), :].drop(columns={
                                      'factory_electricity_new', 'shipment_qty_forecast', 'member_counts_forecast', 'product_qty_forecast', 'shipment_qty_forecast_v2', 'shipment_qty_rate', 'member_counts_rate', 'product_qty_rate'}).reset_index(drop=True).to_json(orient="records"))
        data_result_history_json = json.loads(data_result_history_simulation_all.fillna('null').loc[data_result_history_simulation_all['plant'].isin(x), :].drop(
            columns={'factory_electricity_new', 'shipment_qty_forecast', 'member_counts_forecast', 'product_qty_forecast', 'shipment_qty_forecast_v2', 'shipment_qty_rate', 'member_counts_rate', 'product_qty_rate'}).reset_index(drop=True).to_json(orient="records"))
        input_json = {
            "data_result": data_result_json,
            "data_result_history": data_result_history_json
        }
        ############### Call API #####################
        outputs = model_api_caller(input_json, url0, api_key0)
        outputs
        request = json.loads(outputs)
        data_upload = data_type_checker(request['data_upload_final'])
        print('data_upload is successful')
        data_upload_final = data_upload_final.append(
            data_upload).reset_index(drop=True)
    # Adjust plant name
    data_upload_final.plant = np.where(
        data_upload_final.plant == 'WCQ-1', 'WCQ', data_upload_final.plant)
    data_upload_final = data_upload_final.loc[data_upload_final.year >=
                                              baseline_year, :]
    # Add carbon emission
    data_upload_final['emission_factor'] = np.where(data_upload_final.plant == 'WIH', 0.509,
                                                    np.where(data_upload_final.plant == 'WCZ', 0.39,
                                                             np.where(data_upload_final.plant == 'WMX', 0.423, 0.581)))
    data_upload_final['carb_emission'] = data_upload_final.emission_factor * \
        data_upload_final.predict_electricity/1000
    ## ----------6.generate real factory electricity -------------##
    ## ----- consider Y22-07~Y23-06-----##
    # Y22 by month factory real electricity
    predict_site = ['WGTX', 'WVN', 'WTN', 'WIHK']  # no Y22 data 'KOE'
    data_upload_final_Y23real, data_upload_final_Y25pred, plant_predict_energy_sub_Y22 = real_base_generator(
        conn, data_result_simulation_all, plant_predict_energy_all, base_start_month, base_end_month, predict_site)
    ## --------7.model predict electricity increase rate----------##
    model_site = ['KOE', 'WGKS', 'XTRKS', 'WZS',
                  'WKS', 'WCD', 'WCQ', 'WIH', 'WOK', 'WTZ']
    data_upload_final_Y23base_final, total_energy_site_new = site_electricity_generator(
        data_upload_final, data_upload_final_Y23real, data_upload_final_Y25pred, base_elect_year, base_start_month, base_end_month, model_site, plant_predict_shipment)
    ## ---8.merge Y23-Y30 factory electricity by month or year----##
    # By site & by month
    data_upload_final_Y23base_final_bymonth = data_upload_final_Y23base_final[[
        'datetime', 'year', 'month', 'plant', 'predict_electricity_v2']].rename(columns={'predict_electricity_v2': 'factory_electricity_predict'})
    data_upload_final_Y23base_final_bymonth['Site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in data_upload_final_Y23base_final_bymonth.plant]
    data_upload_final_Y23base_final_bymonth['Site'] = np.where(data_upload_final_Y23base_final_bymonth.Site == 'WIHK',
                                                               data_upload_final_Y23base_final_bymonth.plant, data_upload_final_Y23base_final_bymonth.Site)
    data_upload_final_Y23base_final_bymonth = data_upload_final_Y23base_final_bymonth.drop_duplicates(
    ).reset_index(drop=True)
    data_upload_final_Y23base_final_bymonth_bysite = data_upload_final_Y23base_final_bymonth.loc[data_upload_final_Y23base_final_bymonth.year >= '2023'].groupby(
        ['datetime', 'year', 'month', 'Site'], group_keys=True).agg({'factory_electricity_predict': 'sum'}).reset_index()
    ## --------9.merge workspace & MFG actual electricity---------##
    workspace_site = ['WHC', 'WKH', 'WTN', 'WNH']
    data_result_site_sub_y23 = mfg_workspace_elec_generator(
        conn, workspace_site, target_year, base_year)
    data_upload_final_Y23base_final_bymonth_bysite = pd.merge(data_upload_final_Y23base_final_bymonth_bysite,
                                                              data_result_site_sub_y23[[
                                                                  'Site', 'year', 'month', 'amount_actual']],
                                                              on=['Site', 'year', 'month'], how='left').reset_index(drop=True)
    data_upload_final_Y23base_final_bymonth_bysite['factory_electricity_predict'] = np.where(data_upload_final_Y23base_final_bymonth_bysite.amount_actual.isna(),
                                                                                             data_upload_final_Y23base_final_bymonth_bysite.factory_electricity_predict,
                                                                                             data_upload_final_Y23base_final_bymonth_bysite.amount_actual)
    data_upload_final_Y23base_final_bymonth_bysite.loc[(data_upload_final_Y23base_final_bymonth_bysite.Site.isin(
        ['WOK', 'WIHK-1'])) & (data_upload_final_Y23base_final_bymonth_bysite.year > str(base_year)), 'factory_electricity_predict'] = 0
    # By site & by year
    data_upload_final_Y23base_final_byyear = data_upload_final_Y23base_final_bymonth_bysite.loc[data_upload_final_Y23base_final_bymonth_bysite.year >= '2023'].groupby(
        ['Site', 'year'], group_keys=True).agg({'factory_electricity_predict': 'sum'}).reset_index()
    ## --10.merge Y23-Y30 5% factory electricity by month or year--##
    # Compute by site, month rate in year
    data_upload_final_Y23real['site_rate'] = data_upload_final_Y23real['factory_electricity_base'].astype(
        float)/sum(data_upload_final_Y23real['factory_electricity_base'].astype(float))
    # Generate 5% factory electricity by year
    original_year_elec = pd.DataFrame({'factory_electricity_original': [x*100000000 for x in [
                                      4.6, 4.8, 5.1, 5.3, 5.6, 5.9, 6.2, 6.5, 6.8, 7.2]], 'year': [x+2021 for x in range(10)]})
    original_month_elec = pd.DataFrame({})
    # By site & by month
    for x in range(original_year_elec.shape[0]):
        data_upload_final_Y23real['factory_electricity_original'] = [
            original_year_elec['factory_electricity_original'][x] for y in range(data_upload_final_Y23real.shape[0])]
        data_upload_final_Y23real['year'] = [
            original_year_elec['year'][x] for y in range(data_upload_final_Y23real.shape[0])]
        original_month_elec = original_month_elec.append(
            data_upload_final_Y23real.loc[~data_upload_final_Y23real.plant.isin(['WZS', 'WMY'])]).reset_index(drop=True)
    original_month_elec['factory_electricity_original'] = original_month_elec['factory_electricity_base'].astype(
        float)*(1+0.05*(original_month_elec['year']-min(original_month_elec['year'])))
    # original_month_elec['factory_electricity_original'] = original_month_elec['factory_electricity_original']*(1+0.05*x)
    original_month_elec['datetime'] = [str(original_month_elec['year'][x])+'-'+str(
        original_month_elec['month'][x])+'-01' for x in range(len(original_month_elec['datetime']))]
    original_month_elec['Site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in original_month_elec.plant]
    original_month_elec['Site'] = np.where(original_month_elec.Site == 'WIHK',
                                           original_month_elec.plant, original_month_elec.Site)
    original_month_elec_bymonth_bysite = original_month_elec.loc[original_month_elec.year >= base_year].groupby(
        ['datetime', 'year', 'month', 'Site'], group_keys=True).agg({'factory_electricity_original': 'sum'}).reset_index()
    # By site & by year
    original_month_elec_byyear = original_month_elec_bymonth_bysite.loc[original_month_elec_bymonth_bysite.year >= base_year].groupby(
        ['Site', 'year'], group_keys=True).agg({'factory_electricity_original': 'sum'}).reset_index()
    ## ----------------11.merge decarb site data-----------------##
    load_decarb_site_data_query = "SELECT * FROM raw.decarb_site_plant;"
    decarb_site_data = data_loader(conn, load_decarb_site_data_query)
    decarb_site = list(
        np.unique(list(decarb_site_data.site)+list(decarb_site_data.plant)))
    ## ----------12.upload decarb_elect_simulate data------------##
    decarb_max_datetime_query = "SELECT max(last_update_time) as last_update_time FROM app.decarb_elect_simulate;"
    decarb_max_datetime = data_loader(conn, decarb_max_datetime_query)
    decarb_max_datetime = str(decarb_max_datetime['last_update_time'][0])
    load_decarb_data_query = "SELECT * FROM app.decarb_elect_simulate where last_update_time='" + \
        decarb_max_datetime+"';"
    decarb_data = data_loader(conn, load_decarb_data_query)
    max_version = max(decarb_data['version'])
    max_version_year = np.unique(
        decarb_data.loc[decarb_data.version == max_version, ['year']])[0]
    max_version = 'V'+str(int(max_version[1:])+1)  # fix me V5
    new_version = data_upload_final_Y23base_final_byyear.rename(
        columns={'Site': 'site', 'factory_electricity_predict': 'amount'})
    new_version['version'] = max_version
    original_version = original_month_elec_byyear.rename(
        columns={'Site': 'site', 'factory_electricity_original': 'amount'})
    original_version['version'] = 'base'
    decarb_elect_simulate = new_version.append(
        original_version).reset_index(drop=True)
    decarb_elect_simulate['last_update_time'] = datetime.now().strftime(
        '%Y-%m-%d %H:%M:%S')
    # decarb_elect_simulate['amount'] = decarb_elect_simulate['amount']/1000
    decarb_elect_simulate['version_year'] = max_version_year
    data_uploader_append(decarb_elect_simulate.loc[decarb_elect_simulate.site.isin(
        decarb_site) & (decarb_elect_simulate.version != 'base'), :], 'app', 'decarb_elect_simulate')
    print('Upload simulate electricity data is successful!')
    ## ------13.generate & upload elect_target_month data-------##
    # actual factory electricity in base year bymonth
    actual_version_bymonth = data_upload_final_Y23real.copy().rename(
        columns={'factory_electricity_base': 'amount'})
    actual_version_bymonth = actual_version_bymonth.loc[(actual_version_bymonth.datetime < (datetime.now(
    ) - pd.DateOffset(months=1)).strftime('%Y-%m-%d')) & (actual_version_bymonth.datetime >= str(base_year)+'-01-01')]
    actual_version_bymonth['site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in actual_version_bymonth.plant]
    actual_version_bymonth['site'] = np.where(actual_version_bymonth.site == 'WIHK',
                                              actual_version_bymonth.plant, actual_version_bymonth.site)
    actual_version_bymonth['year'] = str(base_year)
    actual_version_bymonth = actual_version_bymonth.drop_duplicates().reset_index(drop=True)
    actual_version_bymonth = actual_version_bymonth.groupby(
        ['site', 'year', 'month'], group_keys=True).agg({'amount': 'sum'}).reset_index()
    actual_version_bymonth['last_update_time'] = np.unique(
        decarb_elect_simulate['last_update_time'])[0]
    actual_version_bymonth['category'] = 'actual'
    actual_version_byyear = actual_version_bymonth.groupby(
        ['site', 'year'], group_keys=True).agg({'amount': 'sum'}).reset_index()
    actual_version_byyear['last_update_time'] = np.unique(
        decarb_elect_simulate['last_update_time'])[0]
    actual_version_byyear['category'] = 'actual'
    # actual_version_byyear['amount'] = actual_version_byyear['amount']/1000
    # new factory electricity in base year bymonth
    new_version_bymonth = data_upload_final_Y23base_final_bymonth_bysite.copy(
    ).rename(columns={'Site': 'site', 'factory_electricity_predict': 'amount'})
    # new_version_bymonth = new_version_bymonth.loc[new_version_bymonth.datetime<(datetime.now() - pd.DateOffset(months=1)).strftime('%Y-%m-%d')]
    new_version_bymonth['last_update_time'] = np.unique(
        decarb_elect_simulate['last_update_time'])[0]
    new_version_bymonth['category'] = 'predict'
    new_version_bymonth = new_version_bymonth.loc[new_version_bymonth.year.astype(
        int) == base_year, :].reset_index(drop=True)
    elect_target_month = actual_version_bymonth.append(
        new_version_bymonth[actual_version_bymonth.columns])
    elect_target_month['version'] = max_version
    # elect_target_month['amount'] = elect_target_month['amount']/1000
    ## ------14.generate & upload elect_target_year data------##
    plant_predict_energy_sub_Y22['year'] = [x[0:4]
                                            for x in plant_predict_energy_sub_Y22.datetime]
    plant_predict_energy_sub_Y22['Site'] = [str(np.where(real_id.find(
        '-') != -1, str(real_id[0:(real_id.find('-'))]), real_id)) for real_id in plant_predict_energy_sub_Y22.plant]
    plant_predict_energy_sub_Y22['Site'] = np.where(plant_predict_energy_sub_Y22.Site == 'WIHK',
                                                    plant_predict_energy_sub_Y22.plant, plant_predict_energy_sub_Y22.Site)
    plant_predict_energy_sub_Y22_byyear = plant_predict_energy_sub_Y22.groupby(['year', 'Site'], group_keys=True).agg(
        {'factory_electricity': 'sum'}).reset_index().rename(columns={'Site': 'site', 'factory_electricity': 'amount'})
    plant_predict_energy_sub_Y22_byyear['category'] = max_version
    plant_predict_energy_sub_Y22_byyear['last_update_time'] = np.unique(
        decarb_elect_simulate['last_update_time'])[0]
    # plant_predict_energy_sub_Y22_byyear['amount'] = plant_predict_energy_sub_Y22_byyear['amount']/1000
    elect_target_year = decarb_elect_simulate.copy().drop(
        columns={'version_year'}).rename(columns={'version': 'category'})
    elect_target_year = elect_target_year.append(
        plant_predict_energy_sub_Y22_byyear[elect_target_year.columns]).reset_index(drop=True)
    elect_target_year = elect_target_year.append(decarb_elect_simulate.copy().drop(
        columns={'version_year'}).loc[decarb_elect_simulate.version == 'base', :].rename(columns={'version': 'category'})).reset_index(drop=True)
    elect_target_year.loc[(elect_target_year.year.astype(int) >= base_year) & (
        elect_target_year.category != 'base'), ['category']] = 'predict'
    elect_target_year.loc[(elect_target_year.year.astype(int) < base_year) & (
        elect_target_year.category != 'base'), ['category']] = 'actual'
    elect_target_year = elect_target_year.append(
        actual_version_byyear[elect_target_year.columns]).reset_index(drop=True)
    elect_target_year['version'] = max_version
    # elect_target_year['version_year'] = max_version_year
    # data_uploader_delete(elect_target_month, 'app',
    #                      'elect_target_month', "year in ('"+str(base_year)+"')")
    data_uploader_append(elect_target_month.loc[elect_target_month.site.isin(
        decarb_site)], 'app', 'elect_target_month')
    data_uploader_append(elect_target_year.loc[elect_target_year.site.isin(
        decarb_site)], 'app', 'elect_target_year')
    print('Upload target electricity data is successful!')
    print('Upload data is end!')
    # else:
    #     print("It's not neccessary to update annual electricity in "+(datetime.now()).strftime('%Y-%m-%d')+'.')
    print('factory_elct_main_fn end')
    return {"version": str(max_version), "version_year": int(max_version_year)}
