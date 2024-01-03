from datetime import datetime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from app.celery import app
from models import engine


@app.task(name='renewable-energy-ratio-sim-update')
def re_purpose_optimize_main_fn():
    ############### Generate Inputs #####################
    # connect to DB to download data
    connect_string = engine.get_connect_string()
    conn = create_engine(connect_string, echo=True)
    # initial parameters
    now_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    now_year = int(str(now_datetime)[0:4])
    chinese_contract_rate = 0.5
    # load plant mappint data
    plant_mapping_query = "SELECT * from staging.plant_mapping where year='" + \
        str(now_year)+"' and area!='None';"
    plant_mapping = data_loader(conn, plant_mapping_query)
    plant_mapping.loc[plant_mapping.site == 'WIHK',
                      'site'] = plant_mapping.loc[plant_mapping.site == 'WIHK', 'plant_name']
    plant_mapping = plant_mapping[['site', 'area']
                                  ].drop_duplicates().reset_index(drop=True)
    # load data
    ai_elect_data, renewable_data = target_ai_amount_loader(
        conn, plant_mapping)
    green_contracts_data, green_price_data = green_amount_price_loader(conn)
    rec_price_data = rec_price_loader(conn)
    carbon_coef_data = carbon_coef_loader(conn, now_year)
    solor_amount_data = onsite_solar_loader(conn, plant_mapping)
    # optimize green amount
    green_addition_contracts_data = green_amout_optimizer(
        chinese_contract_rate, ai_elect_data, solor_amount_data, green_price_data, green_contracts_data, renewable_data)
    # optimize rec amount
    green_area_maxamount = rec_amount_optimizer(green_addition_contracts_data, green_price_data, rec_price_data,
                                                ai_elect_data, solor_amount_data, green_contracts_data, renewable_data, carbon_coef_data)
    # generate renewable amount result
    renew_optim_rate, renew_optim_amount, renew_add_cost = renewable_result_generator(
        now_datetime, green_area_maxamount)
    # renew_optim_rate upload data
    data_uploader_delete(conn, renew_optim_rate, 'app', 'renew_optim_rate', "")
    print('delete renew_optim_rate table success')
    data_uploader_append(conn, renew_optim_rate, 'app', 'renew_optim_rate')
    print('upload renew_optim_rate table success')
    # renew_optim_amount upload data
    data_uploader_delete(conn, renew_optim_amount,
                         'app', 'renew_optim_amount', "")
    print('delete renew_optim_amount table success')
    data_uploader_append(conn, renew_optim_amount, 'app', 'renew_optim_amount')
    print('upload renew_optim_amount table success')
    # renew_add_cost upload data
    data_uploader_delete(conn, renew_add_cost, 'app', 'renew_add_cost', "")
    print('delete renew_optim_amount table success')
    data_uploader_append(conn, renew_add_cost, 'app', 'renew_add_cost')
    print('upload renew_optim_amount table success')
    return 'update renewable optim data is success'

# -load data function


def data_loader(conn, query):
    # select one table
    sql = query
    data_result = pd.read_sql(sql, con=conn)
    return data_result

# -load target & ai amount data function


def target_ai_amount_loader(conn, plant_mapping):
    decarb_max_datetime_query = "SELECT max(last_update_time) as last_update_time FROM app.decarb_elect_simulate;"
    decarb_max_datetime = data_loader(conn, decarb_max_datetime_query)
    decarb_max_datetime = str(decarb_max_datetime['last_update_time'][0])
    ai_elect_data_query = "SELECT * FROM app.decarb_elect_simulate where last_update_time='" + \
        decarb_max_datetime+"';"
    ai_elect_data = data_loader(conn, ai_elect_data_query)
    ai_elect_data = ai_elect_data.drop(columns={'id', 'last_update_time'})
    ai_elect_data = pd.merge(ai_elect_data, plant_mapping, on=[
                             'site'], how='left').reset_index(drop=True)
    renewable_data_query = "SELECT * FROM staging.renewable_setting_simulate where category in ('solar','PPA','REC');"
    renewable_data = data_loader(conn, renewable_data_query)
    renewable_data_total = renewable_data.groupby(
        ['year']).agg({'amount': 'sum'}).reset_index()
    renewable_data_total['category'] = 'target'
    renewable_data_total['last_update_time'] = max(
        renewable_data.last_update_time)
    renewable_data = renewable_data.drop(columns={'id'}).append(
        renewable_data_total).reset_index(drop=True)
    return ai_elect_data, renewable_data

# -load green amount & price data function


def green_amount_price_loader(conn):
    # contract data
    green_contracts_data_query = "SELECT * FROM app.green_elec_pre_contracts;"
    green_contracts_data = data_loader(conn, green_contracts_data_query)
    green_contracts_data = green_contracts_data.groupby(
        ['area', 'year']).agg({'contract_ytm_amount': 'sum'}).reset_index()
    green_contracts_data['year'] = green_contracts_data['year'].astype(int)
    # price data
    green_price_data_query = "SELECT * FROM app.green_elect_simulate;"
    green_price_data = data_loader(conn, green_price_data_query)
    # green_price_data = green_price_data.groupby(['area','year']).agg({'contract_ytm_amount':'sum'}).reset_index()
    green_price_data = green_price_data.drop(
        columns={'id', 'last_update_time'})
    return green_contracts_data, green_price_data

# -load rec price data function


def rec_price_loader(conn):
    rec_price_data_query = "SELECT * FROM app.green_energy_simulate;"
    rec_price_data = data_loader(conn, rec_price_data_query)
    rec_price_data = rec_price_data.drop(columns={'id', 'last_update_time'})
    return rec_price_data

# -load carbon coef data function


def carbon_coef_loader(conn, now_year):
    carbon_coef_data_query = "SELECT * FROM staging.decarb_carbon_coef;"
    carbon_coef_data = data_loader(conn, carbon_coef_data_query)
    carbon_coef_data = carbon_coef_data.drop(
        columns={'id', 'last_update_time'})
    carbon_coef_data = carbon_coef_data.loc[carbon_coef_data.year >= now_year, :].reset_index(
        drop=True)
    carbon_coef_data['site'] = np.where(carbon_coef_data.site == 'WIHK1', 'WIHK-1',
                                        np.where(carbon_coef_data.site == 'WIHK2', 'WIHK-2',
                                                 np.where(carbon_coef_data.site.isin(['WMYP1', 'WMYP2']), 'WMY', carbon_coef_data.site)))
    carbon_coef_data = carbon_coef_data.groupby(
        ['year', 'site']).agg({'amount': 'mean'}).reset_index()
    return carbon_coef_data

# -load on-site solar data function


def onsite_solar_loader(conn, plant_mapping):
    solor_amount_data_query = "SELECT * FROM raw.solar_target;"
    solor_amount_data = data_loader(conn, solor_amount_data_query)
    solor_amount_data = pd.merge(solor_amount_data, plant_mapping, on=[
                                 'site'], how='left').reset_index(drop=True)
    solor_amount_data['year'] = [
        int(x[0:4]) for x in solor_amount_data.period_start.astype(str)]
    solor_amount_data = solor_amount_data.groupby(
        ['site', 'area', 'year']).agg({'amount': 'sum'}).reset_index()
    return solor_amount_data

# -optimize green amount


def green_amout_optimizer(chinese_contract_rate, ai_elect_data, solor_amount_data, green_price_data, green_contracts_data, renewable_data):
    # site max amount
    green_site_maxamount = pd.merge(ai_elect_data, solor_amount_data.rename(columns={'amount': 'solar_amount'}).drop(
        columns={'area'}), on=['site', 'year'], how='left').reset_index(drop=True)
    green_site_maxamount = pd.merge(green_site_maxamount, green_price_data.rename(columns={'amount': 'price'}).drop(
        columns={'area', 'predict_roc'}), on=['site', 'year'], how='left').reset_index(drop=True)
    green_site_maxamount.loc[green_site_maxamount.solar_amount.isna(
    ), 'solar_amount'] = 0
    green_site_maxamount.loc[green_site_maxamount.price.isna(), 'price'] = max(
        green_site_maxamount.price)+1
    green_site_maxamount.loc[green_site_maxamount.green_full_ratio.isna(
    ), 'green_full_ratio'] = 0
    green_site_maxamount['site_maxamount'] = (
        green_site_maxamount.amount-green_site_maxamount.solar_amount)*green_site_maxamount.green_full_ratio/100
    # area max amount
    green_area_maxamount = green_site_maxamount.groupby(['year', 'area']).agg({'site_maxamount': 'sum', 'amount': 'sum', 'price': 'max'}).reset_index(
    ).rename(columns={'site_maxamount': 'area_maxamount', 'amount': 'ai_amount'})
    green_area_maxamount = pd.merge(green_area_maxamount, green_contracts_data, on=[
                                    'year', 'area'], how='left').reset_index(drop=True)
    green_area_maxamount.loc[green_area_maxamount.contract_ytm_amount.isna(
    ), 'contract_ytm_amount'] = 0
    green_area_maxamount['area_remain_maxamount'] = [np.where(
        x < 0, 0, x) for x in green_area_maxamount.area_maxamount-green_area_maxamount.contract_ytm_amount]
    # green area contract/ai/remain ytm total
    green_area_contract_total = green_area_maxamount.groupby(['year']).agg({'contract_ytm_amount': 'sum', 'ai_amount': 'sum'}).reset_index(
    ).rename(columns={'contract_ytm_amount': 'contract_ytm_total', 'ai_amount': 'ai_ytm_total'})
    green_area_contract_total = pd.merge(green_area_contract_total, renewable_data.loc[renewable_data.category == 'PPA'],
                                         on=['year'], how='left').reset_index(drop=True)
    green_area_contract_total['green_remain_ytm_total'] = [np.where(
        x < 0, 0, x) for x in green_area_contract_total.ai_ytm_total*green_area_contract_total.amount/100-green_area_contract_total.contract_ytm_total]
    # join area & total
    green_area_maxamount = pd.merge(green_area_maxamount, green_area_contract_total, on=[
                                    'year'], how='left').reset_index(drop=True)
    green_area_maxamount['price_rank'] = green_area_maxamount.groupby(['year'])[
        'price'].rank('first')
    # area green amount optimizer
    green_area_maxamount['green_area_addition_amount_'+'1'] = np.where((green_area_maxamount.price_rank == 1) & (green_area_maxamount.green_remain_ytm_total <= 0), 0,
                                                                       np.where((green_area_maxamount.price_rank == 1) & (green_area_maxamount.green_remain_ytm_total*chinese_contract_rate-green_area_maxamount.area_remain_maxamount < 0), green_area_maxamount.green_remain_ytm_total*chinese_contract_rate,
                                                                                np.where((green_area_maxamount.price_rank == 1) & (green_area_maxamount.green_remain_ytm_total*chinese_contract_rate-green_area_maxamount.area_remain_maxamount >= 0), green_area_maxamount.area_remain_maxamount, 0
                                                                                         )))
    green_area_maxamount_total = green_area_maxamount.groupby(['year']).agg({'green_area_addition_amount_'+'1': 'sum'}).reset_index(
    ).rename(columns={'green_area_addition_amount_'+'1': 'green_area_addition_amount_total_'+'1'})
    green_area_maxamount = pd.merge(green_area_maxamount, green_area_maxamount_total, on=[
                                    'year'], how='left').reset_index(drop=True)
    green_area_maxamount['green_area_addition_amount_total_final'] = green_area_maxamount['green_area_addition_amount_'+'1']
    for x in range(2, int(max(green_area_maxamount.price_rank))+1, 1):
        print(x)
        str_x = str(x)
        str_x_1 = str(x-1)
        green_area_maxamount['green_area_addition_amount_'+str_x] = np.where((green_area_maxamount.price_rank == x) & (green_area_maxamount.green_remain_ytm_total-green_area_maxamount['green_area_addition_amount_total_'+str_x_1] <= 0), 0,
                                                                             np.where((green_area_maxamount.price_rank == x) & (green_area_maxamount.green_remain_ytm_total-green_area_maxamount['green_area_addition_amount_total_'+str_x_1]-green_area_maxamount.area_remain_maxamount < 0), green_area_maxamount.green_remain_ytm_total-green_area_maxamount['green_area_addition_amount_total_'+str_x_1],
                                                                                      np.where((green_area_maxamount.price_rank == x) & (green_area_maxamount.green_remain_ytm_total-green_area_maxamount['green_area_addition_amount_total_'+str_x_1]-green_area_maxamount.area_remain_maxamount >= 0), green_area_maxamount.area_remain_maxamount, 0
                                                                                               )))
        green_area_maxamount_total = green_area_maxamount.groupby(['year']).agg({'green_area_addition_amount_total_'+str_x_1: 'max', 'green_area_addition_amount_'+str_x: 'sum'}).reset_index(
        ).rename(columns={'green_area_addition_amount_'+str_x: 'green_area_addition_amount_total_'+str_x})
        green_area_maxamount_total['green_area_addition_amount_total_'+str_x] = green_area_maxamount_total['green_area_addition_amount_total_' +
                                                                                                           str_x_1]+green_area_maxamount_total['green_area_addition_amount_total_'+str_x]
        green_area_maxamount = pd.merge(green_area_maxamount, green_area_maxamount_total.drop(columns={
                                        'green_area_addition_amount_total_'+str_x_1}), on=['year'], how='left').reset_index(drop=True)
        green_area_maxamount['green_area_addition_amount_total_final'] = green_area_maxamount['green_area_addition_amount_total_final'] + \
            green_area_maxamount['green_area_addition_amount_'+str_x]
    # add contract & addition green amount
    green_area_maxamount['green_contract_addition_ytm_amount'] = green_area_maxamount.contract_ytm_amount + \
        green_area_maxamount.green_area_addition_amount_total_final
    green_addition_contracts_data = green_area_maxamount.groupby(['area', 'year']).agg(
        {'green_contract_addition_ytm_amount': 'sum'}).reset_index()
    return green_addition_contracts_data

# -optimize rec amount


def rec_amount_optimizer(green_addition_contracts_data, green_price_data, rec_price_data, ai_elect_data, solor_amount_data, green_contracts_data, renewable_data, carbon_coef_data):
    # combine PPA & EAC price
    green_price_data['category'] = 'PPA'
    rec_price_data['category'] = 'REC'
    re_price_data = green_price_data.append(
        rec_price_data).reset_index(drop=True)
    # site max amount
    green_site_maxamount = pd.merge(ai_elect_data, solor_amount_data.rename(columns={'amount': 'solar_amount'}).drop(
        columns={'area'}), on=['site', 'year'], how='left').reset_index(drop=True)
    green_site_maxamount = pd.merge(green_site_maxamount, re_price_data.rename(columns={'amount': 'price'}).drop(
        columns={'area', 'predict_roc'}), on=['site', 'year'], how='left').reset_index(drop=True)
    green_site_maxamount = pd.merge(green_site_maxamount, carbon_coef_data.rename(
        columns={'amount': 'carbon_coeff'}), on=['site', 'year'], how='left').reset_index(drop=True)
    green_site_maxamount.loc[(green_site_maxamount.carbon_coeff.isna()) & (
        green_site_maxamount.site == 'WVN'), ['carbon_coeff']] = 0.9239
    green_site_maxamount.loc[green_site_maxamount.solar_amount.isna(
    ), 'solar_amount'] = 0
    green_site_maxamount.loc[green_site_maxamount.price.isna(), 'price'] = max(
        green_site_maxamount.price)+1
    green_site_maxamount.loc[green_site_maxamount.green_full_ratio.isna(
    ), 'green_full_ratio'] = 0
    green_site_maxamount['site_maxamount'] = (
        green_site_maxamount.amount-green_site_maxamount.solar_amount)*green_site_maxamount.green_full_ratio/100
    # area max amount
    green_area_maxamount = green_site_maxamount.groupby(['year', 'area', 'category']).agg(
        {'solar_amount': 'sum', 'site_maxamount': 'sum', 'amount': 'sum', 'price': 'max', 'carbon_coeff': 'mean'}).reset_index().rename(columns={'site_maxamount': 'area_maxamount', 'amount': 'ai_amount'})
    green_area_maxamount = pd.merge(green_area_maxamount, green_addition_contracts_data, on=[
                                    'year', 'area'], how='left').reset_index(drop=True)
    green_area_maxamount.loc[green_area_maxamount.green_contract_addition_ytm_amount.isna(
    ), 'green_contract_addition_ytm_amount'] = 0
    green_area_maxamount_ppa = green_area_maxamount.loc[green_area_maxamount.category == 'PPA', :].reset_index(
        drop=True)
    green_area_maxamount_rec = green_area_maxamount.loc[green_area_maxamount.category == 'REC', :].reset_index(
        drop=True)
    green_area_maxamount_ppa['area_remain_maxamount'] = [np.where(
        x < 0, 0, x) for x in green_area_maxamount_ppa.area_maxamount-green_area_maxamount_ppa.green_contract_addition_ytm_amount]
    green_area_maxamount_rec['area_remain_maxamount'] = [np.where(
        x < 0, 0, x) for x in green_area_maxamount_rec.ai_amount-green_area_maxamount_rec.solar_amount-green_area_maxamount_rec.green_contract_addition_ytm_amount]
    green_area_maxamount = green_area_maxamount_ppa.append(
        green_area_maxamount_rec).reset_index(drop=True)
    # green area contract/ai/remain ytm total
    green_area_contract_total = green_area_maxamount[['year', 'area', 'solar_amount', 'ai_amount', 'green_contract_addition_ytm_amount']].drop_duplicates().groupby(['year']).agg(
        {'green_contract_addition_ytm_amount': 'sum', 'ai_amount': 'sum', 'solar_amount': 'sum'}).reset_index().rename(columns={'green_contract_addition_ytm_amount': 'green_contract_addition_ytm_total', 'ai_amount': 'ai_ytm_total', 'solar_amount': 'solar_ytm_total'})
    green_area_contract_total = pd.merge(green_area_contract_total, renewable_data.loc[renewable_data.category == 'target'],
                                         on=['year'], how='left').reset_index(drop=True)
    green_area_contract_total['rec_remain_ytm_total'] = [np.where(x < 0, 0, x) for x in green_area_contract_total.ai_ytm_total *
                                                         green_area_contract_total.amount/100-green_area_contract_total.solar_ytm_total-green_area_contract_total.green_contract_addition_ytm_total]
    # join area & total
    green_area_maxamount = pd.merge(green_area_maxamount, green_area_contract_total.drop(
        columns={'category'}), on=['year'], how='left').reset_index(drop=True)
    green_area_maxamount['price_carbon_stand'] = green_area_maxamount.price - \
        green_area_maxamount.carbon_coeff
    green_area_maxamount['price_rank'] = green_area_maxamount.groupby(
        ['year'])['price_carbon_stand'].rank('first')
    # area rec amount optimizer
    green_area_maxamount['rec_area_addition_amount_'+'1'] = np.where((green_area_maxamount.price_rank == 1) & (green_area_maxamount.rec_remain_ytm_total < 1000), 0,
                                                                     np.where((green_area_maxamount.price_rank == 1) & (green_area_maxamount.rec_remain_ytm_total-np.floor(green_area_maxamount.area_remain_maxamount/1000)*1000 < 1000), green_area_maxamount.rec_remain_ytm_total,
                                                                              np.where((green_area_maxamount.price_rank == 1) & (green_area_maxamount.rec_remain_ytm_total-np.floor(green_area_maxamount.area_remain_maxamount/1000)*1000 >= 1000), np.floor(green_area_maxamount.area_remain_maxamount/1000)*1000, 0
                                                                                       )))
    green_area_maxamount_total = green_area_maxamount.groupby(['year']).agg(
        {'rec_area_addition_amount_'+'1': 'sum'}).reset_index().rename(columns={'rec_area_addition_amount_'+'1': 'rec_area_addition_amount_total_'+'1'})
    green_area_maxamount = pd.merge(green_area_maxamount, green_area_maxamount_total, on=[
                                    'year'], how='left').reset_index(drop=True)
    green_area_maxamount['rec_area_addition_amount_total_final'] = green_area_maxamount['rec_area_addition_amount_'+'1']
    green_area_maxamount_opposite = green_area_maxamount[['year', 'area', 'category', 'rec_area_addition_amount_total_final']].rename(
        columns={'rec_area_addition_amount_total_final': 'rec_area_addition_amount_total_final_opposite'})
    green_area_maxamount_opposite['category'] = np.where(
        green_area_maxamount_opposite.category == 'PPA', 'REC', 'PPA')
    green_area_maxamount = pd.merge(green_area_maxamount, green_area_maxamount_opposite, on=[
                                    'year', 'area', 'category'], how='left').reset_index(drop=True)
    green_area_maxamount.loc[green_area_maxamount.rec_area_addition_amount_total_final_opposite.isna(
    ), ['rec_area_addition_amount_total_final_opposite']] = 0

    for x in range(2, int(max(green_area_maxamount.price_rank))+1, 1):
        print(x)
        str_x = str(x)
        str_x_1 = str(x-1)
        green_area_maxamount['rec_area_addition_amount_'+str_x] = np.where((green_area_maxamount.price_rank == x) & ((green_area_maxamount.rec_remain_ytm_total-green_area_maxamount['rec_area_addition_amount_total_'+str_x_1] < 1000) | (green_area_maxamount.area_remain_maxamount-green_area_maxamount.rec_area_addition_amount_total_final_opposite <= 0)), 0,
                                                                           np.where((green_area_maxamount.price_rank == x) & (green_area_maxamount.rec_remain_ytm_total-green_area_maxamount['rec_area_addition_amount_total_'+str_x_1]-np.floor(green_area_maxamount.area_remain_maxamount/1000)*1000 < 1000), np.floor((green_area_maxamount.rec_remain_ytm_total-green_area_maxamount['rec_area_addition_amount_total_'+str_x_1])/1000)*1000,
                                                                                    np.where((green_area_maxamount.price_rank == x) & (green_area_maxamount.rec_remain_ytm_total-green_area_maxamount['rec_area_addition_amount_total_'+str_x_1]-np.floor(green_area_maxamount.area_remain_maxamount/1000)*1000 >= 1000), np.floor(green_area_maxamount.area_remain_maxamount/1000)*1000, 0
                                                                                             )))
        green_area_maxamount_total = green_area_maxamount.groupby(['year']).agg({'rec_area_addition_amount_total_'+str_x_1: 'max', 'rec_area_addition_amount_'+str_x: 'sum'}).reset_index(
        ).rename(columns={'rec_area_addition_amount_'+str_x: 'rec_area_addition_amount_total_'+str_x})
        green_area_maxamount_total['rec_area_addition_amount_total_'+str_x] = green_area_maxamount_total['rec_area_addition_amount_total_' +
                                                                                                         str_x_1]+green_area_maxamount_total['rec_area_addition_amount_total_'+str_x]
        green_area_maxamount = pd.merge(green_area_maxamount, green_area_maxamount_total.drop(
            columns={'rec_area_addition_amount_total_'+str_x_1}), on=['year'], how='left').reset_index(drop=True)
        green_area_maxamount['rec_area_addition_amount_total_final'] = green_area_maxamount['rec_area_addition_amount_total_final'] + \
            green_area_maxamount['rec_area_addition_amount_'+str_x]
        green_area_maxamount_opposite = green_area_maxamount[['year', 'area', 'category', 'rec_area_addition_amount_total_final']].rename(
            columns={'rec_area_addition_amount_total_final': 'rec_area_addition_amount_total_final_opposite'})
        green_area_maxamount_opposite['category'] = np.where(
            green_area_maxamount_opposite.category == 'PPA', 'REC', 'PPA')
        green_area_maxamount = pd.merge(green_area_maxamount.drop(columns={'rec_area_addition_amount_total_final_opposite'}), green_area_maxamount_opposite, on=[
                                        'year', 'area', 'category'], how='left').reset_index(drop=True)
        green_area_maxamount.loc[green_area_maxamount.rec_area_addition_amount_total_final_opposite.isna(
        ), ['rec_area_addition_amount_total_final_opposite']] = 0

    # add contract & addition green amount
    green_area_maxamount['rec_green_contract_addition_ytm_amount'] = np.where(green_area_maxamount.category == 'PPA',
                                                                              green_area_maxamount.rec_area_addition_amount_total_final +
                                                                              green_area_maxamount.green_contract_addition_ytm_amount,
                                                                              green_area_maxamount.rec_area_addition_amount_total_final)
    return green_area_maxamount

# -generate renewable rate/amount/cost


def renewable_result_generator(now_datetime, green_area_maxamount):
    # renew_optim_rate update
    renew_optim_amount_solar = green_area_maxamount.loc[:, ['area', 'year', 'solar_amount', 'ai_amount']].drop_duplicates(
    ).reset_index(drop=True).rename(columns={'solar_amount': 'amount'})
    # green_area_optim_rate_solar['amount'] = round(green_area_optim_rate_solar.solar_amount/green_area_optim_rate_solar.ai_amount*100,0)
    renew_optim_amount_solar['category'] = 'solar'
    renew_optim_amount_solar['price'] = 0
    renew_optim_amount_solar['cost'] = renew_optim_amount_solar.amount * \
        renew_optim_amount_solar.price

    renew_optim_amount_ppa_rec = green_area_maxamount.loc[:, ['area', 'year', 'category', 'price', 'rec_green_contract_addition_ytm_amount', 'ai_amount']].drop_duplicates(
    ).reset_index(drop=True).rename(columns={'rec_green_contract_addition_ytm_amount': 'amount'})
    renew_optim_amount_ppa_rec['category'] = np.where(
        renew_optim_amount_ppa_rec.category == 'PPA', 'green_elect', 'green_energy')
    renew_optim_amount_ppa_rec['cost'] = renew_optim_amount_ppa_rec.amount * \
        renew_optim_amount_ppa_rec.price/1000
    renew_optim_amount = renew_optim_amount_solar.append(
        renew_optim_amount_ppa_rec).reset_index(drop=True)
    # compute world wide amount
    renew_optim_amount_ww = renew_optim_amount.groupby(
        ['year', 'category']).agg({'amount': 'sum', 'cost': 'sum'}).reset_index()
    renew_optim_ai_amount_ww = renew_optim_amount[['year', 'area', 'ai_amount']].drop_duplicates().groupby(
        ['year']).agg({'ai_amount': 'sum'}).reset_index()  # .rename(columns={'amount':'ai_amount'})
    renew_optim_amount_ww = pd.merge(renew_optim_amount_ww, renew_optim_ai_amount_ww, on=[
                                     'year'], how='left').reset_index(drop=True)
    renew_optim_amount_ww['area'] = '全集團'
    renew_optim_amount = renew_optim_amount.append(
        renew_optim_amount_ww).reset_index(drop=True)
    renew_optim_rate = renew_optim_amount.copy()
    renew_optim_rate['amount'] = [
        np.round(x, 1) for x in renew_optim_rate.amount/renew_optim_rate.ai_amount*100]
    renew_optim_rate = renew_optim_rate[['area', 'category', 'amount', 'year']]
    renew_add_cost = renew_optim_amount[[
        'area', 'category', 'cost', 'year']].rename(columns={'cost': 'amount'})
    renew_optim_amount = renew_optim_amount[[
        'area', 'category', 'amount', 'year']]
    renew_optim_rate['last_update_time'] = now_datetime
    renew_add_cost['last_update_time'] = now_datetime
    renew_optim_amount['last_update_time'] = now_datetime
    return renew_optim_rate, renew_optim_amount, renew_add_cost

# -upload renewable rate/amount/cost table


def data_uploader_append(conn, data, db_name, table_name):
    # Connect to DB to upload data
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    data.to_sql(table_name, conn, index=False, if_exists='append',
                schema=db_name, chunksize=10000)
    return 0

# -delete renewable rate/amount/cost table


def data_uploader_delete(conn, data, db_name, table_name, condition):
    # Delete table
    # conn = create_engine(f'postgresql+psycopg2://{user0}:{password0}@{host0}:{port0}/{database0}')
    conn.execute(f'DELETE from '+db_name+'.'+table_name+';')
    return 0
