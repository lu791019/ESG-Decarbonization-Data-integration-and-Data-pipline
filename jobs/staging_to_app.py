import calendar
from datetime import date
from datetime import datetime as dt
from datetime import timedelta

import numpy as np
import pandas as pd
from dateutil.relativedelta import relativedelta
from sqlalchemy import *

from jobs.raw_to_staging import cal_site
from models import engine
from services.mail_service import MailService


def db_operate(table_name, sqlString, pandaFormat):
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    conn = db.connect()
    conn.execute(sqlString)
    pandaFormat.to_sql(table_name, conn, index=False,
                       if_exists='append', schema='app', chunksize=10000)
    conn.close()

    return True


def useful_datetime(i):

    period_start = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%Y-%m-%d")

    period_year = (date(dt.now().year, dt.now().month, 1) -
                   relativedelta(months=i)).strftime("%Y")

    period_month = (date(dt.now().year, dt.now().month, 1) -
                    relativedelta(months=i)).strftime("%m")

    period_month = str(int(period_month))

    return period_start, period_year, period_month


def data_import_app(table_name, period_start, period_year, period_month, db, stage):
    # 綠購對帳
    if table_name == 'green_elec_transfer_account':

        try:

            if dt.now().month == 1:

                year = dt.now().year-1

            else:

                year = dt.now().year

            # 名詞轉換
            category_dict = {'夏日週六離峰': '離峰', '夏日平日離峰': '離峰', '夏日假日離峰': '離峰', '夏日平日尖峰': '經常尖峰', '非夏日平日離峰': '離峰', '非夏日週六離峰': '離峰', '非夏日假日離峰': '離峰', '非夏日平日尖峰': '經常尖峰',
                             '夏日平日半尖峰': '半尖峰', '夏日周六半尖峰': '週六半尖峰', '非夏日平日半尖峰': '半尖峰', '非夏日周六半尖峰': '週六半尖峰', '夏日週六半尖峰': '週六半尖峰', '非夏日週六半尖峰': '週六半尖峰', '尖峰': '經常尖峰', '周六半尖峰': '週六半尖峰'}

            green_elect_vol = pd.read_sql(
                f"""SELECT site, plant, meter_code, provider_name, category1, category2, amount, period_start FROM staging.green_elect_vol WHERE  period_start ='{period_start}'""", db)
            green_elect_price = pd.read_sql(
                f"""SELECT site, plant, meter_code, provider_name, category1, category2, amount, period_start FROM staging.green_elect_price WHERE  period_start ='{period_start}'""", db)
            meter_mapping = pd.read_sql(
                f"""SELECT code as "meter_code", elec_price_type as "elect_type" FROM app.decarb_ww_site_elec_meter""", db)
            bill_meter = pd.read_sql(
                f"""SELECT category as "category2", price, elect_type, is_summer, base_id FROM app.elec_bill_meter""", db)

            # bill_base = pd.read_sql(f"""SELECT id as "base_id", country, area, guideline_date FROM app.elec_bill_base """,db)
            bill_base = pd.read_sql(
                f"""SELECT id as "base_id", area, guideline_date FROM app.elec_bill_base """, db)

            bill_summer = pd.read_sql(
                f"""SELECT elec_type as "elect_type", start_date, end_date, base_id FROM app.elec_bill_summer""", db)
            # mapping = pd.read_sql(f"""SELECT country, area FROM app.elec_country_mapping""" ,db)
            green_contract = pd.read_sql(
                f"""SELECT provider_name, contract_price FROM app.green_elec_pre_contracts where year = '{year}' AND '光電' = ALL(green_elec_type)""", db)

            """
            轉供度占比 green rate

            依照範例 : (綠電轉供度) / (綠電轉供度 + 灰電用電量)
            """
            green_rate = green_elect_vol.merge(green_elect_price, on=[
                                               'site', 'plant', 'meter_code', 'provider_name', 'category2', 'period_start'], how='left')
            if green_rate.size != 0:
                green_rate['amount'] = green_rate['amount_x'] / \
                    (green_rate['amount_y'])
                green_rate['category1'] = 'green_rate'
            else:
                green_rate['amount'] = ' '
                green_rate['category1'] = ' '
            green_rate = green_rate[['site', 'plant', 'meter_code',
                                     'provider_name', 'category1', 'category2', 'period_start', 'amount']]

            # 電費規則 1. 關聯最接近現在日期的地區和國家 2.電表 3.夏日規則
            # base_summer = bill_summer.merge(bill_base, on='base_id', how='left')
            # con_list = mapping["country"].unique()
            # area_list = mapping["area"].unique()
            # base_summer['guideline_date'] = pd.to_datetime(base_summer['guideline_date'])
            bill_base['guideline_date'] = pd.to_datetime(
                bill_base['guideline_date'])

            bill_base = bill_base[bill_base['guideline_date'] <= period_start]
            now = dt.now()

            # 選取不同country和area，同時選取guideline_date最接近現在的資料
            # bill_base = bill_base.groupby(['country', 'area']).apply(lambda x: x.loc[x['guideline_date'].idxmax() if x['guideline_date'].max() < now else x['guideline_date'].idxmin()]).reset_index(drop = True)
            bill_base = bill_base.groupby(['area']).apply(lambda x: x.loc[x['guideline_date'].idxmax(
            ) if x['guideline_date'].max() < now else x['guideline_date'].idxmin()]).reset_index(drop=True)

            base_summer = bill_summer.merge(
                bill_base, on='base_id', how='inner')

            # base_summer = base_summer[(base_summer['country'].isin(con_list) ) & (base_summer['area'].isin(area_list))]
            # base_summer = base_summer[base_summer['guideline_date'] == str(base_summer['guideline_date'].max())]
            # base_summer = base_summer[base_summer['base_id'] == base_summer['base_id'].max()]

            meter_info = base_summer.merge(
                bill_meter, on=['base_id', 'elect_type'], how='left')

            meter_info = meter_info.replace({'category2': category_dict})

            green_elect_price = green_elect_price.merge(
                meter_mapping, on='meter_code', how='left')
            grey_price = green_elect_price.merge(
                meter_info, on=['elect_type', 'category2'], how='left')

            grey_price_summer = grey_price[(grey_price['period_start'] >= grey_price['start_date']) & (
                grey_price['period_start'] <= grey_price['end_date'])]
            grey_price_summer = grey_price_summer[grey_price_summer['is_summer'] == True]

            grey_price_non_summer = grey_price[(grey_price['period_start'] < grey_price['start_date']) | (
                grey_price['period_start'] > grey_price['end_date'])]
            grey_price_non_summer = grey_price_non_summer[grey_price_non_summer['is_summer'] == False]

            grey_elct_total = grey_price_summer.append(grey_price_non_summer)

            grey_elct_total = grey_elct_total.drop_duplicates().reset_index(drop=True)

            """
            電費計價
            category1: 'grey_elect_price
            """
            grey_elect_price = grey_elct_total[[
                'site', 'plant', 'meter_code', 'provider_name', 'category2', 'period_start', 'price']]
            grey_elect_price['category1'] = 'grey_elect_price'
            grey_elect_price.rename(columns={'price': 'amount'}, inplace=True)

            """
            灰電用電量
            """
            grey_elect = green_elect_price[[
                'site', 'plant', 'meter_code', 'provider_name', 'category1', 'category2', 'amount', 'period_start']]

            """
            灰電用電量 - 總用電量
            category1 : 'grey_elect'
            category2': 'elect_total'
            """
            elect_total = green_elect_price[[
                'site', 'plant', 'meter_code', 'provider_name',  'amount', 'period_start']]

            elect_total = elect_total.groupby(
                ['site', 'plant', 'meter_code', 'provider_name', 'period_start']).sum().reset_index()

            elect_total['category1'] = 'grey_elect'
            elect_total['category2'] = 'elect_total'

            """
            灰電用電量 - 總電費
            category1 : 'grey_elect'
            category2': 'elect_bill'
            """
            elect_bill = grey_elct_total[['site', 'plant', 'meter_code', 'provider_name',
                                          'category1', 'category2', 'amount', 'period_start', 'price']]
            elect_bill['amount'] = elect_bill['amount'] * elect_bill['price']
            elect_bill = elect_bill[[
                'site', 'plant', 'meter_code', 'provider_name', 'period_start', 'amount']]
            elect_bill = elect_bill.groupby(
                ['site', 'plant', 'meter_code', 'provider_name', 'period_start']).sum().reset_index()
            elect_bill['category1'] = 'grey_elect'
            elect_bill['category2'] = 'elect_bill'

            """
            綠電轉供度
            """
            green_elect_vol = green_elect_vol[[
                'site', 'plant', 'meter_code', 'provider_name', 'category1',  'category2', 'amount', 'period_start']]

            """
            綠電轉供度 - 總用電量
            category1 : 'green_elect_vol'
            category2': 'elect_total'
            """
            green_elect_vol_fix1 = green_elect_vol[(green_elect_vol['category2']=='總綠電度數') & (green_elect_vol['plant'].isin(['ALL']))&(green_elect_vol['site'].isin(['WLT','WTN','WIHK-2']))]
            green_elect_vol_fix2 = green_elect_vol[(green_elect_vol['category2']=='總綠電度數') & (green_elect_vol['plant'].isin(['WLT','WTN','WIHK-2']))]
            green_elect_vol_fix3 = green_elect_vol[green_elect_vol['category2']!='總綠電度數']
            green_elect_vol_fix = green_elect_vol_fix1.append(green_elect_vol_fix2).append(green_elect_vol_fix3).reset_index(drop=True)
            green_elect_total = green_elect_vol_fix[[
                'site', 'plant', 'meter_code', 'provider_name',  'amount', 'period_start']]
            green_elect_total = green_elect_total.groupby(
                ['site', 'plant', 'meter_code', 'provider_name', 'period_start']).sum().reset_index()
            green_elect_total['category1'] = 'green_elect_vol'
            green_elect_total['category2'] = 'elect_total'

            """
            綠電轉供度 - 總電費
            category1 : 'green_elect_vol'
            category2': 'elect_bill'
            """
            green_elect_bill = green_elect_vol.merge(
                green_contract, on='provider_name', how='left')
            green_elect_bill['amount'] = green_elect_bill['amount'] * \
                green_elect_bill['contract_price']
            green_elect_bill = green_elect_bill[[
                'site', 'plant', 'meter_code', 'provider_name', 'period_start', 'amount']]
            green_elect_bill = green_elect_bill.groupby(
                ['site', 'plant', 'meter_code', 'provider_name', 'period_start']).sum().reset_index()
            green_elect_bill['category1'] = 'green_elect_vol'
            green_elect_bill['category2'] = 'elect_bill'

            """
            總用電轉供度占比 green rate

            依照範例 : (綠電轉供總用電) / (綠電轉供總用電 + 灰電總用電)
            """

            total_green_rate = green_elect_total.merge(elect_total, on=[
                                                       'site', 'plant', 'meter_code', 'provider_name', 'category2', 'period_start'], how='left')
            if total_green_rate.size != 0:

                # total_green_rate['amount'] = total_green_rate['amount_x'] / \
                #     (total_green_rate['amount_x']+total_green_rate['amount_y'])

                total_green_rate['amount'] = total_green_rate['amount_x'] / \
                    (total_green_rate['amount_y'])

                total_green_rate['category1'] = 'green_rate'
            else:
                total_green_rate['amount'] = ' '
                total_green_rate['category1'] = ' '

            total_green_rate = total_green_rate[[
                'site', 'plant', 'meter_code', 'provider_name', 'category1', 'category2', 'amount', 'period_start']]

            # 所有資料
            category1_dict = {'計費': 'grey_elect', '轉供': 'green_elect_vol'}
            category2_dict = {
                '離峰': 'off_peak', '週六半尖峰': 'sat_half_rush_peak', '經常尖峰': 'peak', '半尖峰': 'half_peak'}

            # cnry_area_mapping = pd.read_sql(f"""SELECT country,area, code as "meter_code" FROM app.decarb_ww_site_elec_meter""" ,db)
            cnry_area_mapping = pd.read_sql(
                f"""SELECT area, code as "meter_code" FROM app.decarb_ww_site_elec_meter""", db)

            green_elec_account = green_elect_vol.append(green_elect_bill).append(green_elect_total).append(grey_elect_price).append(
                elect_total).append(elect_bill).append(grey_elect).append(green_rate).append(total_green_rate)

            green_elec_account = green_elec_account.replace(
                {'category1': category1_dict})

            green_elec_account = green_elec_account.replace(
                {'category2': category2_dict})

            green_elec_account['year'] = pd.to_datetime(
                green_elec_account['period_start']).dt.year

            green_elec_account['month'] = pd.to_datetime(
                green_elec_account['period_start']).dt.month

            green_elec_account = green_elec_account[[
                'site', 'plant', 'meter_code', 'provider_name', 'category1', 'category2', 'amount', 'year', 'month']]

            # 將meter code 關聯國家和地區
            green_elec_account = green_elec_account.merge(
                cnry_area_mapping, on='meter_code', how='left')

            # 計算算打包電保
            meter_group = pd.read_sql(
                f"""SELECT  code as "meter_code", group_id FROM app.decarb_ww_meter_group""", db)
            meter_group_mapping = pd.read_sql(
                f"""SELECT group_id, group_name FROM app.decarb_ww_meter_group_mapping""", db)
            meter_group.dropna(inplace=True)

            meter_group['group_id'] = meter_group['group_id'].astype(int)
            meter_group_mapping['group_id'] = meter_group_mapping['group_id'].astype(
                int)

            df_meter_group = meter_group.merge(
                meter_group_mapping, on=['group_id'], how='left')

            green_elec_account_group = green_elec_account.merge(
                df_meter_group, on=['meter_code'], how='inner')
            green_elec_account_group = green_elec_account_group[green_elec_account_group['category1'].isin(
                ['green_elect_vol', 'grey_elect'])]

            # green_elec_account_group = green_elec_account_group[['site', 'plant', 'provider_name', 'category1', 'category2', 'amount', 'year', 'month', 'country', 'area',  'group_name']]
            # green_elec_account_group1 = green_elec_account_group.groupby(['site', 'plant', 'provider_name','category1', 'category2', 'year', 'month', 'country', 'area',  'group_name']).sum().reset_index()

            green_elec_account_group = green_elec_account_group[[
                'site', 'plant', 'provider_name', 'category1', 'category2', 'amount', 'year', 'month', 'area',  'group_name']]
            green_elec_account_group1 = green_elec_account_group.groupby(
                ['site', 'plant', 'provider_name', 'category1', 'category2', 'year', 'month', 'area',  'group_name']).sum().reset_index()

            green_elec_account_group1.rename(
                columns={'group_name': 'meter_code'}, inplace=True)

            green_elec_account_group2 = green_elec_account_group1[
                green_elec_account_group1['category2'] != 'elect_bill']

            group_grey = green_elec_account_group2[green_elec_account_group2['category1'] == 'grey_elect']
            group_green = green_elec_account_group2[green_elec_account_group2['category1']
                                                    == 'green_elect_vol']

            # green_rate_group = group_green.merge(group_grey, on=['site', 'plant', 'provider_name', 'category2', 'year',  'month', 'country', 'area', 'meter_code'], how='left')
            green_rate_group = group_green.merge(group_grey, on=[
                                                 'site', 'plant', 'provider_name', 'category2', 'year',  'month', 'area', 'meter_code'], how='left')

            if green_rate_group.size != 0:

                # green_rate_group['amount'] = green_rate_group['amount_x'] / \
                #     (green_rate_group['amount_x']+green_rate_group['amount_y'])

                green_rate_group['amount'] = green_rate_group['amount_x'] / \
                    (green_rate_group['amount_y'])

                green_rate_group['category1'] = 'green_rate'
            else:
                green_rate_group['amount'] = ' '
                green_rate_group['category1'] = ' '

            # green_rate_group = green_rate_group[['site', 'plant', 'provider_name', 'category1', 'category2', 'year', 'month', 'country', 'area', 'meter_code', 'amount']]
            green_rate_group = green_rate_group[[
                'site', 'plant', 'provider_name', 'category1', 'category2', 'year', 'month', 'area', 'meter_code', 'amount']]

            green_rate_group.dropna(inplace=True)

            green_elec_account_final = green_elec_account.append(
                green_elec_account_group1).append(green_rate_group)

            green_elec_account_final_other = green_elec_account_final[~green_elec_account_final['meter_code'].isin(['WHC_ALL','WNH_ALL'])]
            green_elec_account_final_WNHC = green_elec_account_final[green_elec_account_final['meter_code'].isin(['WHC_ALL','WNH_ALL'])]

            replace_dict = {'總綠電度數': 'elect_total'}
            green_elec_account_final_WNHC['category2'] = green_elec_account_final_WNHC['category2'].replace(replace_dict)
            green_elec_account_final_WNHC['area'] = '台灣'

            green_elec_WNHC = green_elec_account_final_WNHC[(green_elec_account_final_WNHC['category1']=='green_elect_vol') ]
            elec_WNHC = green_elec_account_final_WNHC[(green_elec_account_final_WNHC['category1']=='grey_elect')]

            green_rate_WNHC = green_elec_WNHC.merge(elec_WNHC, on=[
                                           'site', 'plant', 'meter_code', 'provider_name', 'category2','year','month','area'], how='left')
            if green_rate_WNHC.size != 0:

                # total_green_rate['amount'] = total_green_rate['amount_x'] / \
                #     (total_green_rate['amount_x']+total_green_rate['amount_y'])

                green_rate_WNHC['amount'] = green_rate_WNHC['amount_x'] / \
                    (green_rate_WNHC['amount_y'])

                green_rate_WNHC['category1'] = 'green_rate'
            else:
                green_rate_WNHC['amount'] = ' '
                green_rate_WNHC['category1'] = ' '

            green_rate_WNHC = green_rate_WNHC[[
                'site', 'plant', 'meter_code', 'provider_name', 'category1', 'category2', 'amount','year','month','area']]

            green_elec_account_final_WNHC = green_elec_account_final_WNHC[green_elec_account_final_WNHC['category1']!='green_rate']

            green_elec_account_final_WNHC = green_elec_account_final_WNHC.append(green_rate_WNHC).reset_index(drop=True)

            green_elec_account_final2 = green_elec_account_final_other.append(green_elec_account_final_WNHC).reset_index(drop=True)

            green_elec_account_final2['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            if green_elec_account_final2.size != 0:

                return db_operate(
                    table_name,
                    f"""DELETE FROM app.green_elec_transfer_account WHERE  "year" ='{period_year}' and "month" ='{period_month}' and category1 !='All'""",
                    green_elec_account_final2,
                )
            else:

                return

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] green_elec_transfer_account etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'solar_energy_overview':

        try:

            """
            因應來源端為月報表資料(廠端已扣除餘電)做條件判斷調整 @1106:
            if period_month < dt.now().month -1(如:9月資料): 來源為月報表, 加上餘電資料加上: solar_actual_use + solar_remain
            elif period_month >= dt.now().month -1(如:10月資料): 維持原本邏輯 : 來源為太陽能系統, 扣除餘電資料: solar_actual - solar_remain

            因應來源端全部調整為WZS-ESGI資料(廠端已扣除餘電) @1122:
            來源為WZS-ESGI, 資料為實際用電量 , 加上餘電資料 為 實際值: solar_actual_use + solar_remain = solar_actual
            """

            # if int(period_month) < dt.now().month - 1:

            solar_actual_use = pd.read_sql(
                f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar where category = 'actual' and period_start ='{period_start}'""", db)

            solar_actual_use['category'] = 'actual_use'

            solar_target = pd.read_sql(
                f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar where category = 'target' and period_start ='{period_start}'""", db)

            solar_remain = pd.read_sql(
                f"""SELECT site, plant,  amount, ytm_amount, period_start FROM staging.solar_remain where period_start ='{period_start}'""", db)
            solar_remain['category'] = 'remain'
            solar_remain = solar_remain.fillna(0)

            solar_actual = solar_actual_use.merge(
                solar_remain, on=['site', 'plant', 'period_start'], how='left')
            solar_actual = solar_actual.fillna(0)

            solar_actual['amount'] = solar_actual['amount_x'] + \
                solar_actual['amount_y']
            solar_actual['ytm_amount'] = solar_actual['ytm_amount_x'] + \
                solar_actual['ytm_amount_y']

            solar_actual = solar_actual[[
                'site', 'plant',  'period_start',  'amount',  'ytm_amount']]
            solar_actual['category'] = 'actual'

            solar_info = pd.read_sql(
                f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar_info where period_start ='{period_start}'""", db)

            solar_other = pd.read_sql(
                f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar_other where period_start ='{period_start}'""", db)

            solar_overview = solar_actual.append(solar_target).append(solar_remain).append(
                solar_actual_use).append(solar_info).append(solar_other)

            plant_list = solar_overview['plant'].unique().tolist()

            # 有分攤WZS,WKS的光伏實際/預估占比 先取消
            # 計算光伏實際/預估 占比
            # YTM光伏預估占比 = 光伏預估值YTM / 總用電量預估值YTM
            # YTM光伏實際占比 = 光伏實際值YTM / 總用電量實際值YTM

            # solar_elect : 光伏實際值和光伏預估值,包含單一月份(amount) 和YTM(ytm_amount)
            # solar_elect = solar_overview[solar_overview['category'].isin(['actual','target'])]

            # # elect_total :總用電量實際值和總用電量預估值,包含單一月份(amount) 和YTM(ytm_amount)
            # elect_total = pd.read_sql(f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}'""",db)

            # # 關聯實際值 (category='actual') 和 預估值 (category='target')
            # solar_ratio = solar_elect.merge(elect_total, on=['site', 'plant', 'period_start','category'], how='left')
            # solar_ratio = solar_ratio.fillna(0)

            # solar_ratio['amount'] = (solar_ratio['amount_x'] / solar_ratio['amount_y'])*100
            # solar_ratio['ytm_amount'] = (solar_ratio['ytm_amount_x'] / solar_ratio['ytm_amount_y'])*100

            # #change value name
            # elect_dict = {'actual':'elect_total_actual','target':'elect_total_target'}
            # solar_ratio_dict = {'actual':'solar_ratio_actual','target':'solar_ratio_target'}

            # elect_total = elect_total.replace({'category': elect_dict})
            # solar_ratio = solar_ratio.replace({'category': solar_ratio_dict})

            # solar_ratio = solar_ratio[['site', 'plant', 'category', 'period_start',  'amount',  'ytm_amount']]

            # #solar_overview & elect_total & solar_ratio
            # solar_overview = solar_overview.append(elect_total).append(solar_ratio)

            # 計算光伏實際/預估 占比
            # YTM光伏預估占比 = 光伏預估值YTM / 總用電量預估值YTM
            # YTM光伏實際占比 = 光伏實際值YTM / 總用電量實際值YTM

            # solar_elect : 光伏實際值和光伏預估值,包含單一月份(amount) 和YTM(ytm_amount)
            solar_elect = solar_overview[solar_overview['category'].isin(
                ['actual', 'target'])]

            solar_elect_WZKS = solar_elect[solar_elect['site'].isin([
                                                                    'WKS', 'WZS'])]

            solar_elect = solar_elect[~solar_elect['site'].isin(
                ['WKS', 'WZS'])]

            # change value name
            elect_dict = {'actual': 'elect_total_actual',
                            'target': 'elect_total_target'}
            solar_ratio_dict = {
                'actual': 'solar_ratio_actual', 'target': 'solar_ratio_target'}

            elect_total_WZKS = pd.read_sql(
                f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}' and site in ('WZS','WKS')""", db)

            elect_total = pd.read_sql(
                f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}' and site not in ('WZS','WKS', 'WIHK')""", db)

            # 關聯實際值 (category='actual') 和 預估值 (category='target')

            solar_ratio = solar_elect.merge(
                elect_total, on=['site', 'plant', 'period_start', 'category'], how='left')
            solar_ratio = solar_ratio.fillna(0)

            if solar_ratio.size != 0:
                solar_ratio['amount'] = (
                    solar_ratio['amount_x'] / solar_ratio['amount_y'])*100
                solar_ratio['ytm_amount'] = (
                    solar_ratio['ytm_amount_x'] / solar_ratio['ytm_amount_y'])*100

                elect_total = elect_total.replace({'category': elect_dict})
                solar_ratio = solar_ratio.replace(
                    {'category': solar_ratio_dict})

                solar_ratio = solar_ratio[[
                    'site', 'plant', 'category', 'period_start',  'amount',  'ytm_amount']]

            solar_WZKS = solar_elect_WZKS[[
                'site', 'category', 'amount', 'ytm_amount', 'period_start']]

            elect_WZKS = elect_total_WZKS[[
                'site', 'category', 'amount', 'ytm_amount', 'period_start']]

            solar_WZKS_site = solar_WZKS.groupby(
                ['site', 'category', 'period_start']).sum().reset_index()

            elect_WZKS_site = elect_WZKS.groupby(
                ['site', 'category', 'period_start']).sum().reset_index()

            solar_ratio_WZKS = solar_WZKS_site.merge(
                elect_WZKS_site, on=['site', 'period_start', 'category'], how='left')

            solar_ratio_WZKS = solar_ratio_WZKS.fillna(0)

            # 關聯實際值 (category='actual') 和 預估值 (category='target')
            if solar_ratio_WZKS.size != 0:
                solar_ratio_WZKS['amount'] = (
                    solar_ratio_WZKS['amount_x'] / solar_ratio_WZKS['amount_y'])*100
                solar_ratio_WZKS['ytm_amount'] = (
                    solar_ratio_WZKS['ytm_amount_x'] / solar_ratio_WZKS['ytm_amount_y'])*100
                solar_ratio_WZKS = solar_ratio_WZKS[[
                    'site', 'category', 'period_start',  'amount',  'ytm_amount']]
                solar_ratio_WZKS['plant'] = 'ALL'

            elect_WZKS_site = elect_WZKS_site.replace(
                {'category': elect_dict})
            solar_ratio_WZKS = solar_ratio_WZKS.replace(
                {'category': solar_ratio_dict})

            elect_WZKS_site['plant'] = 'ALL'

            # solar_overview & elect_total & solar_ratio
            solar_overview = solar_overview.append(elect_total).append(solar_ratio).append(
                elect_WZKS_site).append(solar_ratio_WZKS).reset_index(drop=True)

            # filter_plant = ['KOE', 'WCD', 'WCQ', 'WCZ', 'WGKS', 'WHC', 'WIH',
            #                 'WKH', 'WMI', 'WMX', 'WMY', 'WTN', 'WTZ', 'WGTX', 'WIHK']


            solar_overview = solar_overview[solar_overview['plant'].isin(plant_list)]

            # filter_site = ['WMI']

            # solar_overview = solar_overview[~solar_overview['site'].isin(filter_site)]

            solar_overview['last_update_time'] = dt.strptime(
                dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

            if solar_overview.size != 0:

                return db_operate(
                    table_name,
                    f"""DELETE FROM app.solar_energy_overview WHERE period_start ='{period_start}' """,
                    solar_overview,
                )

            else:

                return

            # elif int(period_month) >= dt.now().month - 1:

            #     solar_actual = pd.read_sql(
            #         f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar where category = 'actual' and period_start ='{period_start}'""", db)

            #     solar_target = pd.read_sql(
            #         f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar where category = 'target' and period_start ='{period_start}'""", db)

            #     solar_remain = pd.read_sql(
            #         f"""SELECT site, plant,  amount, ytm_amount, period_start FROM staging.solar_remain where period_start ='{period_start}'""", db)
            #     solar_remain['category'] = 'remain'
            #     solar_remain = solar_remain.fillna(0)

            #     solar_actual_use = solar_actual.merge(
            #         solar_remain, on=['site', 'plant', 'period_start'], how='left')
            #     solar_actual_use = solar_actual_use.fillna(0)

            #     solar_actual_use['amount'] = solar_actual_use['amount_x'] - \
            #         solar_actual_use['amount_y']
            #     solar_actual_use['ytm_amount'] = solar_actual_use['ytm_amount_x'] - \
            #         solar_actual_use['ytm_amount_y']

            #     solar_actual_use = solar_actual_use[[
            #         'site', 'plant',  'period_start',  'amount',  'ytm_amount']]
            #     solar_actual_use['category'] = 'actual_use'

            #     solar_info = pd.read_sql(
            #         f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar_info where period_start ='{period_start}'""", db)

            #     solar_other = pd.read_sql(
            #         f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.solar_other where period_start ='{period_start}'""", db)

            #     solar_overview = solar_actual.append(solar_target).append(solar_remain).append(
            #         solar_actual_use).append(solar_info).append(solar_other)

                # 有分攤WZS,WKS的光伏實際/預估占比 先取消
                # 計算光伏實際/預估 占比
                # YTM光伏預估占比 = 光伏預估值YTM / 總用電量預估值YTM
                # YTM光伏實際占比 = 光伏實際值YTM / 總用電量實際值YTM

                # solar_elect : 光伏實際值和光伏預估值,包含單一月份(amount) 和YTM(ytm_amount)
                # solar_elect = solar_overview[solar_overview['category'].isin(['actual','target'])]

                # # elect_total :總用電量實際值和總用電量預估值,包含單一月份(amount) 和YTM(ytm_amount)
                # elect_total = pd.read_sql(f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}'""",db)

                # # 關聯實際值 (category='actual') 和 預估值 (category='target')
                # solar_ratio = solar_elect.merge(elect_total, on=['site', 'plant', 'period_start','category'], how='left')
                # solar_ratio = solar_ratio.fillna(0)

                # solar_ratio['amount'] = (solar_ratio['amount_x'] / solar_ratio['amount_y'])*100
                # solar_ratio['ytm_amount'] = (solar_ratio['ytm_amount_x'] / solar_ratio['ytm_amount_y'])*100

                # #change value name
                # elect_dict = {'actual':'elect_total_actual','target':'elect_total_target'}
                # solar_ratio_dict = {'actual':'solar_ratio_actual','target':'solar_ratio_target'}

                # elect_total = elect_total.replace({'category': elect_dict})
                # solar_ratio = solar_ratio.replace({'category': solar_ratio_dict})

                # solar_ratio = solar_ratio[['site', 'plant', 'category', 'period_start',  'amount',  'ytm_amount']]

                # #solar_overview & elect_total & solar_ratio
                # solar_overview = solar_overview.append(elect_total).append(solar_ratio)

                # 計算光伏實際/預估 占比
                # YTM光伏預估占比 = 光伏預估值YTM / 總用電量預估值YTM
                # YTM光伏實際占比 = 光伏實際值YTM / 總用電量實際值YTM

                # solar_elect : 光伏實際值和光伏預估值,包含單一月份(amount) 和YTM(ytm_amount)
                # solar_elect = solar_overview[solar_overview['category'].isin(
                #     ['actual', 'target'])]

                # solar_elect_WZKS = solar_elect[solar_elect['site'].isin([
                #                                                         'WKS', 'WZS'])]

                # solar_elect = solar_elect[~solar_elect['site'].isin(
                #     ['WKS', 'WZS'])]

                # # change value name
                # elect_dict = {'actual': 'elect_total_actual',
                #               'target': 'elect_total_target'}
                # solar_ratio_dict = {
                #     'actual': 'solar_ratio_actual', 'target': 'solar_ratio_target'}

                # elect_total_WZKS = pd.read_sql(
                #     f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}' and site in ('WZS','WKS')""", db)

                # elect_total = pd.read_sql(
                #     f"""SELECT site, plant, category, amount, ytm_amount, period_start FROM staging.elect_total where period_start ='{period_start}' and site not in ('WZS','WKS', 'WIHK')""", db)

                # # 關聯實際值 (category='actual') 和 預估值 (category='target')

                # solar_ratio = solar_elect.merge(
                #     elect_total, on=['site', 'plant', 'period_start', 'category'], how='left')
                # solar_ratio = solar_ratio.fillna(0)

                # if solar_ratio.size != 0:
                #     solar_ratio['amount'] = (
                #         solar_ratio['amount_x'] / solar_ratio['amount_y'])*100
                #     solar_ratio['ytm_amount'] = (
                #         solar_ratio['ytm_amount_x'] / solar_ratio['ytm_amount_y'])*100

                #     elect_total = elect_total.replace({'category': elect_dict})
                #     solar_ratio = solar_ratio.replace(
                #         {'category': solar_ratio_dict})

                #     solar_ratio = solar_ratio[[
                #         'site', 'plant', 'category', 'period_start',  'amount',  'ytm_amount']]

                # solar_WZKS = solar_elect_WZKS[[
                #     'site', 'category', 'amount', 'ytm_amount', 'period_start']]

                # elect_WZKS = elect_total_WZKS[[
                #     'site', 'category', 'amount', 'ytm_amount', 'period_start']]

                # solar_WZKS_site = solar_WZKS.groupby(
                #     ['site', 'category', 'period_start']).sum().reset_index()

                # elect_WZKS_site = elect_WZKS.groupby(
                #     ['site', 'category', 'period_start']).sum().reset_index()

                # solar_ratio_WZKS = solar_WZKS_site.merge(
                #     elect_WZKS_site, on=['site', 'period_start', 'category'], how='left')

                # solar_ratio_WZKS = solar_ratio_WZKS.fillna(0)

                # # 關聯實際值 (category='actual') 和 預估值 (category='target')
                # if solar_ratio_WZKS.size != 0:
                #     solar_ratio_WZKS['amount'] = (
                #         solar_ratio_WZKS['amount_x'] / solar_ratio_WZKS['amount_y'])*100
                #     solar_ratio_WZKS['ytm_amount'] = (
                #         solar_ratio_WZKS['ytm_amount_x'] / solar_ratio_WZKS['ytm_amount_y'])*100
                #     solar_ratio_WZKS = solar_ratio_WZKS[[
                #         'site', 'category', 'period_start',  'amount',  'ytm_amount']]
                #     solar_ratio_WZKS['plant'] = 'ALL'

                # elect_WZKS_site = elect_WZKS_site.replace(
                #     {'category': elect_dict})
                # solar_ratio_WZKS = solar_ratio_WZKS.replace(
                #     {'category': solar_ratio_dict})

                # elect_WZKS_site['plant'] = 'ALL'

                # # solar_overview & elect_total & solar_ratio
                # solar_overview = solar_overview.append(elect_total).append(solar_ratio).append(
                #     elect_WZKS_site).append(solar_ratio_WZKS).reset_index(drop=True)

                # filter_plant = ['KOE', 'WCD', 'WCQ', 'WCZ', 'WGKS', 'WHC', 'WIH',
                #                 'WKH', 'WMI', 'WMX', 'WMY', 'WTN', 'WTZ', 'WGTX', 'WIHK']

                # filter_site = ['WMI']

                # solar_overview = solar_overview[~solar_overview['plant'].isin(
                #     filter_plant)]

                # solar_overview = solar_overview[~solar_overview['site'].isin(
                #     filter_site)]

                # solar_overview['last_update_time'] = dt.strptime(
                #     dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                # if solar_overview.size != 0:

                #     return db_operate(
                #         table_name,
                #         f"""DELETE FROM app.solar_energy_overview WHERE period_start ='{period_start}' """,
                #         solar_overview,
                #     )

                # else:

                #     return

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] solar_energy_overview etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error

    if table_name == 'green_elect_overview':

        try:
            if int(period_year) <=2022:
                pass

            else:

                # 單月目標 target / YTM目標 target_ytm
                provider_target = pd.read_sql(
                    f"""SELECT site, amount as target, ytm_amount as target_ytm, period_start FROM staging.provider_plant_list where period_start ='{period_start}' """, db)

                # 總電量單月目標 target_all / 總電量YTM目標 target_ytm_all

                elect_total_site = pd.read_sql(
                    f"""SELECT site, amount as target_all, ytm_amount as target_ytm_all, period_start FROM staging.elect_total where category = 'target' and period_start ='{period_start}' and site not in ('WZS','WKS','WIHK')""", db)
                elect_total_WZKS = pd.read_sql(
                    f"""SELECT site, plant, amount as target_all , ytm_amount as target_ytm_all, period_start FROM staging.elect_total where category = 'target' and period_start ='{period_start}' and site in ('WZS','WKS','WIHK')""", db)
                elect_WZKS = elect_total_WZKS[[
                    'site',  'target_all', 'target_ytm_all', 'period_start']]
                elect_WZKS_site = elect_WZKS.groupby(
                    ['site',  'period_start']).sum().reset_index()

                elect_total_target = elect_total_site.append(elect_WZKS_site)

                # 總單月用電量 actual_all / 總用電量YTM actual_ytm_all
                """由於WIHK site過去為 WIHK-1, WIHK-2 現以合併"""
                elect_actual_other = pd.read_sql(
                    f"""SELECT  site, amount as actual_all, ytm_amount as actual_ytm_all, period_start FROM staging.electricity_decarb where  period_start ='{period_start}' and bo = 'ALL' and site !='ALL' """, db)


                elect_actual = elect_actual_other

                """由於WIHK site過去為 WIHK-1, WIHK-2 現以合併"""
                # 單月實際綠電 actual / 實際綠電YTM actual_ytm
                green_vol_site_other = pd.read_sql(
                    f"""SELECT site, amount as actual, ytm_amount as actual_ytm, period_start FROM staging.renewable_energy_decarb where  period_start ='{period_start}'and bo = 'ALL'  and site !='ALL' and category = 'green_electricity' """, db)
                # green_vol_site_WIHK = pd.read_sql(
                #     f"""SELECT site, amount as actual, ytm_amount as actual_ytm, period_start FROM staging.renewable_energy_decarb where  period_start ='{period_start}' and bo = 'ALL' and site in ('WIHK')  and category = 'green_electricity'""", db)
                # green_vol = pd.read_sql(f"""SELECT site, amount as actual, ytm_amount as actual_ytm, period_start FROM staging.green_elect_vol where  period_start ='{period_start}'and  site !='ALL' and plant ='ALL' """,db)
                # green_vol_site = green_vol.groupby(['site',  'period_start']).sum().reset_index()
                green_vol_site = green_vol_site_other

                df1 = pd.merge(pd.merge(pd.merge(provider_target, elect_total_target, on=['site', 'period_start'], how='outer'),  elect_actual, on=[
                            'site', 'period_start'], how='outer'), green_vol_site, on=['site', 'period_start'], how='outer')

                df1_site = df1[['site', 'period_start', 'target',
                                'target_ytm', 'actual', 'actual_ytm']]

                df1_all = df1[['period_start', 'target_all',
                            'target_ytm_all', 'actual_all', 'actual_ytm_all']]
                df1_all = df1_all.fillna(0)
                df1_all = df1_all.groupby(['period_start']).sum().reset_index()

                df1 = pd.merge(df1_site, df1_all, on=['period_start'], how='left')
                df1 = df1.fillna(0)

                # 綠電價差費用 price_diff
                # 綠電價差費用YTM price_diff_ytm

                # 單位費用減碳量 unit
                # 單位費用減碳量YTM unit_ytm

                # 灰電用電量 - 總用電量
                # category1 : 'grey_elect'
                # category2': 'elect_total'

                # 灰電用電量 - 總電費
                # category1 : 'grey_elect'
                # category2': 'elect_bill'
                # if dt.now().month == 1:

                #     year = dt.now().year-1
                #     month_start = 1

                # else:

                #     year = dt.now().year
                #     month_start = 1

                # green_price_diff = pd.read_sql(
                #     f"""SELECT "year", "month", site,category2 as category,  amount FROM app.green_elec_transfer_account where "year" = '{period_year}' and "month" >= '{month_start}' and "month" <= '{period_month}' and category1 in ('grey_elect') and category2 in ('elect_total','elect_bill') and site !='ALL' and plant !='ALL' and meter_code not in ('WNH_ALL','WHC_ALL') """, db)

                # green_price_diff['year'] = green_price_diff['year'].astype(str)
                # green_price_diff['month'] = green_price_diff['month'].astype(str)

                # green_price_diff['period_start'] = green_price_diff['year'] + \
                #     '-' + green_price_diff['month'] + '-01'
                # green_price_diff['period_start'] = pd.to_datetime(
                #     green_price_diff['year'] + '-' + green_price_diff['month'] + '-01')
                # green_price_diff['period_start'] = green_price_diff['period_start'].astype(
                #     str)
                # green_price_diff = green_price_diff[[
                #     'year', 'site', 'period_start', 'category', 'amount']]

                # green_price_diff = green_price_diff.groupby(
                #     ['year', 'site', 'period_start', 'category']).sum().reset_index()
                # green_price_diff = green_price_diff.sort_values(
                #     by=['year', 'category', 'site'])

                # if green_price_diff.size != 0:

                #     green_price_diff['amount'] = green_price_diff['amount'].astype(
                #         float)

                #     green_price_diff['ytm_amount'] = green_price_diff.groupby(
                #         ['year', 'site', 'category'])['amount'].cumsum()
                #     green_price_diff = green_price_diff.drop(
                #         'year', axis=1).reset_index(drop=True)
                #     elect_bill = green_price_diff[green_price_diff['category']
                #                                 == 'elect_bill']
                #     elect_total = green_price_diff[green_price_diff['category']
                #                                 == 'elect_total']
                #     green_diff = pd.merge(elect_bill, elect_total, on=[
                #                         'site', 'period_start'], how='left')

                #     # Sark 提供公式 :灰電電費/灰電總用電 當作是 台灣的灰電平均 再用綠電5.5-台灣灰電平均 就是綠電價差
                #     green_diff['price_diff'] = 5.5 - \
                #         (green_diff['amount_x']/green_diff['amount_y'])
                #     green_diff['price_diff_ytm'] = 5.5 - \
                #         (green_diff['ytm_amount_x']/green_diff['ytm_amount_y'])
                #     green_diff = green_diff[[
                #         'site', 'period_start', 'price_diff', 'price_diff_ytm']]

                # unit_df = pd.read_sql(
                #     f"""SELECT "year", "month", site,category2 as category,  amount FROM app.green_elec_transfer_account where "year" = '{year}' and "month" >= '{month_start}' and "month" <= '{period_month}' and category1 in ('green_elect_vol') and category2 in ('elect_total','elect_bill') and site !='ALL' and plant !='ALL' and meter_code not in ('WNH_ALL','WHC_ALL')""", db)
                # unit_df['year'] = unit_df['year'].astype(str)
                # unit_df['month'] = unit_df['month'].astype(str)

                # unit_df['period_start'] = unit_df['year'] + \
                #     '-' + unit_df['month'] + '-01'
                # unit_df['period_start'] = pd.to_datetime(
                #     unit_df['year'] + '-' + unit_df['month'] + '-01')
                # unit_df['period_start'] = unit_df['period_start'].astype(str)
                # unit_df = unit_df[['year', 'site',
                #                 'period_start', 'category', 'amount']]

                # unit_df = unit_df.groupby(
                #     ['year', 'site', 'period_start', 'category']).sum().reset_index()

                # unit_df = unit_df.sort_values(by=['year', 'category', 'site'])

                # if unit_df.size != 0:

                #     unit_df['ytm_amount'] = unit_df.groupby(
                #         ['year', 'site', 'category'])['amount'].cumsum()
                #     unit_df = unit_df.drop('year', axis=1).reset_index(drop=True)

                #     green_bill = unit_df[unit_df['category'] == 'elect_bill']
                #     green_total = unit_df[unit_df['category'] == 'elect_total']

                #     unit = pd.merge(green_total, green_bill, on=[
                #                     'site', 'period_start'], how='left')
                #     coef = pd.read_sql(
                #         f"""SELECT site, amount as coef FROM staging.cfg_carbon_coef where "year" = {year} """, db)
                #     unit_coef = pd.merge(unit, coef, on=['site'], how='left')

                #     unit_coef['unit'] = (unit_coef['amount_x']
                #                         * unit_coef['coef']) / unit_coef['amount_y']
                #     unit_coef['unit_ytm'] = (
                #         unit_coef['ytm_amount_x']*unit_coef['coef']) / unit_coef['ytm_amount_y']

                #     unit_coef = unit_coef[[
                #         'site', 'period_start', 'unit', 'unit_ytm']]

                # if (green_price_diff.size != 0) & (unit_df.size != 0):

                #     df2 = pd.merge(green_diff, unit_coef, on=[
                #                 'site', 'period_start'], how='outer')

                #     df2 = df2[df2['period_start'] == period_start]
                # else:
                #     # 创建空的 DataFrame
                #     df2 = pd.DataFrame(columns=[
                #                     'site', 'period_start', 'price_diff', 'price_diff_ytm', 'unit', 'unit_ytm'])

                if dt.now().month == 1:

                    year = dt.now().year-1
                    month_start = 1
                    month_end = 12

                else:

                    year = dt.now().year
                    month_start = 1
                    month_end = 12

                year_target = pd.read_sql(
                    f"""SELECT  site, provider, amount as year_target FROM app.provider_plant_list where "year" = {year} and "month" >= {month_start} and "month" <= {month_end}""", db)

                year_target = year_target.groupby(
                    ['site', 'provider']).sum().reset_index()

                year_target['period_start'] = period_start

                year_target_all = pd.read_sql(
                    f"""SELECT site, amount as year_target_all FROM app.decarb_elect_simulate where "year" = {year} and "version" = (SELECT MAX("version") FROM app.decarb_elect_simulate where version_year ={year} and "year" = {year} and validate is true) """, db)

                year_target_all['period_start'] = period_start

                year_target_all = year_target_all.groupby(
                    ['period_start']).sum().reset_index()

                df3 = pd.merge(year_target, year_target_all,
                            on=['period_start'], how='left')

                if df1.shape[0] > 0:
                    df1['site'] = df1['site'].astype(str)
                    df1['period_start'] = df1['period_start'].astype(str)

                if df3.shape[0] > 0:
                    df3['site'] = df3['site'].astype(str)
                    df3['period_start'] = df3['period_start'].astype(str)

                green_elect = pd.merge(df1, df3, on=['site', 'period_start'], how='outer')

                green_elect = green_elect[green_elect['actual_ytm'] !=0]

                site_replace = {'WIHK1':'WIHK','WIHK2':'WIHK','WIHK-1':'WIHK','WIHK-2':'WIHK','WMIP1':'WMI','WMIP2':'WMI','WMYP1':'WMY','WMIP2':'WMI'}

                """'price_diff', 'price_diff_ytm' 計算 同步 app.green_elect_simulate
                    coef 取raw.carbon_coef
                    兩者 merge by site , 故要 rename site
                """

                green_price = pd.read_sql(f"""SELECT site,  amount as price_diff,amount as price_diff_ytm  FROM app.green_elect_simulate  where "year" = {year} """, db)

                green_price['site'] = green_price['site'].replace(site_replace)
                green_price = green_price.drop_duplicates()

                coef = pd.read_sql(f"""SELECT site, amount as coef FROM raw.carbon_coef  where "year" = {year} """, db)

                coef['site'] = coef['site'].replace(site_replace)
                coef = coef.drop_duplicates()

                green_price_coef = pd.merge(green_price, coef, on=['site'], how='left')

                green_elect_overview = pd.merge(green_elect, green_price_coef, on=['site'], how='left')

                green_elect_overview = green_elect_overview.fillna(0)

                """'unit', 'unit_ytm'
                (實際總量[千度]*碳排係數)  / (實際總量[千度]*綠電價差) = 單位費用減碳量
                (YTM實際總量[千度]*碳排係數)  / (YTM實際總量[千度]*綠電價差) = 單位費用減碳量

                """
                green_elect_overview['unit'] = ((green_elect_overview['actual'] / 1000) * green_elect_overview['coef'])/ ((green_elect_overview['actual'] / 1000) * green_elect_overview['price_diff'])

                green_elect_overview['unit_ytm'] = ((green_elect_overview['actual_ytm'] / 1000) * green_elect_overview['coef'])/ ((green_elect_overview['actual_ytm'] / 1000) * green_elect_overview['price_diff_ytm'])

                area_mapping = pd.read_sql(
                    f"""SELECT distinct site, nation, area FROM staging.plant_mapping where "year" = {year} """, db)

                green_elect_overview = green_elect_overview.merge(
                    area_mapping, on=['site'], how='left')

                green_elect_overview['year'] = pd.to_datetime(
                    green_elect_overview['period_start']).dt.year

                green_elect_overview['month'] = pd.to_datetime(
                    green_elect_overview['period_start']).dt.month

                green_elect_overview = green_elect_overview[['site', 'year', 'month', 'target', 'target_ytm', 'target_all', 'target_ytm_all', 'actual_all',
                                                            'actual_ytm_all', 'actual', 'actual_ytm', 'year_target', 'year_target_all', 'price_diff', 'price_diff_ytm', 'unit', 'unit_ytm', 'area', 'provider']]
                green_elect_overview = green_elect_overview.dropna(subset=['area'], how='any')

                green_elect_overview = green_elect_overview[green_elect_overview['actual_ytm'] !=0]

                green_elect_overview['last_update_time'] = dt.strptime(
                    dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")

                if green_elect_overview.size != 0:

                    return db_operate(
                        table_name,
                        f"""DELETE FROM app.green_elect_overview WHERE  "year" ='{period_year}' and "month" ='{period_month}'""",
                        green_elect_overview,
                    )

                else:

                    return

        except Exception as e:
            error = str(e)
            mail = MailService(
                '[failed][{}] green elect overview etl info cron job report'.format(stage))
            mail.send_text('failed: {}'.format(error))
            return error


def staging_to_app(table_name, stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    table_name = str(table_name)

    current_month = dt.now().month
    current_day = dt.now().day

    # if stage == 'development':  # DEV - 10號更新上個月
    #     checkpoint = 10
    # else:  # PRD - 15號更新上個月
    #     checkpoint = 12

    # try:

    # if current_day < checkpoint:

    for i in range(1, 13):

        period_start, period_year, period_month = useful_datetime(i)
        data_import_app(table_name, period_start,
                        period_year, period_month, db, stage)

    #     return True

    # except:

    #     return False
