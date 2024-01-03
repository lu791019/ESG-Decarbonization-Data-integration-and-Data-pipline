import pandas as pd
import numpy as np
from datetime import datetime as dt, date
from sqlalchemy import *
import calendar
import urllib.parse

from datetime import datetime
from models import engine


def macc_input_to_summary_scope2_func():
    
    # 讀設定檔接DB
    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)


    green_cer_electricity_cost = pd.read_sql(f"""SELECT * FROM app.green_energy_simulate""", con=db)
    green_electricity_cost = pd.read_sql(f"""SELECT * FROM app.green_elect_simulate""", con=db)
    raw_solar_first = pd.read_sql(f"""SELECT * FROM raw.solar""", con=db) #太陽能預估量
    energy_demand_first = pd.read_sql(f"""SELECT * FROM app.decarb_elect_simulate""", con=db) #app.elect_target_year
    decarb_wacc = pd.read_sql(f"""SELECT * FROM app.decarb_wacc""", con=db)

    #------抓系統
    versions = energy_demand_first['version'].unique()
    version_numbers = [int(version[1:]) for version in versions if version.startswith('V')] # 提取版本号中的数字部分
    max_version_number = max(version_numbers)                                               # 找到最大的版本号
    max_version = 'V' + str(max_version_number)                                             # 构建具有最大版本号的版本字符串
    print (max_version)
    #---------------------------------------------   Step1
    #----總用電量預估----#

    energy_demand = energy_demand_first[energy_demand_first['version']==max_version]
    energy_demand = energy_demand.sort_values(by=['site','year'])

    #---------------------------------------------   Step2
    #----既有太陽能發電----#

    #---------用電模擬 推估出2024年 XTRKS和WKS的比例-------------#
    #----------2025後 只有XTRKS-----------#

    xtrks_2024 = energy_demand[(energy_demand['year']==2024)&(energy_demand['site']=='XTRKS')]['amount'].values[0]
    wks_2024 = energy_demand[(energy_demand['year']==2024)&(energy_demand['site']=='WKS')]['amount'].values[0]
    wks_xtrks_2024_ratio = wks_2024/(wks_2024+xtrks_2024)

    raw_solar_first['year'] = pd.to_datetime(raw_solar_first['period_start']).dt.year
    raw_solar = raw_solar_first[raw_solar_first['category']=='target']
    raw_solar = raw_solar[raw_solar['year']==2024]
    raw_solar = raw_solar.groupby(['plant','year'],as_index=False).sum()
    wks_solar_2024 = raw_solar[raw_solar['plant']=='WKS/XTRKS']['amount'].values[0] * wks_xtrks_2024_ratio
    xtrks_solar_2024 = raw_solar[raw_solar['plant']=='WKS/XTRKS']['amount'].values[0] * (1-wks_xtrks_2024_ratio)

    #----------2025後的太陽能彙整-----------#
    raw_solar_temp = raw_solar_first[raw_solar_first['year']>=2025]
    raw_solar_temp = raw_solar_temp.groupby(['plant','year'],as_index=False).sum()
    raw_solar_temp['plant'] = raw_solar_temp['plant'].replace('WKS/XTRKS', 'XTRKS')

    new_row = {'plant': 'WKS', 'year': 2024, 'amount':wks_solar_2024 }
    raw_solar = raw_solar.append(new_row, ignore_index=True)
    new_row = {'plant': 'XTRKS', 'year': 2024, 'amount':xtrks_solar_2024 }
    raw_solar = raw_solar.append(new_row, ignore_index=True)

    #---------加入2025年後的太陽能彙整--------#
    raw_solar = raw_solar.append(raw_solar_temp, ignore_index=True)

    raw_solar = raw_solar.rename(columns={'plant':'site','amount':'solar_demand'})
    energy_demand = energy_demand.merge(raw_solar, on=['site','year'],how='left')
    energy_demand['solar_demand'] = energy_demand['solar_demand'].fillna(0)
    energy_demand['solar_rate'] = energy_demand['solar_demand']/energy_demand['amount']*100 

    #---------------------------------------------   Step3   
    #----電力來源佔比分配----#   
    #拿綠電的佔比
    renewable_setting = pd.read_sql(f"""SELECT * FROM staging.renewable_setting""", con=db)
    renewable_setting = renewable_setting[renewable_setting['category']=='PPA']
    renewable_setting = renewable_setting.sort_values(by=['year'])
    renewable_setting = renewable_setting.drop(columns=['id','last_update_time'])
    renewable_setting_PPA = renewable_setting[renewable_setting['category']=='PPA']
    renewable_setting_PPA = renewable_setting_PPA.rename(columns={'amount':'PPA_rate'})

    #綠證 = 100-太陽能-綠電
    energy_demand = energy_demand.merge(renewable_setting_PPA, on=['year'],how='left')
    energy_demand = energy_demand.drop(columns=['category'])

    #綠電有幾個site沒有
    energy_demand.loc[energy_demand['site'] == 'WCD', 'PPA_rate'] = 0
    energy_demand.loc[energy_demand['site'] == 'WCQ', 'PPA_rate'] = 0
    energy_demand.loc[energy_demand['site'] == 'WCZ', 'PPA_rate'] = 0
    energy_demand.loc[energy_demand['site'] == 'WMX', 'PPA_rate'] = 0
    energy_demand.loc[energy_demand['site'] == 'WVN', 'PPA_rate'] = 0

    energy_demand.loc[energy_demand['site']== 'KOE', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WHC', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WIH', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WIHK-1', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WIHK-2', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WKH', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WLT', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WNH', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'WTN', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== 'N2', 'REC_rate'] = 0
    energy_demand.loc[energy_demand['site']== '竹北AI', 'REC_rate'] = 0

    #綠證比例 台灣沒綠證
    energy_demand['REC_rate'] = 100-energy_demand['solar_rate']-energy_demand['PPA_rate']
    energy_demand['PPA_rate'] = 100-energy_demand['solar_rate']-energy_demand['REC_rate']


    #防止XTRKS REC出現負值, 先以PPA目標為準,所以REC在2029後沒有
    energy_demand.loc[energy_demand['REC_rate']<0, 'REC_rate'] = 0
    energy_demand['PPA_rate'] = 100-energy_demand['solar_rate']-energy_demand['REC_rate']


    #---------------------------------------------   Step4
    #----電力需求量----#

    energy_demand['PPA_demand'] = energy_demand['amount']*energy_demand['PPA_rate']/100 #比例/100
    energy_demand['REC_demand'] = energy_demand['amount']*energy_demand['REC_rate']/100

    #---------------------------------------------   Step5   
    #Step 5 - 碳排減排量
    #----碳排放係數----#
    decarb_carbon_coef = pd.read_sql(f"""SELECT * FROM staging.decarb_carbon_coef""", con=db) 

    #-----暫時補上台灣廠區的碳排係數------#
    temp1 = decarb_carbon_coef[decarb_carbon_coef['site']=='WIH']
    temp1['site'] = 'N2'
    temp2 = decarb_carbon_coef[decarb_carbon_coef['site']=='WIH']
    temp2['site'] = 'WKH'
    temp3 = decarb_carbon_coef[decarb_carbon_coef['site']=='WIH']
    temp3['site'] = 'WTN'
    temp4 = decarb_carbon_coef[decarb_carbon_coef['site']=='WIH']
    temp4['site'] = '竹北AI'

    decarb_carbon_coef = decarb_carbon_coef.append(temp1)
    decarb_carbon_coef = decarb_carbon_coef.append(temp2)
    decarb_carbon_coef = decarb_carbon_coef.append(temp3)
    decarb_carbon_coef = decarb_carbon_coef.append(temp4)
    #-----------------------------------

    decarb_carbon_coef = decarb_carbon_coef.drop(columns=['id','last_update_time'])
    decarb_carbon_coef = decarb_carbon_coef.rename(columns={'amount':'decarb_carbon_coef'})
    decarb_carbon_coef['site'] = np.where(decarb_carbon_coef['site']=='WIHK1','WIHK-1',decarb_carbon_coef['site'])
    decarb_carbon_coef['site'] = np.where(decarb_carbon_coef['site']=='WIHK2','WIHK-2',decarb_carbon_coef['site'])
    decarb_carbon_coef['site'] = np.where(decarb_carbon_coef['site']=='WMYP1','WMY',decarb_carbon_coef['site'])

    #PPA和REC省下的電量 用碳排係數 Tonnes CO2e
    energy_demand = energy_demand.merge(decarb_carbon_coef, on=['site','year'],how='left')
    energy_demand['PPA_abatement'] = energy_demand['PPA_demand'] * energy_demand['decarb_carbon_coef']/1000
    energy_demand['REC_abatement'] = energy_demand['REC_demand'] * energy_demand['decarb_carbon_coef']/1000
    #總電量的 碳
    energy_demand['target_abatement'] = energy_demand['amount'] * energy_demand['decarb_carbon_coef']/1000
    #-----未解----#
    #太陽能有改變(如2022-2023年的容量增加) 才計算
    #energy_demand['solar_save_volume'] = energy_demand['solar_demand'] * energy_demand['decarb_carbon_coef'] 

    #---------------------------------------------   Step6   
    # 邊際減排成本計算 : Step 6 - 購買電力成本

    #----購買電力成本 綠證價格----#

    #----手動補 N2和竹北AI 成台灣的綠證價格------#
    temp1 = green_electricity_cost[green_electricity_cost['site']=='WIH']
    temp1['site'] = 'N2'
    temp2 = green_electricity_cost[green_electricity_cost['site']=='WIH']
    temp2['site'] = '竹北AI'

    green_electricity_cost = green_electricity_cost.append(temp1)
    green_electricity_cost = green_electricity_cost.append(temp2)

    green_cer_electricity_cost = green_cer_electricity_cost.drop(columns=['id','area','predict_roc','last_update_time']) #,'green_full_ratio'
    green_cer_electricity_cost = green_cer_electricity_cost.rename(columns={'amount':'green_cer_electricity_cost'})

    energy_demand = energy_demand.merge(green_cer_electricity_cost, on=['site','year'],how='left')

    #----購買電力成本 綠電價差----#
    green_electricity_cost = green_electricity_cost.drop(columns=['id','area','predict_roc','last_update_time']) #'green_full_ratio',
    green_electricity_cost = green_electricity_cost.rename(columns={'amount':'green_electricity_diff'})
    green_electricity_cost = green_electricity_cost.drop_duplicates()

    energy_demand = energy_demand.merge(green_electricity_cost, on=['site','year'],how='left')

    exchange_rate_rmb_to_usd = pd.read_sql(f"""SELECT * FROM raw.macc_rmb_to_usd""", con=db)
    exchange_rate_rmb_to_usd = exchange_rate_rmb_to_usd[exchange_rate_rmb_to_usd['exchange_rate']=='RMB_to_USD']
    exchange_rate_rmb_to_usd = exchange_rate_rmb_to_usd.drop(columns=['exchange_rate','id','last_update_time'])
    exchange_rate_rmb_to_usd = exchange_rate_rmb_to_usd.rename(columns={'value':'exchange_rate_rmb_to_usd'})

    energy_demand = energy_demand.merge(exchange_rate_rmb_to_usd, on=['year'],how='left')

    #從 MWh轉到KWh
    energy_demand['green_cer_electricity_cost_usd_kwh'] = energy_demand['green_cer_electricity_cost']*energy_demand['exchange_rate_rmb_to_usd']/1000
    energy_demand['green_electricity_diff_usd_kwh'] = energy_demand['green_electricity_diff']*energy_demand['exchange_rate_rmb_to_usd']/1000

    #---------------------------------------------   Step7
    # Step 7 - 各電力來源每單位增量成本
    # 注意綠電有_usd_kwh -> 綠電 : green_electricity_diff_usd_kwh
    # 綠證 : green_cer_electricity_diff, 太陽能 : solar_electricity_diff

    energy_demand['green_cer_electricity_diff'] = energy_demand['green_cer_electricity_cost_usd_kwh'] 

    #---------------------------------------------   Step8   
    # Step 8 - 每年用電增量成本 : 電力需求量 * 單位成本

    energy_demand['green_cer_electricity_add_year_cost'] = energy_demand['REC_demand']*energy_demand['green_cer_electricity_cost_usd_kwh']
    energy_demand['green_electricity_add_year_cost'] = energy_demand['PPA_demand']*energy_demand['green_electricity_diff_usd_kwh']

    #---------------------------------------------   Step9    
    # Step 9 - 每年用電增量成本 - 折現

    # 抓 wacc

    wacc_value = decarb_wacc[decarb_wacc['source']=='Bloomberg']
    wacc_value = wacc_value[wacc_value['period_start']==max(wacc_value['period_start'])]
    wacc_value = float(wacc_value['wacc'].values[0])  #5.637
    energy_demand['wacc'] = wacc_value*0.01
    energy_demand['wacc'] = energy_demand['wacc'].astype(float)

    energy_demand['wacc_green_cer_cost'] = energy_demand['green_cer_electricity_add_year_cost']/((energy_demand['wacc']+1)**((energy_demand['year']-2023)))
    energy_demand['wacc_green_cost'] =  energy_demand['green_electricity_add_year_cost']/((energy_demand['wacc']+1)**((energy_demand['year']-2023)))


    #---------------------------------------------   Step10    
    # Step 10 - 邊際減排成本 : step9(總減排成本)/step5(總減排量)

    energy_demand['PPA_abatement'] = energy_demand['PPA_abatement'].replace(0, np.nan)
    energy_demand['REC_abatement'] = energy_demand['REC_abatement'].replace(0, np.nan)

    try:
        energy_demand['wacc_green_cer_cost_margin'] = energy_demand['wacc_green_cer_cost'] / energy_demand['REC_abatement']
    except ZeroDivisionError:
        energy_demand['wacc_green_cer_cost_margin'] = np.nan  

    #-------------------------
    try:
        energy_demand['wacc_green_cost_margin'] = energy_demand['wacc_green_cost'] / energy_demand['PPA_abatement']
    except ZeroDivisionError:
        energy_demand['wacc_green_cost_margin'] = np.nan 

    # result

    result_df = pd.DataFrame()

    for item in ['wacc_green_cost','wacc_green_cer_cost','PPA_abatement','REC_abatement','wacc_green_cost_margin','wacc_green_cer_cost_margin']:

        temp = energy_demand[['site','year',item]]
        temp = temp.rename(columns={item:'amount'})
        temp['category'] = 'Scope 2'

        if item in ['wacc_green_cost','PPA_abatement','wacc_green_cost_margin']:
            temp['lever'] = '2.2 PPA'
        if item in ['wacc_green_cer_cost','REC_abatement','wacc_green_cer_cost_margin']:
            temp['lever'] = '2.3 Unbundled EAC'

        if item in ['wacc_green_cost','wacc_green_cer_cost']:
            temp['item'] = '減碳成本'
        if item in ['PPA_abatement','REC_abatement']:
            temp['item'] = '碳排減排量'
        if item in ['wacc_green_cost_margin','wacc_green_cer_cost_margin']:
            temp['item'] = '邊際減排成本'


        temp['last_update_time'] = pd.to_datetime(datetime.now().strftime("%H:%M:%S"))
        result_df = result_df.append(temp)


    result_df = result_df[result_df['year']>2023]
    result_df = result_df.dropna()
    print (result_df)

    # #--------------------把資料到進資料庫----------------------#
    db = create_engine(connect_string, echo=True)
    conn = db.connect()

    category = 'Scope 2'
    lever_1 = '2.2 PPA'
    lever_2 = '2.3 Unbundled EAC'
    conn.execute(f"""DELETE FROM app.decarb_macc_summary WHERE category = '{category}' and lever = '{lever_1}'""")
    conn.execute(f"""DELETE FROM app.decarb_macc_summary WHERE category = '{category}' and lever = '{lever_2}'""")

    result_df.to_sql('decarb_macc_summary', conn, index=False,if_exists='append', schema='app', chunksize=1000)
    
    conn.close()
