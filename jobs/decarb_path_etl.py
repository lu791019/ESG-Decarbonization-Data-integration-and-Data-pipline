from datetime import datetime as dt

import numpy as np
import pandas as pd
from sqlalchemy import *

from models import engine
from services.mail_service import MailService


def decarb_path_etl(stage):

    connect_string = engine.get_connect_string()
    db = create_engine(connect_string, echo=True)

    #脫碳目標
    try:
        #get AI模擬用電
        elect_target_year = pd.read_sql(f"""SELECT site, "year", amount, version_year FROM app.decarb_elect_simulate where "version" = (SELECT MAX("version") FROM app.decarb_elect_simulate) and "version_year" = (SELECT MAX("version_year") FROM app.decarb_elect_simulate) """, db)
        if elect_target_year.shape[0] > 0:
            version_year = elect_target_year['version_year'][0]
            elect_target_year = elect_target_year.drop('version_year',axis = 1)
            #可再生能源比例
            renewable_ratio = pd.read_sql(f"""SELECT year,category,amount/100 as "ratio" FROM staging.renewable_setting WHERE category in ('REC','PPA','solar')""", db)

            #碳排係數
            coef = pd.read_sql( f"""SELECT site , year, amount as coef FROM staging.decarb_carbon_coef  """, db)
            site_dict = {'WIHK1': 'WIHK-1', 'WIHK2': 'WIHK-2','WMYP1': 'WMY', 'WMYP2': 'WMY'}
            coef['site'] = coef['site'].replace(site_dict)
            coef.drop_duplicates(inplace=True)

            #merge elec and coef
            elect_target_year = elect_target_year.merge(coef,on = ['site','year'],how = 'left')


            #計算 Scope2 - location-based
            renewable_ratio_location_based = renewable_ratio[renewable_ratio['category'] != 'REC']
            renewable_ratio_location_based = renewable_ratio_location_based.groupby('year').sum().reset_index()
            renewable_ratio_location_based['ratio'] = 1 - renewable_ratio_location_based['ratio']

            carbon_scope2_location = elect_target_year.merge(renewable_ratio_location_based,on = 'year',how = 'left')
            carbon_scope2_location['amount'] = carbon_scope2_location['amount'] * carbon_scope2_location['coef'] * carbon_scope2_location['ratio']/1000

            # 計算 Scope1 = Scope 2 Location-based * 6 % / (1-6%)
            carbon_scope1 = carbon_scope2_location.copy()
            carbon_scope1['amount'] = carbon_scope1['amount'] * 0.06 / (1-0.06)
            carbon_scope1 = carbon_scope1.groupby('year')['amount'].sum().reset_index()
            carbon_scope1['category'] = 'Scope 1'

            #計算 Scope 2 (Scope 2-REC) - market-based
            #get percentage of non-renewable
            renewable_ratio_scope2 = renewable_ratio.groupby('year').sum().reset_index()
            renewable_ratio_scope2['ratio'] = 1 - renewable_ratio_scope2['ratio']

            carbon_scope2 = elect_target_year.merge(renewable_ratio_scope2,on = 'year',how = 'left')
            carbon_scope2['amount'] = carbon_scope2['amount'] * carbon_scope2['coef'] * carbon_scope2['ratio']/1000
            carbon_scope2 = carbon_scope2.groupby('year')['amount'].sum().reset_index()
            carbon_scope2['category'] = 'Scope 2 (Scope 2-REC)'

            #計算 RECs
            #get percentage of REC
            renewable_ratio_REC = renewable_ratio[renewable_ratio['category'] == 'REC']
            renewable_ratio_REC = renewable_ratio_REC.groupby('year').sum().reset_index()

            carbon_REC = elect_target_year.merge(renewable_ratio_REC,on = 'year',how = 'left')
            carbon_REC['amount'] = carbon_REC['amount'] * carbon_REC['coef'] * carbon_REC['ratio']/1000
            carbon_REC = carbon_REC.groupby('year')['amount'].sum().reset_index()
            carbon_REC['category'] = 'RECs'


            #計算 Solar Power Generation
            #get percentage of solar
            renewable_ratio_solar = renewable_ratio[renewable_ratio['category'] == 'solar']
            renewable_ratio_solar = renewable_ratio_solar.groupby('year').sum().reset_index()

            carbon_solar = elect_target_year.merge(renewable_ratio_solar,on = 'year',how = 'left')
            carbon_solar['amount'] = carbon_solar['amount'] * carbon_solar['coef'] * carbon_solar['ratio']/1000
            carbon_solar = carbon_solar.groupby('year')['amount'].sum().reset_index()
            carbon_solar['category'] = 'Solar Power Generation'


            #計算 PPA
            #get percentage of PPA
            renewable_ratio_PPA = renewable_ratio[renewable_ratio['category'] == 'PPA']
            renewable_ratio_PPA = renewable_ratio_PPA.groupby('year').sum().reset_index()

            carbon_PPA = elect_target_year.merge(renewable_ratio_PPA,on = 'year',how = 'left')
            carbon_PPA['amount'] = carbon_PPA['amount'] * carbon_PPA['coef'] * carbon_PPA['ratio']/1000
            carbon_PPA = carbon_PPA.groupby('year')['amount'].sum().reset_index()
            carbon_PPA['category'] = 'PPA'


            carbon_all = carbon_scope1.append(carbon_scope2).append(carbon_REC).append(carbon_solar).append(carbon_PPA).reset_index(drop = True)
            carbon_all['unit'] = 'Tonnes CO2e'
            carbon_all['version'] = version_year
            carbon_all['last_update_time'] = dt.strptime(dt.now().strftime("%Y-%m-%d %H:%M:%S"), "%Y-%m-%d %H:%M:%S")
            carbon_all['type'] = 'target'

            if carbon_all.shape[0] > 0:
                conn = db.connect()
                category_list = carbon_all['category'].unique()
                category_list = "','".join(category_list)
                conn.execute(f"DELETE FROM app.decarb_path WHERE category IN ('{category_list}') AND year >= {carbon_all['year'].min()} AND year <= {carbon_all['year'].max()} AND version = {version_year} and type ='target'")
                carbon_all.to_sql('decarb_path', conn, index=False,if_exists='append', schema='app', chunksize=10000)
                conn.close()
    
    except Exception as e:
        error = str(e)
        mail = MailService(
            '[failed][{}] decarb path etl'.format(stage))
        mail.send_text('failed: {}'.format(error))
        return error