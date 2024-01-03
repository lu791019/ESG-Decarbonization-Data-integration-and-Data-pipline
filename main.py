
from config import Config
from elec_transfer.next_year_green_power_transfer_suggest import \
    next_year_green_power_transfer_suggest
from factories.source_to_raw_factory import main as source_to_raw
from factories.SourceToRawFactoryException import SourceToRawFactoryException
from jobs.csr_etl import (csr_rawsolar_replace, csr_replace, csr_solar_replace,
                          office2raw)
from jobs.decarb_path_etl import decarb_path_etl
from jobs.elect_target_etl import decarb_renew_setting_etl
from jobs.fix_data import (fix_raw, fix_raw_elect_decarb, import_actual_elect,
                           source_status, source_status_old)
from jobs.raw_to_staging import raw_to_staging
from jobs.renew_green_energy import green_energy_overview
# from jobs.source_to_raw import source_to_raw
from jobs.staging_cal import staging_cal
from jobs.staging_to_app import staging_to_app
from jobs.wzsesgi_etl import esgi2raw, esgi2solar
from macc_summary.macc_input_to_summary import \
    macc_input_to_summary_scope2_func
from Model.Factory_elect_simulator_update import factory_elct_main_fn
from services.mail_service import send_fail_mail, send_success_mail


def get_stage():
    return Config.FLASK_ENV


def main():

    #get ESGI data
    esgi2raw()

    office2raw('electricity_total_decarb')

    esgi2solar()

    source_to_raw('fem_ratio')
    source_to_raw('fem_ratio_solar')
    source_to_raw('solar_ratio')

    """raw.soalr 暫不更新 @1106 : remark source_to_raw('solar') , raw.solar來源改為月報表資料, 月報表資料不足的部分則維持原本太陽能系統的資料 """
    # source_to_raw('solar')

    stage = get_stage()

    csr_replace('raw', 'electricity_total_decarb', 'electricity_backstage_update')

    csr_replace('raw', 'renewable_energy_decarb', 'whq_esgcsrdatabase_view_csrindicatordetail_all')

    csr_solar_replace('renewable_energy_decarb',stage)

    csr_rawsolar_replace('solar',stage)

    fix_raw(1, 'renewable_energy_decarb','光伏')

    fix_raw(1, 'renewable_energy_decarb','綠電')

    fix_raw(1, 'renewable_energy_decarb','綠證')

    fix_raw_elect_decarb(1, 'electricity_total_decarb')

    raw_to_staging('electricity_decarb', stage)

    raw_to_staging('renewable_energy_decarb', stage)

    raw_to_staging('solar', stage)
    raw_to_staging('solar_remain', stage)
    raw_to_staging('solar_other', stage)
    raw_to_staging('solar_info', stage)

    raw_to_staging('green_elect_price', stage)
    raw_to_staging('green_elect_vol', stage)
    raw_to_staging('green_elect_contract', stage)
    raw_to_staging('grey_elect', stage)

    raw_to_staging('elect_total', stage)

    raw_to_staging('provider_plant_list', stage)

    #copy actual electricity to elect_target_month
    import_actual_elect()

    #綠電轉供對帳
    staging_to_app('green_elec_transfer_account', stage)

    #太陽能用電量總覽
    staging_to_app('solar_energy_overview', stage)

    #直購綠電總覽
    staging_to_app('green_elect_overview', stage)
    staging_cal('green_elec_pre_contracts', stage)

    #總用電量管理
    staging_cal('decarb_elec_overview', stage)
    decarb_renew_setting_etl(stage)

    #脫碳目標
    decarb_path_etl(stage)

    #綠證需量估計
    green_energy_overview('add_customer_data')
    green_energy_overview('summarize_all_data')

    #資料更新狀態
    source_status('source_decarb_confirm')

    # factory_elct_main_fn(stage)

    next_year_green_power_transfer_suggest(stage)

    # macc_input_to_summary
    macc_input_to_summary_scope2_func()




if __name__ == '__main__':
    try:
        main()
        send_success_mail('ETL')
    except SourceToRawFactoryException as e:
        send_fail_mail('ETL', e)
    except Exception as inst:
        send_fail_mail('ETL', inst)
