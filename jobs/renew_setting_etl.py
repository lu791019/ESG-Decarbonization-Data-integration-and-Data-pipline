import os

from app.celery import app
from jobs.elect_target_etl import decarb_renew_setting_etl
from services.mail_service import MailService


def get_stage():
    return os.environ['FLASK_ENV'] if 'FLASK_ENV' in os.environ else 'development'


@app.task(name='electricity-summary-update')
def renew_setting_etl():
    stage = get_stage()
    try:
        decarb_renew_setting_etl(stage)

        mail = MailService(
            '[success][{}] decarb_renew_setting_etl'.format(stage))
        mail.send_text("func {} update success".format(
            renew_setting_etl.__name__))
    except Exception as inst:
        mail = MailService(
            '[failed][{}] decarb_renew_setting_etl'.format(stage))
        mail.send_text("func {} update fail".format(
            renew_setting_etl.__name__))
