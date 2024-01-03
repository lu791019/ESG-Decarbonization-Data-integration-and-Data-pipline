from app.celery import app
from config import Config
from services.mail_service import MailService


@app.task
def example():
    stage = Config.FLASK_ENV

    try:
        # do something

        mail = MailService(
            f'[success][{stage}] decarb-etl cron job report')
        mail.send_text(f"func {example.__name__} update success")
    except Exception as inst:
        mail = MailService(
            f'[failed][{stage}] decarb-etl cron job report')
        mail.send_text("func {example.__name__} update fail")
