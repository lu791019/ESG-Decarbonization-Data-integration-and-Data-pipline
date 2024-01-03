import smtplib
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List

from app.celery import app
from app.logger import logger
from app.sign_off.constant import DEVELOPER_EMAIL_LIST
from config import Config

stage = Config.FLASK_ENV

MAIL_TO_LIST = [
    'Felix_ye@wistron.com',
    'Vincent_ku@wistron.com',
    'Dex_Lu@wistron.com',
    'Sark_Liu@wistron.com',
    'Zack_Li@wistron.com',
    'shelly_shiu@wistron.com'
]

HOST = 'whqsmtp.wistron.com'
SENDER_EMAIL = 'DECARB@wistron.com'


class MailService:
    def __init__(self, subject, to_list: List[str] = None):
        if to_list is None:
            to_list = MAIL_TO_LIST

        if not (Config.BYPASS_MAIL_SEND == '1'):
            self.host = HOST
            self.port = 25
            self.smtp = smtplib.SMTP(self.host, self.port)

        self.subject = subject
        self.to_list = to_list

    def send_text(self, content):
        msg = EmailMessage()
        if not (Config.BYPASS_MAIL_SEND == '1'):
            msg['Subject'] = self.subject
            msg['From'] = SENDER_EMAIL
            msg['To'] = ','.join(self.to_list)
            msg['CC'] = ''
            msg.set_content(content)

            self.smtp.sendmail(SENDER_EMAIL, self.to_list,
                               msg.as_string())
            self.smtp.quit()

    def send_html(self, html_content):
        msg = MIMEMultipart()
        if not (Config.BYPASS_MAIL_SEND == '1'):
            msg['Subject'] = self.subject
            msg['From'] = SENDER_EMAIL
            msg['To'] = ','.join(self.to_list)
            msg['CC'] = ''
            html_message = MIMEText(html_content, "html")
            msg.attach(html_message)

            self.smtp.sendmail(SENDER_EMAIL, self.to_list,
                               msg.as_string())
            self.smtp.quit()


@app.task(name='notify')
def notify(topic: str, content: str, to_list: List[str] = None):
    logger.info('[notify] topic: %s, content: %s', topic, content)
    try:
        MailService(
            f'{topic}', to_list).send_html(content)
    except Exception as e:
        logger.exception('[notify] error: %s', str(e))
        return False


@app.task(name='task-fail-mail-send')
def send_task_fail_mail(request_from_error, exc, traceback, topic: str):
    message = f'{topic} task failed, Task {request_from_error.id}, {exc}'
    logger.exception('[failed]: %s', str(message))
    logger.exception('[failed]: %s', str(traceback))

    MailService(
        f'[failed][{stage}][{topic}] decarb-etl cron job report',
        DEVELOPER_EMAIL_LIST
    ).send_text(f'[failed]: {message}')


def send_success_mail(topic: str):
    MailService(
        f'[success][{stage}][{topic}] decarb-etl cron job report').send_text(f'All {topic} success.')


def send_fail_mail(topic: str, message: str):
    logger.exception('[failed]: %s', message)
    MailService(
        f'[failed][{stage}][{topic}] decarb-etl cron job report').send_text(f'[failed]: {message}')
