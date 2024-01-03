import requests
from flask import Blueprint, request
from nanoid import generate

from app.celery import app
from app.logger import logger
from app.models import (DecarbElectSimulate, DecarbSignOff, ElectTargetMonth,
                        ElectTargetYear)
from app.sign_off.constant import (AUDIT_FLOW_STATE_CONFIG,
                                   DEVELOPER_EMAIL_LIST, signatureState,
                                   system_base_url)
from config import Config
from Model.Factory_elect_simulator_update import factory_elct_main_fn
from models.engine import Session
from services.mail_service import (MAIL_TO_LIST, MailService, notify,
                                   send_task_fail_mail)
from utils.response import task_response

sign_off = Blueprint(
    'sign_off', __name__, url_prefix='/sign_off')

stage = Config.FLASK_ENV


@app.task(name='sign-off-update-status-notified')
def update_sign_off(preview, sign_off_id: str):
    with Session as session:
        row = session.query(DecarbSignOff).filter_by(
            id=sign_off_id).first()

        if row:
            row.status = signatureState["NOTIFIED"],

        session.commit()

    return True


@app.task(name='sign-off-establish')
def create_sign_off(preview, pic: str, reviewer: str):
    version = preview['version']
    version_year = preview['version_year']

    if version is None:
        raise ValueError('version must be string')
    if version_year is None:
        raise ValueError('version_year must be integer')

    with Session as session:
        nanoid = generate()
        sign_off_type = 'power_consumption-summary'

        # create sign off
        sign_off_item = DecarbSignOff(id=nanoid,
                                      type=sign_off_type,
                                      year=version_year,
                                      status=signatureState["ESTABLISHED"],
                                      pic=pic,
                                      reviewer=reviewer
                                      )
        session.add(sign_off_item)

        # update elec sim sign_off_id
        rows = session.query(DecarbElectSimulate).filter_by(
            version=version, version_year=version_year).all()

        for row in rows:
            row.sign_off_id = nanoid

        # update elec target year sign_off_id
        rows = session.query(ElectTargetYear).filter_by(
            version=version).all()

        for row in rows:
            row.sign_off_id = nanoid

        # update elec target month sign_off_id
        rows = session.query(ElectTargetMonth).filter_by(
            version=version, year=version_year).all()

        for row in rows:
            row.sign_off_id = nanoid

        session.commit()

        logger.info('sign off create success, id: %s', nanoid)
        MailService(f'[Debug][{stage}][sign-off][establish]', DEVELOPER_EMAIL_LIST).send_text(
            f'this message will be send after ai simulate electricity update success, will create sign off and given sign_off_id: {nanoid}')

        return sign_off_item.to_dict()


@app.task(name='sign-off-send-pic-mail')
def send_pic_mail(preview):
    baseUrl = system_base_url()
    url = f'{baseUrl}/api/mail/electricity-review'
    sign_off_id = preview['id']
    data = {
        "topic": '測試 pic',
        "receivers": [preview['pic'], 'felix_ye@wistron.com', 'viviana_fu@wistron.com'],
        "reviewUrl": f'/electricity/summary/review?sign_off_id={sign_off_id}',
        "yearInterval": '2023-2024',
        "status": AUDIT_FLOW_STATE_CONFIG['ESTABLISHED'],
        "sign_off_id": sign_off_id
    }
    headers = {
        'user-agent': 'Mozilla/4.0 MDN Example',
        'content-type': 'application/json',
    }
    logger.info(data)
    # this api will be timeout when create preview image
    response = requests.post(
        url, json=data, headers=headers, timeout=10, verify=False)

    if response.status_code == 200:
        logger.info('send pic mail success')

        return response.json()

    logger.error(
        "Failed to send pic mail, Status code: %s, Response: %s",
        response.status_code, response.text)


@sign_off.route('/notification-reviewer', methods=['POST'])
def send_notification():
    """
        Email 通知使用者，並更改狀態
        ---
        tags:
            - 簽核
        parameters:
          - name: params
            in: body
            required: true
            schema:
                type: object
                required: true
                properties:
                    sign_id:
                        type: string
                    topic:
                        type: string
                    content:
                        type: string
                    to_list:
                        type: array
                        items:
                            type: string
        produces: application/json
        responses:
            200:
                description: success
                schema:
                id: Result
                properties:
                    message:
                    type: string
                    default: {'msg': 'notification success'}
    """

    sign_id = request.json.get('sign_id')
    topic = request.json.get('topic', 'AI simulated electricity update')
    content = request.json.get('content', """
    <html>
    <head></head>
    <body>
        <h1>Hello, World!</h1>
        <p>This is an HTML email example.</p>
    </body>
    </html>
    """)
    to_list = request.json.get('to_list', MAIL_TO_LIST)

    logger.info('sign off send notification start')
    # 目前只適用於單一 reviewer
    result = notify.s(topic, content, to_list) | update_sign_off.s(sign_id)

    result = result.on_error(
        send_task_fail_mail.s('sign_off send_notification, notify() | update_sign_off()')).delay()
    return task_response(result.id, result.state)


@sign_off.route('/establish', methods=['POST'])
def establish_sign_off():
    """
        AI 模擬用電量，後建立簽核單，並進行寄信通知
        ---
        tags:
            - 簽核
        produces: application/json
        parameters:
          - name: params
            in: body
            required: true
            schema:
                type: object
                required: true
                properties:
                    pic:
                        type: string
                    reviewer:
                        type: string
        responses:
            200:
                description: success
                schema:
                id: Result
                properties:
                    message:
                    type: string
                    default: {'msg': 'establish sign off success'}
    """
    logger.info('sign off send establish start')

    pic = request.json.get('pic')
    reviewer = request.json.get('reviewer')

    result = factory_elct_main_fn.s() | create_sign_off.s(
        pic, reviewer) | send_pic_mail.s()

    result = result.on_error(
        send_task_fail_mail.s('sign_off establish_sign_off, factory_elct_main_fn() | create_sign_off() | send_pic_mail()')).delay()
    return task_response(result.id, result.state)
