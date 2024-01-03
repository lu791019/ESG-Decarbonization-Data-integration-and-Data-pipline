from flask import Blueprint

from app.logger import logger
from jobs.green_energy_etl import green_energy_etl
from services.mail_service import send_task_fail_mail
from utils.response import task_response

certificate = Blueprint('certificate', __name__, url_prefix='/certificate')


@certificate.route('/green_energy/update', methods=['POST'])
def update_green_energy_amount():
    """
        作用於綠證採購管理與綠證需量估計, 使用於新增,編輯綠證採購管理,以及新增,編輯綠證需量估計的各site的客戶資料,更新計算綠證需量估計資料
        ---
        tags:
            - 綠電憑證管理
        produces: application/json
        responses:
            200:
                description: success
                schema:
                id: Result
                properties:
                    message:
                    type: string
                    default: {'msg': 'green_energy_overview etl success'}
    """
    try:

        logger.info('execute green_energy_overview')

        green_energy_etl()

        return {'msg': 'success'}

        # return {'msg': 'green_energy_overview etl success'}
    except Exception as error:  # pylint: disable=broad-except

        logger.exception("Exception ERROR => %s", str(error))

        return {'msg': 'green_energy_overview etl failed'}
