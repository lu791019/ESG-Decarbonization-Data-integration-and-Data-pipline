from flask import Blueprint, jsonify, make_response, request

from app.logger import logger
from elec_transfer.upload_excel_to_DB import upload_excel_to_DB
from exceptions.ReconciliationException import ReconciliationException
from services.mail_service import send_task_fail_mail
from utils.constants import EXCEL_FILE_TYPE
from utils.response import task_response

reconciliation_bp = Blueprint(
    'reconciliation', __name__, url_prefix='/reconciliation')


@reconciliation_bp.route('/upload', methods=['POST'])
def upload_reconciliation_excel():
    """
        直購綠電管理/綠電轉供對帳中，使用者匯入資料
        ---
        tags:
            - 直購綠電管理
        parameters:
          - name: params
            in: body
            required: true
            schema:
                type: object
                required: true
                properties:
                    file:
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
                    default: {'msg': 'reconciliation upload success'}
    """
    try:
        data = request.json
        file = data.get('file')

        if file is None:
            return jsonify({"error": "file is required"}), 400

        if ',' in file:
            file_type, file = file.split(',')
        else:
            file_type = ''
            file = ''

        if file_type != EXCEL_FILE_TYPE:
            raise ReconciliationException('4001')

        logger.info('execute upload_reconciliation')
        result = upload_excel_to_DB.s().on_error(
            send_task_fail_mail.s('green_power reconciliation, upload_excel_to_DB()')).delay(file)
        # return {"result": upload_excel_to_DB(file)}
        return task_response(result.id, result.state)

    except ReconciliationException as error:
        logger.exception("Exception ERROR => %s", str(error))
        return make_response({"error": {
            "code": error.code,
            "message": error.message,
        }}, 400)
    except Exception as error:  # pylint: disable=broad-except
        logger.exception('Exception ERROR => %s', str(error))
        return make_response({"error": str(error)}, 400)
