
from flask import Blueprint, jsonify, make_response, request

from app.logger import logger
from exceptions.ShipmentException import ShipmentException
from jobs.shipments_etl import upload_shipment
from services.mail_service import send_task_fail_mail
from utils.constants import EXCEL_FILE_TYPE
from utils.response import task_response

shipment_bp = Blueprint('shipment', __name__, url_prefix='/shipment')


@shipment_bp.route('/upload', methods=['POST'])
def upload_shipment_excel():
    """
        作用於總用電量管理/出貨量變量維護中，使用者匯入資料
        ---
        tags:
            - 總用電量管理
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
                    default: {'msg': 'shipment upload success'}
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
            raise ShipmentException('3001')

        logger.info('execute upload_shipment')
        # implement here.
        result = upload_shipment.s().on_error(
            send_task_fail_mail.s('shipment, upload_shipment()')).delay(file)

        return task_response(result.id, result.state)

    except ShipmentException as error:
        logger.exception("Exception ERROR => %s", str(error))
        return make_response({"error": {
            "code": error.code,
            "message": error.message,
        }}, 400)
    except Exception as error:  # pylint: disable=broad-except
        logger.exception('Exception ERROR => %s', str(error))
        return make_response({"error": str(error)}, 400)
