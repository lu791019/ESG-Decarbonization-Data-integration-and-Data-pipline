

from flask import Blueprint, jsonify, make_response

from app.logger import logger
from jobs.solar_etl import solar_etl

remaining_power_bp = Blueprint(
    'remaining_power', __name__, url_prefix='/remaining_power')


@remaining_power_bp.route('/', methods=['POST'])
def update_remaining_power():
    """
        作用於太陽能用電管理/餘電上網維護，觸發餘電 ETL
        ---
        tags:
            - 太陽能用電管理
        produces: application/json
        responses:
            200:
                description: success
                schema:
                id: Result
                properties:
                    message:
                    type: string
                    default: {'msg': 'solar etl success'}
    """
    try:
        # solar etl : after user add solar remain data then exec :
        # source2raw & raw2stage & stage2app
        logger.info('execute solar_etl')
        solar_etl()

        return {'msg': 'solar etl success'}
    except Exception as error:  # pylint: disable=broad-except
        logger.exception("Exception ERROR => %s", str(error))
        return make_response(jsonify({'error': str(error)}), 400)
