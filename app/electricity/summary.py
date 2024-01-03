from flask import Blueprint

from jobs.renew_setting_etl import renew_setting_etl
from services.mail_service import send_task_fail_mail
from utils.response import task_response

summary_bp = Blueprint('summary', __name__, url_prefix='/summary')


@summary_bp.route('update', methods=['POST'])
def update_electricity_summary():
    """
        總用電量總攬, 使用於當更新目標設定時, 更新對比基準年與 SBTI 的 Scope 1+2 減排模擬
        ---
        tags:
            - 總用電量管理
        produces: application/json
        responses:
            200:
                description: success
                schema:
                id: Result
                properties:
                    message:
                    type: string
                    default: {'msg': 'elect_overview etl success'}
    """
    result = renew_setting_etl.s().on_error(
        send_task_fail_mail.s('electricity summary, renew_setting_etl()')).delay()

    return task_response(result.id, result.state)
