
from flask import Blueprint

from Model.RE_purpose_optimizer import re_purpose_optimize_main_fn
from services.mail_service import send_task_fail_mail
from utils.response import task_response

sim_bp = Blueprint('sim', __name__, url_prefix='/sim')


@sim_bp.route('/ratio', methods=['POST'])
def retrain_renewable_energy_ration_sim():
    """
       重新訓練再生能源比例模擬
        ---
        tags:
            - 再生能源最佳化
        produces: application/json
        responses:
            200:
                description: Successfully retrieved task status
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                id:
                                type: integer
                                description: task id
                                state:
                                type: string
                                description: task state
                                enum:
                                    - SUCCESS   # 任務成功完成
                                    - FAILURE   # 任務失敗
                                    - PENDING   # 任務等待中 // 當 id 不存在或還在處理時
                                    - RETRY     # 任務重試中
                                    - REJECTED  # 任務被拒絕
                                    - REVOKED   # 任務被撤銷
                        examples:
                            id: "abc"
                            state: SUCCESS
    """
    result = re_purpose_optimize_main_fn.s().on_error(
        send_task_fail_mail.s('renewable_energy sim, re_purpose_optimize_main_fn()')).delay()

    return task_response(result.id, result.state)
