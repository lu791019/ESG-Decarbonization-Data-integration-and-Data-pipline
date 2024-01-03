from celery.result import AsyncResult
from flask import Blueprint, jsonify, make_response, request
from sqlalchemy import select

from app.celery import app as celery_app
from app.models import CeleryTaskmeta
from jobs.task_names import mapping_list
from models.engine import Session
from utils.response import task_response

task = Blueprint('task', __name__, url_prefix='/tasks')


@task.route('/<task_id>', methods=['GET'])
def find_task(task_id):
    """
        取得任務狀態
        ---
        tags:
            - Task
        produces: application/json
        parameters:
            - name: task_id
              in: path
              required: true
              schema:
                type: string
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
    async_result = AsyncResult(task_id, app=celery_app)

    return task_response(task_id, async_result.state)


@task.route('/ids', methods=['GET'])
def query_tasks():
    """
        利用 route name, 取得任務 id 
        ---
        tags:
            - Task
        produces: application/json
        parameters:
            - name: route_name
              in: query
              required: true
              schema:
                type: string
        responses:
            200:
                description: Successfully retrieved task status
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                data:
                                    type: object
                                    properties:
                                id:
                                    type: integer
                                    description: task id
                                status:
                                    type: string
                                    description: task status

    """
    route_name = request.args.get('route_name', default='', type=str)

    if route_name not in mapping_list:
        return make_response(jsonify({'error': 'route_name not found'}), 400)

    with Session as session:
        statement = select(CeleryTaskmeta.task_id, CeleryTaskmeta.status).filter_by(
            name=mapping_list[route_name])
        data = session.execute(statement).all()

        return {"data": [{"id": item[0], "status": item[1]} for item in data]}
