from flask import Blueprint, request

from app.logger import logger
from services.mail_service import MAIL_TO_LIST, notify
from utils.response import task_response

notification = Blueprint('notification', __name__, url_prefix='/notification')


@notification.route('/', methods=['POST'])
def send_notification():
    """
        Email 通知使用者
        ---
        tags:
            - 通知
        parameters:
          - name: params
            in: body
            required: true
            schema:
                type: object
                required: true
                properties:
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

    logger.info('send notification start')

    result = notify.delay(topic, content, to_list)

    return task_response(result.id, result.state)
