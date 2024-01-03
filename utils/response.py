from flask import jsonify


def task_response(task_id, state):
    """ task response """

    return jsonify({"task_id": task_id, "state": state})
