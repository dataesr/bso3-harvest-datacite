import redis
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, current_app

from project.server.main.tasks import create_task_harvest
from project.server.main.utils_dataclasses import split_dump_file

from project.server.main.logger import get_logger

main_blueprint = Blueprint("main", __name__,)

logger = get_logger(__name__)

TARGET_DUMP = '/data/dump'


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")


@main_blueprint.route("/harvest", methods=["POST"])
def run_task_tmp():
    # args = request.get_json(force=True) request is from flask
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(create_task_harvest, TARGET_DUMP)
    response_object = {
        "status": "success",
        "data": {
            "task_id": task.get_id()
        }
    }
    return jsonify(response_object), 202


@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-hal")
        task = q.fetch_job(task_id)
    if task:
        response_object = {
            "status": "success",
            "data": {
                "task_id": task.get_id(),
                "task_status": task.get_status(),
                "task_result": task.result,
            },
        }
    else:
        response_object = {"status": "error"}
    return jsonify(response_object)


@main_blueprint.route("/split_ndjson")
def run_split_ndjson():
    split_dump_file()
    return("split complet")
