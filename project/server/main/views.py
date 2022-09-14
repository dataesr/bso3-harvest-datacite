import redis

from application.processor import Processor
from application.utils_processor import split_dump_file_concat_and_save_doi_files
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, current_app, request


from config.global_config import config_harvester
from project.server.main.tasks import (
    create_task_harvest,
    create_task_match_affiliations_partition,
    create_task_consolidate_results,
    create_task_process_and_match_dois,
)
from project.server.main.utils_dataclasses import split_dump_file


from project.server.main.logger import get_logger

main_blueprint = Blueprint(
    "main",
    __name__,
)

logger = get_logger(__name__)

TARGET_DUMP = "/data/dump"


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")


@main_blueprint.route("/harvest", methods=["POST"])
def run_task_tmp():
    # args = request.get_json(force=True) request is from flask
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(create_task_harvest, TARGET_DUMP)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/affiliations", methods=["POST"])
def run_task_affiliations():
    args = request.get_json(force=True)
    # partition file to parallelize workload (if affiliation matcher is not too limiting)
    response_objects = []
    number_of_partitions = args.get("number_of_partitions", 10_000)
    affiliations_source_file = args.get("affiliations_source_file")
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=150 * 3600)
        for partition_index in range(number_of_partitions + 1):
            task_kwargs = {
                "affiliations_source_file": affiliations_source_file,
                "partition_index": partition_index,
                "total_partition_number": number_of_partitions,
                "job_timeout": 2 * 3600,
            }
            task = q.enqueue(create_task_match_affiliations_partition, **task_kwargs)
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})

        # concatenate the files
        task = q.enqueue(create_task_consolidate_results)
    return jsonify(response_objects), 202


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


@main_blueprint.route("/split_ndjson/<path:source_directory>")
def run_split_ndjson(source_directory):
    split_dump_file_concat_and_save_doi_files(source_directory)
    return "split complet"


@main_blueprint.route("/process_and_match", methods=["GET"])
def process_and_match():
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(create_task_process_and_match_dois)
    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202
