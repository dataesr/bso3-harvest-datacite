import redis

from application.utils_processor import _get_partitions, _is_files_list_splittable_into_mutiple_partitions, \
    _list_files_in_directory
from rq import Queue, Connection
from flask import render_template, Blueprint, jsonify, current_app, request


from config.global_config import config_harvester
from project.server.main.tasks import (
    create_task_harvest,
    create_task_match_affiliations_partition,
    create_task_consolidate_results,
    create_task_process_and_match_dois, create_task_process_dois,
)


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


@main_blueprint.route("/process_and_match", methods=["GET"])
def process_and_match():
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(create_task_process_and_match_dois)

    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/process", methods=["POST"])
def process_dois():
    args = request.get_json(force=True)
    # partition file to parallelize workload (if affiliation matcher is not too limiting)
    response_objects = []
    total_number_of_partitions = args.get("total_number_of_partitions", 100)

    if _is_files_list_splittable_into_mutiple_partitions(total_number_of_partitions):
        partitions = list(_get_partitions(total_number_of_partitions))
    else:
        logger.info(f"Number of files in directory {config_harvester['raw_dump_folder_name']} not enough to create partitions")
        total_number_of_partitions = 1
        partitions = [_list_files_in_directory(config_harvester['raw_dump_folder_name'],
                                                            config_harvester['files_extenxion'])]

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        for index_of_partition in range(total_number_of_partitions):
            task_kwargs = {
                "partition_index": index_of_partition,
                "files_in_partition": partitions[index_of_partition],
            }
            print(f"printing task kwargs {task_kwargs}")
            task = q.enqueue(create_task_process_dois, **task_kwargs)
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})

    response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202