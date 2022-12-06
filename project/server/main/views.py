import datetime
from glob import glob
import os
from typing import List

import redis
from application.utils_processor import _list_files_in_directory
from config.global_config import config_harvester
from flask import Blueprint, current_app, jsonify, render_template, request
from project.server.main.logger import get_logger
from project.server.main.tasks import (
    run_task_consolidate_processed_files, run_task_consolidate_results,
    run_task_enrich_dois, run_task_harvest_dois,
    run_task_match_affiliations_partition, run_task_process_dois,
    run_task_import_elastic_search)
from rq import Connection, Queue

main_blueprint = Blueprint(
    "main",
    __name__,
)

logger = get_logger(__name__)


def get_partitions(files: List[str], partition_size: int) -> List[List[str]]:
    """Return a list of partitions of files. If partition_size > len(files), returns one partition"""
    if len(files) > partition_size:
        return [files[i : i + partition_size] for i in range(0, len(files), partition_size)]
    return [files]


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/home.html")


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


@main_blueprint.route("/harvest_dois", methods=["POST"])
def create_task_harvest_dois():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    args = request.get_json(force=True)
    task_kwargs = {
        "target_directory": args.get("target_directory", config_harvester["raw_dump_folder_name"]),
        "start_date": args.get("start_date", config_harvester["dump_default_start_date"]),
        "end_date": args.get("end_date", current_date),
        "interval": args.get("interval", "day"),
    }
    if args.get("use_threads"):
        task_kwargs.update({"use_threads": args.get("use_thread")})
    if args.get("force"):
        task_kwargs.update({"force": args.get("force")})

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(run_task_harvest_dois, **task_kwargs)

    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/process", methods=["POST"])
def create_task_process_dois():
    args = request.get_json(force=True)
    response_objects = []
    total_number_of_partitions = args.get("total_number_of_partitions", 100)
    file_prefix = args.get("file_prefix")
    dump_files = _list_files_in_directory(config_harvester["raw_dump_folder_name"], "*" + config_harvester["datacite_file_extension"])
    partition_size = len(dump_files) // total_number_of_partitions
    partitions = get_partitions(dump_files, partition_size)
    tasks_list = []
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        for index_of_partition in range(total_number_of_partitions):
            task_kwargs = {
                "partition_index": index_of_partition,
                "files_in_partition": partitions[index_of_partition],
            }
            task = q.enqueue(run_task_process_dois, **task_kwargs)
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
            tasks_list.append(task)
        # consolidate files
        consolidate_task_kwargs = {
            "total_number_of_partitions": total_number_of_partitions,
            "file_prefix": file_prefix,
        }
        task_consolidate_processed_files = q.enqueue(run_task_consolidate_processed_files,
                                                     **consolidate_task_kwargs,
                                                     depends_on=tasks_list
                                                    )
        response_objects.append({"status": "success", "data": {"task_id": task_consolidate_processed_files.get_id()}})
    return jsonify(response_objects), 202


@main_blueprint.route("/affiliations", methods=["POST"])
def create_task_affiliations():
    args = request.get_json(force=True)
    response_objects = []
    number_of_partitions = args.get("number_of_partitions", 1_000)
    file_prefix = args.get("file_prefix")
    tasks_list = []
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=150 * 3600)
        for partition_index in range(number_of_partitions + 1):
            task_kwargs = {
                "file_prefix": file_prefix,
                "partition_index": partition_index,
                "total_partition_number": number_of_partitions,
                "job_timeout": 2 * 3600,
            }
            task = q.enqueue(run_task_match_affiliations_partition, **task_kwargs)
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
            tasks_list.append(task)
        # consolidate files
        consolidate_task_kwargs = {
            "file_prefix": file_prefix,
        }
        task_consolidate_affiliation_files = q.enqueue(run_task_consolidate_results,
                                                       **consolidate_task_kwargs,
                                                       depends_on=tasks_list
                                                       )
        response_objects.append(
            {"status": "success", "data": {"task_id": task_consolidate_affiliation_files.get_id()}}
        )

    return jsonify(response_objects), 202


@main_blueprint.route("/enrich_dois", methods=["POST"])
def create_task_enrich_doi():
    args = request.get_json(force=True)
    response_objects = []
    partition_size = args.get("partition_size", 10)
    datacite_dump_files = glob(os.path.join(
        config_harvester['raw_dump_folder_name'],
        '*' + config_harvester['datacite_file_extension'])
    )
    partitions = get_partitions(datacite_dump_files, partition_size)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=150 * 3600)
        for partition in partitions:
            task_kwargs = {
                "partition_files": partition,
                "job_timeout": 72 * 3600,
            }
            task = q.enqueue(run_task_enrich_dois, **task_kwargs)
            response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
    return jsonify(response_objects), 202

@main_blueprint.route("/create_index", methods=["POST"])
def create_task_import_elastic_search():
    args = request.get_json(force=True)
    index_name = args.get("index_name")
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(run_task_import_elastic_search, index_name=index_name)
        response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202
