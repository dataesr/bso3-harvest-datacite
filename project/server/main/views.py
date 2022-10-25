import datetime
from typing import List
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
    create_task_enrich_dois,
    create_task_process_dois, create_task_consolidate_processed_files,
    create_task_harvest_dois,
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


def get_partitions(files, partition_size: int) -> List:
    """Return a list of partitions of files"""
    partitions = [files[i: i + partition_size] for i in range(0, len(files), partition_size)]
    return partitions


@main_blueprint.route("/enrich_dois", methods=["POST"])
def run_task_enrich_doi():
    args = request.get_json(force=True)
    response_objects = []
    partition_size = args.get("partition_size", 90)
    # datacite_dump_files = glob('/data/dump/*.ndjson')
    datacite_dump_file = '/data/dump/dcdump-20220603000000-20220603235959.ndjson'
    # partitions = get_partitions(datacite_dump_files, partition_size)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=150 * 3600)
        # for partition in partitions:
        task_kwargs = {
            "partition_files": [datacite_dump_file],
            "job_timeout": 2 * 3600,
        }
        task = q.enqueue(create_task_enrich_dois, **task_kwargs)
        response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
        # break
    return jsonify(response_objects), 202


@main_blueprint.route("/affiliations", methods=["POST"])
def run_task_affiliations():
    args = request.get_json(force=True)
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
    return jsonify(response_objects), 202


@main_blueprint.route("/consolidate_affiliations_files", methods=["POST"])
def run_task_consolidate_affiliations_files():
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(create_task_consolidate_results)
        response_object = {"status": "success", "data": {"task_id": task.get_id()}}
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


@main_blueprint.route("/process", methods=["POST"])
def process_dois():
    args = request.get_json(force=True)
    # partition file to parallelize workload (if affiliation matcher is not too limiting)
    response_objects = []
    total_number_of_partitions = args.get("total_number_of_partitions", 100)

    if _is_files_list_splittable_into_mutiple_partitions(total_number_of_partitions):
        partitions = list(_get_partitions(total_number_of_partitions))
    else:
        logger.info(
            f"Number of files in directory {config_harvester['raw_dump_folder_name']} not enough to create partitions")
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


@main_blueprint.route("/harvest_dois", methods=["POST"])
def start_harvest_dois():
    current_date = datetime.date().strftime("%Y-%m-%d")
    args = request.get_json(force=True)
    task_kwargs = {
        "target_directory": args.get("target_directory", config_harvester['raw_dump_folder_name']),
        "start_date": args.get("start_date", config_harvester['dump_default_start_date']),
        "end_date": args.get("end_date", current_date),
        "interval": args.get("interval", 'day')
    }

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite", default_timeout=150 * 3600)
        task = q.enqueue(create_task_harvest_dois, **task_kwargs)

    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


@main_blueprint.route("/full_pipeline", methods=["POST"])
def start_full_process_pipeline():
    args = request.get_json(force=True)
    response_objects = []
    # harvester parameters
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    harvester_kwargs = {
        "target_directory": args.get("target_directory", config_harvester['raw_dump_folder_name']),
        "start_date": args.get("start_date", config_harvester['dump_default_start_date']),
        "end_date": args.get("end_date", current_date),
        "interval": args.get("interval", 'day')
    }
    # process arguments
    total_number_of_partitions = args.get("total_number_of_partitions", 100)
    config_harvester['files_prefix'] = args.get("process_files_prefix", datetime.datetime.now().strftime("%Y-%m-%d"))

    if _is_files_list_splittable_into_mutiple_partitions(total_number_of_partitions):
        partitions = list(_get_partitions(total_number_of_partitions))
    else:
        logger.info(
            f"Number of files in directory {config_harvester['raw_dump_folder_name']} not enough to create partitions")
        total_number_of_partitions = 1
        partitions = [_list_files_in_directory(config_harvester['raw_dump_folder_name'],
                                               config_harvester['files_extenxion'])]

    # affiliation arguments
    number_of_partitions = args.get("affiliations_number_of_partitions", 10_000)
    affiliations_source_file = args.get("affiliations_source_file")

    with Connection(redis.from_url(current_app.config["REDIS_URL"])):

        q = Queue("harvest-datacite", default_timeout=150 * 3600)

        # harvest
        task_harvest_dois = q.enqueue(create_task_harvest_dois, **harvester_kwargs)
        response_objects.append({"status": "success", "data": {"task_id": task_harvest_dois.get_id()}})

        # Create task process
        task_process_list = []
        for index_of_partition in range(total_number_of_partitions):
            process_task_kwargs = {
                "partition_index": index_of_partition,
                "files_in_partition": partitions[index_of_partition],
            }
            task_process = q.enqueue(create_task_process_dois, depends_on=task_harvest_dois,
                                     **process_task_kwargs)
            task_process_list.append(task_process)
            response_objects.append({"status": "success", "data": {"task_id": task_process.get_id()}})

        # create task consolidation
        task_consolidate_processed_files = q.enqueue(create_task_consolidate_processed_files,
                                                     total_number_of_partitions,
                                                     depends_on=task_process_list)

        response_objects.append({"status": "success", "data": {"task_id": task_consolidate_processed_files.get_id()}})

        # create task affiliations
        task_affiliations_list = []
        for partition_index in range(number_of_partitions + 1):
            affiliations_task_kwargs = {
                "affiliations_source_file": affiliations_source_file,
                "partition_index": partition_index,
                "total_partition_number": number_of_partitions,
                "job_timeout": 2 * 3600,
            }
            task_affiliation = q.enqueue(create_task_match_affiliations_partition,
                                         depends_on=task_consolidate_processed_files,
                                         **affiliations_task_kwargs)
            task_affiliations_list.append(task_affiliation)
            response_objects.append({"status": "success", "data": {"task_id": task_affiliation.get_id()}})

        # create task affiliation files consolidation
        task_consolidate_affiliations_files = q.enqueue(create_task_consolidate_results,
                                                        depends_on=task_affiliations_list)
        response_objects.append(
            {"status": "success", "data": {"task_id": task_consolidate_affiliations_files.get_id()}})

        # create task enrichment
        task_enrichment_list = []
        for partition in partitions:
            enrichment_task_kwargs = {
                "partition_files": partition,
                "job_timeout": 2 * 3600,
            }
            task_enrichment = q.enqueue(create_task_enrich_dois,
                                        depends_on=task_consolidate_affiliations_files,
                                        **enrichment_task_kwargs)

            task_enrichment_list.append(task_enrichment)
            response_objects.append({"status": "success", "data": {"task_id": task_enrichment.get_id()}})

    return jsonify(response_objects), 202
