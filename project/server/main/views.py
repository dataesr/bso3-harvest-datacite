import datetime
# from glob import glob
import os
from typing import List

import redis
from application.utils_processor import _list_files_in_directory
from config.global_config import config_harvester
from flask import Blueprint, current_app, jsonify, render_template, request
from project.server.main.logger import get_logger
from project.server.main.re3data import get_list_re3data_repositories, enrich_re3data 
from config.global_config import MOUNTED_VOLUME_PATH
from project.server.main.tasks import (
    run_task_consolidate_processed_files,
    run_task_consolidate_results,
    run_task_dump_files,
    run_task_enrich_dois,
    run_task_harvest_dois,
    run_task_import_elastic_search,
    run_task_match_affiliations_partition,
    run_task_process_dois,
    update_bso_publications,
    update_french_authors,
    update_french_rors
)
from project.server.main.pdb import update_pdbs
from rq import Connection, Queue

main_blueprint = Blueprint(
    "main",
    __name__,
)

logger = get_logger(__name__)

# @deprecated("This function is not use anymore")
def get_partitions(files: List[str], number_of_partitions: int = None, partition_size: int = None) -> List[List[str]]:
    """Return a list of partitions of files.
    If partition_size > len(files), returns one partition
    If number_of_partitions > len(files), returns as many partitions as there are files
    If len(files) % number_of_partitions != 0, returns (number_of_partitions + 1) partitions
    """
    if len(files) == 0:
        return []
    if partition_size is None:
        number_of_partitions = min(max(1, number_of_partitions), len(files))
        partition_size = len(files) // number_of_partitions
    else:
        partition_size = min(max(1, partition_size), len(files))
    return [files[i : i + partition_size] for i in range(0, len(files), partition_size)]


@main_blueprint.route("/", methods=["GET"])
def home():
    return render_template("main/index.html")


@main_blueprint.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue("harvest-datacite")
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


# @deprecated("This endpoint is not use anymore")
@main_blueprint.route("/harvest_dois", methods=["GET"])
def create_task_harvest_dois():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    # args = request.get_json(force=True)
    args = {}
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
        q = Queue("harvest-datacite", default_timeout=1500 * 3600)
        task = q.enqueue(run_task_harvest_dois, **task_kwargs)

    response_object = {"status": "success", "data": {"task_id": task.get_id()}}

    return jsonify(response_object), 202


# @deprecated("This endpoint is not use anymore")
@main_blueprint.route("/process", methods=["POST"])
def create_task_process_dois():
    args = request.get_json(force=True)
    response_objects = []
    if args.get('process', True):
        total_number_of_partitions = args.get("total_number_of_partitions", 100)
        file_prefix = args.get("file_prefix")
        dump_files = _list_files_in_directory(config_harvester["raw_dump_folder_name"], "*" + config_harvester["datacite_file_extension"])
        logger.debug(f'{len(dump_files)} dump_files')
        partitions = get_partitions(dump_files, number_of_partitions=total_number_of_partitions)
        tasks_list = []
        with Connection(redis.from_url(current_app.config["REDIS_URL"])):
            q = Queue("harvest-datacite", default_timeout=1500 * 3600)
            for i, partition in enumerate(partitions):
                task_kwargs = {
                    "partition_index": i,
                    "files_in_partition": partition,
                }
                task = q.enqueue(run_task_process_dois, **task_kwargs)
                response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
                logger.debug({task.get_id()})
                tasks_list.append(task)
            # consolidate files
            consolidate_task_kwargs = {
                "file_prefix": file_prefix,
            }
            task_consolidate_processed_files = q.enqueue(run_task_consolidate_processed_files,
                                                     **consolidate_task_kwargs,
                                                     depends_on=tasks_list
                                                    )
            response_objects.append({"status": "success", "data": {"task_id": task_consolidate_processed_files.get_id()}})
    return jsonify(response_objects), 202


# @deprecated("This endpoint is not use anymore")
@main_blueprint.route("/affiliations", methods=["POST"])
def create_task_affiliations():
    args = request.get_json(force=True)
    response_objects = []
    number_of_partitions = args.get("number_of_partitions", 100)
    file_prefix = args.get("file_prefix")
    tasks_list = []
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=1500 * 3600)
        for partition_index in range(number_of_partitions + 1):
            task_kwargs = {
                "file_prefix": file_prefix,
                "partition_index": partition_index,
                "total_partition_number": number_of_partitions,
                "job_timeout": 20 * 3600,
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
    skip_re3data = args.get("skip_re3data", False)
    if skip_re3data == False:
        get_list_re3data_repositories()
        enrich_re3data()
    response_objects = []
    # partition_size = args.get("partition_size", 8)
    index_name = args.get("index_name")
    # new_index_name = args.get("new_index_name")
    output_filename =  f'{MOUNTED_VOLUME_PATH}/{index_name}.jsonl'
    logger.debug(f'remove {output_filename}')
    os.system(f'rm -rf {output_filename}')
    # datacite_dump_files = glob(os.path.join(
    #     config_harvester['raw_dump_folder_name'],
    #     '*' + config_harvester['datacite_file_extension'])
    # )
    # partitions = get_partitions(datacite_dump_files, partition_size=partition_size)
    # partition = get_partitions(datacite_dump_files, number_of_partitions=1)[0] # only one partition
    # datacite_dump_files.sort(reverse=True)
    # partition = datacite_dump_files
    datacite_files = []
    for f in os.listdir('/data/dois/'):
        if f.startswith('updated'):
            for f2 in os.listdir(f'/data/dois/{f}'):
                if f2.endswith('.jsonl.gz'):
                    datacite_files.append(f'/data/dois/{f}/{f2}')
    datacite_files.sort()
    partition = datacite_files
    if args.get('update_publications', True):
        update_bso_publications()
    if args.get('update_french_authors', False):
        update_french_authors()
    if args.get('update_french_rors', True):
        update_french_rors()
    if args.get('update_pdb', False):
        update_pdbs()
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=1500 * 3600)
        # for partition in partitions:
            # task = q.enqueue(run_task_enrich_dois, partition, index_name)
        task = q.enqueue(run_task_enrich_dois, partition, index_name, index_name)
        response_objects.append({"status": "success", "data": {"task_id": task.get_id()}})
    return jsonify(response_objects), 202


# @deprecated("This endpoint is not use anymore")
@main_blueprint.route("/create_index", methods=["POST"])
def create_task_import_elastic_search():
    args = request.get_json(force=True)
    index_name = args.get("index_name")
    new_index_name = args.get("new_index_name", index_name)
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=1500 * 3600)
        task = q.enqueue(run_task_import_elastic_search, index_name, new_index_name)
        response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202


@main_blueprint.route("/dump_files", methods=["POST"])
def create_task_dump_files():
    with Connection(redis.from_url(current_app.config["REDIS_URL"])):
        q = Queue(name="harvest-datacite", default_timeout=1500 * 3600)
        task = q.enqueue(run_task_dump_files)
        response_object = {"status": "success", "data": {"task_id": task.get_id()}}
    return jsonify(response_object), 202