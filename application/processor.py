import json
import shutil
from datetime import datetime
from json import JSONDecodeError
from os import path
from pathlib import Path
from typing import Union, Dict, List, Generator, Any, Tuple
import pandas as pd

from adapters.databases.postgres_session import PostgresSession
from adapters.databases.process_state_repository import ProcessStateRepository
from adapters.databases.process_state_table import ProcessStateTable
from adapters.storages.swift_session import SwiftSession
from domain.api.abstract_processor import AbstractProcessor
from domain.ovh_path import OvhPath
from application.utils_processor import (
    _create_file, _load_csv_file_and_drop_duplicates,
    _append_file, _format_string, _concat_affiliation, _get_partitions, _list_files_in_directory, _merge_files,
    _get_path,
)
from config.global_config import config_harvester
from project.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL
from tqdm import tqdm

from project.server.main.utils_swift import upload_object

logger = get_logger(__name__, level=LOGGER_LEVEL)


class Processor(AbstractProcessor):
    """
    The Processor object is able to process downloaded files, split into doi.json and save it to mongodb

    Args:

    Attributes:

    """
    source_folder: str = ""
    target_folder_name: str = ""
    target_directory: str = ""

    # TODO remove this part of the code
    list_of_files: List = []

    list_of_files_in_partition: List = [Union[str, Path]]

    detailed_affiliation_file_path: str = ""
    global_affiliation_file_path: str = ""

    partition_detailed_affiliation_file_name: str = ""
    partition_consolidated_affiliation_file_name: str = ""

    partition_consolidated_affiliation_file_path: Union[str, Path] = None
    partition_detailed_affiliation_file_path: Union[str, Path] = None

    partition_index: int = 0
    total_number_of_partitions: int = 1
    partitions: List[Generator[List, Any, None]] = None

    process_state_repository: ProcessStateRepository

    def __init__(self, config, index_of_partition: int, files_in_partition: List[Union[str, Path]],
                 repository: ProcessStateRepository):
        self.config = config
        self.source_folder = config['raw_dump_folder_name']
        self.target_folder_name = config['processed_dump_folder_name']
        self.target_directory = _get_path(config['processed_dump_folder_name'])

        # TODO : Remove this part of the code

        # TODO : Remove this part of the code
        self.detailed_affiliation_file_path = config['detailed_affiliation_file_name']
        self.global_affiliation_file_path = config['global_affiliation_file_name']

        self.swift = None
        self.process_state = None

        self.partition_consolidated_affiliation_file_name = f"partition_consolidated_affiliations_{index_of_partition}.csv"
        self.partition_detailed_affiliation_file_name = f"partition_detailed_affiliations_{index_of_partition}.csv"
        self._create_affiliation_files()

        self.partition_index = index_of_partition
        self.process_state_repository = repository

        self.list_of_files_in_partition = self.retrieve_files_status_in_db_and_filter_out_list_of_files_in_partition(
            files_in_partition)

        is_swift_config = ("swift" in self.config) and len(self.config["swift"]) > 0
        if is_swift_config:
            self.swift = SwiftSession(self.config['swift'])

    @staticmethod
    def get_list_creators_or_contributors_and_affiliations(path_file: Path, partition_index: int):
        json_obj: Dict
        list_creators_or_contributors_and_affiliations = []
        number_of_non_null_dois = 0
        number_of_null_dois = 0
        number_of_processed_dois_per_file = 0

        for jsonstring in path_file.open("r", encoding="utf-8"):
            if jsonstring.strip() != "":
                try:
                    json_obj: dict = json.loads(jsonstring)
                except (TypeError, JSONDecodeError) as e:
                    logger.error(f"error  reading line in file {str(path_file)}. Detailed error {e}")
                if json_obj["data"]:
                    for idx, doi in enumerate(json_obj["data"]):
                        doi["mapped_id"] = _format_string(doi["id"])
                        current_size = len(list_creators_or_contributors_and_affiliations)
                        try:
                            list_creators_or_contributors_and_affiliations += _concat_affiliation(doi, "creators")
                            list_creators_or_contributors_and_affiliations += _concat_affiliation(doi, "contributors")
                        except BaseException as e:
                            logger.exception(f'Error while creating concat for {doi["id"]}. \n Detailed error {e}')

                        if len(list_creators_or_contributors_and_affiliations) > current_size:
                            number_of_non_null_dois += 1
                        else:
                            number_of_null_dois += 1

                    number_of_processed_dois_per_file = number_of_processed_dois_per_file + len(json_obj["data"])

        logger.info(
            f'partition_index : {partition_index} {path_file} number of dois {number_of_processed_dois_per_file} number of non null dois {number_of_non_null_dois} and null dois {number_of_null_dois}')

        return number_of_processed_dois_per_file, number_of_non_null_dois, number_of_null_dois, list_creators_or_contributors_and_affiliations

    def process_list_of_files_in_partition(self) -> Tuple[int, List[Dict]]:
        """
            Process a partition of files, retrieve the affiliations per creator or contributors
            store the result in a detailed affiliation file and consolidated affiliation files
        :return: the number of total processed dois and the list of files and their associated status
        """

        processed_files_and_status = []

        global_number_of_processed_dois = 0
        global_number_of_non_null_dois = 0
        global_number_of_null_dois = 0

        for index, path_file in enumerate(tqdm(self.list_of_files_in_partition)):

            number_of_processed_dois_per_file, number_of_non_null_dois, \
            number_of_null_dois, list_creators_or_contributors_and_affiliations = \
                self.get_list_creators_or_contributors_and_affiliations(path_file, self.partition_index)

            global_number_of_processed_dois = global_number_of_processed_dois + number_of_processed_dois_per_file
            global_number_of_non_null_dois = global_number_of_non_null_dois + number_of_non_null_dois
            global_number_of_null_dois = global_number_of_null_dois + number_of_null_dois

            affiliation = pd.DataFrame.from_dict(list_creators_or_contributors_and_affiliations)

            if affiliation.shape[0] > 0:
                global_affiliation = affiliation[["doi_publisher", "doi_client_id", "affiliation"]].drop_duplicates()
                _append_file(global_affiliation, self.partition_consolidated_affiliation_file_path)

            _append_file(affiliation, self.partition_detailed_affiliation_file_path)
            # Append list of path dictionary
            processed_status = {
                "file_name": path_file.name,
                "file_path": path_file,
                "number_of_dois": number_of_processed_dois_per_file,
                "number_of_dois_with_null_attributes": number_of_null_dois,
                "number_of_non_null_dois": number_of_non_null_dois,
                "process_date": datetime.now(),
                "processed": True,
            }

            self.push_state_to_database(processed_status)
            # push to postgreSQL
            processed_files_and_status.append(processed_status)

        logger.info(
            f' Partition index : {self.partition_index} Total number of processed dois {global_number_of_processed_dois}, total non null dois '
            f'{global_number_of_non_null_dois} total null dois {global_number_of_null_dois}')

        # Filter out global affiliation files
        _load_csv_file_and_drop_duplicates(self.partition_consolidated_affiliation_file_path,
                                           ["doi_publisher", "doi_client_id", "affiliation"])

        return global_number_of_processed_dois, processed_files_and_status

    def retrieve_files_status_in_db_and_filter_out_list_of_files_in_partition(self, files_list_in_partition):
        existing_list_of_files = [filepath['file_path'] for filepath in self.process_state_repository.get()]
        return list(set(files_list_in_partition) - set(existing_list_of_files))

    def _create_affiliation_files(self):
        logger.info(f'Creating files {self.partition_detailed_affiliation_file_name}'
                    f' and {self.partition_consolidated_affiliation_file_name}')

        already_exist, self.partition_consolidated_affiliation_file_path = \
            _create_file(target_directory=self.target_folder_name,
                         file_name=self.partition_consolidated_affiliation_file_name)
        already_exist, self.partition_detailed_affiliation_file_path = \
            _create_file(target_directory=self.target_folder_name,
                         file_name=self.partition_detailed_affiliation_file_name)

    def push_state_to_database(self, state: Dict):

        process_state = ProcessStateTable(
            file_name=state['file_name'],
            file_path=str(state['file_path']),
            number_of_dois=state['number_of_dois'],
            number_of_dois_with_null_attributes=state['number_of_dois_with_null_attributes'],
            number_of_non_null_dois=state['number_of_non_null_dois'],
            process_date=state['process_date'],
            processed=state['processed'],
        )
        return self.process_state_repository.create(process_state)

    def push_dois_to_ovh(self):
        self.swift.upload_files_to_swift(self.config['datacite_container'],
                                         [OvhPath(self.config['processed_datacite_container'],
                                                  path.basename(self.partition_detailed_affiliation_file_path)),
                                          OvhPath(self.config['processed_datacite_container'],
                                                  path.basename(self.partition_consolidated_affiliation_file_path))])


class ProcessorController:
    """
        The master controller that will enable the creation of multiple processor and the retrieval of their respective
        status
        Args: the number of partitions to create
        Attributes:
        """

    target_folder_name: str = ""
    ovh_directory: str = ""
    list_of_files: List = []

    global_detailed_affiliation_file_path: Path
    global_consolidated_affiliation_file_path: Path
    total_number_of_partitions: int = 1
    partitions: List[Dict] = None

    def __init__(self, config, total_number_of_partitions):
        self.config = config
        self.target_folder_name = config['processed_dump_folder_name']

        self.partition_consolidated_affiliation_file_name_pattern = f"partition_consolidated_affiliations_*.csv"
        self.partition_detailed_affiliation_file_name_pattern = f"partition_detailed_affiliations_*.csv"

        self.list_of_consolidated_affiliation_files, self.list_of_detailed_affiliation_files = self._get_list_of_files()

        self.global_detailed_affiliation_file_path = config['detailed_affiliation_file_name']
        self.global_consolidated_affiliation_file_path = config['global_affiliation_file_name']

        self.swift = None
        is_swift_config = ("swift" in self.config) and len(self.config["swift"]) > 0
        if is_swift_config:
            self.swift = SwiftSession(self.config['swift'])

        self.total_number_of_partitions = total_number_of_partitions

    def process_files(self):
        # Merge consolidated files
        _merge_files(self.list_of_consolidated_affiliation_files, self.global_consolidated_affiliation_file_path)
        # Merge detailed files
        _merge_files(self.list_of_detailed_affiliation_files, self.global_detailed_affiliation_file_path)

    def _get_list_of_files(self) -> Tuple[List[Union[str, Path]], List[Union[str, Path]]]:
        return _list_files_in_directory(self.target_folder_name,
                                        self.partition_consolidated_affiliation_file_name_pattern), \
               _list_files_in_directory(self.target_folder_name, self.partition_detailed_affiliation_file_name_pattern)

    def _push_to_ovh(self):
        upload_object(
            self.config["datacite_container"],
            source=str(self.global_consolidated_affiliation_file_path),
            target=OvhPath(self.config['processed_datacite_container'],
                           path.basename(self.global_consolidated_affiliation_file_path)).__str__(),
        )
        upload_object(
            self.config["datacite_container"],
            source=str(self.global_detailed_affiliation_file_path),
            target=OvhPath(self.config['processed_datacite_container'],
                           path.basename(self.global_detailed_affiliation_file_path)).__str__(),
        )

    def _clear_local_directory(self):
        shutil.rmtree(self.config['processed_tmp_folder_name'])


if __name__ == "__main__":
    processor_controller = ProcessorController(config_harvester, 100)
    processor_controller.process_files()
    processor_controller._push_to_ovh()