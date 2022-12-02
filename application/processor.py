from glob import glob
import json
import os
import shutil
from datetime import datetime
from json import JSONDecodeError
from os import path
from pathlib import Path
from typing import Union, Dict, List, Generator, Any, Tuple
import pandas as pd

from adapters.databases.process_state_repository import ProcessStateRepository
from adapters.databases.process_state_table import ProcessStateTable
from adapters.storages.swift_session import SwiftSession
from domain.api.abstract_processor import AbstractProcessor
from domain.model.ovh_path import OvhPath
from application.utils_processor import (
    _create_file, _load_csv_file_and_drop_duplicates,
    _append_file, _format_string, _concat_affiliation, _list_files_in_directory, _merge_files,
    _get_path, compress, json_line_generator,
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
    tmp_directory_name: str = ""
    tmp_directory_path: str = ""

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
        self.tmp_directory_name = config['processed_tmp_folder_name']
        self.tmp_directory_path = _get_path(config['processed_tmp_folder_name'])

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
    def get_affiliations(path_file: Path, partition_index: int):
        affiliations = []
        non_null_dois = 0
        null_dois = 0
        processed_dois_per_file = 0

        for json_obj in json_line_generator(path_file):
            for doi in json_obj.get('data'):
                doi["mapped_id"] = _format_string(doi["id"])
                current_size = len(affiliations)
                try:
                    affiliations += _concat_affiliation(doi, "creators", path_file)
                    affiliations += _concat_affiliation(doi, "contributors", path_file)
                except BaseException as e:
                    logger.exception(f'Error while creating concat for {doi["id"]}. \n Detailed error {e}')

                if len(affiliations) > current_size:
                    non_null_dois += 1
                else:
                    null_dois += 1

            processed_dois_per_file += len(json_obj["data"])

        logger.info(
            f'partition_index : {partition_index} {path_file} number of dois {processed_dois_per_file} number of non null dois {non_null_dois} and null dois {null_dois}')

        return processed_dois_per_file, non_null_dois, null_dois, affiliations

    def process_list_of_files_in_partition(self) -> Tuple[int, List[Dict]]:
        """
            Process a partition of files, retrieve the affiliations per creator or contributors
            store the result in a detailed affiliation file and consolidated affiliation files
        :return: the number of total processed dois and the list of files and their associated status
        """

        # TODO Modify state to False if needed
        processed_files_and_status = []
        # counter variables
        global_processed_dois = 0
        global_non_null_dois = 0
        global_null_dois = 0

        for index, path_file in enumerate(self.list_of_files_in_partition):
            logger.info(f"Processing {index} / {len(self.list_of_files_in_partition)}")
            (processed_dois_per_file, non_null_dois, null_dois, affiliations_list)\
                = self.get_affiliations(path_file, self.partition_index)

            global_processed_dois += processed_dois_per_file
            global_non_null_dois += non_null_dois
            global_null_dois += null_dois

            affiliations_df = pd.DataFrame(affiliations_list)

            if affiliations_df.shape[0] > 0:
                global_affiliations = affiliations_df[["doi_publisher", "doi_client_id", "affiliation"]].drop_duplicates()
                _append_file(global_affiliations, self.partition_consolidated_affiliation_file_path)

            _append_file(affiliations_df, self.partition_detailed_affiliation_file_path)
            # Append list of path dictionary
            processed_status = {
                "file_name": path_file.name,
                "file_path": path_file,
                "number_of_dois": processed_dois_per_file,
                "number_of_dois_with_null_attributes": null_dois,
                "number_of_non_null_dois": non_null_dois,
                "process_date": datetime.now(),
                "processed": True,
            }

            self.push_state_to_database(processed_status)
            # push to postgreSQL
            processed_files_and_status.append(processed_status)

        logger.info(
            f' Partition index : {self.partition_index} Total number of processed dois {global_processed_dois}, total non null dois '
            f'{global_non_null_dois} total null dois {global_null_dois}')

        # Filter out global affiliation files
        _load_csv_file_and_drop_duplicates(self.partition_consolidated_affiliation_file_path,
                                           ["doi_publisher", "doi_client_id", "affiliation"])

        return global_processed_dois, processed_files_and_status

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

    def __init__(self, config, total_number_of_partitions, file_prefix):
        self.config = config

        self.target_folder_name = config['processed_dump_folder_name']
        self.tmp_directory_name = config['processed_tmp_folder_name']
        self.tmp_directory_path = config['processed_tmp_folder_path']

        self.partition_consolidated_affiliation_file_name_pattern = f"partition_consolidated_affiliations_*.csv"
        self.partition_detailed_affiliation_file_name_pattern = f"partition_detailed_affiliations_*.csv"

        self.list_of_consolidated_affiliation_files, self.list_of_detailed_affiliation_files = self._get_list_of_files()

        _, self.global_detailed_affiliation_file_path = _create_file(self.target_folder_name,
                                                                     f"{file_prefix}_{self.config['detailed_affiliation_file_name']}")
        _, self.global_consolidated_affiliation_file_path = _create_file(self.target_folder_name,
                                                                         f"{file_prefix}_{self.config['global_affiliation_file_name']}")

        self.swift = None
        is_swift_config = ("swift" in self.config) and len(self.config["swift"]) > 0
        if is_swift_config:
            self.swift = SwiftSession(self.config['swift'])

        self.total_number_of_partitions = total_number_of_partitions

    def process_files(self):
        # Merge consolidated files
        logger.debug(f"merging consolidated_affiliation_files: {self.list_of_consolidated_affiliation_files}")
        _merge_files(self.list_of_consolidated_affiliation_files, self.global_consolidated_affiliation_file_path)
        # Merge detailed files
        logger.debug(f"merging detailed_affiliation_files: {self.list_of_detailed_affiliation_files}")
        _merge_files(self.list_of_detailed_affiliation_files, self.global_detailed_affiliation_file_path)

    def _get_list_of_files(self) -> Tuple[List[Union[str, Path]], List[Union[str, Path]]]:
        return _list_files_in_directory(self.target_folder_name,
                                        self.partition_consolidated_affiliation_file_name_pattern), \
               _list_files_in_directory(self.target_folder_name, self.partition_detailed_affiliation_file_name_pattern)

    def push_to_ovh(self):
        compressed_global_file = compress(str(self.global_consolidated_affiliation_file_path))
        upload_object(
            self.config["datacite_container"],
            source=compressed_global_file,
            target=OvhPath(self.config['processed_datacite_container'],
                           path.basename(compressed_global_file)).__str__(),
        )
        compressed_detailed_file = compress(str(self.global_detailed_affiliation_file_path))
        upload_object(
            self.config["datacite_container"],
            source=compressed_detailed_file,
            target=OvhPath(self.config['processed_datacite_container'],
                           path.basename(compressed_detailed_file)).__str__(),
        )

    def clear_local_directory(self):
        for f in glob(self.config['processed_dump_folder_name'] + "/partition_*"):
            os.remove(f)


if __name__ == "__main__":
    processor_controller = ProcessorController(config_harvester, 100, "")
    processor_controller.process_files()
    processor_controller.push_to_ovh()
    processor_controller.clear_local_directory()
