import os

from adapters.databases.process_state_repository import ProcessStateRepository
from adapters.storages.swift_session import SwiftSession
from domain.api.abstract_processor import AbstractProcessor
from domain.ovh_path import OvhPath
from application.utils_processor import (
    _list_dump_files_and_get_target_directory,
    split_dump_file_concat_and_save_doi_files,
)
from typing import List
from config.global_config import config_harvester


class Processor(AbstractProcessor):
    """
    The Processor object is able to process downloaded files, split into doi.json and save it to mongodb

    Args:

    Attributes:

    """

    source_folder: str = ""
    target_folder_name: str = ""
    target_directory: str = ""
    list_of_files: List = []
    detailed_affiliation_file_path: str = ""
    global_affiliation_file_path: str = ""

    def __init__(self, config):
        self.config = config
        self.source_folder = config['raw_dump_folder_name']
        self.target_folder_name = config['processed_dump_folder_name']
        self.list_of_files, self.target_directory = _list_dump_files_and_get_target_directory(
            config['raw_dump_folder_name'], target_folder_name=config['processed_dump_folder_name']
        )
        self.detailed_affiliation_file_path = config['detailed_affiliation_file_name']
        self.global_affiliation_file_path = config['global_affiliation_file_name']
        self.swift = None
        self.process_state = None

        is_swift_config = ("swift" in self.config) and len(self.config["swift"]) > 0
        if is_swift_config:
            self.swift = SwiftSession(self.config['swift'])

        is_process_state_config = ()
        if is_process_state_config:
            ProcessStateRepository()

    def process(self, use_thread=True):
        """
            The process function,
            loop through all dumped files from datacite,
            split those in doi.json
            proceed to concatenation.

        Args:

        Returns:

        """
        return split_dump_file_concat_and_save_doi_files(
            self.source_folder, target_folder_name=self.target_folder_name,
            detailed_affiliation_file_name=self.detailed_affiliation_file_path,
            global_affiliation_file_name=self.global_affiliation_file_path
        )

    # TODO push Doi to db once
    def push_dois_to_db(self):
        pass

    # TODO push Doi to db once
    def push_dois_to_ovh(self):
        self.swift.upload_files_to_swift(config_harvester['datacite_container'],
                                         [(self.global_affiliation_file_path,
                                           OvhPath(config_harvester['processed_datacite_container'],
                                                   os.path.basename(self.global_affiliation_file_path)))])
        self.swift.upload_files_to_swift(config_harvester['datacite_container'],
                                         [(self.detailed_affiliation_file_path,
                                           OvhPath(config_harvester['processed_datacite_container'],
                                                   os.path.basename(self.detailed_affiliation_file_path)))])


if __name__ == "__main__":
    processor = Processor(config_harvester)
    processor.process()
    processor.push_dois_to_ovh()
    # processor.process
