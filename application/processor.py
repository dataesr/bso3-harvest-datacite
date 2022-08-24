from domain.api.abstract_processor import AbstractProcessor
from .utils_processor import (
    _list_dump_files_and_get_target_directory,
    split_dump_file_concat_and_save_doi_files,
)
from os import PathLike
from typing import Union, List


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

    def __init__(self, source_folder: Union[str, PathLike], target_folder_name: str):
        self.source_folder = source_folder
        self.target_folder_name = target_folder_name
        self.list_of_files, self.target_directory = _list_dump_files_and_get_target_directory(
            source_folder, target_folder_name=target_folder_name
        )

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
            self.source_folder, target_folder_name=self.target_folder_name
        )

    # TODO push Doi to db once
    def push_dois_to_db(self):
        pass


if __name__ == "__main__":
    processor = Processor(
        r"C:\Users\maurice.ketevi\Documents\Projects\BSO\bso3-harvest-datacite\tests\unit_test\application", "test_dois"
    )
    from pathlib import Path
    #processor.process
    files = Path(processor.source_folder).glob("*.ndjson")
    list_files = list(files)
    print(len(processor.list_of_files))
