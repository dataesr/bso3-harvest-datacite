import itertools
from datetime import datetime
import json
from json import JSONDecodeError
from pathlib import Path
from os import PathLike
from typing import Union, Dict, List, Tuple, Generator, Any
import pandas as pd

from config.exceptions import FileLoadingException
from config.global_config import config_harvester
from project.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL
from tqdm import tqdm

logger = get_logger(__name__, level=LOGGER_LEVEL)


def _list_dump_files_in_directory():
    dump_folder_path = _get_path(config_harvester['raw_dump_folder_name'])
    return list(dump_folder_path.glob(config_harvester["files_extenxion"]))


def _merge_files(list_of_files: List[Union[str, Path]], target_file_path: Path):
    pd.concat([pd.read_csv(file) for file in list_of_files]).to_csv(f"{target_file_path}", index=False, low_memory=False)


def _list_files_in_directory(folder: Union[str, Path], regex: str):
    folder_path = _get_path(folder)
    return list(folder_path.glob(regex))


def _is_files_list_splittable_into_mutiple_partitions(total_number_of_partitions) -> bool:
    list_of_files_in_dump_folder = _list_files_in_directory(config_harvester['raw_dump_folder_name'],
                                                            config_harvester['files_extenxion'])
    return len(list_of_files_in_dump_folder) > total_number_of_partitions


def _get_partitions(total_number_of_partitions) -> Generator[List, Any, None]:
    # TODO optimize the partition to have list of equal size in Mb
    list_of_files_in_dump_folder = _list_files_in_directory(config_harvester['raw_dump_folder_name'],
                                                            config_harvester['files_extenxion'])
    try:
        size_of_partitions = len(list_of_files_in_dump_folder) // total_number_of_partitions if len(
            list_of_files_in_dump_folder) > total_number_of_partitions else len(list_of_files_in_dump_folder)
        for index in range(0, len(list_of_files_in_dump_folder), size_of_partitions):
            yield list(itertools.islice(list_of_files_in_dump_folder, index, index + size_of_partitions))
    except KeyboardInterrupt as e:
        logger.exception(f"Exception occured while processing files. \n Detailed exception  {e}")
        raise SystemExit


def _concat_affiliation(doi: Dict, objects_to_use_for_concatenation: str):
    list_creators_or_contributors_and_affiliations = []

    for object_to_use_for_concatenation in doi["attributes"][objects_to_use_for_concatenation]:

        doi_id: Union[str, List[str]] = str(doi["id"]).lower()
        doi_file_name: Union[str, List[str]] = str(doi["mapped_id"]).lower()
        doi_publisher: Union[str, List[str]] = str(_safe_get("", doi, "attributes", "publisher")).lower()
        doi_client_id: Union[str, List[str]] = str(_safe_get("", doi, "relationships", "client", "data", "id"))
        list_affiliation_of_object: List[Dict[str, Union[str, List[str]]]]

        if len(object_to_use_for_concatenation) > 0 and len(object_to_use_for_concatenation["affiliation"]) > 0:
            list_of_affiliation = [
                _concat_affiliation_of_creator_or_contributor(affiliation, exclude_list=["affiliationIdentifierScheme"])
                if len(affiliation) > 0
                else ""
                for affiliation in object_to_use_for_concatenation["affiliation"]
            ]

            list_affiliation_of_object = [
                {
                    "doi_id": doi_id,
                    "doi_file_name": doi_file_name,
                    "type": objects_to_use_for_concatenation,
                    "name": _retrieve_object_name_or_given_name(object_to_use_for_concatenation),
                    "doi_publisher": doi_publisher,
                    "doi_client_id": doi_client_id,
                    "affiliation": affiliation,
                }
                for affiliation in list_of_affiliation
            ]
            list_creators_or_contributors_and_affiliations += list_affiliation_of_object
        else:
            list_affiliation_of_object = [
                {
                    "doi_id": doi_id,
                    "doi_file_name": doi_file_name,
                    "type": objects_to_use_for_concatenation,
                    "name": _retrieve_object_name_or_given_name(object_to_use_for_concatenation),
                    "doi_publisher": doi_publisher,
                    "doi_client_id": doi_client_id,
                    "affiliation": "",
                }
            ]
            list_creators_or_contributors_and_affiliations += list_affiliation_of_object
    return list_creators_or_contributors_and_affiliations


def _retrieve_object_name_or_given_name(creator_or_contributor: Dict):
    if "name" in creator_or_contributor.keys():
        return str(creator_or_contributor["name"])
    elif (
            "givenName" in creator_or_contributor.keys()
            and "familyName" in creator_or_contributor.keys()
    ):
        return str(creator_or_contributor["givenName"]) + " " + str(creator_or_contributor["familyName"])
    elif "givenName" in creator_or_contributor.keys():
        return str(creator_or_contributor["givenName"])
    elif "familyName" in creator_or_contributor.keys():
        return str(creator_or_contributor["familyName"])
    else:
        return ""


def _concat_affiliation_of_creator_or_contributor(affiliation: Dict, exclude_list: List):
    processed_affiliation = {
        key: value
        for (key, value) in affiliation.items()
        if (key not in exclude_list and value is not None)
    }
    if "affiliationIdentifier" in processed_affiliation:
        return str(processed_affiliation["affiliationIdentifier"]).lower()
    else:
        return " ".join(map(str, filter(None, processed_affiliation.values()))).lower()


def _get_path(folder_name: Union[str, PathLike]):
    current_path = Path.cwd()
    if isinstance(folder_name, str):
        current_path = Path(folder_name)
        if not current_path.exists():
            current_path.mkdir()
    assert current_path.exists() and current_path.is_dir()
    return current_path


def _format_string(string):
    return string.replace("/", "_").replace(":", "-").replace("*", "")


def _create_file(target_directory: Union[Path, str], file_name: str = "global_affiliation.csv") -> Tuple[bool, Path]:
    """
    :type target_directory: Path or str
    """
    file_path = Path(target_directory) / file_name
    already_exist = True

    if not file_path.is_file():
        already_exist = False
        file_path.touch()

    return already_exist, file_path


def _safe_get(default_value, _dict, *keys):
    for key in keys:
        try:
            _dict = _dict[key]
        except (TypeError, KeyError):
            return default_value
    return _dict


def _append_file(affiliation: pd.DataFrame, target_file: Union[str, Path], append_header=False):
    if affiliation.shape[0] > 0:
        affiliation.to_csv(target_file, mode="a", index=False, header=append_header, encoding="utf-8")


def _load_csv_file_and_drop_duplicates(global_affiliations_file_path: Union[Path, str],
                                       names=None,
                                       separator: str = ",", header=None):
    if names is None:
        names = ["doi_publisher", "doi_client_id", "affiliation"]

    try:
        global_affiliation = pd.read_csv(
            global_affiliations_file_path,
            sep=separator,
            names=names,
            header=header,
            lineterminator="\n"
        )
    except FileLoadingException as e:
        logger.exception(f"Failed to load file {global_affiliations_file_path} . Detailed messages {e}")

    if global_affiliation.shape[0] > 0:
        global_affiliation.drop_duplicates(inplace=True)
        global_affiliation.to_csv(global_affiliations_file_path, index=False, header=False, encoding="utf-8")
