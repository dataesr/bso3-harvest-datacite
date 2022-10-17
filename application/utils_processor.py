import itertools
from datetime import datetime
import json
from json import JSONDecodeError
from pathlib import Path
from os import PathLike
from typing import Union, Dict, List, Tuple, Generator, Any
import pandas as pd

from config.global_config import config_harvester
from project.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL
from tqdm import tqdm

logger = get_logger(__name__, level=LOGGER_LEVEL)


def _list_dump_files_and_get_target_directory(
        dump_folder: Union[str, PathLike], target_folder_name: str
):
    path = Path.cwd()

    if isinstance(dump_folder, str):
        path = Path(dump_folder)

    assert path.exists() and path.is_dir()

    target_dois_directory = path.parent / Path(target_folder_name)

    if not target_dois_directory.exists():
        target_dois_directory.mkdir()

    files = path.glob("*.ndjson")

    files_list = list(files)

    return files_list, target_dois_directory


def get_list_creators_or_contributors_and_affiliations(path_file):
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
        f'{path_file} number of dois {number_of_processed_dois_per_file} number of non null dois {number_of_non_null_dois} and null dois {number_of_null_dois}')

    return number_of_processed_dois_per_file, number_of_non_null_dois, number_of_null_dois, list_creators_or_contributors_and_affiliations


def process_list_of_files_per_partition(partition_index: int, list_path: List[Union[Path, str]],
                                        target_directory: str = "dois") -> Tuple[int, List[Dict]]:
    """
        Process a partition of files, retrieve the affiliations per creator or contributors
        store the result in a detailed affiliation file and consolidated affiliation files
    :param partition_index: index of the partition to process
    :param list_path: List of file path to process
    :param target_directory: target directory where to store the processed files
    :return: the list of
    """

    detailed_affiliations_file_name: str = f"detailed_affiliations_{partition_index}.csv",
    concatenated_affiliations_file_name: str = f"global_affiliation{partition_index}.csv"

    logger.info(
        f' Partition index : {partition_index} Processing json files'
        f' Creating files {detailed_affiliations_file_name} and {concatenated_affiliations_file_name}')

    concatenated_affiliations_file_path_already_exist, concatenated_affiliations_file_path = _create_file(
        target_directory, concatenated_affiliations_file_name)
    detailed_affiliations_file_path_already_exist, detailed_affiliations_file_path = _create_file(
        target_directory, detailed_affiliations_file_name)

    processed_files_and_status = []

    global_number_of_processed_dois = 0
    global_number_of_non_null_dois = 0
    global_number_of_null_dois = 0

    for index, path_file in enumerate(tqdm(list_path)):

        number_of_processed_dois_per_file, number_of_non_null_dois, \
        number_of_null_dois, list_creators_or_contributors_and_affiliations = \
            get_list_creators_or_contributors_and_affiliations(path_file)

        global_number_of_processed_dois = global_number_of_processed_dois + number_of_processed_dois_per_file
        global_number_of_non_null_dois = global_number_of_non_null_dois + number_of_non_null_dois
        global_number_of_null_dois = global_number_of_null_dois + number_of_null_dois

        affiliation = pd.DataFrame.from_dict(list_creators_or_contributors_and_affiliations)

        if affiliation.shape[0] > 0:
            global_affiliation = affiliation[["doi_publisher", "doi_client_id", "affiliation"]].drop_duplicates()
            _append_file(global_affiliation, concatenated_affiliations_file_path)

        _append_file(affiliation, detailed_affiliations_file_path)

        # Append list of path dictionary
        processed_status = {'path_file': path_file, 'processed': True, 'processed_date': datetime.now()}
        # push to postgreSQL
        processed_files_and_status.append(processed_status)

    logger.info(
        f' Total number of processed dois {global_number_of_processed_dois}, total non null dois '
        f'{global_number_of_non_null_dois} total null dois {global_number_of_null_dois}')

    # Filter out global affiliation files
    _load_csv_file_and_drop_duplicates(concatenated_affiliations_file_path,
                                       ["doi_publisher", "doi_client_id", "affiliation"])

    return global_number_of_processed_dois, processed_files_and_status


# TODO rework to have list of files in parameter
# TODO Push status to database
# TODO Retrieve list of filename and status from DB
def split_dump_file_concat_and_save_doi_files(
        dump_folder: Union[str, PathLike],
        target_folder_name: str = "dois",
        detailed_affiliation_file_name: str = "detailed_affiliations.csv",
        global_affiliation_file_name: str = "global_affiliation.csv",
):
    list_path, target_directory = _list_dump_files_and_get_target_directory(dump_folder, target_folder_name)

    concatenated_affiliations_file_path_already_exist, concatenated_affiliations_file_path = _create_file(
        target_directory, global_affiliation_file_name)
    detailed_affiliations_file_path_already_exist, detailed_affiliations_file_path = _create_file(
        target_directory, detailed_affiliation_file_name)

    processed_files_and_status = []

    global_number_of_processed_dois = 0
    global_number_of_non_null_dois = 0
    global_number_of_null_dois = 0

    for index, path_file in enumerate(tqdm(list_path)):

        number_of_processed_dois_per_file, number_of_non_null_dois, number_of_null_dois, list_creators_or_contributors_and_affiliations = get_list_creators_or_contributors_and_affiliations(
            path_file)

        global_number_of_processed_dois = global_number_of_processed_dois + number_of_processed_dois_per_file
        global_number_of_non_null_dois = global_number_of_non_null_dois + number_of_non_null_dois
        global_number_of_null_dois = global_number_of_null_dois + number_of_null_dois

        affiliation = pd.DataFrame.from_dict(list_creators_or_contributors_and_affiliations)

        if affiliation.shape[0] > 0:
            global_affiliation = affiliation[["doi_publisher", "doi_client_id", "affiliation"]].drop_duplicates()
            _append_file(global_affiliation, concatenated_affiliations_file_path)

        _append_file(affiliation, detailed_affiliations_file_path)

        # Append list of path dictionary
        processed_files_and_status.append({'path_file': path_file, 'processed': True, 'processed_date': datetime.now()})

    logger.info(
        f' Total number of processed dois {global_number_of_processed_dois}, total non null dois '
        f'{global_number_of_non_null_dois} total null dois {global_number_of_null_dois}')

    # Filter out global affiliation files
    _load_csv_file_and_drop_duplicates(concatenated_affiliations_file_path,
                                       ["doi_publisher", "doi_client_id", "affiliation"])

    return global_number_of_processed_dois, processed_files_and_status


def _list_dump_files_in_directory():
    dump_folder_path = _get_path(config_harvester['raw_dump_folder_name'])
    return list(dump_folder_path.glob(config_harvester["files_extenxion"]))


def _merge_files(list_of_files: List[Union[str, Path]], target_file_path: Path):
    pd.concat([pd.read_csv(file) for file in list_of_files]).to_csv(f"{target_file_path}", index=False)


def _list_files_in_directory(folder: Union[str, Path], regex: str):
    folder_path = _get_path(folder)
    return list(folder_path.glob(regex))


def _get_partitions(total_number_of_partitions) -> Generator[List, Any, None]:
    # TODO optimize the partition to have list of equal size in Mb
    list_of_files_in_dump_folder = _list_files_in_directory(config_harvester['raw_dump_folder_name'],
                                                            config_harvester['files_extenxion'])
    size_of_partitions = len(list_of_files_in_dump_folder) // total_number_of_partitions
    for index in range(0, len(list_of_files_in_dump_folder), size_of_partitions):
        yield list(itertools.islice(list_of_files_in_dump_folder, index, index + size_of_partitions))


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
    global_affiliation = pd.read_csv(
        global_affiliations_file_path,
        sep=separator,
        names=names,
        header=header,
    )

    if global_affiliation.shape[0] > 0:
        global_affiliation.drop_duplicates(inplace=True)
        global_affiliation.to_csv(global_affiliations_file_path, index=False, header=False, encoding="utf-8")
