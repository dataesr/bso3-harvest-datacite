import json
from json import JSONDecodeError
from pathlib import Path
from os import PathLike
from typing import Union, Dict, List, Tuple
import pandas as pd
from project.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL

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


def _format_doi(doi_id):
    doi = doi_id.replace("/", "_").replace(":", "-").replace("*", "")
    return doi


# TODO rework to have list of files in parameter
def split_dump_file_concat_and_save_doi_files(
        dump_folder: Union[str, PathLike], target_folder_name: str = "dois"
        , detailed_affiliation_file_name: str = "detailed_affiliations.csv"
        , global_affiliation_file_name: str = "global_affiliation.csv"
):
    list_path, target_directory = _list_dump_files_and_get_target_directory(dump_folder, target_folder_name)

    global_affiliations_file_path_already_exist, global_affiliations_file_path = _create_affiliation_file(
        target_directory, global_affiliation_file_name)

    detailed_affiliations_file_path_already_exist, detailed_affiliations_file_path = _create_affiliation_file(
        target_directory, detailed_affiliation_file_name)

    number_of_processed_dois = 0

    print(f'list files {list_path}')

    for index, path_file in enumerate(list_path):

        number_of_non_null_dois = 0
        number_of_null_dois = 0

        list_creators_or_contributors_and_affiliations = []

        for jsonstring in path_file.open("r", encoding="utf-8"):

            if jsonstring.strip() != "":

                try:
                    json_obj: dict = json.loads(jsonstring)
                except (TypeError, JSONDecodeError) as e:
                    print(f'error {e}')
                    print(jsonstring[1665930:])

                if json_obj["data"]:

                    for idx, doi in enumerate(json_obj["data"]):
                        doi["mapped_id"] = _format_doi(doi["id"])
                        current_size = len(list_creators_or_contributors_and_affiliations)
                        list_creators_or_contributors_and_affiliations += concat_affiliation(doi, "creators")
                        list_creators_or_contributors_and_affiliations += concat_affiliation(doi, "contributors")

                        if len(list_creators_or_contributors_and_affiliations) > current_size:
                            number_of_non_null_dois += 1
                        else:
                            number_of_null_dois += 1

                    number_of_processed_dois = number_of_processed_dois + idx + 1

        print(
            f'{path_file} number of dois {number_of_processed_dois} number of non null dois {number_of_non_null_dois} and null dois {number_of_null_dois}')

        affiliation = pd.DataFrame.from_dict(list_creators_or_contributors_and_affiliations)

        if affiliation.shape[0] > 0:
            global_affiliation = affiliation[['doi_publisher', 'doi_client_id', 'affiliation']].drop_duplicates()
            _append_affiliation_file(global_affiliation, global_affiliations_file_path)

        _append_affiliation_file(affiliation, detailed_affiliations_file_path)

    # Reduce file size
    # print(f'number of processed dois {number_of_processed_dois}')
    _load_global_affiliation_file_and_drop_duplicates(global_affiliations_file_path)

    return number_of_processed_dois


def _load_global_affiliation_file_and_drop_duplicates(global_affiliations_file_path: Union[Path, str]):
    global_affiliation = pd.read_csv(filepath_or_buffer=global_affiliations_file_path,
                                     sep=",",
                                     names=['doi_publisher', 'doi_client_id', 'affiliation'],
                                     header=None)

    if global_affiliation.shape[0] > 0:
        global_affiliation.drop_duplicates(inplace=True)
        global_affiliation.to_csv(global_affiliations_file_path, index=False, header=False, encoding="utf-8")


def _create_affiliation_file(target_directory: Union[Path, str],
                             file_name: str = "global_affiliation.csv") -> tuple[bool, Path]:
    """

    :type target_directory: Path or str
    """
    file_path = Path(target_directory) / file_name
    already_exist = True

    if not file_path.is_file():
        already_exist = False
        file_path.touch()

    return already_exist, file_path


def _retrieve_object_name_or_given_name(creator_or_contributor: Dict):
    if 'name' in creator_or_contributor.keys():
        return creator_or_contributor['name']
    elif 'givenName' in creator_or_contributor.keys() and 'familyName' in creator_or_contributor.keys():
        return creator_or_contributor['givenName'] + " " + creator_or_contributor['familyName']
    elif 'givenName' in creator_or_contributor.keys():
        return creator_or_contributor['givenName']
    elif 'familyName' in creator_or_contributor.keys():
        return creator_or_contributor['familyName']
    else:
        return ''


def _append_affiliation_file(affiliation: pd.DataFrame, target_file: str, append_header=False):
    if affiliation.shape[0] > 0:
        affiliation.to_csv(target_file, mode='a', index=False, header=append_header, encoding="utf-8")


def concat_affiliation(doi: Dict, objects_to_use_for_concatenation: str):
    list_creators_or_contributors_and_affiliations = []

    for index, object_to_use_for_concatenation in enumerate(doi["attributes"][objects_to_use_for_concatenation]):

        doi_id: Union[str, List[str]] = str(doi["id"]).lower()
        doi_file_name: Union[str, List[str]] = str(doi["mapped_id"]).lower()
        doi_publisher: Union[str, List[str]] = str(doi["attributes"]["publisher"]).lower()
        doi_client_id: Union[str, List[str]] = str(doi["relationships"]["client"]["data"]["id"])
        list_affiliation_of_object: Union[str, List[str]] = None

        if len(object_to_use_for_concatenation) > 0 and len(object_to_use_for_concatenation['affiliation']) > 0:
            list_of_affiliation = [
                _concat_affiliation_of_creator_or_contributor(affiliation, exclude_list=['affiliationIdentifierScheme'])
                if len(affiliation) > 0 else '' for affiliation in object_to_use_for_concatenation["affiliation"]]

            list_affiliation_of_object = [
                {'doi_id': doi_id, 'doi_file_name': doi_file_name, 'type': objects_to_use_for_concatenation,
                 'name': _retrieve_object_name_or_given_name(object_to_use_for_concatenation),
                 'doi_publisher': doi_publisher,
                 'doi_client_id': doi_client_id, 'affiliation': affiliation}
                for affiliation in list_of_affiliation
            ]

            list_creators_or_contributors_and_affiliations += list_affiliation_of_object

    return list_creators_or_contributors_and_affiliations


def _concat_affiliation_of_creator_or_contributor(affiliation: Dict, exclude_list: List):
    processed_affiliation = {key: value for (key, value) in affiliation.items() if
                             (key not in exclude_list and value is not None)}

    if 'affiliationIdentifier' in processed_affiliation:
        return str(processed_affiliation['affiliationIdentifier']).lower()
    else:
        return " ".join(map(str, filter(None, processed_affiliation.values()))).lower()


if __name__ == "__main__":
    split_dump_file_concat_and_save_doi_files(
        r"C:\Users\maurice.ketevi\Documents\Projects\BSO\bso3-harvest-datacite\dcdump", "dois"
    )
