import itertools
import json
from json import JSONDecodeError
from pathlib import Path
from os import PathLike
from adapters.databases.doi_collection import get_mongo_repo
from typing import Union, Dict, List, Tuple, Generator, Any
import pandas as pd

from config.exceptions import FileLoadingException
from config.global_config import config_harvester
from project.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL
from tqdm import tqdm
import os

logger = get_logger(__name__, level=LOGGER_LEVEL)


def _list_dump_files_in_directory():
    dump_folder_path = _get_path(config_harvester['raw_dump_folder_name'])
    return list(dump_folder_path.glob(config_harvester["files_extenxion"]))


def _merge_files(list_of_files: List[Union[str, Path]], target_file_path: Path):
    pd.concat([pd.read_csv(file) for file in list_of_files]).to_csv(f"{target_file_path}", index=False)


def json_line_generator(ndjson_file):
    with ndjson_file.open("r", encoding="utf-8") as f:
        for jsonstring in f:
            if jsonstring.strip() != "":
                try:
                    yield json.loads(jsonstring)
                except (TypeError, JSONDecodeError) as e:
                    print(f"Error reading line in file. Detailed error {e}")


def listify(obj):
    """Returns the obj if the obj is a list.
    If the obj is a string of a list, runs eval to output a list.
    Otherwise returns an empty list"""
    if isinstance(obj, list):
        return obj
    elif isinstance(obj, str) and obj[0] == '[':
        return eval(obj)
    else:
        return []


def get_matched_affiliations(merged_affiliations_df, aff_str):
    """Get the matched affiliations in the dataframe corresponding to the affiliation string"""
    countries = next(
        iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['countries'].values), [])
    ror = next(iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['ror'].values), [])
    grid = next(iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['grid'].values), [])
    rnsr = next(iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['rnsr'].values), [])
    return {
        "countries": listify(countries),
        "ror": listify(ror),
        "grid": listify(grid),
        "rnsr": listify(rnsr),
    }


def enrich_doi(doi, merged_affiliations_df):
    """Adds a matched_affiliations field at the level of the affiliation containing
    the countries, ror, grid, rnsr detected by the affiliation matcher
    Returns a list of matched_affiliations objects added for the doi"""
    CREATORS = "creators"
    CONTRIBUTORS = "contributors"
    this_doi = merged_affiliations_df["doi"] == doi["id"]
    matched_affiliations_list = []
    for obj in doi["attributes"][CREATORS] + doi["attributes"][CONTRIBUTORS]:
        for affiliation in obj.get("affiliation"):
            if affiliation:
                aff_str = _concat_affiliation_of_creator_or_contributor(affiliation,
                                                                        exclude_list=["affiliationIdentifierScheme"])
                matched_affiliations = get_matched_affiliations(merged_affiliations_df[this_doi], aff_str)
                obj['matched_affiliations'] = matched_affiliations
                matched_affiliations_list.append(matched_affiliations)
    return matched_affiliations_list


def count_newlines(fname):
    def _make_gen(reader):
        while True:
            b = reader(2 ** 16)
            if not b: break
            yield b

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count


def push_to_mongo(doi, matched_affiliations_list, mongo_repo):
    mongo_repo.create(**{
        "doi": str(doi["id"]),
        "matched_affiliations_list": matched_affiliations_list,
        "clientId": str(safe_get("", doi, "relationships", "client", "data", "id")),
        "publisher": str(safe_get("", doi, "attributes", "publisher")),
        "update_date": str(safe_get("", doi, "attributes", "updated")),
    })


def write_doi_files(merged_affiliations_df: pd.DataFrame,
                    mask: pd.Series,
                    dump_file: PathLike,
                    output_dir: str
                    ):
    """Writes a json file for each doi, as is, if not contained in the mask,
    otherwise with the matched affiliation to each creator or contributor object in the doi
    """
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    mongo_repo = get_mongo_repo()
    for json_obj in tqdm(json_line_generator(dump_file), total=count_newlines(str(dump_file))):
        for doi in json_obj.get('data'):
            doi_contains_selected_affiliations = not (
                merged_affiliations_df[mask][merged_affiliations_df[mask]["doi"] == doi["id"]]
            ).empty
            if doi_contains_selected_affiliations:
                matched_affiliations_list = enrich_doi(doi, merged_affiliations_df)
                push_to_mongo(doi, matched_affiliations_list, mongo_repo)
            with open(f"{output_dir}/{_format_string(doi['id'])}.json", 'w') as f:
                json.dump(doi, f, indent=None)


def get_list_creators_or_contributors_and_affiliations(path_file):
    list_creators_or_contributors_and_affiliations = []
    number_of_non_null_dois = 0
    number_of_null_dois = 0
    number_of_processed_dois_per_file = 0

    for json_obj in json_line_generator(path_file):
        for doi in json_obj.get('data'):
            doi["mapped_id"] = _format_string(doi["id"])
            current_size = len(list_creators_or_contributors_and_affiliations)
            try:
                list_creators_or_contributors_and_affiliations += _concat_affiliation(doi, "creators")
                list_creators_or_contributors_and_affiliations += _concat_affiliation(doi, "contributors")
            except BaseException as e:
                logger.exception(f'Error while creating concat for {doi["id"]}. \n Details of the error {e}')

            if len(list_creators_or_contributors_and_affiliations) > current_size:
                number_of_non_null_dois += 1
            else:
                number_of_null_dois += 1

        number_of_processed_dois_per_file = number_of_processed_dois_per_file + len(json_obj["data"])

    logger.info(
        f'{path_file} number of dois {number_of_processed_dois_per_file} number of non null dois '
        f'{number_of_non_null_dois} and null dois {number_of_null_dois}')

    return number_of_processed_dois_per_file, number_of_non_null_dois, number_of_null_dois, \
           list_creators_or_contributors_and_affiliations


def _list_files_in_directory(folder: Union[str, Path], regex: str):
    folder_path = _get_path(folder)
    return list(folder_path.glob(regex))


def _is_files_list_splittable_into_mutiple_partitions(total_number_of_partitions) -> bool:
    list_of_files_in_dump_folder = _list_files_in_directory(config_harvester['raw_dump_folder_name'],
                                                            config_harvester['files_extenxion'])
    return len(list_of_files_in_dump_folder) > total_number_of_partitions


def _create_affiliation_file(target_directory: Union[Path, str],
                             file_name: str = "global_affiliation.csv"
                             ) -> Tuple[bool, Path]:
    """
    :type target_directory: Path or str
    """
    file_path = Path(target_directory) / file_name
    already_exist = True

    if not file_path.is_file():
        already_exist = False
        file_path.touch()

    return already_exist, file_path


def _append_affiliation_file(affiliation: pd.DataFrame, target_file: Union[str, Path], append_header=False):
    if affiliation.shape[0] > 0:
        affiliation.to_csv(target_file, mode="a", index=False, header=append_header, encoding="utf-8")


def safe_get(default_value, _dict, *keys):
    for key in keys:
        try:
            _dict = _dict[key]
        except (TypeError, KeyError):
            return default_value
    return _dict


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
