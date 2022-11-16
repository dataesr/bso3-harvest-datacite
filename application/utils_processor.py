import json
from json import JSONDecodeError
from pathlib import Path
from os import PathLike
from typing import Union, Dict, List, Tuple
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


def _merge_files(list_of_files: List[Union[str, Path]], target_file_path: Path, is_compressed=True):
    if is_compressed:
        pd.concat([pd.read_csv(file, header=None, dtype='str') for file in list_of_files if file.stat().st_size > 0]).to_csv(target_file_path, compression='zip', index=False)
    else:
        pd.concat([pd.read_csv(file, header=None, dtype='str') for file in list_of_files if file.stat().st_size > 0]).to_csv(target_file_path, index=False)


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


def get_matched_affiliations(merged_affiliations_df: pd.DataFrame, aff_str: str, aff_ror: str, aff_name: str) -> [Dict,
                                                                                                                  str]:
    """Get the matched affiliations in the dataframe corresponding to the affiliation string"""
    countries = next(
        iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['countries'].values), [])
    ror = next(iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['ror'].values), [])
    grid = next(iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['grid'].values), [])
    rnsr = next(iter(merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['rnsr'].values), [])
    creator_or_contributor = next(iter(
        merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['creator_contributor'].values), [])
    is_publisher_fr = next(iter(
        merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['is_publisher_fr'].values), [])
    is_clientId_fr = next(iter(
        merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['is_clientId_fr'].values), [])
    is_countries_fr = next(iter(
        merged_affiliations_df[merged_affiliations_df["affiliation_str"] == aff_str]['is_countries_fr'].values), [])
    return {
               "name": aff_name,
               "ror": aff_ror,
               "detected_countries": listify(countries),
               "detected_ror": listify(ror),
               "detected_grid": listify(grid),
               "detected_rnsr": listify(rnsr),
           }, creator_or_contributor, is_publisher_fr, is_clientId_fr, is_countries_fr


def enrich_doi(doi, merged_affiliations_df):
    """Adds a matched_affiliations field at the level of the affiliation containing
    the countries, ror, grid, rnsr detected by the affiliation matcher
    Returns a list of matched_affiliations objects added for the doi"""
    CREATORS = "creators"
    CONTRIBUTORS = "contributors"
    this_doi = merged_affiliations_df["doi"] == doi["id"]
    matched_affiliations_list = []
    enriched_creators_or_contributors = []
    creators = []
    contributors = []
    is_publisher = []
    is_clientId = []
    is_countries = []
    creator_or_contributor = ""

    for obj in doi["attributes"][CREATORS] + doi["attributes"][CONTRIBUTORS]:
        for affiliation in obj.get("affiliation"):
            if affiliation:
                aff_str = _concat_affiliation_of_creator_or_contributor(affiliation,
                                                                        exclude_list=["affiliationIdentifierScheme"])

                aff_ror = get_ror_or_orcid(affiliation, "affiliationIdentifierScheme", "ROR", "affiliationIdentifier")
                aff_name = affiliation.get('name')
                matched_affiliations, creator_or_contributor, is_publisher_fr, is_clientId_fr, is_countries_fr = get_matched_affiliations(
                    merged_affiliations_df[this_doi], aff_str, aff_ror, aff_name)

                matched_affiliations_list.append(matched_affiliations)
                obj['affiliations'] = matched_affiliations_list

                is_publisher.append("publisher" if is_publisher_fr else "")
                is_clientId.append("clientid" if is_clientId_fr else "")
                is_countries.append("creators" if (is_countries_fr and creator_or_contributor == "creators")
                                    else "contributors")

        obj['orcid'] = next(iter([get_ror_or_orcid(name_identifier, "nameIdentifierScheme", "ORCID", "nameIdentifier")
                                  for name_identifier in obj.get("nameIdentifiers") if name_identifier]), "")

        if _safe_get("", obj, 'givenName') != "":
            obj['first_name'] = obj.get('givenName')
        if _safe_get("", obj, 'familyName') != "":
            obj['last_name'] = obj.get('familyName')
        if _safe_get("", obj, 'name') != "":
            obj['full_name'] = obj.get('name')

        for key in ['givenName', 'familyName', 'name', 'nameType', 'affiliation', 'matched_affiliations',
                    'nameIdentifiers']:
            if key in obj:
                del obj[key]

        if creator_or_contributor == "creators":
            creators.append(obj)
        elif creator_or_contributor == "contributors":
            contributors.append(obj)

        enriched_creators_or_contributors.append(obj)

    fr_reasons = sorted(set(list(filter(None, is_publisher + is_clientId + is_countries))))
    fr_reasons_concat = ";".join(fr_reasons)

    return matched_affiliations_list, enriched_creators_or_contributors, creators, contributors, fr_reasons, fr_reasons_concat


def get_orcid(name_identifier: Dict) -> str:
    if str(_safe_get("", name_identifier, "nameIdentifierScheme")) == "ORCID":
        return _parse_url_and_retrieve_last_part(name_identifier.get('nameIdentifier'))


def get_ror_or_orcid(affiliation_or_name_identifier: Dict, schema: str, ror_or_orcid: str, identifier: str) -> str:
    if str(_safe_get("", affiliation_or_name_identifier, schema)) == ror_or_orcid:
        return _parse_url_and_retrieve_last_part(affiliation_or_name_identifier.get(identifier))


def _parse_url_and_retrieve_last_part(uri: str) -> str:
    return uri.split('/')[-1]


def count_newlines(fname):
    def _make_gen(reader):
        while True:
            b = reader(2 ** 16)
            if not b: break
            yield b

    with open(fname, "rb") as f:
        count = sum(buf.count(b"\n") for buf in _make_gen(f.raw.read))
    return count


def append_to_es_index_sourcefile(doi, matched_affiliations_list, enriched_creators_or_contributors,
                                  creators, contributors, fr_reasons, fr_reasons_concat):

    creators = strip_creators_or_contributors(creators)
    contributors = strip_creators_or_contributors(contributors)

    enriched_doi = {
        "doi": str(doi["id"]),
        "creators": creators,
        "contributors": contributors,
        "client_id": str(_safe_get("", doi, "relationships", "client", "data", "id")),
        "publisher": str(_safe_get("", doi, "attributes", "publisher")),
        "update_date": str(_safe_get("", doi, "attributes", "updated")),
        "fr_reasons": fr_reasons,
        "fr_reasons_concat": fr_reasons_concat
    }

    # Get title
    if str(_safe_get("", doi, "titles", "title")) != "":
        enriched_doi["title"] = str(_safe_get("", doi, "titles", "title"))

    # Get classification_subject
    if str(_safe_get("", doi, "subjects")) != "" and isinstance(doi["subjects"], List):
        enriched_doi["classification_subject"] = [str(_safe_get("", subject, "subject")) for subject in doi["subjects"]
                                                  if subject and "subjectScheme" not in subject]
    # Get classification FOS
    if str(_safe_get("", doi, "subjects")) != "" and isinstance(doi["subjects"], List):
        enriched_doi["classification_FOS"] = [str(_safe_get("", subject, "subject")).replace("FOS:", "") for subject in
                                              doi["subjects"] if "subjects" in doi and subject and
                                              str(_safe_get("", subject, "subject",
                                                            "subjectScheme")) == "Fields of Science and Technology (FOS)"]
    # Get publication year
    enriched_doi["publicationYear"] = str(_safe_get("", doi, "publicationYear"))

    # Get language
    enriched_doi["language"] = str(_safe_get("", doi, "language"))

    # Get resourceTypeGeneral
    enriched_doi["resourceTypeGeneral"] = str(_safe_get("", doi, "resourceTypeGeneral")).lower()

    # Get resourceType
    enriched_doi["resourceType"] = str(_safe_get("", doi, "types", "resourceType")).lower()

    # Get license
    enriched_doi["license"] = str(_safe_get("", doi, "attributes", "license")).lower()

    if str(_safe_get("", doi, "descriptions")) != "" and isinstance(doi["descriptions"], List):
        # Get methods
        enriched_doi["abstract"] = "".join([str(_safe_get("", description, "description"))
                                            for description in doi["descriptions"]
                                            if description is not None and str(
                _safe_get("", doi, "descriptionType")) == "Abstract"])
        # Get description
        enriched_doi["description"] = "".join([str(_safe_get("", description, "description"))
                                               for description in doi["descriptions"]
                                               if description is not None and str(
                _safe_get("", doi, "descriptionType")) == "Other"])
        # Get abstract
        enriched_doi["methods"] = "".join([str(_safe_get("", description, "description"))
                                           for description in doi["descriptions"]
                                           if description is not None and str(
                _safe_get("", doi, "descriptionType")) == "Methods"])

    if str(_safe_get("", doi, "fundingReferences")) != "" and isinstance(doi["fundingReferences"], List):
        # Get grants
        enriched_doi["grants"] = [{"name": str(_safe_get("", funding_reference, "funderName"))}
                                  for funding_reference in doi["fundingReferences"]]
    # Get created
    enriched_doi["created"] = str(_safe_get("", doi, "created"))
    # Get registered
    enriched_doi["registered"] = str(_safe_get("", doi, "registered"))

    if str(_safe_get("", doi, "relatedIdentifiers")) != "" and isinstance(doi["relatedIdentifiers"], List):
        # Get "doi_supplement_to
        enriched_doi["doi_supplement_to"] = [related_identifier["relatedIdentifier"] for related_identifier in
                                             doi["relatedIdentifiers"]
                                             if related_identifier is not None
                                             and str(
                _safe_get("", related_identifier, "relationType")) == "IsSupplementTo"
                                             and str(
                _safe_get("", related_identifier, "relatedIdentifierType")) == "DOI"]
        # Get doi_version_of
        enriched_doi["doi_version_of"] = [related_identifier["relatedIdentifier"] for related_identifier in
                                          doi["relatedIdentifiers"]
                                          if related_identifier is not None
                                          and str(_safe_get("", related_identifier, "relationType")) == "IsVersionOf"
                                          and str(_safe_get("", related_identifier, "relatedIdentifierType")) == "DOI"]

    stripped_enriched_doi = stripper(enriched_doi)

    append_to_file(
        file=config_harvester["es_index_sourcefile"],
        _str=json.dumps(stripped_enriched_doi))


def strip_creators_or_contributors(creators_or_contributors: List) -> List:
    stripped_creators = []

    for creator_or_contributor in creators_or_contributors:
        if creator_or_contributor not in ('', {}) and isinstance(creator_or_contributor, Dict) \
                and "affiliations" in creator_or_contributor.keys():
            stripped_affiliations = [stripper(affiliation) for affiliation in creator_or_contributor["affiliations"]]
            stripped_creator = stripper(creator_or_contributor)
            stripped_creator["affiliations"] = stripped_affiliations
            stripped_creators.append(stripped_creator)

    return stripped_creators


def append_to_file(file, _str):
    with open(file, "a") as f:
        f.write(_str)
        f.write(os.linesep)


def write_doi_files(merged_affiliations_df: pd.DataFrame,
                    mask: pd.Series,
                    dump_file: PathLike,
                    output_dir: str
                    ):
    """Writes a json file for each doi, as is, if not contained in the mask,
    otherwise with the matched affiliation to each creator or contributor object in the doi
    """
    creators = []
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    for json_obj in tqdm(json_line_generator(dump_file), total=count_newlines(str(dump_file))):
        for doi in json_obj.get('data'):
            doi_contains_selected_affiliations = not (
                merged_affiliations_df[mask][merged_affiliations_df[mask]["doi"] == doi["id"]]
            ).empty

            if doi_contains_selected_affiliations:
                matched_affiliations_list, enriched_creators_or_contributors, creators, contributors, fr_reasons, \
                fr_reasons_concat = enrich_doi(doi, merged_affiliations_df)

                append_to_es_index_sourcefile(doi, matched_affiliations_list, enriched_creators_or_contributors,
                                              creators, contributors, fr_reasons, fr_reasons_concat)

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
        if target_file.stat().st_size > 0:
            checkpoint_target_df = pd.read_csv(target_file, header=None, dtype='str')
        else:
            checkpoint_target_df = pd.DataFrame()
        try:
            affiliation.to_csv(target_file, mode="a", index=False, header=append_header, encoding="utf-8")
            # is the file still readable or is there a malformed line?
            pd.read_csv(target_file, header=None, dtype='str')
        except:
            logger.exception(f"Error when adding {affiliation} to {target_file}", exc_info=True)
            checkpoint_target_df.to_csv(target_file, index=False, header=False, encoding="utf-8")

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


def stripper(data):
    new_data = {}
    for k, v in data.items():
        if isinstance(v, dict):
            v = stripper(v)
        if not v in (u'', None, {}, []):
            new_data[k] = v
    return new_data


if __name__ == "__main__":
    PROJECT_DIRNAME = os.path.dirname(os.path.dirname(__file__))
    fixture_path = Path(Path(PROJECT_DIRNAME) / "tests/unit_test/fixtures")
    sample_affiliations = pd.read_csv(fixture_path / "sample_affiliations.csv")
    is_fr = (
            sample_affiliations.is_publisher_fr | sample_affiliations.is_clientId_fr | sample_affiliations.is_countries_fr)

    output_dir = Path("./processed_dois")
    write_doi_files(
        sample_affiliations, is_fr, fixture_path / "sample.ndjson", output_dir=str(output_dir)
    )
    # Then
    output_files = [file.name for file in output_dir.glob("*.json")]
