import json
import os
from copy import deepcopy
from json import JSONDecodeError
from os import PathLike
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd
from project.server.main.re3data import find_re3 

from config.exceptions import FileLoadingException
from config.global_config import config_harvester, COMPRESSION_SUFFIX, MOUNTED_VOLUME_PATH
from config.logger_config import LOGGER_LEVEL
from project.server.main.logger import get_logger

logger = get_logger(__name__, level=LOGGER_LEVEL)

re3_existing_signatures = {}

def gzip_cli(file, keep=True, decompress=False):
    if decompress:
        os.system(f"gzip -d --force {'--keep' if keep else ''} {file}")
        return os.path.splitext(file)[0]
    else:
        os.system(f"gzip --force {'--keep' if keep else ''} {file}")
        return f"{file}{COMPRESSION_SUFFIX}"


def _merge_files(list_of_files: List[Union[str, Path]], target_file_path: Path, header=None):
    """Write the concatenation of multiple csv files in a csv file"""
    tmp = []
    for file in list_of_files:
        #logger.debug(file)
        if file.stat().st_size > 0:
            tmp.append(pd.read_csv(file, header=header, dtype='str'))
    df = pd.concat(tmp)
    df.to_csv(target_file_path, index=False)


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
    elif isinstance(obj, str) and obj.startswith('['):
        return eval(obj)
    else:
        return []


def get_matched_affiliations(aff_str_df: pd.DataFrame, aff_ror: str, aff_name: str) -> dict:
    """Get the matched affiliations in the dataframe corresponding to the affiliation string"""
    countries = next(iter(aff_str_df['countries'].values), [])
    ror = next(iter(aff_str_df['ror'].values), [])
    grid = next(iter(aff_str_df['grid'].values), [])
    rnsr = next(iter(aff_str_df['rnsr'].values), [])
    creator_or_contributor = next(iter(aff_str_df['creator_contributor'].values), "")
    is_publisher_fr = next(iter(aff_str_df['is_publisher_fr'].values), False)
    is_clientId_fr = next(iter(aff_str_df['is_clientId_fr'].values), False)
    is_countries_fr = next(iter(aff_str_df['is_countries_fr'].values), False)
    return {
               "name": aff_name,
               "ror": aff_ror,
               "detected_countries": listify(countries),
               "detected_ror": listify(ror),
               "detected_grid": listify(grid),
               "detected_rnsr": listify(rnsr),
           }, creator_or_contributor, is_publisher_fr, is_clientId_fr, is_countries_fr


def enrich_doi(doi, this_doi_df):
    """Add a matched_affiliations field at the level of the affiliation containing
    the countries, ror, grid, rnsr detected by the affiliation matcher
    Return a list of matched_affiliations objects added for the doi"""
    CREATORS = "creators"
    CONTRIBUTORS = "contributors"
    #this_doi_df = merged_affiliations_df[merged_affiliations_df["doi"] == doi["id"]]
    creators = []
    contributors = []
    is_publisher = []
    is_clientId = []
    is_countries = []
    creator_or_contributor = ""

    for obj in doi["attributes"][CREATORS] + doi["attributes"][CONTRIBUTORS]:
        obj['affiliations'] = []
        for affiliation in obj.get("affiliation"):
            if affiliation:
                aff_str = _create_affiliation_string(affiliation, exclude_list=["affiliationIdentifierScheme"])

                aff_ror = get_ror_or_orcid(affiliation, "affiliationIdentifierScheme", "ROR", "affiliationIdentifier")
                aff_name = affiliation.get('name')
                aff_str_df = this_doi_df[this_doi_df["affiliation_str"] == aff_str]
                matched_affiliations, creator_or_contributor, is_publisher_fr, is_clientId_fr, is_countries_fr \
                    = get_matched_affiliations(aff_str_df, aff_ror, aff_name)

                obj['affiliations'].append(matched_affiliations)

                is_publisher.append("publisher" if is_publisher_fr else "")
                is_clientId.append("clientid" if is_clientId_fr else "")
                is_countries.append("countries" if is_countries_fr else "")
        if obj['affiliations'] == []:
            del obj['affiliations']
        es_obj = deepcopy(obj)
        es_obj['orcid'] = next(
            iter([get_ror_or_orcid(name_identifier, "nameIdentifierScheme", "ORCID", "nameIdentifier")
                  for name_identifier in obj.get("nameIdentifiers") if name_identifier]), "")

        es_obj['first_name'] = es_obj.get('givenName')
        es_obj['last_name'] = es_obj.get('familyName')
        es_obj['full_name'] = es_obj.get('name')

        for key in ['givenName', 'familyName', 'name', 'nameType', 'affiliation', 'matched_affiliations',
                    'nameIdentifiers']:
            if key in es_obj:
                del es_obj[key]

        trim_null_values(es_obj)
        if creator_or_contributor == "creators":
            creators.append(es_obj)
        elif creator_or_contributor == "contributors":
            contributors.append(es_obj)

    fr_reasons = list(set(filter(None, is_publisher + is_clientId + is_countries)))
    fr_reasons.sort()
    fr_reasons_concat = ";".join(fr_reasons)

    return creators, contributors, fr_reasons, fr_reasons_concat


def get_ror_or_orcid(affiliation_or_name_identifier: Dict, schema: str, ror_or_orcid: str, identifier: str) -> str:
    if affiliation_or_name_identifier.get(schema) == ror_or_orcid:
        return _parse_url_and_retrieve_last_part(affiliation_or_name_identifier.get(identifier))


def _parse_url_and_retrieve_last_part(uri: str) -> str:
    if uri:
        return uri.split("/")[-1]
    return ""


def get_classification_subject(doi):
    try:
        return list(filter(None, (
            subject.get("subject", "")
            for subject in doi["attributes"]["subjects"]
            if subject and "subjectScheme" not in subject
        )))
    except KeyError:
        return []


def get_classification_FOS(doi):
    try:
        subjects_filtered = [
            trim_null_values(subject)
            for subject in doi["attributes"]["subjects"]
            if (subject is not None
            and subject.get("subjectScheme", "") == "Fields of Science and Technology (FOS)")
        ]
        return list(filter(None, (str(subject.get("subject", "")).lstrip("FOS: ") for subject in subjects_filtered)))
    except KeyError:
        return []


def get_description_element(doi, element):
    try:
        descriptions_filtered = [
            trim_null_values(description)
            for description in doi["attributes"]["descriptions"]
            if description
            and description.get("descriptionType", "") == element
            ]
        return " ".join(filter(None, (description.get("description", "") for description in descriptions_filtered))).strip()
    except KeyError:
        return ""

def get_abstract(doi):
    return get_description_element(doi, "Abstract")


def get_description(doi):
    return get_description_element(doi, "Other")


def get_methods(doi):
    return get_description_element(doi, "Methods")


def get_grants(doi):
    grants = []
    if 'fundingReferences' in doi['attributes'] and isinstance(doi["attributes"]["fundingReferences"], list):
        for e in doi["attributes"]["fundingReferences"]:
            elt = {}
            if e.get('funderName'):
                elt['agency'] = e['funderName']
            if e.get('awardNumber'):
                elt['grantid'] = e['awardNumber']
            if elt:
                grants.append(elt)
    return grants


def get_doi_element(doi, element):
    try:
        return list(filter(None, (
            related_identifier.get("relatedIdentifier")
            for related_identifier in doi["attributes"]["relatedIdentifiers"]
            if related_identifier
            and related_identifier.get("relationType", "") == element
            and related_identifier.get("relatedIdentifierType", "") == "DOI"
        )))
    except KeyError:
        return []


def get_doi_supplement_to(doi):
    return get_doi_element(doi, "IsSupplementTo")


def get_doi_version_of(doi):
    return get_doi_element(doi, "IsVersionOf")


def get_title(doi):
    try:
        title_filtered = filter(None, (
            title["title"]
            for title in doi["attributes"]["titles"]
            if title
        ))
        return " ".join(title_filtered).strip()
    except KeyError:
        return ""


def get_registered(doi):
    return _safe_get("", doi, "attributes", "registered")


def get_created(doi):
    return _safe_get("", doi, "attributes", "created")


def get_license(doi):
    try:
        rights_filtered = filter(None, (
            right["rightsIdentifier"]
            for right in doi["attributes"]["rightsList"]
            if right
        ))
        return " ".join(rights_filtered).strip()
    except KeyError:
        return ""


def get_resourceType(doi):
    return str(_safe_get("", doi, "attributes", "types", "resourceType")).lower()


def get_resourceTypeGeneral(doi):
    return str(_safe_get("", doi, "attributes", "types", "resourceTypeGeneral")).lower()


def get_language(doi):
    return _safe_get("", doi, "attributes", "language")


def get_publicationYear(doi):
    return str(_safe_get("", doi, "attributes", "publicationYear"))

def get_url(doi):
    return str(_safe_get("", doi, "attributes", "url"))

def get_creators(doi):
    return _safe_get("", doi, "attributes", "creators")

def get_contributors(doi):
    return _safe_get("", doi, "attributes", "contributors")

def get_updated(doi):
    return _safe_get("", doi, "attributes", "updated")


def get_publisher(doi):
    return _safe_get("", doi, "attributes", "publisher")


def get_client_id(doi):
    return _safe_get("", doi, "relationships", "client", "data", "id")

def make_author(c, role, affiliations):
    elt = {}
    elt['role'] = role
    full_name = ''
    if 'givenName' in c and isinstance(c['givenName'], str) and c['givenName']:
        elt['first_name'] = c['givenName']
        full_name = c['givenName']
    if 'familyName' in c and isinstance(c['familyName'], str) and c['familyName']:
        elt['last_name'] = c['familyName']
        full_name += ' '+c['familyName']
    full_name = full_name.strip()
    if not full_name and 'name' in c and isinstance(c['name'], str) and c['name']:
        full_name = c['name']
    if full_name:
        elt['full_name'] = full_name.strip()
    if 'affiliation' in c and c['affiliation'] and isinstance(c['affiliation'], list):
        elt['affiliations'] = c['affiliation']
        for aff in c['affiliation']:
            if aff not in affiliations:
                affiliations.append(aff)
    for idi in c.get('nameIdentifiers', []):
        if idi.get('nameIdentifierScheme') == 'ORCID' and isinstance(idi.get('nameIdentifier'), str):
            elt['orcid'] = idi['nameIdentifier'].split('/')[-1]
    return elt, affiliations

def append_to_es_index_sourcefile(doi, fr_reasons, fr_reasons_concat, index_name):

    global re3_existing_signatures
    if len(re3_existing_signatures) == 0:
        re3_existing_signatures = json.load(open(f'{MOUNTED_VOLUME_PATH}/re3data_dict.json', 'r'))

    creators = get_creators(doi)
    contributors = get_contributors(doi)
    authors = []
    affiliations = []
    for c in creators:
        elt, affiliations = make_author(c, 'creator', affiliations)
        authors.append(elt)
    for c in contributors:
        elt, affiliations = make_author(c, 'contributor', affiliations)
        authors.append(elt)

    external_ids = [{'id_type': 'datacite', 'id_value': doi['id']}]

    genre_raw = get_resourceTypeGeneral(doi)
    genre = ''
    if genre_raw in ['journalarticle', 'datapaper']:
        genre = 'journal-article'
    elif genre_raw in ['bookchapter']:
        genre = 'book-chapter'
    elif genre_raw in ['book']:
        genre = 'book'
    elif genre_raw in ['conferencepaper', 'conferenceproceeding']:
        genre = 'proceedings'
    elif genre_raw in ['dataset']:
        genre = 'dataset'
    else:
        genre = 'other'

    classifications = []
    for s in get_classification_subject(doi):
        elt = {'label': s, 'reference': 'subject-datacite'}
        if elt not in classifications:
            classifications.append(elt)
    for s in get_classification_FOS(doi):
        elt = {'label': s, 'reference': 'FOS-datacite'}
        if elt not in classifications:
            classifications.append(elt)

    enriched_doi = {
        "doi": doi.get("id", ""),
        "external_ids": external_ids,
        "year": get_publicationYear(doi),
        "authors": authors,
        "affiliations": affiliations,
        "title": get_title(doi),
        "genre_raw": genre_raw,
        "genre": genre,
        "resourceType": get_resourceType(doi),
        "abstract": [{'abstract': get_abstract(doi), 'lang': get_language(doi)}],
        "grants": get_grants(doi),
        "url": get_url(doi),
        "publisher": get_publisher(doi),
        "classifications": classifications,
        "language": get_language(doi),
        "license": get_license(doi),
        "methods": get_methods(doi),
        "description": get_description(doi),
        "created": get_created(doi),
        "registered": get_registered(doi),
        "doi_supplement_to": get_doi_supplement_to(doi),
        "doi_version_of": get_doi_version_of(doi),
        "client_id": get_client_id(doi),
        "fr_reasons": fr_reasons,
        "fr_reasons_concat": fr_reasons_concat,
        "update_date": get_updated(doi),
    }

    #if enriched_doi.get('url'):
    #    re3_found = find_re3(enriched_doi['url'], re3_existing_signatures)
    #    if re3_found:
    #        for f in re3_found:
    #            enriched_doi[f're3data_{f}'] = re3_found[f]

    # Keep only non-null values
    stripped_enriched_doi = trim_null_values(enriched_doi)
    append_to_file(
        file=f'{MOUNTED_VOLUME_PATH}/{index_name}.jsonl',
        _str=json.dumps(stripped_enriched_doi))


def strip_creators_or_contributors(creators_or_contributors: List) -> List:
    stripped_creators = []

    for creator_or_contributor in creators_or_contributors:
        if creator_or_contributor not in ('', {}) and isinstance(creator_or_contributor, Dict) \
                and "affiliations" in creator_or_contributor.keys():
            stripped_affiliations = [trim_null_values(affiliation) for affiliation in
                                     creator_or_contributor["affiliations"]]
            stripped_creator = trim_null_values(creator_or_contributor)
            stripped_creator["affiliations"] = stripped_affiliations
            stripped_creators.append(stripped_creator)

    return stripped_creators


def append_to_file(file, _str):
    with open(file, "a") as f:
        f.write(_str)
        f.write(os.linesep)


#TODO
# ré-écrire write en lisant le consolidated_ avant (dans un dict sur les aff)
# lire les dump un par un, et gérér les publisher / clientid etc
# gérer aussi les dates ?

def write_doi_files(merged_affiliations_df: pd.DataFrame,
                    mask: pd.Series,
                    dump_file: PathLike,
                    output_dir: str,
                    index_name: str
                    ):
    """Write a json file for each doi, as is, if not contained in the mask,
    otherwise with the matched affiliation to each creator or contributor
    object in the doi.
    Add a line with the matched affiliation and doi infos in the ES index
    source file for each doi contained in the mask.
    """
    creators = []
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
    for json_obj in json_line_generator(dump_file):
        for doi in json_obj.get('data'):
            doi_contains_selected_affiliations = (
                    len(merged_affiliations_df[mask][merged_affiliations_df[mask]["doi"] == doi["id"]].index) != 0
            )

            if doi_contains_selected_affiliations:
                creators, contributors, fr_reasons, fr_reasons_concat = enrich_doi(doi, merged_affiliations_df)

                append_to_es_index_sourcefile(doi, creators, contributors, fr_reasons, fr_reasons_concat, index_name)

            with open(f"{output_dir}/{_format_string(doi['id'])}.json", 'w') as f:
                json.dump(doi, f, indent=None)


def _list_files_in_directory(folder: Union[str, Path], regex: str):
    """List files in the directory matching the regex pattern"""
    folder_path = _get_path(folder)
    return list(folder_path.glob(regex))


def _concat_affiliation(doi: Dict, objects_to_use_for_concatenation: str, origin_file: str):
    """Create a list of affiliation objects for a doi"""
    list_creators_or_contributors_and_affiliations = []

    for object_to_use_for_concatenation in doi["attributes"][objects_to_use_for_concatenation]:

        doi_id: Union[str, List[str]] = str(doi["id"]).lower()
        doi_file_name: Union[str, List[str]] = str(doi["mapped_id"]).lower()
        doi_publisher: Union[str, List[str]] = str(_safe_get("", doi, "attributes", "publisher")).lower()
        doi_client_id: Union[str, List[str]] = str(_safe_get("", doi, "relationships", "client", "data", "id"))
        list_affiliation_of_object: List[Dict[str, Union[str, List[str]]]]

        if len(object_to_use_for_concatenation) > 0 and len(object_to_use_for_concatenation["affiliation"]) > 0:
            list_of_affiliation = [
                _create_affiliation_string(affiliation, exclude_list=["affiliationIdentifierScheme"])
                if len(affiliation) > 0
                else ""
                for affiliation in object_to_use_for_concatenation["affiliation"]
            ]

            list_affiliation_of_object = [
                {
                    "doi_id": doi_id,
                    "doi_file_name": doi_file_name,
                    "type": objects_to_use_for_concatenation,
                    "name": _retrieve_object_name(object_to_use_for_concatenation),
                    "doi_publisher": doi_publisher,
                    "doi_client_id": doi_client_id,
                    "affiliation": affiliation,
                    "origin_file": os.path.basename(origin_file)
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
                    "name": _retrieve_object_name(object_to_use_for_concatenation),
                    "doi_publisher": doi_publisher,
                    "doi_client_id": doi_client_id,
                    "affiliation": "",
                    "origin_file": os.path.basename(origin_file)
                }
            ]
            list_creators_or_contributors_and_affiliations += list_affiliation_of_object
    return list_creators_or_contributors_and_affiliations


def _retrieve_object_name(creator_or_contributor: Dict):
    name = creator_or_contributor.get("name", "")
    if not name:
        name = (
                creator_or_contributor.get("givenName", "")
                + " "
                + creator_or_contributor.get("familyName", "")
        )
    return name.strip()


def _create_affiliation_string(affiliation: Dict, exclude_list: List):
    """Creates an affiliation string by concatenating all the values from an affiliation dict"""
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


def _create_file(target_directory: Union[Path, str], file_name: str) -> Path:
    """
    Create file if it doesn't exist.
    """
    file_path = Path(target_directory) / file_name
    file_path.touch()
    return file_path


def _safe_get(default_value, _dict, *keys):
    for key in keys:
        try:
            _dict = _dict[key]
        except (TypeError, KeyError):
            return default_value
    if _dict:
        return _dict
    else:
        return default_value


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


def trim_null_values(data: dict) -> dict:
    new_data = {}
    for k, v in data.items():
        if isinstance(v, dict):
            v = trim_null_values(v)
        if not v in (u'', None, {}, []):
            new_data[k] = v
    return new_data
