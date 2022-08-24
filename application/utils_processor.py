import json
from pathlib import Path
from os import PathLike
from typing import Union, Dict, List


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


def split_dump_file_concat_and_save_doi_files(
    dump_folder: Union[str, PathLike], target_folder_name: str = "dois"
):

    list_path, target_directory = _list_dump_files_and_get_target_directory(
        dump_folder, target_folder_name
    )

    for path_file in list_path:

        for jsonstring in path_file.open("r", encoding="utf-8"):

            if jsonstring.strip() != "":

                jsonObj: dict = json.loads(jsonstring)

                if jsonObj["data"]:
                    for doi in jsonObj["data"]:
                        doi["mapped_id"] = _format_doi(doi["id"])

                        name_file = _format_doi(doi["id"])

                        # concatenating creators affiliation

                        (
                            doi,
                            concat_all_creators_affiliation_identifier,
                        ) = concat_affiliation(doi, "creators")

                        doi[
                            "concatAllCreatorsAffiliationIdentifier"
                        ] = concat_all_creators_affiliation_identifier

                        # concatenating contributors affiliation
                        (
                            doi,
                            concat_all_contributors_affiliation_identifier,
                        ) = concat_affiliation(doi, "contributors")

                        doi[
                            "concatAllContributorsAffiliationIdentifier"
                        ] = concat_all_contributors_affiliation_identifier

                        with open(
                            target_directory / "{0:s}.json".format(name_file), "w", encoding="utf-8"
                        ) as outfile:
                            json.dump(doi, outfile, indent=4)


def concat_affiliation(doi: Dict, objects_to_use_for_concatenation: str):
    list_affiliation_identifier = []
    concatenated_objects = []

    for object_to_use_for_concatenation in doi["attributes"][objects_to_use_for_concatenation]:
        joined_affiliation_per_object = list(
            set(
                [
                    _concat_affiliation_object(affiliation)
                    for affiliation in object_to_use_for_concatenation["affiliation"]
                ]
            )
        )

        concatenated_affiliation_identifier_per_object = " ".join(joined_affiliation_per_object)

        object_to_use_for_concatenation[
            "concatAffiliation"
        ] = concatenated_affiliation_identifier_per_object

        concatenated_objects.append(object_to_use_for_concatenation)

        list_affiliation_identifier.append(concatenated_affiliation_identifier_per_object)

    doi["attributes"][objects_to_use_for_concatenation] = concatenated_objects
    list_affiliation_identifier = list(set(list_affiliation_identifier))

    return doi, " ".join(list_affiliation_identifier)


def _concat_affiliation_object(affiliation: Dict):
    return " ".join(map(str, filter(None, affiliation.values())))


if __name__ == "__main__":
    split_dump_file_concat_and_save_doi_files(
        r"C:\Users\maurice.ketevi\Documents\Projects\BSO\bso3-harvest-datacite\tests\unit_test\application", "test_dois"
    )
