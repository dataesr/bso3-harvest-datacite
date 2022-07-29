import json
from pathlib import Path


def list_file_dumps():
    dirpath = Path.cwd()
    files = dirpath.glob("**/*.ndjson")
    files_list = list(files)
    path_output = Path("project/results").resolve()

    return (files_list, path_output)


def format_doi(doi_id):
    doi = doi_id.replace("/", "_").replace(":", "-").replace("*", "")
    return doi


def split_dump_file():
    list_path = list_file_dumps()[0]
    for path_file in list_path:
        print(path_file)
        for jsonstring in path_file.open("r", encoding="utf-8"):
            # Skip the first line empty(all ndjson file begin by the empty line)
            if jsonstring.strip() != "":

                jsonObj: dict = json.loads(jsonstring)

                if jsonObj["data"]:
                    for obj in jsonObj["data"]:
                        name_file = format_doi(obj["id"])
                        path_result = list_file_dumps()[1]
                        res = open(path_result / "{0:s}.json".format(name_file), mode="w")
                        json.dump(obj, res, indent=4)
