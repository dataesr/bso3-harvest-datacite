from glob import glob
import os
import requests
import pandas as pd

from urllib import parse
from project.server.main.utils_swift import download_container, upload_object, download_object
from project.server.main.logger import get_logger
from adapters.api.affiliation_matcher import AffiliationMatcher


logger = get_logger(__name__)

volume = "/data"
container = "bso3_publications_dump"

FRENCH_ALPHA2 = ["fr", "gp", "gf", "mq", "re", "yt", "pm", "mf", "bl", "wf", "tf", "nc", "pf"]

ES_LOGIN_BSO_BACK = os.getenv("ES_LOGIN_BSO_BACK", "")
ES_PASSWORD_BSO_BACK = os.getenv("ES_PASSWORD_BSO_BACK", "")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")


def import_es(args):
    index_name = "bso-datacite"
    enriched_output_file = "/data/datacite_fr.jsonl"
    # elastic
    es_url_without_http = ES_URL.replace("https://", "").replace("http://", "")
    es_host = f"https://{ES_LOGIN_BSO_BACK}:{parse.quote(ES_PASSWORD_BSO_BACK)}@{es_url_without_http}"
    logger.debug("loading datacite index")
    # reset_index(index=index_name)
    elasticimport = (
        f"elasticdump --input={enriched_output_file} --output={es_host}{index_name} --type=data --limit 50 "
        + "--transform='doc._source=Object.assign({},doc)'"
    )
    logger.debug(f"{elasticimport}")
    logger.debug("starting import in elastic")
    os.system(elasticimport)


def create_task_download(args):
    dump_file = args.get("dump_file", "datacite_dump_20211022")
    CHUNK_SIZE = 128
    DATACITE_DUMP_URL = f"https://archive.org/download/{dump_file}/{dump_file}.json.zst"
    response = requests.get(url=DATACITE_DUMP_URL, stream=True)
    filename = DATACITE_DUMP_URL.split("/")[-1]
    datacite_downloaded_file = f"{volume}/dump/{filename}"
    os.system(f"mkdir -p {volume}/dump")
    with open(file=datacite_downloaded_file, mode="wb") as file:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            file.write(chunk)
    cmd = f"cd {volume}/dump && unzstd {dump_file}.json.zst"
    os.system(cmd)
    cmd = f"cd {volume}/dump && split -l 1000000 {dump_file}.json"
    os.system(cmd)


def download_file(container, filename, destination_dir, prefix=""):
    """
    Download file on object storage if it has not been already downloaded.
    Returns the path of the file once downloaded
    """
    local_file_destination = os.path.normpath(os.path.join(f"{destination_dir}", f"{filename}"))
    logger.debug(f"Downloading {filename} at {local_file_destination}")
    if not os.path.isdir(destination_dir):
        os.makedirs(destination_dir)

    if not os.path.exists(local_file_destination):
        download_object(container, filename, local_file_destination)
    return local_file_destination


def get_partition_size(source_metadata_file, total_partition_number):
    """
    Divide the number of lines of the file by the number of partitions
    Return the integer part of the division.
    """
    with open(source_metadata_file, "rt") as f:
        number_of_lines = len(f.readlines())
    partition_size = number_of_lines // total_partition_number
    return partition_size


def create_task_match_affiliations_partition(affiliations_source_file, partition_index, total_partition_number):
    # Download csv file from ovh storage
    dest_dir = "."
    container = "tmp"
    local_affiliation_file = download_file(container=container, filename=affiliations_source_file, destination_dir=dest_dir)
    # read partition
    partition_size = get_partition_size(local_affiliation_file, total_partition_number)
    not_in_partition = lambda x: not x in range(partition_index * partition_size, (partition_index + 1) * partition_size)
    affiliations_df = pd.read_csv(local_affiliation_file, header=None, names=["doi_publisher", "doi_creator_id", "affiliation"], skiprows=not_in_partition)
    logger.debug("affiliations_df before get_affiliation")
    logger.debug(affiliations_df)
    # process partition
    # AFFILIATION_MATCHER_SERVICE = "http://affiliation-matcher:5001"
    AFFILIATION_MATCHER_SERVICE = "http://host.docker.internal:5000"
    affiliation_matcher = AffiliationMatcher(base_url=AFFILIATION_MATCHER_SERVICE)
    affiliations_df["country"] = affiliations_df["affiliation"].apply(lambda x: affiliation_matcher.get_affiliation("country", x))
    affiliations_df["is_fr"] = affiliations_df.apply(lambda row: affiliation_matcher.is_affiliation_fr(row['doi_publisher'], row["doi_creator_id"], row["country"]), axis=1)
    processed_filename = f"{local_affiliation_file.split('.')[0]}_{partition_index}.csv"
    logger.debug(affiliation_matcher.get_affiliation.cache_info())
    logger.debug("affiliations_df after get_affiliation")
    logger.debug(affiliations_df)
    logger.debug(f"Saving affiliations_df at {processed_filename}")
    affiliations_df.to_csv(processed_filename, index=False)
    # upload file containing only the affiliated entries made by the worker to ovh
    upload_object(container=container, source=processed_filename, target=f"processed_partitions/{processed_filename}", segments=False)
    os.remove(processed_filename)


def create_task_consolidate_results():
    # retrieve all files
    container = "tmp"
    dest_dir = '.'
    partitions_dir = download_container(container, skip_download=False, download_prefix="processed_partitions", volume_destination=dest_dir)
    # aggregate results in one file
    consolidated_affiliations_file = "consolidated_affiliations.csv"
    pd.concat([pd.read_csv(f) for f in glob(f"{partitions_dir}/*")]).to_csv(f"{partitions_dir}/{consolidated_affiliations_file}", index=False)
    # upload the resulting file
    upload_object(container, source=f"{partitions_dir}/{consolidated_affiliations_file}", target=consolidated_affiliations_file)


def create_task_harvest(target):
    cmd = f"cd dcdump && ./dcdump -d {target}"
    logger.debug(cmd)
    os.system(cmd)


def create_task_tmp(filename):
    elements = pd.read_json(filename, lines=True).to_dict(orient="records")
    affiliations_cache = {}
    for elt in elements:
        for c in elt.get("attributes", {}).get("contributors", []) + elt.get("attributes", {}).get("creators", []):
            for aff in c.get("affiliation", []):
                if aff not in affiliations_cache:
                    affiliations_cache[aff] = {"nb": 0, "dois": []}
                affiliations_cache[aff]["nb"] += 1
    logger.debug(f"{len(elements)} elements, {len(affiliations_cache)} affiliations")
    for aff in affiliations_cache:
        res = requests.post("http://matcher-affiliation:5001/match_api", json={"query": aff, "type": "country"}).json()
        if res["results"]:
            affiliations_cache[aff]["detected_countries"] = res["results"]
    fr_elements = []
    for elt in elements:
        detected_countries = []
        is_fr = False
        for c in elt.get("attributes", {}).get("contributors", []) + elt.get("attributes", {}).get("creators", []):
            for aff in c.get("affiliation", []):
                if aff in affiliations_cache:
                    detected_countries += affiliations_cache[aff].get("detected_countries", [])
        detected_countries = list(set(detected_countries))
        elt["detected_countries"] = detected_countries
        for x in detected_countries:
            if x in FRENCH_ALPHA2:
                is_fr = True
        if is_fr:
            fr_elements.append(elt)
    pd.DataFrame(fr_elements).to_json(f"{filename}_fr.jsonl", lines=True, orient="records")
    # if args.get('extract_affiliations', False):
    #    quote = '"'
    #    cmd_1 = f"cd {volume} && cat {dump_file}.json | jq -rc '.attributes.contributors[].affiliation | @csv' | fgrep '{quote}' > {dump_file}_affiliations_contributors.jsonl"
    #    logger.debug(cmd_1)
    #    os.system(cmd_1)
    #    cmd_2 = f"cd {volume} && cat {dump_file}.json | jq -rc '.attributes.creators[].affiliation | @csv' | fgrep '{quote}' > {dump_file}_affiliations_creators.jsonl"
    #    logger.debug(cmd_2)
    #    os.system(cmd_2)
    #    cmd_11 = f'cd {volume} && cat {dump_file}_affiliations_contributors.jsonl | sort -u > {dump_file}_affiliations_contributors_uniq.jsonl'
    #    cmd_21 = f'cd {volume} && cat {dump_file}_affiliations_creators.jsonl | sort -u > {dump_file}_affiliations_creators_uniq.jsonl'
    #    logger.debug(cmd_11)
    #    os.system(cmd_11)
    #    logger.debug(cmd_21)
    #    os.system(cmd_21)
    #    cmd_3 = f'cd {volume} && cat {dump_file}_affiliations_contributors_uniq.jsonl > {dump_file}_affiliations.jsonl'
    #    cmd_3 += f' && cat {dump_file}_affiliations_creators_uniq.jsonl >> {dump_file}_affiliations.jsonl'
    #    cmd_3 += f' && cat {dump_file}_affiliations.jsonl | sort -u > {dump_file}_affiliations_uniq.jsonl'
    #    logger.debug(cmd_3)
    #    os.system(cmd_3)
    # if args.get('match_affiliations', False):
    #    os.system(f'mkdir -p {volume}/match')
    #    chunk_res = []
    #    ix = 0
    #    with open(f'{volume}/{dump_file}_affiliations_creators_uniq.jsonl') as infile:
    #        for line in infile:
    #            query = line.replace('"', ' ').strip()
    #            for match_type in ['country']:
    #                elt = {"query": query, "type": match_type}
    #                res = requests.post('http://matcher-affiliation:5001/match_api', json=elt).json()
    #                elt['results'] = res['results']
    #                if len(chunk_res) == 1000:
    #                    logger.debug(f'write file nb {ix}')
    #                    pd.DataFrame(chunk_res).to_json(f'{volume}/match/match_{ix}.jsonl', lines=True, orient='records')
    #                    chunk_res = []
    #                    ix += 1
    #                chunk_res.append(elt)


def create_task_analyze(args):
    for fileType in args.get("fileType", []):
        logger.debug(f"getting {fileType} data")
        if args.get("download", False):
            download_container(container, False, fileType, volume)
    if args.get("concat", False):
        df = read_all("softcite")
        df.to_json(f"{volume}/softcite.jsonl", orient="records", lines=True)
        upload_object("tmp", f"{volume}/softcite.jsonl", "softcite.jsonl")


def read_all(fileType):
    all_dfs = []
    ix = 0
    for root, dirs, files in os.walk(f"{volume}/{container}/{fileType}"):
        if files:
            for f in files:
                filename_softcite = f"{root}/{f}"
                root_metadata = root.replace(fileType, "metadata")
                filename_metadata = f"{root_metadata}/{f}".replace(".software.json", ".json.gz")
                try:
                    df_tmp_softcite = pd.read_json(filename_softcite, orient="records", lines=True)
                    df_tmp_softcite.columns = [f"softcite_{c}" for c in df_tmp_softcite.columns]
                    try:
                        df_tmp_metadata = pd.read_json(filename_metadata, orient="records", lines=True)
                        df_tmp_metadata.columns = [f"metadata_{c}" for c in df_tmp_metadata.columns]
                    except Exception:
                        logger.debug(f"missing metadata {filename_metadata}")
                        download_container(container, False, "/".join(filename_metadata.split("/")[3:-1]), volume)
                        df_tmp_metadata = pd.read_json(filename_metadata, orient="records", lines=True)
                        df_tmp_metadata.columns = [f"metadata_{c}" for c in df_tmp_metadata.columns]
                    df_tmp = pd.concat([df_tmp_softcite, df_tmp_metadata], axis=1)
                    all_dfs.append(df_tmp)
                except Exception:
                    logger.debug(f"error in reading {filename_softcite}")
                ix += 1
                if ix % 1000 == 0:
                    logger.debug(f"{ix} files read")
    return pd.concat(all_dfs)


#    url_hal_update = "https://api.archives-ouvertes.fr/search/?fq=doiId_s:*%20AND%20structCountry_s:fr%20AND%20modifiedDate_tdate:[{0}T00:00:00Z%20TO%20{1}T00:00:00Z]%20AND%20producedDate_tdate:[2013-01-01T00:00:00Z%20TO%20{1}T00:00:00Z]&fl=halId_s,doiId_s,openAccess_bool&rows={2}&start={3}"
