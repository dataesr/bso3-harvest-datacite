import os
from datetime import datetime
from glob import glob
from pathlib import Path
import re
from time import time
from urllib import parse

import pandas as pd
import requests
from adapters.api.affiliation_matcher import AffiliationMatcher
from adapters.databases.harvest_state_repository import HarvestStateRepository
from adapters.databases.postgres_session import PostgresSession
from adapters.databases.process_state_repository import ProcessStateRepository
from adapters.storages.swift_session import SwiftSession
from application.elastic import reset_index
from application.harvester import Harvester
from application.processor import Processor, ProcessorController
from application.utils_processor import _merge_files, write_doi_files
from config.global_config import config_harvester
from domain.model.ovh_path import OvhPath
from project.server.main.logger import get_logger
from project.server.main.utils_swift import (download_container,
                                             download_object, upload_object)
import dask.dataframe as dd


logger = get_logger(__name__)

volume = "/data"
container = "bso3_publications_dump"

FRENCH_ALPHA2 = ["fr", "gp", "gf", "mq", "re", "yt", "pm", "mf", "bl", "wf", "tf", "nc", "pf"]


def run_task_import_elastic_search(index_name):
    # elastic.py
    es_url_without_http = config_harvester["ES_URL"].replace("https://", "").replace("http://", "")
    es_host = f"https://{config_harvester['ES_LOGIN_BSO3_BACK']}:{parse.quote(config_harvester['ES_PASSWORD_BSO3_BACK'])}@{es_url_without_http}"
    logger.debug("loading datacite index")
    reset_index(index=index_name)
    elasticimport = (
            f"elasticdump --input={config_harvester['es_index_sourcefile']} --output={es_host}{index_name} --type=data --limit 50 "
            + "--transform='doc._source=Object.assign({},doc)'"
    )
    logger.debug(f"{elasticimport}")
    logger.debug("starting import in elastic.py")
    os.system(elasticimport)


def run_task_download(args):
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


def download_file(container, filename, destination_dir):
    """
    Download file on object storage if it has not been already downloaded.
    Returns the path of the file once downloaded
    """
    local_file_destination = os.path.normpath(os.path.join(destination_dir, os.path.basename(filename)))
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


def run_task_match_affiliations_partition(file_prefix, partition_index, total_partition_number):
    # Download csv file from ovh storage
    local_affiliation_file = os.path.join(config_harvester['processed_dump_folder_name'], f"{file_prefix}_{config_harvester['global_affiliation_file_name']}")
    # read partition
    partition_size = get_partition_size(local_affiliation_file, total_partition_number)
    not_in_partition = lambda x: not x in range(
        partition_index * partition_size, (partition_index + 1) * partition_size
    )
    affiliations_df = pd.read_csv(local_affiliation_file, header=None,
                                  names=["doi_publisher", "doi_client_id", "affiliation_str"],
                                  skiprows=not_in_partition
                                  )
    if affiliations_df.empty:
        logger.debug("affiliations_df is empty")
        return
    # process partition
    affiliation_matcher = AffiliationMatcher(base_url=config_harvester["affiliation_matcher_service"])
    affiliation_matcher_version = affiliation_matcher.get_version()
    affiliations_df["countries"] = affiliations_df["affiliation_str"].apply(
        lambda x: affiliation_matcher.get_affiliation("country", x))
    affiliations_df["is_publisher_fr"] = affiliations_df["doi_publisher"].apply(str).apply(
        affiliation_matcher.is_publisher_fr)
    affiliations_df["is_clientId_fr"] = affiliations_df["doi_client_id"].apply(str).apply(
        affiliation_matcher.is_clientId_fr)
    affiliations_df["is_countries_fr"] = affiliations_df["countries"].apply(affiliation_matcher.is_countries_fr)
    is_fr = (affiliations_df.is_publisher_fr | affiliations_df.is_clientId_fr | affiliations_df.is_countries_fr)
    affiliations_df["grid"] = [[]] * len(affiliations_df)
    affiliations_df["rnsr"] = [[]] * len(affiliations_df)
    affiliations_df["ror"] = [[]] * len(affiliations_df)
    affiliations_df.loc[is_fr, "grid"] = affiliations_df.loc[is_fr, "affiliation_str"].apply(
        lambda x: affiliation_matcher.get_affiliation("grid", x))
    affiliations_df.loc[is_fr, "rnsr"] = affiliations_df.loc[is_fr, "affiliation_str"].apply(
        lambda x: affiliation_matcher.get_affiliation("rnsr", x))
    affiliations_df.loc[is_fr, "ror"] = affiliations_df.loc[is_fr, "affiliation_str"].apply(
        lambda x: affiliation_matcher.get_affiliation("ror", x))

    processed_filename = f"{affiliation_matcher_version}_partition_{partition_index}.csv"
    logger.debug(affiliation_matcher.get_affiliation.cache_info())
    logger.debug(f"Saving affiliations_df at {processed_filename}")
    affiliations_df.to_csv(os.path.join(config_harvester['affiliation_folder_name'], processed_filename), index=False)


def get_affiliation_matcher_version():
    """Parse the affiliation matcher version from partition filenames"""
    return next(iter(re.findall(r"(\d+\.\d+\.\d+)_partition_.*", next(iter(glob(f"{config_harvester['affiliation_folder_name']}/*_partition_*.csv"))))), None)


def run_task_consolidate_results(file_prefix):
    # aggregate results in one file
    affiliation_matcher_version = get_affiliation_matcher_version()
    consolidated_affiliations_filepath = f"{config_harvester['affiliation_folder_name']}/{file_prefix}_"
    consolidated_affiliations_filepath += f"{affiliation_matcher_version}_" if affiliation_matcher_version else ""
    consolidated_affiliations_filepath += "consolidated_affiliations.csv"
    _merge_files(Path(config_harvester['affiliation_folder_name']).glob('*'), Path(consolidated_affiliations_filepath), header=0)
    # upload the resulting file
    upload_object(
        config_harvester["datacite_container"],
        source=consolidated_affiliations_filepath,
        target=os.path.basename(consolidated_affiliations_filepath),
    )
    # remove partitions files
    for f in glob(f"{config_harvester['affiliation_folder_name']}/*_partition_*.csv"):
        os.remove(f)


def get_merged_affiliations(partition_files) -> pd.DataFrame:
    """Read consolidated and detailled csv files and returns the merged DataFrame"""
    consolidated_affiliations_file = next(Path(config_harvester["affiliation_folder_name"]).glob('*consolidated_affiliations.csv'))
    # remove header param when consolidated_affiliations_file is formed correctly
    consolidated_affiliations = pd.read_csv(consolidated_affiliations_file, dtype=str, header=1, compression="zip")
    # remove this line when consolidated_affiliations_file is formed correctly
    consolidated_affiliations = consolidated_affiliations.query('is_publisher_fr != "is_publisher_fr"')
    consolidated_affiliations.is_publisher_fr = consolidated_affiliations.is_publisher_fr.apply(eval)
    consolidated_affiliations.is_clientId_fr = consolidated_affiliations.is_clientId_fr.apply(eval)
    consolidated_affiliations.is_countries_fr = consolidated_affiliations.is_countries_fr.apply(eval)
    detailed_affiliations_file = next(Path(config_harvester["processed_dump_folder_name"]).glob('*detailed_affiliations.csv'))
    # Can't use pandas because detailed_affiliations is ~30Go and don't fit in RAM
    # Use dask to filter down on partition_files then use pandas
    detailed_affiliations = dd.read_csv(detailed_affiliations_file,
                                         names=[
                                             "doi", "doi_file_name",
                                             "creator_contributor",
                                             "name", "doi_publisher",
                                             "doi_client_id", "affiliation_str", "origin_file"
                                         ], header=None, dtype=str)
    partition_files_basename = list(map(os.path.basename, partition_files))
    logger.info("Filtering the detailed file on partition...")
    start = time()
    detailed_affiliations.origin_file = detailed_affiliations.origin_file.apply(os.path.basename)
    detailed_affiliations = detailed_affiliations[detailed_affiliations.origin_file.isin(partition_files_basename)].compute()
    stop = time()
    logger.info(f"Filtering done in {stop - start:.2f}s")
    merged_affiliations = pd.merge(consolidated_affiliations, detailed_affiliations, 'left',
                    on=["doi_publisher", "doi_client_id", "affiliation_str"])
    merged_affiliations = merged_affiliations[[
                                                "doi", "doi_file_name", "affiliation_str",
                                                "countries", "ror", "grid",
                                                "rnsr", "creator_contributor",
                                                "is_publisher_fr", "is_clientId_fr",
                                                "is_countries_fr"
                                              ]].drop_duplicates()

    logger.debug(
        f"Memory Usage: merged_affiliations {merged_affiliations.memory_usage(deep=True).sum() / 2**20 :.2f} Mo"
    )
    return merged_affiliations


def clean_up(output_dir):
    for file in glob(output_dir):
        os.remove(file)


def upload_doi_files(files, prefix):
    """Upload doi files to processed container"""
    swift = SwiftSession(config_harvester['swift'])
    file_path_dest_path_tuples = [(file, OvhPath(prefix, os.path.basename(file))) for file in files]
    swift.upload_files_to_swift(config_harvester["datacite_container"], file_path_dest_path_tuples)


def run_task_enrich_dois(partition_files):
    merged_affiliations = get_merged_affiliations(partition_files)
    is_fr = (merged_affiliations.is_publisher_fr | merged_affiliations.is_clientId_fr | merged_affiliations.is_countries_fr)
    fr_doi_file_name = merged_affiliations[is_fr].doi_file_name.to_list()
    output_dir = './dois/'
    for i, file in enumerate(partition_files):
        logger.debug(f"Processing {i} / {len(partition_files)}")
        write_doi_files(merged_affiliations, is_fr, Path(file), output_dir)

    # Upload and clean up
    all_files = glob(output_dir + '*.json')
    fr_files = [
        file
        for file in all_files
        if os.path.splitext(os.path.basename(file))[0] in fr_doi_file_name
    ]
    upload_doi_files(fr_files, prefix=config_harvester["fr_doi_files_prefix"])
    upload_doi_files(all_files, prefix=config_harvester["doi_files_prefix"])
    for file in all_files:
        os.remove(file)


def run_task_harvest(target):
    cmd = f"cd dcdump && ./dcdump -d {target}"
    logger.debug(cmd)
    os.system(cmd)


def run_task_tmp(filename):
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


def run_task_analyze(args):
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


def run_task_process_and_match_dois():
    processor = Processor(config_harvester, 0, 1, None)
    processor.process()
    processor.push_dois_to_ovh()


def run_task_process_dois(partition_index, files_in_partition):
    postgres_session = PostgresSession(host=config_harvester['db']['db_host'],
                                       port=config_harvester['db']['db_port'],
                                       database_name=config_harvester['db']['db_name'],
                                       password=config_harvester['db']['db_password'],
                                       username=config_harvester['db']['db_user'])

    process_state_repository = ProcessStateRepository(postgres_session)

    processor = Processor(config=config_harvester, index_of_partition=partition_index,
                          files_in_partition=files_in_partition,
                          repository=process_state_repository)
    processor.process_list_of_files_in_partition()


def run_task_harvest_dois(target_directory, start_date, end_date, interval, use_thread=False, force=True):
    postgres_session = PostgresSession(host=config_harvester['db']['db_host'],
                                       port=config_harvester['db']['db_port'],
                                       database_name=config_harvester['db']['db_name'],
                                       password=config_harvester['db']['db_password'],
                                       username=config_harvester['db']['db_user'])
    harvester_repository = HarvestStateRepository(postgres_session)
    harvester = Harvester(harvester_repository)
    harvester.download(
        target_directory=target_directory,
        start_date=datetime.strptime(start_date, "%Y-%m-%d"),
        end_date=datetime.strptime(end_date, "%Y-%m-%d"),
        interval=interval,
        use_thread=use_thread,
        force=force
    )


def run_task_consolidate_processed_files(total_number_of_partitions, file_prefix):
    logger.info("Consolidating of processed files")
    processor_controller = ProcessorController(config_harvester, total_number_of_partitions, file_prefix)
    processor_controller.process_files()
    processor_controller.push_to_ovh()
    processor_controller.clear_local_directory()
