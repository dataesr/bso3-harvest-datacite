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
from application.processor import Processor, PartitionsController
from application.utils_processor import _merge_files, write_doi_files, json_line_generator, enrich_doi, append_to_es_index_sourcefile, _create_affiliation_string, get_publisher, get_client_id
from config.global_config import config_harvester, MOUNTED_VOLUME_PATH
from config.business_rules import FRENCH_PUBLISHERS
from domain.model.ovh_path import OvhPath
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object
import dask.dataframe as dd


logger = get_logger(__name__)


def run_task_import_elastic_search(index_name):
    """Create an ES index from a file using elasticdump, deleting it if it already exists."""
    # todo gz before upload
    upload_object(
        container='bso_dump',
        source=f'{MOUNTED_VOLUME_PATH}/{index_name}.jsonl',
        target=f'{index_name}.jsonl',
    )
    # elastic.py
    es_url_without_http = config_harvester["ES_URL"].replace("https://", "").replace("http://", "")
    es_host = f"https://{config_harvester['ES_LOGIN_BSO3_BACK']}:{parse.quote(config_harvester['ES_PASSWORD_BSO3_BACK'])}@{es_url_without_http}"
    logger.debug("loading datacite index")
    reset_index(index=index_name)
    elasticimport = (
            f"elasticdump --input={MOUNTED_VOLUME_PATH}/{index_name}.jsonl --output={es_host}{index_name} --type=data --limit 50 "
            + "--transform='doc._source=Object.assign({},doc)'"
    )
    logger.debug(f"{elasticimport}")
    logger.debug("starting import in elastic.py")
    os.system(elasticimport)


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
    """Run Affiliation Matcher on a partition of global affiliation file and write the result in a CSV file"""
    # Read csv file from volume
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
    logger.debug(f'start country matching with {affiliation_matcher_version} affiliation-matcher for {len(affiliations_df)} cases')
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
    """Concatenate all the partitions files into one consolidated affiliation
    file. Upload it to OVH. Remove partition files."""
    # aggregate results in one file
    affiliation_matcher_version = get_affiliation_matcher_version()
    consolidated_affiliations_filepath = f"{config_harvester['affiliation_folder_name']}/{file_prefix}_"
    consolidated_affiliations_filepath += f"{affiliation_matcher_version}_" if affiliation_matcher_version else ""
    consolidated_affiliations_filepath += "consolidated_affiliations.csv"
    _merge_files(Path(config_harvester['affiliation_folder_name']).glob(f'{affiliation_matcher_version}_partition*'), Path(consolidated_affiliations_filepath), header=0)
    # upload the resulting file
    upload_object(
        config_harvester["datacite_container"],
        source=consolidated_affiliations_filepath,
        target=os.path.basename(consolidated_affiliations_filepath),
    )
    # remove partitions files
    for f in glob(f"{config_harvester['affiliation_folder_name']}/*_partition_*.csv"):
        os.remove(f)


def get_affiliations_matches_df():
    consolidated_affiliations_file = next(Path(config_harvester["affiliation_folder_name"]).glob('*consolidated_affiliations.csv'))
    logger.debug(f'start reading {consolidated_affiliations_file} ...')
    consolidated_affiliations = pd.read_csv(consolidated_affiliations_file, dtype=str).drop_duplicates()
    logger.debug(f'done')
    for f in  ['countries', 'grid', 'rnsr', 'ror']:
        consolidated_affiliations[f] = consolidated_affiliations[f].apply(eval)
    return consolidated_affiliations

def get_affiliations_matches():
    consolidated_affiliations = get_affiliations_matches_df()
    matches = {}
    for row in consolidated_affiliations.itertuples():
        aff = row.affiliation_str
        if isinstance(aff, str):
            elt = row._asdict()
            new_entry = {}
            for f in ['countries', 'grid', 'rnsr', 'ror']:
                if elt.get(f) and isinstance(elt[f], list):
                    elt[f].sort()
                    new_entry[f] = elt[f]
            existing_entry = {}
            if aff in matches:
                existing_entry = matches[aff]
            else:
                matches[aff] = {}
            for f in new_entry:
                if f in existing_entry:
                    assert(isinstance(existing_entry[f], list))
                    if (existing_entry[f] != new_entry[f]):
                        logger.debug(f'{aff} {f} {existing_entry[f]} vs {new_entry[f]}')
                        if len(new_entry[f]) < len(existing_entry[f]):
                            matches[aff][f] = new_entry[f]
                            logger.debug(f'setting {matches[aff][f]}')
                        else:
                            logger.debug(f'keeping {matches[aff][f]}')
                            pass
                else:
                    matches[aff][f] = new_entry[f]
    logger.debug(f"{len(matches)} affiliations in dict")
    return matches
                 

def get_merged_affiliations(partition_files) -> pd.DataFrame:
    """Read consolidated and detailled csv files.
    Return the filtered and merged DataFrame"""
    consolidated_affiliations = get_affiliations_matches_df()
    detailed_affiliations_file = next(Path(config_harvester["processed_dump_folder_name"]).glob('*detailed_affiliations.csv'))
    use_dask = False
    if use_dask:
        # Can't use pandas because detailed_affiliations is ~30Go and doesn't fit in RAM
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
    else: # do not use partitions, do it for the whole thing
        logger.debug(f'start reading {detailed_affiliations_file} ...')
        detailed_affiliations = pd.read_csv(detailed_affiliations_file,
                                         names=[
                                             "doi", "doi_file_name",
                                             "creator_contributor",
                                             "name", "doi_publisher",
                                             "doi_client_id", "affiliation_str", "origin_file"
                                         ], header=None, dtype=str)
        logger.debug(f'done')
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


def upload_doi_files(files, prefix):
    """Upload doi files to datacite container"""
    swift = SwiftSession(config_harvester['swift'])
    file_path_dest_path_tuples = [(file, OvhPath(prefix, os.path.basename(file))) for file in files]
    swift.upload_files_to_swift(config_harvester["datacite_container"], file_path_dest_path_tuples)


def run_task_enrich_dois(partition_files, index_name):
    """Read downloaded datacite files and :
        - write a file for each doi. If the doi contains a french affiliation,
        it is enriched with informations from Affiliation Matcher
        - write a file for creating an ES index with french affiliation containing dois infos
    """
    # sort partition files to start by the lastest
    partition_files.sort(reverse=True)
    #affiliations_matches = get_affiliations_matches()
    #merged_affiliations = get_merged_affiliations(partition_files)
    #is_fr = (merged_affiliations.is_publisher_fr | merged_affiliations.is_clientId_fr | merged_affiliations.is_countries_fr)
    #fr_doi_file_name = merged_affiliations[is_fr].doi_file_name.to_list()
    #merged_affiliations_fr = merged_affiliations[merged_affiliations.is_fr].set_index('doi')
    #merged_affiliations_fr['doi'] = merged_affiliations_fr.index
    #fr_dois = set(merged_affiliations_fr.doi.to_list())
    output_dir = './dois/'

    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)

    matches = get_affiliations_matches()

    known_dois = set([]) # to handle dois present multiple times
    for dump_file in partition_files:
        logger.debug(f'treating {dump_file}')
        nb_new_doi, nb_new_country, nb_new_publisher, nb_new_client = 0, 0, 0, 0
        for json_obj in json_line_generator(Path(dump_file)):
            for doi in json_obj.get('data'):
                if doi['id'] in known_dois:
                    continue
                countries, affiliations = [], []
                for obj in doi["attributes"]["creators"] + doi["attributes"]["contributors"]:
                    for affiliation in obj.get("affiliation"):
                        if affiliation:
                            local_matches = {}
                            aff_str = _create_affiliation_string(affiliation, exclude_list = [])
                            if aff_str in matches:
                                local_matches = matches[aff_str]
                            for f in local_matches:
                                affiliation[f] = local_matches[f]
                                if 'countries' in affiliation:
                                    countries += affiliation['countries']
                                if affiliation not in affiliations:
                                    affiliations.append(affiliation)
                countries = list(set(countries))
                doi['countries'] = countries
                doi['affiliations'] = affiliations
                fr_reasons = []
                if 'fr' in countries:
                    fr_reasons.append('country')
                    nb_new_country += 1
                if get_publisher(doi) in FRENCH_PUBLISHERS:
                    fr_reasons.append('publisher')
                    nb_new_publisher += 1
                if get_client_id(doi).startswith('inist.'):
                    fr_reasons.append("clientId")
                    nb_new_client += 1
                fr_reasons.sort()
                fr_reasons_concat = ';'.join(fr_reasons)
                if fr_reasons:
                    append_to_es_index_sourcefile(doi, fr_reasons, fr_reasons_concat, index_name)
                    nb_new_doi += 1
                known_dois.add(doi['id'])
        logger.debug(f'{nb_new_doi} doi added to {index_name} - country {nb_new_country} - publisher {nb_new_publisher} - client {nb_new_client}')

    #for i, file in enumerate(partition_files):
    #    logger.debug(f"Processing {i} / {len(partition_files)}")
    #    write_doi_files(merged_affiliations, is_fr, Path(file), output_dir, index_name)

    # Upload and clean up
    #all_files = glob(output_dir + '*.json')
    upload_files = False
    #if upload_files:
    #    fr_files = [
    #        file
    #        for file in all_files
    #        if os.path.splitext(os.path.basename(file))[0] in fr_doi_file_name
    #    ]
    #    upload_doi_files(fr_files, prefix=config_harvester["fr_doi_files_prefix"])
    #    upload_doi_files(all_files, prefix=config_harvester["doi_files_prefix"])
    #for file in all_files:
    #    os.remove(file)


def run_task_process_dois(partition_index, files_in_partition):
    """Read downloaded datacite files and extracts affiliations infos from
    downloaded datacite files. Write it in CSV files to be optimally processed
    by Affiliation Matcher. Track the progress with a Postgres Session"""
    postgres_session = PostgresSession(host=config_harvester['db']['db_host'],
                                       port=config_harvester['db']['db_port'],
                                       database_name=config_harvester['db']['db_name'],
                                       password=config_harvester['db']['db_password'],
                                       username=config_harvester['db']['db_user'])

    process_state_repository = ProcessStateRepository(postgres_session)

    processor = Processor(config=config_harvester, index_of_partition=partition_index,
                          files_in_partition=files_in_partition,
                          repository=process_state_repository)
    processor.process_partition()


def run_task_harvest_dois(target_directory, start_date, end_date, interval, use_thread=False, force=True):
    """Run dcdump go script. Track the progress with a Postgres Session"""
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


def run_task_consolidate_processed_files(file_prefix):
    """Concatenate all the partitions files into one detailed affiliation file
    and one consolidated affiliation file. Upload the files to OVH.
    Remove partition files."""
    logger.info("Consolidating of processed files")
    partitions_controller = PartitionsController(config_harvester, file_prefix)
    partitions_controller.concat_files()
    partitions_controller.push_to_ovh()
    partitions_controller.clear_local_directory()
