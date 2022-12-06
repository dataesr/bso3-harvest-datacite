import json
import os
from dotenv import load_dotenv

# Project path
PROJECT_DIRNAME = os.path.dirname(os.path.dirname(__file__))

# debug level
DEBUG_LEVEL = 1
# destination folder ovh

# Output file suffixes
COMPRESSION_SUFFIX = '.gz'

# folder in OVH
DATACITE_DUMP = 'datacite'
RAW_DATACITE_DUMP = 'raw'
PROCESSED_DATACITE_DUMP = 'processed'

# folder in project
MOUNTED_VOLUME_PATH = os.getenv("MOUNTED_VOLUME_PATH", PROJECT_DIRNAME)
RAW_DUMP_FOLDER_NAME = os.getenv("RAW_DUMP_FOLDER_NAME", os.path.join(PROJECT_DIRNAME, "sample-dump"))
PROCESSED_DUMP_FOLDER_NAME = "dois" # consolidated processed output dir
AFFILIATION_FOLDER_NAME = "affiliations" # consolidated affiliation output dir
PROCESSED_TMP_FOLDER_NAME = "tmp"
GLOBAL_AFFILIATION_FILE_NAME = "global_affiliations.csv"
DETAILED_AFFILIATION_FILE_NAME = "detailed_affiliations.csv"

# Datacite configuration
DATACITE_FILE_EXTENSION = ".ndjson"
DEFAULT_START_DATE = "2018-01-01"

# Elastic Searh configurations
ES_LOGIN_BSO3_BACK = os.getenv("ES_LOGIN_BSO3_BACK", "")
ES_PASSWORD_BSO3_BACK = os.getenv("ES_PASSWORD_BSO3_BACK", "")
ES_URL = os.getenv("ES_URL", "http://localhost:9200")


def get_harvester_config() -> dict:
    load_environment_variables()

    config_harvester = {'swift': {}}

    # Add env var secrets & pwd for swift - ovh
    config_harvester['swift']['os_username'] = os.getenv('OS_USERNAME2')
    config_harvester['swift']['os_password'] = os.getenv('OS_PASSWORD2')
    config_harvester['swift']['os_user_domain_name'] = os.getenv('OS_USER_DOMAIN_NAME')
    config_harvester['swift']['os_project_domain_name'] = os.getenv('OS_PROJECT_DOMAIN_NAME')
    config_harvester['swift']['os_project_name'] = os.getenv('OS_PROJECT_NAME')
    config_harvester['swift']['os_project_id'] = os.getenv('OS_PROJECT_ID')
    config_harvester['swift']['os_region_name'] = os.getenv('OS_REGION_NAME')
    config_harvester['swift']['os_auth_url'] = os.getenv('OS_AUTH_URL')
    # PostGreSQL
    config_harvester["db"] = {}
    config_harvester["db"]["db_user"] = os.getenv("DB_USER")
    config_harvester["db"]["db_password"] = os.getenv("DB_PASSWORD")
    config_harvester["db"]["db_host"] = os.getenv("DB_HOST")
    config_harvester["db"]["db_port"] = os.getenv("DB_PORT")
    config_harvester["db"]["db_name"] = os.getenv("DB_NAME")
    # ovh folder
    config_harvester['datacite_container'] = DATACITE_DUMP
    config_harvester['raw_datacite_container'] = RAW_DATACITE_DUMP
    config_harvester['processed_datacite_container'] = PROCESSED_DATACITE_DUMP
    config_harvester['affiliations_prefix'] = "affiliations"
    config_harvester['doi_files_prefix'] = "country_matched"
    config_harvester['fr_doi_files_prefix'] = "fr"
    config_harvester['detailed_partition_files_prefix'] = "partition_detailed_affiliations_"
    config_harvester['consolidated_partition_files_prefix'] = "partition_consolidated_affiliations_"

    config_harvester['is_level_debug'] = DEBUG_LEVEL
    # local dump folder
    config_harvester['raw_dump_folder_name'] = RAW_DUMP_FOLDER_NAME
    config_harvester['processed_dump_folder_name'] = os.path.join(MOUNTED_VOLUME_PATH, PROCESSED_DUMP_FOLDER_NAME)
    config_harvester['affiliation_folder_name'] = os.path.join(MOUNTED_VOLUME_PATH, AFFILIATION_FOLDER_NAME)
    config_harvester['processed_tmp_folder_path'] = os.path.join(PROJECT_DIRNAME, PROCESSED_TMP_FOLDER_NAME)
    config_harvester['processed_tmp_folder_name'] = PROCESSED_TMP_FOLDER_NAME
    # config_harvester['global_affiliation_file_name'] = os.path.join(config_harvester['processed_dump_folder_name'],
    #                                                                GLOBAL_AFFILIATION_FILE_NAME)
    # config_harvester['detailed_affiliation_file_name'] = os.path.join(config_harvester['processed_dump_folder_name'],
    #                                                                   DETAILED_AFFILIATION_FILE_NAME)
    config_harvester['global_affiliation_file_name'] = GLOBAL_AFFILIATION_FILE_NAME
    config_harvester['detailed_affiliation_file_name'] = DETAILED_AFFILIATION_FILE_NAME
    config_harvester['affiliation_matcher_service'] = os.getenv("AFFILIATION_MATCHER_SERVICE")
    config_harvester['dump_default_start_date'] = DEFAULT_START_DATE
    config_harvester['es_index_sourcefile'] = os.path.join(MOUNTED_VOLUME_PATH, "datacite_fr.jsonl")
    # Datacite configuration
    config_harvester['datacite_file_extension'] = DATACITE_FILE_EXTENSION

    # Elastic Search configuration
    config_harvester['ES_LOGIN_BSO3_BACK'] = ES_LOGIN_BSO3_BACK
    config_harvester['ES_PASSWORD_BSO3_BACK'] = ES_PASSWORD_BSO3_BACK
    config_harvester['ES_URL'] = ES_URL

    return config_harvester


def load_environment_variables() -> None:
    try:
        load_dotenv()
    except Exception as e:
        print(f'File .env not found: {str(e)}')


config_harvester = get_harvester_config()
