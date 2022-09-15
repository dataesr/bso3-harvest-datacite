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
METADATA_SUFFIX = '.json'
PUBLICATION_SUFFIX = '.pdf'
SOFTCITE_SUFFIX = '.software.json'
GROBID_SUFFIX = '.tei.xml'

# folder in OVH
DATACITE_DUMP = 'datacite'
RAW_DATACITE_DUMP = 'raw'
PROCESSED_DATACITE_DUMP = 'processed'

# folder in project
RAW_DUMP_FOLDER_NAME = os.getenv("RAW_DUMP_FOLDER_NAME", os.path.join(PROJECT_DIRNAME, "sample-dump"))
PROCESSED_DUMP_FOLDER_NAME = "dois"
GLOBAL_AFFILIATION_FILE_NAME = "global_affiliations.csv"
DETAILED_AFFILIATION_FILE_NAME = "detailed_affiliations.csv"


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
    # ovh folder
    config_harvester['datacite_container'] = DATACITE_DUMP
    config_harvester['raw_datacite_container'] = RAW_DATACITE_DUMP
    config_harvester['processed_datacite_container'] = PROCESSED_DATACITE_DUMP

    config_harvester['is_level_debug'] = DEBUG_LEVEL
    # local dump folder
    config_harvester['raw_dump_folder_name'] = RAW_DUMP_FOLDER_NAME
    config_harvester['processed_dump_folder_name'] = os.path.join(PROJECT_DIRNAME, PROCESSED_DUMP_FOLDER_NAME)
    config_harvester['global_affiliation_file_name'] = os.path.join(config_harvester['processed_dump_folder_name'],
                                                                    GLOBAL_AFFILIATION_FILE_NAME)
    config_harvester['detailed_affiliation_file_name'] = os.path.join(config_harvester['processed_dump_folder_name'],
                                                                      DETAILED_AFFILIATION_FILE_NAME)
    config_harvester['affiliation_matcher_service'] = os.getenv("AFFILIATION_MATCHER_SERVICE")

    print(f"config_harvester {config_harvester}")

    return config_harvester


def load_environment_variables() -> None:
    try:
        load_dotenv()
    except Exception as e:
        print(f'File .env not found: {str(e)}')


config_harvester = get_harvester_config()
