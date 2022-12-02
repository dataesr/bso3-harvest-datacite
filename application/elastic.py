from elasticsearch import Elasticsearch, helpers
from config.global_config import config_harvester
from project.server.main.logger import get_logger

client = None
logger = get_logger(__name__)


def get_client():
    global client
    if client is None:
        client = Elasticsearch(config_harvester["ES_URL"], http_auth=(config_harvester["ES_LOGIN_BSO3_BACK"],
                                                                      config_harvester["ES_PASSWORD_BSO3_BACK"]))
    return client


def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)


def get_analyzers() -> dict:
    return {
        'light': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding'
            ]
        }
    }


def get_filters() -> dict:
    return {
        'french_elision': {
            'type': 'elision',
            'articles_case': True,
            'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c', 'jusqu', 'quoiqu', 'lorsqu', 'puisqu']
        }
    }


def reset_index(index: str) -> None:
    es = get_client()
    delete_index(index)

    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        }
    }

    dynamic_match = None
    if 'bso-publications' in index:
        # dynamic_match = "*oa_locations"
        dynamic_match = None
    elif 'publications-' in index:
        dynamic_match = "*authors"

    mappings = {'properties': {}}
    # attention l'analyzer .keyword ne sera pas présent pour ce champs !
    for f in ['z_authors.family', 'z_authors.given', 'title', 'journal_name', 'keywords.keyword',
              'affiliations.name', 'authors.first_name', 'authors.last_name', 'authors.full_name',
              'authors.affiliations.name', 'title_first_author']:
        mappings['properties'][f] = {
            'type': 'text',
            'analyzer': 'light'
        }

    if dynamic_match:
        mappings["dynamic_templates"] = [
            {
                "objects": {
                    "match": dynamic_match,
                    "match_mapping_type": "object",
                    "mapping": {
                        "type": "nested"
                    }
                }
            }
        ]
    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')
