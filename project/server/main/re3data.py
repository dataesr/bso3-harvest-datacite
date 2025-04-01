import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from project.server.main.logger import get_logger
from config.global_config import MOUNTED_VOLUME_PATH

logger = get_logger(__name__)

RE3DATA_API_BASE_URL = 'https://www.re3data.org/api/v1/'

def get_list_re3data_repositories():
    url = f'{RE3DATA_API_BASE_URL}repositories'
    logger.debug(f'retrieve re3data from {url}')
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    repositories = soup.find_all('repository')
    data = []
    for ix, repository in enumerate(repositories):
        elt = {}
        elt['id'] = repository.find('id').text
        elt['name'] = repository.find('name').text
        if repository.find('doi'):
            elt['doi'] = repository.find('doi').text.replace('https://doi.org/', '').lower()
        details = get_re3data_repository(elt['id'])
        elt.update(details)
        data.append(elt)
        #if ix%50 == 0:
        #    print(ix, end=',')
    re3data_dict_filename = f'{MOUNTED_VOLUME_PATH}/re3data.json'
    logger.debug(f'writing {re3data_dict_filename}')
    json.dump(data, open(re3data_dict_filename, 'w'))
    logger.debug(f"{len('data')} repositories re3data dumped to {re3data_dict_filename}")
    return data

def enrich_re3data():
    re3 = pd.read_json(f'{MOUNTED_VOLUME_PATH}/re3data.json')
    signature_count = {}
    re3['url_signatures'] = None
    re3['url_signature'] = None
    ix = -1
    for row in re3.itertuples():
        ix += 1
        u = row.url
        if not isinstance(u, str):
            continue
        if len(u.strip().lower())<2:
            continue
        if len(u)<2:
            continue
        url_signatures = get_url_signature(u)['normalized']
        re3.at[ix, 'url_signature'] = url_signatures
    re3_existing_signatures = {}
    for row in re3.itertuples():
        signature = row.url_signature
        if not isinstance(signature, str):
            continue
        if signature in re3_existing_signatures:
            logger.debug('---- DUPLICATE -----')
            logger.debug(f"{row._asdict()['name']}, {row._asdict()['id']}, {signature}")
            logger.debug(f"{re3_existing_signatures[signature]['name']}, {re3_existing_signatures[signature]['id']}")
        re3_existing_signatures[signature] = row._asdict()

    re3_existing_signatures['urgi.versailles.inrae.fr'] = re3_existing_signatures['urgi.versailles.inrae.fr/gnpis']
    re3_existing_signatures['urgi.versailles.inra.fr'] = re3_existing_signatures['urgi.versailles.inrae.fr/gnpis']
    re3_existing_signatures['campagnes.flotteoceanographique.fr'] = re3_existing_signatures['data.ifremer.fr']
    re3_existing_signatures['cdsarc.cds.unistra.fr'] = re3_existing_signatures['cdsweb.u-strasbg.fr']
    json.dump(re3_existing_signatures, open(f'{MOUNTED_VOLUME_PATH}/re3data_dict.json', 'w'))

def find_re3(url, re3_existing_signatures):
    signatures = get_url_signature(url)['signatures']
    for s in signatures:
        if s in re3_existing_signatures:
            return re3_existing_signatures[s]


def get_re3data_repository(re3data_id):
    url = f'{RE3DATA_API_BASE_URL}repository/{re3data_id}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'lxml')
    elt = {}
    #names
    names = []
    for e in soup.find_all('r3d:repositoryname'):
        lang = e.attrs['language']
        name = {'name': e.get_text(), 'lang': lang}
        names.append(name)
    elt['names'] = names
    #url
    if soup.find('r3d:repositoryurl'):
        elt['url'] = soup.find('r3d:repositoryurl').get_text()
    #descriptions
    descriptions = []
    for e in soup.find_all('r3d:description'):
        lang = e.attrs['language']
        description = {'description': e.get_text(), 'lang': lang}
        descriptions.append(description)
    elt['descriptions'] = descriptions
    #type
    if soup.find('r3d:type'):
        elt['type'] = soup.find('r3d:type').get_text()
    #repositorylanguage
    if soup.find('r3d:repositorylanguage'):
        elt['language'] = soup.find('r3d:repositorylanguage').get_text()
    #subjects
    subjects = []
    for e in soup.find_all('r3d:subject'):
        scheme = e.attrs['subjectscheme']
        subject_name = e.get_text()
        subject = {'subject': subject_name, 'scheme': scheme}
        if scheme == 'DFG':
            subject_code = subject_name.split(' ')[0]
            level = len(subject_code)
            subject_name = ' '.join(subject_name.split(' ')[1:])
            subject = {'subject': subject_name, 'scheme': scheme, 'subject_code': subject_code, 'level': level}
        subjects.append(subject)
    elt['subjects'] = subjects
    #contents
    contents = []
    for e in soup.find_all('r3d:contenttype'):
        scheme = e.attrs['contenttypescheme']
        content_name = e.get_text()
        content = {'content': content_name, 'scheme': scheme}
        contents.append(content)
    elt['contents'] = contents
    #keywords
    keywords = []
    for e in soup.find_all('r3d:keyword'):
        keyword = e.get_text()
        keywords.append(keyword)
    elt['keywords'] = keywords
    #institutions
    institutions = []
    for e in soup.find_all('r3d:institution'):
        institution = {}
        for f in ['name', 'country', 'type', 'url']:
            x =  e.find(f'r3d:institution{f}')
            if x and x.get_text():
                institution[f] = x.get_text()
        if institution:
            institutions.append(institution)
    elt['institutions'] = institutions
    #pidsystem
    pids = []
    for e in soup.find_all('r3d:pidsystem'):
        pid = e.get_text()
        pids.append(pid)
    elt['pid'] = pids
    return elt

def get_url_signature(x):
    if not isinstance(x, str):
        return None
    x = x.lower().strip().split('?')[0]
    x = x.replace('http://', '').replace('https://', '').replace('www.', '').strip()
    if len(x)<2:
        return None
    split_1 = []
    for ex, e in enumerate(x.split('/')):
        skip = False
        if len(e)==0:
            skip = True
        if ex > 0:
            for w in ['en', 'index', 'home']:
                if w == e:
                    skip = True
                if e.startswith(w+'.'):
                    skip = True
        if skip == False:
            split_1.append(e)
    if len(split_1) == 0:
        print(x)
    base = split_1[0]
    res1, res2 = [], []
    for ix in range(1, len(split_1) + 1):
        res1.append('/'.join(split_1[0:ix]))
    split_2 = [e for e in base.split('.') if e]
    for ix in range(2, len(split_2) + 1):
        res2.append('.'.join(split_2[-ix:]))
    res = []
    for r in res2:
        res.append(r)
    for r in res1:
        if r not in res:
            res.append(r)
    return {'normalized': '/'.join(split_1), 'signatures': res}


