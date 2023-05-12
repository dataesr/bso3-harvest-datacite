import requests
import json
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
        elt['re3data_id'] = repository.find('id').text
        elt['re3data_name'] = repository.find('name').text
        if repository.find('doi'):
            elt['re3data_doi'] = repository.find('doi').text.replace('https://doi.org/', '').lower()
        details = get_re3data_repository(elt['re3data_id'])
        elt.update(details)
        data.append(elt)
        #if ix%50 == 0:
        #    print(ix, end=',')
    json.dump(data, open(f'{MOUNTED_VOLUME_PATH}re3data.json', 'w'))
    logger.debug(f"{len('data')} repositories re3data dumped to {MOUNTED_VOLUME_PATH}re3data.json")
    return data

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
