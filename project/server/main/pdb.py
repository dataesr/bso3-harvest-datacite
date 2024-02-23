import requests
import json
import pickle
import pandas as pd
from project.server.main.logger import get_logger
from application.utils_processor import append_to_file
from retry import retry
logger = get_logger(__name__)

pdbs = {}
def get_all_pdb_entry_ids():
    pdb_entry_url = 'https://data.rcsb.org/rest/v1/holdings/current/entry_ids'
    pdb_ids = pd.read_json(pdb_entry_url)[0].to_list()
    logger.debug(f'{len(pdb_ids)} pdb ids')
    return pdb_ids

@retry(delay=200, tries=5)
def harvest_one_pdb(pdb):
    logger.debug(f'harvesting PDB {pdb}')
    res = requests.get(f'https://data.rcsb.org/rest/v1/core/entry/{pdb}').json()
    for f in ['cell', 'diffrn', 'diffrn_detector', 'diffrn_radiation', 'diffrn_source', 'exptl_crystal', 'exptl_crystal_grow', 'reflns_shell', 'refine', 'refine_hist', 'refine_ls_restr', 'reflns', 'pdbx_audit_revision_details', 'pdbx_audit_revision_group', 'pdbx_audit_revision_item', 'pdbx_reflns_twin', 'rcsb_entry_info', 'symmetry', 'pdbx_audit_revision_category', 'pdbx_audit_revision_history', 'pdbx_database_status', 'pdbx_initial_refinement_model', 'rcsb_entry_container_identifiers']:
        if f in res:
            del res[f]
    return res

def load_pdbs():
    global pdbs
    try:
        pdbs = pickle.load(open('/data/pdbs.pickle', 'rb'))
    except:
        pdbs = {}
    logger.debug(f'{len(pdbs)} pdbs loaded')
    return pdbs

def save_pdbs():
    global pdbs
    pickle.dump(pdbs, open('/data/pdbs.pickle', 'wb'))
    logger.debug(f'{len(pdbs)} pdbs saved')

def get_one_pdb(pdb):
    global pdbs
    if pdb.lower() in pdbs:
        return {'pdb_elt': pdbs[pdb.lower()], 'update':0}
    new_pdb = harvest_one_pdb(pdb)
    pdbs[pdb.lower()] = new_pdb
    return {'pdb_elt': new_pdb, 'update': 1}

def update_pdbs():
    global pdbs
    load_pdbs()
    pdb_ids = get_all_pdb_entry_ids()
    ix = 0
    for pdb in pdb_ids:
        elt = get_one_pdb(pdb)
        ix += elt['update']
        if ix % 1000 == 0:
            save_pdbs()
    save_pdbs()

def parse_pdb(e, bso_doi_dict):
    elt = {}
    rors, bso_local_affiliations = [], []
    title = e['struct']['title']
    elt['title'] = title
    pdb_id = e['rcsb_id'].lower()
    doi=f'10.2210/pdb{pdb_id.lower()}/pdb'
    elt['doi'] = doi
    elt["external_ids"] = []
    elt["external_ids"].append({
              "id_type": "crossref",
              "id_value": doi})
    elt['year'] = int(e['rcsb_accession_info']['initial_release_date'][0:4])
    elt['publisher'] = "Worldwide Protein Data Bank"
    elt['genre'] = 'dataset'
    elt['genre_raw'] = 'dataset'
    elt['genre_detail'] = 'dataset'
    doi_supplement_to = []
    if e.get('rcsb_primary_citation', {}).get('pdbx_database_id_doi'):
        doi_supplement_to.append(e.get('rcsb_primary_citation', {}).get('pdbx_database_id_doi').lower().strip())
    if doi_supplement_to:
        elt['doi_supplement_to'] = list(set(doi_supplement_to))
    else:
        elt['doi_supplement_to'] = []
    fr_reasons=[]
    fr_publications_linked = []
    for c in elt['doi_supplement_to']:
        if c in bso_doi_dict:
            fr_reasons.append('linked_article')
            publi_info = bso_doi_dict[c]
            rors += publi_info['rors']
            bso_local_affiliations += publi_info['bso_local_affiliations']
            fr_publications_linked.append({'doi': c, 'rors': publi_info['rors'], 'bso_local_affiliations': publi_info['bso_local_affiliations']})
    authors=[]
    for a in e.get('audit_author'):
        if a.get('name'):
            new_a = {'full_name': a['name'], 'role': 'creator'}
            authors.append(new_a)
    elt['authors'] = authors
    elt['fr_reasons'] = fr_reasons
    fr_reasons.sort()
    fr_reasons_concat = ";".join(fr_reasons)
    elt['fr_reasons_concat'] = fr_reasons_concat
    elt['fr_publications_linked'] = fr_publications_linked
    if rors:
        elt['rors'] = rors
    if bso_local_affiliations:
        elt['bso_local_affiliations'] = bso_local_affiliations
    return elt

def treat_pdb(e, bso_doi_dict, index_name):
    elt = parse_pdb(e, bso_doi_dict)
    if len(elt.get('fr_reasons', []))>0:
        logger.debug(f"french pdb {elt['doi']}")
        append_to_file(file=f'/data/{index_name}.jsonl', _str=json.dumps(elt))
