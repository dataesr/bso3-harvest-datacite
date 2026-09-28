import requests
import json
from retry import retry
from project.server.main.logger import get_logger
logger = get_logger(__name__)

BASE = "https://api.datacite.org"
SIZE = 1000

@retry(delay=200, tries=5)
def fetch_all(endpoint):
    """Récupère toutes les pages d'un endpoint (pagination manuelle)."""
    items, page = [], 1
    while True:
        r = requests.get(f"{BASE}/{endpoint}",
                         params={"page[size]": SIZE, "page[number]": page})
        r.raise_for_status()
        d = r.json()
        items += d["data"]
        if page >= d["meta"]["totalPages"]:
            return items
        page += 1

def get_client_ids_infos():

    providers = fetch_all("providers")
    clients = {c["id"]: c["attributes"] for c in fetch_all("clients")}
    hard_coded_map = json.load(open('client_id_hard_coded_names.json', 'r'))
    client_id_infos = {}
    for p in providers:
        a, rel = p["attributes"], p["relationships"]
        cons = ((rel.get("consortium") or {}).get("data") or {}).get("id")
        client_ids = [c["id"] for c in rel["clients"]["data"]] or [None]
        for cid in client_ids:  # une ligne par entrepôt
            if not isinstance(cid, str):
                continue
            ca = clients.get(cid, {})
            elt = {
            "provider_id": p["id"],
            "provider_name": a["name"],
            "provider_display_name": a.get('displayName'),
            "provider_member_type": a["memberType"],
            "provider_country": a["country"],
            "provider_region": a["region"],
            "provider_ror": a["rorId"],
            "provider_consortium": cons,
            "client_id": cid,
            "client_name": ca.get("name"),
            "client_alternate_name": ca.get('alternateName'),
            "client_year": ca.get('year'),
            "client_type": ca.get("clientType"),   # repository, periodical…
            "client_url": ca.get("url"),
            "client_re3data": ca.get("re3data")
            }
            if cid in hard_coded_map:
                elt['client_name'] = hard_coded_map[cid]
            client_id_infos[cid] = elt
    logger.debug(f"{len(providers)} providers, {len(clients)} clients, {len(client_id_infos)} client_ids infos")
    return client_id_infos

