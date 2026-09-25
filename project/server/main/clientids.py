import requests
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

    client_id_infos = {}
    for p in providers:
        a, rel = p["attributes"], p["relationships"]
        cons = ((rel.get("consortium") or {}).get("data") or {}).get("id")
        client_ids = [c["id"] for c in rel["clients"]["data"]] or [None]
        for cid in client_ids:  # une ligne par entrepôt
            ca = clients.get(cid, {})
            elt = {
                "provider_id": p["id"],
                "provider_name": a["name"],
                "member_type": a["memberType"],
                "provider_country": a["country"],
                "provider_ror": a["rorId"], 
                "consortium": cons,
                "client_id": cid,
                "client_name": ca.get("name"),
                "client_type": ca.get("clientType"),   # repository, periodical…
                "client_url": ca.get("url"),
                "re3data": ca.get("re3data"),
            }
            client_id_infos[cid] = elt
    logger.debug(f"{len(providers)} providers, {len(clients)} clients, {len(client_id_infos)} client_ids infos")
    return client_id_infos

