import os
from typing import Union

from retry import retry

from domain.model.ovh_path import OvhPath
from project.server.main.logger import get_logger

logger = get_logger(__name__)
key = os.getenv("OS_PASSWORD2")
project_name = os.getenv("OS_PROJECT_NAME")
project_id = os.getenv("OS_TENANT_ID")
tenant_name = os.getenv("OS_TENANT_NAME")
username = os.getenv("OS_USERNAME2")
user = f"{tenant_name}:{username}"
init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"


@retry(delay=2, tries=50)
def upload_object(container: str, source: str, target: Union[str, OvhPath], segments=True) -> str:
    logger.debug(f"Uploading {source} in {container} as {target}")
    cmd = init_cmd + f" upload {container} {source} --skip-identical --object-name {target}"
    if segments:
        cmd += " --segment-size 1048576000 --segment-threads 100"
    os.system(cmd)
    return f"https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{target}"
