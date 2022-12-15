from typing import List
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject

from config.global_config import config_harvester
from domain.model.ovh_path import OvhPath
from domain.storages.abstract_swift_session import AbstractSwiftSession
from config.logger_config import LOGGER_LEVEL
from project.server.main.logger import get_logger

logger = get_logger(__name__, level=LOGGER_LEVEL)


class SwiftSession(AbstractSwiftSession):
    session: SwiftService

    def __init__(self, config: dict):
        self._session = SwiftService(options=config)
        container_names = []
        try:
            list_account_part = self._session.list()
            for page in list_account_part:
                if page["success"]:
                    for item in page["listing"]:
                        i_name = item["name"]
                        container_names.append(i_name)
                        if i_name == config_harvester['datacite_container']:
                            print("using input SWIFT", config_harvester['datacite_container'], "container:", item)
                else:
                    logger.error("error listing SWIFT object storage containers")

        except SwiftError as e:
            logger.exception(f"error listing containers: {str(e)}")

        if config_harvester['datacite_container'] not in container_names:
            # create the container
            try:
                self._session.post(container=config_harvester['datacite_container'])
            except SwiftError:
                logger.exception(
                    "error creating SWIFT object storage container " + config_harvester['datacite_container'])
        else:
            logger.debug("container already exists on SWIFT object storage: " + config_harvester['datacite_container'])

    def getSession(self) -> SwiftService:
        return self._session

    def upload_files_to_swift(self, container, file_path_dest_path_tuples: List):
        """
        Bulk upload of a list of files to current SWIFT object storage container
        """
        # Slightly modified to be able to upload to more than one dest_path
        objs = [SwiftUploadObject(file_path, object_name=str(dest_path)) for file_path, dest_path in
                file_path_dest_path_tuples if isinstance(dest_path, OvhPath)]
        try:
            for result in self._session.upload(container, objs):
                if not result['success']:
                    error = result['error']
                    if result['action'] == "upload_object":
                        logger.error("Failed to upload object %s to container %s: %s" % (
                            container, result['object'], error))
                        print(f'failed to upload files to ovh {error}')
                    else:
                        logger.exception("%s" % error, exc_info=True)
                else:
                    if result['action'] == "upload_object":
                        logger.debug(
                            f'Result upload : {result["object"]} succesfully uploaded on {result["container"]} (from {result["path"]})')
        except SwiftError as e:
            logger.exception("error uploading file to SWIFT container", exc_info=True)
