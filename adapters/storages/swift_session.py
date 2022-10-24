import shutil
from typing import List
from swiftclient.service import SwiftService, SwiftError, SwiftUploadObject

from config.global_config import config_harvester
from domain.ovh_path import OvhPath
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

        # TODO change to only datacite folder
        for container_name in [config_harvester['raw_datacite_container'], config_harvester['processed_datacite_container']]:
            if container_name not in container_names:
                # create the container
                try:
                    self._session.post(container=container_name)
                except SwiftError:
                    logger.exception(
                        "error creating SWIFT object storage container " + container_name)
            else:
                logger.debug("container already exists on SWIFT object storage: " + container_name)

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

    def download_files(self, container, file_path, dest_path):
        """
        Download a file given a path and returns the download destination file path.
        """
        if type(file_path) == str:
            objs = [file_path]
        elif type(file_path) == list:
            objs = file_path
        try:
            for down_res in self._session.download(container=container, objects=objs):
                if down_res['success']:
                    local_path = down_res['path']
                    shutil.move(local_path, dest_path)
                else:
                    logger.error("'%s' download failed" % down_res['object'])
        except SwiftError:
            logger.exception("error downloading file from SWIFT container")

    def get_swift_list(self, container, dir_name=None):
        """
        Return all contents of a given dir in SWIFT object storage.
        Goes through the pagination to obtain all file names.
        """
        result = []
        try:
            options = {"prefix": dir_name} if dir_name else None
            list_parts_gen = self._session.list(container=container, options=options)
            for page in list_parts_gen:
                if page["success"]:
                    for item in page["listing"]:
                        result.append(item["name"])
                else:
                    logger.error(page["error"])
        except SwiftError as e:
            logger.error(e.value)
        return result