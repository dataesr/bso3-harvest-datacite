from typing import Tuple
from project.server.main.logger import get_logger
from config.logger_config import LOGGER_LEVEL
from domain.api.abstract_harvester import AbstractHarvester

from adapters.databases.harvest_state_repository import HarvestStateRepository
from adapters.databases.harvest_state_table import HarvestStateTable

from threading import Thread

from datetime import datetime

from pathlib import Path

from subprocess import run, PIPE, STDOUT

logger = get_logger(__name__, level=LOGGER_LEVEL)


class Harvester(AbstractHarvester):
    """
    The Harvester object is able to harvest data from dcdump and communicate with harvest_state table.

    Args:
        harvest_state_repository (adapters.databases.harvest_state_repository.HarvestStateRepository): The harvest_state_repository is used to communicate and make some operation with harvest_state table.

    Attributes:
        harvest_state_repository (adapters.databases.harvest_state_repository.HarvestStateRepository): This is where we store harvest_state_repository.
    """

    harvest_state_repository: HarvestStateRepository

    def __init__(self, harvest_state_repository: HarvestStateRepository):
        self.harvest_state_repository = harvest_state_repository

    def download(
            self,
            target_directory: str,
            start_date: datetime,
            end_date: datetime,
            interval: str,
            max_requests: int = 16777216,
            file_prefix: str = "dcdump-",
            workers: int = 4,
            sleep_duration: str = "3m0s",
            use_thread=False,
            force=False
    ) -> Tuple[bool, HarvestStateTable]:

        """
        The download function check if we can download. If we can, the function prepares and launch harvesting.

        Args:
            target_directory (str): Directory where we are going to store the data downloaded.
            start_date (datetime): Date that filter the download, it means that we only download data with an updated date attribute upper that this date.
            end_date (datetime): Date that filter the download, it means that we only download data with an updated date attribute lesser that this date.
            interval (str): Interval that decide which type of interval we download (minute, hour, day, week)
            max_requests (int): Max requests that we can do when downloading
            file_prefix (str): Prefix of the file downloaded in the target_directory.
            workers (int): Numbers of thread used when harvesting.
            sleep_duration (str): How much time do we wait when we have an error for a download.
            use_thread (bool): If True, we launch harvesting in a thread and we return begin_harvesting. If False, we wait the end of harvesting to return begin_harvesting.

        Returns:
            begin_harvesting, harvest_state  (bool, HarvestStateTable): For the first one, iff True the harvesting has begun and if False, no harvesting launched. For the last, it's the state of the harvesting (launched or not).
        """

        harvest_state = HarvestStateTable(start_date, end_date, "in progress", target_directory, slice_type=interval)

        begin_harvesting: bool = True

        HarvestStateTable.createTable(self.harvest_state_repository.session.getEngine())

        if not self.harvest_state_repository.create(harvest_state) and (not force):
            begin_harvesting = False
            harvest_state.status = "already exists"

        if begin_harvesting:
            logger.info(f"Begin Harvesting")
            dcdump_interval: str = self.selectInterval(harvest_state.slice_type)
            number_of_slices: int = self.getNumberSlices(harvest_state.start_date, harvest_state.end_date,
                                                         dcdump_interval)

            harvest_state.number_slices = number_of_slices
            if use_thread:
                thread: Thread = Thread(target=self.harvesting, args=(
                    harvest_state, dcdump_interval, max_requests, file_prefix, workers, sleep_duration))
                thread.start()
            else:
                self.harvesting(harvest_state, dcdump_interval, max_requests, file_prefix, workers, sleep_duration)

        return begin_harvesting, harvest_state

    def harvesting(self, harvest_state: HarvestStateTable, dcdump_interval, max_requests, file_prefix, workers,
                   sleep_duration):
        """
        The harvesting function executes dcdump, check different information and update harvest state

        Args:
            harvest_state (adapters.databases.harvest_state_table.HarvestStateTable): Harvest State containing different information mandatoring for harvesting and useful to update Harvest State.
            dcdump_interval (str): Interval that decide which type of interval we download (minute, hour, day, week)
            max_requests (int): Max requests that we can do when downloading
            file_prefix (str): Prefix of the file downloaded in the target_directory.
            workers (int): Numbers of thread used when harvesting.
            sleep_duration (str): How much time do we wait when we have an error for a download.

        Returns:
            Nothing (void).
        """
        self.executeDcdump(harvest_state.current_directory, harvest_state.start_date, harvest_state.end_date,
                           dcdump_interval, max_requests, file_prefix, workers, sleep_duration)

        number_downloaded: int = self.getNumberDownloaded(harvest_state.current_directory, file_prefix,
                                                          harvest_state.start_date, harvest_state.end_date)

        harvest_state.number_missed = harvest_state.number_slices - number_downloaded

        if harvest_state.number_missed == 0:
            harvest_state.status = "done"
        else:
            harvest_state.status = "error"

        elements_update: dict = {"number_missed": harvest_state.number_missed, "status": harvest_state.status,
                                 "number_slices": harvest_state.number_slices}
        filter: dict = {"id": harvest_state.id}
        self.harvest_state_repository.update(elements_update, filter)

        # OVH part Missing

    def selectInterval(self, input: str) -> str:
        """
        The selectInterval function gets an input interval from the user and convert it in dcdump format. If the input is not correct, by default the interval is by minute.

        Args:
            input (str): Input from the user.

        Returns:
            The interval in dcdump format.
        """
        interval: str = None
        # [w]eekly, [d]daily, [h]ourly, [e]very minute (default "d")
        if input == "minute":
            interval = "e"
        elif input == "day":
            interval = "d"
        elif input == "hour":
            interval = "h"
        elif input == "week":
            interval = "w"
        else:
            interval = "e"

        return interval

    def getNumberSlices(self, start_date: datetime, end_date: datetime, interval: str) -> int:
        """
        The getNumberSlices function executes dcdump debug to get the number of slices.

        Args:
            start_date (datetime): Date that filter the download of dcdump, it means that we only download data with an updated date attribute upper that this date.
            end_date (datetime): Date that filter the download of dcdump, it means that we only download data with an updated date attribute upper that this date.
            interval (str): Interval used in dcdump.

        Returns:
            The number of files that we will download if we use these arguments.
        """
        cmd = [
            "./dcdump/dcdump",
            "-i",
            interval,
            "-s",
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "-e",
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
            "-debug",
        ]

        p = run(cmd, stdout=PIPE, stderr=STDOUT, text=True)

        if p.returncode != 0:
            raise Exception(p.stdout)

        result = int(p.stdout.split('="')[-1].split()[0])

        return result

    def executeDcdump(self, target_directory: str, start_date: datetime, end_date: datetime, interval: str,
                      max_requests: int, file_prefix: str, workers: int, sleep_duration: int):
        """
        The executeDcdump function executes dcdump with the arguments passed.

        Args:
            target_directory (str): Directory where we are going to store the data downloaded.
            start_date (datetime): Date that filter the download of dcdump, it means that we only download data with an updated date attribute upper that this date.
            end_date (datetime): Date that filter the download of dcdump, it means that we only download data with an updated date attribute upper that this date.
            interval (str): Interval used in dcdump.
            max_requests (int): Max requests that we can do when downloading.
            file_prefix (str): Prefix of the file downloaded in the target_directory.
            workers (int): Numbers of thread used when harvesting.
            sleep_duration (str): How much time do we wait when we have an error for a download.

        Raises:
            Exception from the command line.

        Returns:
            Nothing (void)
        """
        # create directory if not exists
        Path(target_directory).mkdir(exist_ok=True)

        cmd = [
            "./dcdump/dcdump",
            "-d",
            target_directory,
            "-s",
            start_date.strftime("%Y-%m-%d %H:%M:%S"),
            "-e",
            end_date.strftime("%Y-%m-%d %H:%M:%S"),
            "-i",
            interval,
            "-l",
            str(max_requests),
            "-p",
            file_prefix,
            "-w",
            str(workers),
            "-sleep",
            str(sleep_duration),
        ]
        p = run(' '.join(cmd), text=True, capture_output=True)

        if p.returncode != 0:
            raise Exception(p.stdout)

        return str(p.stdout)

    def getNumberDownloaded(self, target_directory: str, file_prefix: str, start_date: datetime,
                            end_date: datetime) -> int:
        """
        The getNumberDownloaded function gets the number of files between start_date and end_date in target_directory and with as prefix file_prefix.

        Args:
            target_directory (str): Directory where we stored the data downloaded.
            start_date (datetime): Date used to filter the download of dcdump, it means that we only downloaded data with an updated date attribute upper that this date. Here, we are going to filter with this date as the lesser limit.
            end_date (datetime): Date used to filter the download of dcdump, it means that we only downloaded data with an updated date attribute upper that this date. Here, we are going to filter with this date as the upper limit.
            file_prefix (str): Prefix of the file downloaded in the target_directory.

        Raises:
            Exception from the command line.

        Returns:
            The number of files downloaded.
        """

        lower_limit: str = f"{file_prefix}{start_date.strftime('%Y%m%d%H%M%S')}"
        upper_limit: str = f"{file_prefix}{end_date.strftime('%Y%m%d%H%M%S')}"

        getListFromTargetDirectory: list = ["ls", "-f", target_directory]
        grepByFilePrefix: list = ["grep", file_prefix]
        filterByLimit: list = ["awk",
                               '{ if ($0 < "' + upper_limit + '" && $0 > "' + lower_limit + '") {print "1"; } } ']
        countNumberOfLine: list = ["wc", "-l"]

        cmds: list = [getListFromTargetDirectory, grepByFilePrefix, filterByLimit, countNumberOfLine]

        p = run(cmds[0], stdout=PIPE, stderr=STDOUT, text=True)

        if p.returncode != 0:
            raise Exception(p.stdout)

        for cmd in cmds[1:]:
            p = run(cmd, stdout=PIPE, stderr=STDOUT, text=True, input=p.stdout)

            if p.returncode != 0:
                raise Exception(p.stdout)

        return int(p.stdout)
