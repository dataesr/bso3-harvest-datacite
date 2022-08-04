from domain.api.abstract_harvester import AbstractHarvester

from adapters.databases.harvest_state_repository import HarvestStateRepository
from adapters.databases.harvest_state_table import HarvestStateTable
from adapters.databases.postgres_session import PostgresSession

from datetime import datetime

from subprocess import run, PIPE, STDOUT


class Harvester(AbstractHarvester):
    harvest_state_repository: HarvestStateRepository

    def __init__(self, harvest_state_repository: HarvestStateRepository):
        self.harvest_state_repository = harvest_state_repository

    def download(
        self, target_directory: str, start_date: datetime, end_date: datetime, interval: str, max_requests: int = 16777216, file_prefix: str = "dcdump-", workers: int = 4, sleep_duration: str = "3m0s"
    ) -> bool:

        harvest_state = HarvestStateTable(start_date, end_date, "in progess", target_directory, slice_type=interval)

        begin_harvesting: bool = True

        if not self.harvest_state_repository.create(harvest_state):
            begin_harvesting = False

        if begin_harvesting:
            interval = self.selectInterval(harvest_state.slice_type)
            number_of_slices: int = self.getNumberSlices(start_date, end_date, interval)

            self.executeDcdump(target_directory, start_date, end_date, interval, max_requests, file_prefix, workers, sleep_duration)

            number_downloaded: int = self.getNumberDownloaded(target_directory, file_prefix, start_date, end_date)

            harvest_state.number_missed = number_of_slices - number_downloaded

            if harvest_state.number_missed == 0:
                harvest_state.status = "done"
            else:
                harvest_state.status = "error"

            harvest_state.number_slices = number_of_slices

            elements_update: dict = {"number_missed": harvest_state.number_missed, "status": harvest_state.status, "number_slices": harvest_state.number_slices}
            filter: dict = {"id": harvest_state.id}
            self.harvest_state_repository.update(elements_update, filter)

            # OVH part Missing

        return begin_harvesting

    def selectInterval(self, input: str) -> str:
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

    def executeDcdump(self, target_directory: str, start_date: datetime, end_date: datetime, interval: str, max_requests: int, file_prefix: str, workers: int, sleep_duration: int):
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

        p = run(cmd, stdout=PIPE, stderr=STDOUT, text=True)

        if p.returncode != 0:
            raise Exception(p.stdout)

    def getNumberDownloaded(self, target_directory: str, file_prefix: str, start_date: datetime, end_date: datetime) -> int:
        lower_limit: str = f"{file_prefix}{start_date.strftime('%Y%m%d%H%M%S')}"
        upper_limit: str = f"{file_prefix}{end_date.strftime('%Y%m%d%H%M%S')}"

        getListFromTargetDirectory: list = ["ls", "-f", target_directory]
        grepByFilePrefix: list = ["grep", file_prefix]
        filterByLimit: list = ["awk", '{ if ($0 < "' + upper_limit + '" && $0 > "' + lower_limit + '") {print "1"; } } ']
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
