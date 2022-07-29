from domain.api.AbstractHarvester import AbstractHarvester

from adapters.databases.harvest_state_respository import HarvestStateRepository
from adapters.databases.harvest_state_table import HarvestStateTable
from adapters.databases.postgres_session import PostgresSession

from datetime import datetime


class Harvester(AbstractHarvester):
    postgres_session: PostgresSession
    harvest_state_repository: HarvestStateRepository

    def __init__(self, harvest_state_repository: HarvestStateRepository, postgres_session: PostgresSession):
        self.harvest_state_repository = harvest_state_repository
        self.postgres_session = postgres_session

    def download(
        self, target_directory: str, start_date: datetime, end_date: datetime, interval: str, max_requests: int = 16777216, file_prefix: str = "dcdump-", workers: int = 4, sleep_duration: int = 5
    ):

        result: list = self.harvest_state_repository.get({"current_directory": target_directory})
        harvest_state: HarvestStateTable = None

        if result:

            harvest_state = result[0]
            # check status
            # check date
        else:
            harvest_state = HarvestStateTable(0, start_date, end_date, "in progess", target_directory)
            # if self.harvest_state_repository.create(harvest_state):
            # insert
            # check insert ok

        # [w]eekly, [d]daily, [h]ourly, [e]very minute (default "d")
        if interval == "minute":
            interval = "e"
        elif interval == "daily":
            interval = "d"
        elif interval == "hour":
            interval = "h"
        elif interval == "week":
            interval = "w"
        else:
            interval = "e"

        return super().download()
