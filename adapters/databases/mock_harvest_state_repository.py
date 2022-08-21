from adapters.databases.harvest_state_repository import HarvestStateRepository
from adapters.databases.mock_postgres_session import MockPostgresSession
from adapters.databases.harvest_state_table import HarvestStateTable

from adapters.databases.utils import check_conformity, check_conformity_and_get_where_clauses


class MockHarvestStateRepository(HarvestStateRepository):
    session: MockPostgresSession

    create_same_job_already_exists: bool = False
    create_same_current_directory_already_exists: bool = False
    nb_calls_create: int = 0
    nb_calls_update: int = 0

    def __init__(self, session: MockPostgresSession):
        self.session = session

        self.update_args_calls: list = []

    def create(self, harvest_state: HarvestStateTable) -> bool:
        self.nb_calls_create += 1

        added: bool = False

        if not self.create_same_job_already_exists:
            if self.create_same_current_directory_already_exists:
                harvest_state.slice_type = "day"

            harvest_state.id = 1

            added = True

        return added

    def update(self, values_args: dict, where_args: dict = {}):
        self.nb_calls_update += 1
        self.update_args_calls.append((values_args, where_args))

        check_conformity(values_args, HarvestStateTable)

        check_conformity_and_get_where_clauses(where_args, HarvestStateTable)

        return None
