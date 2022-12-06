from adapters.databases.process_state_repository import ProcessStateRepository
from adapters.databases.mock_postgres_session import MockPostgresSession
from adapters.databases.process_state_table import ProcessStateTable

from adapters.databases.utils import check_conformity, check_conformity_and_get_where_clauses


class MockHarvestStateRepository(ProcessStateRepository):
    session: MockPostgresSession

    create_same_file_in_table_already_exist: bool = False
    nb_calls_create: int = 0
    nb_calls_update: int = 0

    def __init__(self, session: MockPostgresSession):
        self.session = session

        self.update_args_calls: list = []

    def create(self, process_state: ProcessStateTable) -> bool:
        self.nb_calls_create += 1

        added: bool = False

        if not self.create_same_file_in_table_already_exist:
            process_state.id = 1

            added = True

        return added

    def update(self, values_args: dict, where_args: dict = {}):
        self.nb_calls_update += 1
        self.update_args_calls.append((values_args, where_args))

        check_conformity(values_args, ProcessStateTable)

        check_conformity_and_get_where_clauses(where_args, ProcessStateTable)

        return None
