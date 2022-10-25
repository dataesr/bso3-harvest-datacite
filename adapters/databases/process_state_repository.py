from domain.databases.abstract_process_state_repository import AbstractProcessStateRepository
from adapters.databases.process_state_table import ProcessStateTable
from adapters.databases.postgres_session import PostgresSession
from adapters.databases.utils import check_conformity, check_conformity_and_get_where_clauses

from sqlalchemy import select, update, and_, asc


class ProcessStateRepository(AbstractProcessStateRepository):
    session: PostgresSession

    def __init__(self, session: PostgresSession):
        self.session = session

    def create(self, process_state: ProcessStateTable) -> bool:
        added: bool = False
        with self.session.sessionScope() as session:
            error_message: str = "error"

            # check if the file has already been processed in the past
            statement = select(ProcessStateTable).where(
                ProcessStateTable.file_name == process_state.file_name
            )
            result = session.execute(statement).scalars().all()

            if not result:
                session.add(process_state)
                added = True
                session.flush()
                session.expunge(process_state)

        return added

    def get(self, where_args: dict = {}):
        where_clauses: list = check_conformity_and_get_where_clauses(where_args, ProcessStateTable)

        if not ProcessStateTable.checkExistence(self.session.getEngine()):
            ProcessStateTable.createTable(self.session.getEngine())
            return []

        with self.session.sessionScope() as session:
            statement = select(ProcessStateTable).order_by(asc(ProcessStateTable.__table__.c.id))

            if where_args:
                statement = statement.where(and_(*where_clauses))

            results = session.execute(statement).scalars().all()

            for result in results:
                session.expunge(result)

        return [result.__dict__ for result in results]

    def update(self, values_args: dict, where_args: dict = {}):
        check_conformity(values_args, ProcessStateTable)

        where_clauses: list = check_conformity_and_get_where_clauses(where_args, ProcessStateTable)

        with self.session.sessionScope() as session:
            statement = update(ProcessStateTable).values(values_args)

            if where_args:
                statement = statement.where(and_(*where_clauses))

            statement = statement.execution_options(synchronize_session="fetch")
            result = session.execute(statement)

        return result
