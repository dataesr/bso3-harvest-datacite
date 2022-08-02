from operator import and_
from unicodedata import name
from domain.databases.abstract_harvest_state_repository import AbstractHarvestStateRepository

from adapters.databases.harvest_state_table import HarvestStateTable
from adapters.databases.postgres_session import PostgresSession
from adapters.databases.utils import check_conformity, check_conformity_and_get_where_clauses

from sqlalchemy import select, update, and_, asc


class HarvestStateRepository(AbstractHarvestStateRepository):
    session: PostgresSession

    def __init__(self, session: PostgresSession):
        self.session = session

    def create(self, harvest_state: HarvestStateTable) -> bool:
        added: bool = False
        with self.session.sessionScope() as session:
            statement = select(HarvestStateTable).where(
                HarvestStateTable.current_directory == harvest_state.current_directory,
            )
            result = session.execute(statement).all()

            if not result:
                session.add(harvest_state)
                added = True
                session.flush()
                session.expunge(harvest_state)

        return added

    def get(self, where_args: dict = {}):
        where_clauses: list = check_conformity_and_get_where_clauses(where_args, HarvestStateTable)

        with self.session.sessionScope() as session:
            statement = select(HarvestStateTable).order_by(asc(HarvestStateTable.__table__.c.id))

            if where_args:
                statement = statement.where(and_(*where_clauses))

            results = session.execute(statement).scalars().all()

            for result in results:
                session.expunge(result)
        return results

    def update(self, values_args: dict, where_args: dict = {}):
        check_conformity(values_args, HarvestStateTable)

        where_clauses: list = check_conformity_and_get_where_clauses(where_args, HarvestStateTable)

        with self.session.sessionScope() as session:
            statement = update(HarvestStateTable).values(values_args)

            if where_args:
                statement = statement.where(and_(*where_clauses))

            statement = statement.execution_options(synchronize_session="fetch")
            result = session.execute(statement)

        return result
