from domain.databases.abstract_harvest_state_repository import AbstractHarvestStateRepository

from adapters.databases.harvest_state_table import HarvestStateTable
from adapters.databases.postgres_session import PostgresSession

from sqlalchemy import select, update


class HarvestStateRepository(AbstractHarvestStateRepository):
    session: PostgresSession

    def __init__(self, session: PostgresSession):
        self.session = session

    def get(self):
        with self.session.sessionScope() as session:
            statement = select(HarvestStateTable)
            result = session.execute(statement).all()
            session.expunge_all()
        
        return result

    def update(self, elements_updated: dict):
        keys_arg: list = list(elements_updated.keys())
        
        harvest_state_table_attribues: dict = HarvestStateTable.__annotations__.keys()

        for key in keys_arg:
            if key not in harvest_state_table_attribues:
                raise Exception('Element to update not available in HarvestStateTable')

        with self.session.sessionScope() as session:
            statement = update(HarvestStateTable)
            
            #for key in keys_arg:
            statement = statement.values(elements_updated)

            statement = statement.execution_options(synchronize_session="fetch")
            result = session.execute(statement)

        return result
        