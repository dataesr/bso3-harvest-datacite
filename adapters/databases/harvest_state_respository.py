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

        harvest_state_table_attributes: dict = HarvestStateTable.__annotations__

        for key in keys_arg:
            if key not in harvest_state_table_attributes:
                raise Exception(f"Element to update not available in HarvestStateTable : '{key}'")

            type_element_to_update_arg: type = type(elements_updated[key])
            type_element_to_update_expected: type = harvest_state_table_attributes[key]

            if type_element_to_update_arg != type_element_to_update_expected:
                raise Exception(f"Element type to update not correct for HarvestStateTable : got '{type_element_to_update_arg}' and expected '{type_element_to_update_expected}' for '{key}' attribute")

        with self.session.sessionScope() as session:
            statement = update(HarvestStateTable)

            statement = statement.values(elements_updated)

            statement = statement.execution_options(synchronize_session="fetch")
            result = session.execute(statement)

        return result
