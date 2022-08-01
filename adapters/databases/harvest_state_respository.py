from domain.databases.abstract_harvest_state_repository import AbstractHarvestStateRepository

from adapters.databases.harvest_state_table import HarvestStateTable
from adapters.databases.postgres_session import PostgresSession

from sqlalchemy import select, update


class HarvestStateRepository(AbstractHarvestStateRepository):
    session: PostgresSession

    def __init__(self, session: PostgresSession):
        self.session = session

    def create(self, harvest_state: HarvestStateTable) -> bool:
        added: bool = False
        with self.session.sessionScope() as session:
            if not self.get({"current_directory": harvest_state.current_directory}):
                session.add(harvest_state)
                added = True

        return added

    def get(self, where_args={}):
        with self.session.sessionScope() as session:
            statement = select(HarvestStateTable).where(where_args)
            result = session.execute(statement).all()
            session.expunge_all()
        return result

    def update(self, where_args: dict, values_args: dict):
        keys_arg: list = list(values_args.keys())
        harvest_state_table_attributes: dict = HarvestStateTable.__annotations__

        for key in keys_arg:
            if key not in harvest_state_table_attributes:
                raise Exception(f"Element to update not available in HarvestStateTable : '{key}'")
            type_element_to_update_arg: type = type(values_args[key])
            type_element_to_update_expected: type = harvest_state_table_attributes[key]

            if type_element_to_update_arg != type_element_to_update_expected:
                raise Exception(f"Element type to update not correct for HarvestStateTable : got '{type_element_to_update_arg}' and expected '{type_element_to_update_expected}' for '{key}' attribute")

        with self.session.sessionScope() as session:
            statement = update(HarvestStateTable).where(where_args).values(values_args)

            statement = statement.execution_options(synchronize_session="fetch")
            result = session.execute(statement)

        return result
