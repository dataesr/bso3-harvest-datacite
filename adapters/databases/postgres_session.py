from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from domain.databases.abstract_session import AbstractSession


class PostgresSession(AbstractSession):
    session: Engine

    def __init__(self, host: str, port: int, username: str, password: str, database_name: str):
        connection_string = (f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}")

        self._session: Engine = create_engine(connection_string)

    def getSession(self) -> Engine:
        return self._session
