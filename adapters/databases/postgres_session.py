from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from contextlib import contextmanager

from domain.databases.abstract_session import AbstractSession


class PostgresSession(AbstractSession):
    session: Session
    engine: Engine

    def __init__(self, host: str, port: int, username: str, password: str, database_name: str):
        connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{database_name}"

        self.engine: Engine = create_engine(connection_string)
        self.session: Session = Session(self.engine)

    def getSession(self) -> Session:
        return self.session

    def getEngine(self) -> Engine:
        return self.engine

    @contextmanager
    def sessionScope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.getSession()
        try:
            session.begin()
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
