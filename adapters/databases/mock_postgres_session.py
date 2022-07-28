from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from unittest.mock import Mock

from contextlib import contextmanager

from adapters.databases.postgres_session import PostgresSession


class MockPostgresSession(PostgresSession):
    session: Session
    engine: Engine
    nb_calls_init: int = 0
    nb_calls_getSession: int = 0
    nb_calls_getEngine: int = 0
    nb_calls_sessionScope: int = 0

    def __init__(self, host: str, port: int, username: str, password: str, database_name: str):
        self.nb_calls_init += 1
        self.engine: Engine = Mock(spec=Engine)
        self.session: Session = Mock(spec=Session)

    def getSession(self) -> Session:
        self.nb_calls_getSession += 1
        return self.session

    def getEngine(self) -> Engine:
        self.nb_calls_getEngine += 1
        return self.engine

    @contextmanager
    def sessionScope(self):
        self.nb_calls_sessionScope += 1
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
