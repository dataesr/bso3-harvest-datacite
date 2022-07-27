from unittest import TestCase
from unittest.mock import patch

from datetime import datetime

from adapters.databases.harvest_state_respository import HarvestStateRepository
from adapters.databases.mock_postgres_session import MockPostgresSession
from adapters.databases.harvest_state_table import HarvestStateTable 

from os import getenv

TESTED_MODULE = 'adapters.databases.harvest_state_repository'


class TestHarvestStateRepository(TestCase):
    def test_init_and_get_session(self):
        # Given
        host: str = "fake_host"
        port: int = 0
        username: str = 'fake_username'
        password: str = 'fake_password'
        database_name: str = 'fake_db_name'

        session = MockPostgresSession(host, port, username, password, database_name)
        
        repo = HarvestStateRepository(session)

        # When
        repo.get()

        # Then
        assert session.nb_calls_getSession == 0
        assert session.nb_calls_sessionScope == 1
        
