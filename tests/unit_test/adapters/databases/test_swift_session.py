from unittest import TestCase
from unittest.mock import patch

from adapters.storages.swift_session import SwiftSession

TESTED_MODULE = 'adapters.storages.swift_session'

class TestMongoSession(TestCase):
    @patch(f"{TESTED_MODULE}.SwiftService")
    def test_init_and_get_session(self, mock_create_engine):
        # Given
        config: dict = {}

        #When
        session: SwiftSession = SwiftSession(config)
        get_session_result = session.getSession()

        #Then
        mock_create_engine.assert_called_once()
        assert get_session_result != None 

