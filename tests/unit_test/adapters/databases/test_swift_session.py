from unittest import TestCase
from unittest.mock import patch

from adapters.storages.swift_session import SwiftSession

TESTED_MODULE = 'adapters.storages.swift_session'


class TestMongoSession(TestCase):
    @patch(f"{TESTED_MODULE}.SwiftService")
    def test_init_and_get_session(self, mock_create_engine):
        # Given
        config: dict = {
            'os_username': 'fake_os_username',
            'os_password': 'fake_os_password',
            'os_user_domain_name': 'fake_os_user_domain_name',
            'os_project_domain_name': 'fake_os_project_domain_name',
            'os_project_name': 'fake_os_project_name',
            'os_project_id': 'fake_os_project_id',
            'os_region_name': 'fake_os_region_name',
            'os_auth_url': 'fake_os_auth_url',
        }

        # When
        session: SwiftSession = SwiftSession(config)
        get_session_result = session.getSession()

        # Then
        mock_create_engine.assert_called_once()
        assert get_session_result is not None
