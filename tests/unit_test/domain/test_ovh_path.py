from unittest.mock import MagicMock, patch
from unittest import TestCase
from domain.model.ovh_path import OvhPath

TESTED_MODULE = "domain.model.ovh_path"


class TestOvhPath(TestCase):
    def test_OvhPath(self):
        # Given
        # When
        ovh_path = OvhPath('a', 'standard', 'path')
        # Then
        self.assertEqual(str(ovh_path), "a/standard/path")
    
    def test_OvhPath_with_backward_slashes(self):
        # Given
        # When
        ovh_path = OvhPath('a\\', 'standard\\', 'path\\')
        # Then
        self.assertEqual(str(ovh_path), "a/standard/path")

    def test_OvhPath_with_forward_slashes(self):
        # Given
        # When
        ovh_path = OvhPath('a/', 'standard/', 'path/')
        # Then
        self.assertEqual(str(ovh_path), "a/standard/path")

    def test_OvhPath_with_forward_slashes_in_string(self):
        # Given
        # When
        ovh_path = OvhPath('a/standard/', 'path/')
        # Then
        self.assertEqual(str(ovh_path), "a/standard/path")

    def test_OvhPath_with_backward_slashes_in_string(self):
        # Given
        # When
        ovh_path = OvhPath('a\standard\\', 'path/')
        # Then
        self.assertNotEqual(str(ovh_path), "a/standard/path")
        self.assertEqual(str(ovh_path), "a\standard/path")
