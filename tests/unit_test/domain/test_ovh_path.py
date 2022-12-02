import os
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

    def test_OvhPath_eq(self):
        # Given
        # When
        ovh_path_0 = OvhPath('a', 'standard', 'path')
        ovh_path_1 = OvhPath('a/standard', 'path')
        # Then
        self.assertNotEqual(ovh_path_0, "a/standard/path")
        self.assertEqual(str(ovh_path_0), "a/standard/path")
        self.assertEqual(ovh_path_0, ovh_path_1)

    def test_OvhPath_lt(self):
        # Given
        # When
        ovh_path_0 = OvhPath('a', 'standard', 'path', '0')
        ovh_path_1 = OvhPath('a', 'standard', 'path', '1')
        # Then
        self.assertTrue(ovh_path_0 < ovh_path_1)
        self.assertTrue(ovh_path_0 < "a/standard/path/1")

    def test_OvhPath_gt(self):
        # Given
        # When
        ovh_path_0 = OvhPath('a', 'standard', 'path', '0')
        ovh_path_1 = OvhPath('a', 'standard', 'path', '1')
        # Then
        self.assertFalse(ovh_path_0 > ovh_path_1)
        self.assertFalse(ovh_path_0 > "a/standard/path/1")

    def test_OvhPath_lo_local(self):
        # Given
        # When
        ovh_path = OvhPath('a', 'standard', 'path')
        # Then
        self.assertEqual(ovh_path.to_local(), os.path.join('a', 'standard', 'path'))
