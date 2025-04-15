from unittest import TestCase

from project.server.main.views import get_partitions

TESTED_MODULE = "project.server.main.views"


class TestGetPartitions(TestCase):
    def test_get_partitions_partition_list_empty(self):
        # Given
        _list = []
        # When
        partitions = get_partitions(_list, partition_size=1)
        # Then
        self.assertEqual(partitions, [])
    
    def test_get_partitions_partition_list_gte_1(self):
        # Given
        _list = [1]
        # When
        partitions = get_partitions(_list, partition_size=1)
        # Then
        self.assertEqual(partitions, [[1]])
    
    def test_get_partitions_partition_size_eq_0(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, partition_size=0)
        # Then
        self.assertEqual(partitions, [[1], [2], [3], [4], [5]])
    
    def test_get_partitions_partition_size_eq_1(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, partition_size=1)
        # Then
        self.assertEqual(partitions, [[1], [2], [3], [4], [5]])
    
    def test_get_partitions_partition_size(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, partition_size=2)
        # Then
        self.assertEqual(partitions, [[1, 2], [3, 4], [5]])
    
    def test_get_partitions_partition_size_eq_list_size(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, partition_size=5)
        # Then
        self.assertEqual(partitions, [[1, 2, 3, 4, 5]])
    
    def test_get_partitions_partition_size_gt_list_size(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, partition_size=6)
        # Then
        self.assertEqual(partitions, [[1, 2, 3, 4, 5]])
    
    def test_get_partitions_number_of_partitions_eq_0(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, number_of_partitions=0)
        # Then
        self.assertEqual(partitions, [[1, 2, 3, 4, 5]])
    
    def test_get_partitions_number_of_partitions_eq_1(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, number_of_partitions=1)
        # Then
        self.assertEqual(partitions, [[1, 2, 3, 4, 5]])
    
    def test_get_partitions_number_of_partitions(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, number_of_partitions=4)
        # Then
        self.assertEqual(partitions, [[1], [2], [3], [4], [5]])
    
    def test_get_partitions_number_of_partitions_eq_list_size(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, number_of_partitions=5)
        # Then
        self.assertEqual(partitions, [[1], [2], [3], [4], [5]])
    
    def test_get_partitions_number_of_partitions_gt_list_size(self):
        # Given
        _list = [1,2,3,4,5]
        # When
        partitions = get_partitions(_list, number_of_partitions=6)
        # Then
        self.assertEqual(partitions, [[1], [2], [3], [4], [5]])
    
