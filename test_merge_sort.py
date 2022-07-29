import copy
from multiprocessing import Array

import pytest

from merge_sort_parallel import _merge_sorted_arrays, _merge_sort, ParallelMergeSort


@pytest.fixture
def parallel_merge_sort():
    return ParallelMergeSort(4)


@pytest.mark.parametrize(
    "left_array,right_array,expected",
    [
        ([1, 2, 2, 3], [1, 2, 4], [1, 1, 2, 2, 2, 3, 4]),
        ([1], [2], [1, 2]),
        ([], [], []),
        ([1, 1], [], [1, 1]),
        ([1, 2, 3], [4, 5, 6], [1, 2, 3, 4, 5, 6]),
        ([1, 1, 5], [1, 1, 1, 2, 3], [1, 1, 1, 1, 1, 2, 3, 5])
    ]
)
def test_merge(left_array, right_array, expected):
    assert _merge_sorted_arrays(left_array, right_array) == expected


@pytest.mark.parametrize(
    "array,expected",
    [
        ([3, 2, 1, 4], [1, 2, 3, 4]),
        ([], []),
        ([1], [1]),
        ([2, 1, 1, 3], [1, 1, 2, 3]),
        ([3, 1, 2, 1, 4, 2, 3, 4], [1, 1, 2, 2, 3, 3, 4, 4])
    ]
)
def test_merge_sort(array, expected):
    assert _merge_sort(array) == expected


@pytest.mark.timeout(3)
def test_merge_sort_time_limited():
    array = [i for i in range(10 ** 6)]
    array.reverse()
    assert _merge_sort(array) == [i for i in range(10 ** 7)]


@pytest.mark.timeout(3)
def test_merge_time_limited():
    array1 = [i for i in range(10 ** 6)]
    array2 = [i + 10 for i in range(10 ** 6)]

    assert _merge_sorted_arrays(array1, array2) == sorted(array1 + array2)


@pytest.mark.timeout(30)
def test_parallel_merge_sort(parallel_merge_sort):
    array = [i for i in range(10 ** 6)]
    expected = Array('i', copy.deepcopy(array))
    array.reverse()
    assert parallel_merge_sort.execute(array) == expected
