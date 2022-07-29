import copy
import random
from pstats import SortKey

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


def test_parallel_merge_sort(parallel_merge_sort):
    array = [i for i in range(100)]
    expected = copy.deepcopy(array)

    random.shuffle(array)

    result = parallel_merge_sort.execute(array)
    result = [a for a in result]
    assert result == expected


@pytest.mark.timeout(3)
def test_merge_time_limited():
    array1 = [i for i in range(10 ** 6)]
    array2 = [i + 10 for i in range(10 ** 6)]

    assert _merge_sorted_arrays(array1, array2) == sorted(array1 + array2)


@pytest.mark.timeout(100)
def test_merge_sort_time_limited():
    array = [i for i in range(10 ** 6)]
    array.reverse()
    import cProfile

    with cProfile.Profile() as pr:
        assert _merge_sort(array) == [i for i in range(10 ** 6)]

    pr.print_stats(sort=SortKey.CUMULATIVE)


@pytest.mark.timeout(100)
def test_parallel_merge_sort_time_limited(parallel_merge_sort):
    array = [i for i in range(10 ** 6)]
    expected = copy.deepcopy(array)

    array.reverse()

    result = parallel_merge_sort.execute(array)
    result = [a for a in result]
    assert result == expected
