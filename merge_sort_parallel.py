import cProfile
from multiprocessing import Pool
from concurrent.futures import ProcessPoolExecutor
from pstats import SortKey
from typing import List

from memory_profiler import profile


class ParallelMergeSort:
    def __init__(self, cpu: int):
        self.pool = Pool(cpu)
        self.cpu = cpu

    @profile
    def execute(self, array: List[int]) -> List[int]:
        answer_arrays = []

        with cProfile.Profile() as pr:
            with ProcessPoolExecutor(max_workers=self.cpu) as executor:
                chunk_step = len(array) // self.cpu
                current = 0
                futures = []

                for i in range(self.cpu):
                    if i != self.cpu - 1:
                        futures.append(executor.submit(
                            _merge_sort,
                            **{'array': array[current:current + chunk_step]}
                        ))

                        current += chunk_step
                    else:
                        futures.append(executor.submit(
                            _merge_sort,
                            **{'array': array[current:len(array)]}
                        ))

                for future in futures:
                    answer_arrays.append(future.result())

        pr.print_stats(sort=SortKey.CUMULATIVE)

        result_array = []
        for array in answer_arrays:
            for element in array:
                result_array.append(element)

        answer = _merge_sort(result_array)
        return answer


def _merge_sort(array: List[int]):
    array_length = len(array)
    if array_length <= 1:
        return array
    if array_length == 2:
        return [array[0], array[1]] if array[0] <= array[1] else [array[1], array[0]]
    left_half = _merge_sort(array[0:(array_length // 2)])
    right_half = _merge_sort(array[(array_length // 2):array_length])
    return _merge_sorted_arrays(left_half, right_half)


def _merge_sorted_arrays(array1, array2):
    first_array_length = len(array1)
    second_array_length = len(array2)
    result_array = [None] * (first_array_length + second_array_length)
    i, j = 0, 0
    k = 0

    while i + j < first_array_length + second_array_length:
        if i < first_array_length and j < second_array_length:
            if array1[i] <= array2[j]:
                result_array[k] = array1[i]
                i += 1
            else:
                result_array[k] = array2[j]
                j += 1
        elif i < first_array_length:
            result_array[k] = array1[i]
            i += 1
        else:
            result_array[k] = array2[j]
            j += 1

        k += 1

    return result_array
