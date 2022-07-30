import cProfile
from multiprocessing import Process, Array
from pstats import SortKey
from typing import List

from memory_profiler import profile


class ParallelMergeSort:
    def __init__(self, cpu: int):
        self.cpu = cpu

    @profile
    def execute(self, array: List[int]) -> List[int]:
        with cProfile.Profile() as pr:
            chunk_step = len(array) // self.cpu
            current = 0
            chunks = []
            answer_arrays = []

            for i in range(self.cpu):
                if i != self.cpu - 1:
                    chunks.append(array[current:current + chunk_step])
                    answer_arrays.append(Array('i', chunk_step))

                    current += chunk_step
                else:
                    chunks.append(array[current:len(array)])
                    answer_arrays.append(Array('i', len(array) - current))

            processes = [
                Process(
                    target=_sort_in_process,
                    kwargs={
                        'array': chunks[i],
                        'process_chunk_answer_array': answer_arrays[i]
                    }
                )
                for i in range(self.cpu)
            ]

            for p in processes:
                p.start()

            for p in processes:
                p.join()

        pr.print_stats(sort=SortKey.CUMULATIVE)

        result_array = []
        for array in answer_arrays:
            for element in array:
                result_array.append(element)

        answer = _merge_sort(result_array)
        return answer


def _sort_in_process(*,
                     array: List[int],
                     process_chunk_answer_array: Array):
    sorted_array = _merge_sort(array)

    for i in range(len(array)):
        process_chunk_answer_array[i] = sorted_array[i]


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
