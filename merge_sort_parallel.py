import cProfile
import io
import pstats
from multiprocessing import Process, Array
from pstats import SortKey
from typing import List


class ParallelMergeSort:
    def __init__(self, cpu: int):
        self.cpu = cpu

    def execute(self, array: List[int]):
        with cProfile.Profile() as pr:
            chunk_step = len(array) // self.cpu
            current = 0
            chunks = []
            answer_array = Array('i', len(array))

            for i in range(self.cpu):
                if i != self.cpu - 1:
                    chunks.append([current, current + chunk_step])
                    current += chunk_step
                else:
                    chunks.append([current, len(array)])
            processes = [
                Process(
                    target=_sort_in_process,
                    kwargs={
                        'array': array[chunks[i][0]:chunks[i][1]],
                        'left_bound': chunks[i][0],
                        'right_bound': chunks[i][1],
                        'answer_array': answer_array
                    }
                )
                for i in range(self.cpu)
            ]
            _ = [p.start() for p in processes]
            _ = [p.join() for p in processes]
        sortby = SortKey.CUMULATIVE
        s = io.StringIO()
        ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
        ps.print_stats()
        print(s.getvalue())
        return answer_array


def _sort_in_process(*,
                     array: List[int], left_bound: int,
                     right_bound: int, answer_array: Array):
    if len(array) == 1:
        return

    sorted_array = _merge_sort(array)
    j = 0
    for i in range(left_bound, right_bound):
        answer_array[i] = sorted_array[j]


def _merge_sort(array: List[int]):
    if len(array) <= 1:
        return array
    if len(array) == 2:
        return [array[0], array[1]] if array[0] <= array[1] else [array[1], array[0]]
    left = len(array) // 2
    right = len(array)
    left_half = _merge_sort(array[0:left])
    right_half = _merge_sort(array[left:right])
    return _merge_sorted_arrays(left_half, right_half)


def _merge_sorted_arrays(array1, array2):
    result_array = []
    i, j = 0, 0

    while i + j < len(array1) + len(array2):
        if i < len(array1) and j < len(array2):
            if array1[i] <= array2[j]:
                result_array.append(array1[i])
                i += 1
            else:
                result_array.append(array2[j])
                j += 1
        elif i < len(array1):
            result_array.append(array1[i])
            i += 1
        else:
            # import pdb; pdb.set_trace()
            result_array.append(array2[j])
            j += 1

    return result_array
