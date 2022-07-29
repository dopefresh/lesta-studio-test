def is_even(num: int):
    return num & 1 == 0

"""
Данная реализация чуть быстрее обычной, 
в любом случае обычно такое время выполнения пренебрежительно мало:

def is_even(num: int):
    return num % 2 == 0
"""