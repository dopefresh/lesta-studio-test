class CircularQueue:
    def __init__(self, size):
        self.size = size
        self.queue = [None for i in range(size)]
        self.front = self.rear = -1

    def enqueue(self, data):
        if (self.rear + 1) % self.size == self.front:
            print(" Queue is Full\n")

        elif self.front == -1:
            self.front = 0
            self.rear = 0
            self.queue[self.rear] = data
        else:
            self.rear = (self.rear + 1) % self.size
            self.queue[self.rear] = data

    def dequeue(self):
        if self.front == -1:
            print("Queue is Empty\n")

        # condition for only one element
        elif self.front == self.rear:
            temp = self.queue[self.front]
            self.front = -1
            self.rear = -1
            return temp
        else:
            temp = self.queue[self.front]
            self.front = (self.front + 1) % self.size
            return temp


"""
Второй вариант реализован на двусвязанном списке, 
пока что односвязный так как вакансия может закрыться

Плюси, минусы: по алгоритмической сложности реализации одинаковы, вторая реализация сложнее
"""


class Node:
    def __init__(self, data=None):
        self.data = data
        self.next = None


class MyLinkedList:
    def __init__(self):
        self.head = Node()

    def get(self, index: int) -> int:
        cur = self.head
        total = 0
        arr = []
        while cur.next is not None:
            cur = cur.next
            arr.append(cur.data)
            total += 1
        if len(arr) <= index:
            return -1
        else:
            return arr[index]

    def add_at_head(self, val: int) -> None:
        node = Node(val)
        cur = self.head
        if self.head.next is None:
            self.head.next = node
        else:
            cur = cur.next
            node.next = cur
            self.head.next = node

    def add_at_tail(self, val: int) -> None:
        cur = self.head
        while cur.next is not None:
            cur = cur.next
        node = Node(val)
        cur.next = node
        return

    def add_at_index(self, index: int, val: int) -> None:
        total = -1
        cur = self.head
        while total < index - 1:
            if cur.next is None:
                break
            cur = cur.next
            total += 1
        if total < index - 1:
            return -1
        if cur.next is None:
            node = Node(val)
            cur.next = node
            return
        node = Node(val)
        node.next = cur.next
        cur.next = node
        return

    def delete_at_index(self, index: int) -> None:
        total = -1
        cur = self.head
        while total < index - 1:
            if cur.next is None:
                break
            total += 1
            cur = cur.next
        if total < index - 1:
            return
        if cur.next is None:
            return
        Next = cur.next
        delete = Next
        if Next.next is None:
            cur.next = None
            return
        Next = Next.next
        cur.next = Next
        delete.next = None
        return
