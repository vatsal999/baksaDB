class Node:
    '''
    @params
    Linked List Node
    key = primary key value (identifier)
    row_ref = reference to row data (row is dict)
    next = pointer to next node
    '''

    def __init__(self, key, row_ref):
        self.key = key
        self.row_ref = row_ref
        self.next = None


class HashIndex:
    '''
    @params
    capacity = capacity of the hash index
    buckets = array of linked list heads (chaining)
    '''

    def __init__(self, capacity=128):
        self.capacity = capacity
        self.buckets = [None] * capacity

    def _hash(self, key):
        return hash(key) % self.capacity

    def insert(self, key, row_ref):
        idx = self._hash(key)
        head = self.buckets[idx]

        if head is None:
            self.buckets[idx] = Node(key, row_ref)
            return

        # traverse our linkedlist chain
        current = head
        while True:
            if current.key == key:
                current.row_ref = row_ref
                return
            if current.next is None:
                break
            current = current.next
        current.next = Node(key, row_ref)

    def find_by_key(self, key):
        idx = self._hash(key)
        current = self.buckets[idx]
        # linkedlist trav and search the key
        while current:
            if current.key == key:
                return current.row_ref
            current = current.next
        return None

    def delete(self, key):
        idx = self._hash(key)
        current = self.buckets[idx]
        prev = None

        # linked list deletion by key
        while current:
            if current.key == key:
                if prev:
                    prev.next = current.next
                else:
                    self.buckets[idx] = current.next
                return True
            prev = current
            current = current.next
        return False

    def __repr__(self):
        s = []
        for i, b in enumerate(self.buckets):
            head = b
            if not head:
                continue

            s.append(f"{i} == [")
            while head:
                s.append(f"{head.key},")
                head = head.next

            s.append("]\n")

        my_rep = ''.join(s)
        if len(my_rep) == 0:
            return "Empty Hash Index"

        return my_rep
