"""
Thread-safe data structures for concurrent crawler framework.
"""

import queue
import threading
from typing import Any, Optional, Set, Iterator, List
from collections import deque


class ThreadSafeCounter:
    """Thread-safe counter with atomic operations."""
    
    def __init__(self, initial_value: int = 0):
        """
        Initialize counter with initial value.
        
        Args:
            initial_value: Starting value for the counter
        """
        self._value = initial_value
        self._lock = threading.Lock()
    
    def increment(self, amount: int = 1) -> int:
        """
        Atomically increment counter and return new value.
        
        Args:
            amount: Amount to increment by (default: 1)
            
        Returns:
            New counter value after increment
        """
        with self._lock:
            self._value += amount
            return self._value
    
    def decrement(self, amount: int = 1) -> int:
        """
        Atomically decrement counter and return new value.
        
        Args:
            amount: Amount to decrement by (default: 1)
            
        Returns:
            New counter value after decrement
        """
        with self._lock:
            self._value -= amount
            return self._value
    
    def get_value(self) -> int:
        """
        Get current counter value.
        
        Returns:
            Current counter value
        """
        with self._lock:
            return self._value
    
    def set_value(self, value: int) -> int:
        """
        Set counter to specific value.
        
        Args:
            value: New value to set
            
        Returns:
            The value that was set
        """
        with self._lock:
            self._value = value
            return self._value
    
    def compare_and_swap(self, expected: int, new_value: int) -> bool:
        """
        Atomically compare current value with expected and swap if equal.
        
        Args:
            expected: Expected current value
            new_value: New value to set if current equals expected
            
        Returns:
            True if swap occurred, False otherwise
        """
        with self._lock:
            if self._value == expected:
                self._value = new_value
                return True
            return False
    
    def get_and_increment(self, amount: int = 1) -> int:
        """
        Get current value and then increment.
        
        Args:
            amount: Amount to increment by (default: 1)
            
        Returns:
            Value before increment
        """
        with self._lock:
            old_value = self._value
            self._value += amount
            return old_value
    
    def increment_and_get(self, amount: int = 1) -> int:
        """
        Increment and then get new value.
        
        Args:
            amount: Amount to increment by (default: 1)
            
        Returns:
            Value after increment
        """
        with self._lock:
            self._value += amount
            return self._value
    
    def reset(self) -> int:
        """
        Reset counter to zero and return previous value.
        
        Returns:
            Previous value before reset
        """
        with self._lock:
            old_value = self._value
            self._value = 0
            return old_value
    
    def __str__(self) -> str:
        """String representation of counter."""
        return f"ThreadSafeCounter(value={self.get_value()})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"ThreadSafeCounter(value={self.get_value()})"


class ThreadSafeQueue:
    """Thread-safe queue with additional utility methods."""
    
    def __init__(self, maxsize: int = 0):
        """
        Initialize thread-safe queue.
        
        Args:
            maxsize: Maximum queue size (0 for unlimited)
        """
        self._queue = queue.Queue(maxsize)
        self._lock = threading.Lock()
        self._put_count = 0
        self._get_count = 0
    
    def put(self, item: Any, timeout: Optional[float] = None) -> None:
        """
        Put item into queue.
        
        Args:
            item: Item to put in queue
            timeout: Optional timeout in seconds
            
        Raises:
            queue.Full: If queue is full and timeout expires
        """
        self._queue.put(item, timeout=timeout)
        with self._lock:
            self._put_count += 1
    
    def get(self, timeout: Optional[float] = None) -> Any:
        """
        Get item from queue.
        
        Args:
            timeout: Optional timeout in seconds
            
        Returns:
            Item from queue
            
        Raises:
            queue.Empty: If queue is empty and timeout expires
        """
        item = self._queue.get(timeout=timeout)
        with self._lock:
            self._get_count += 1
        return item
    
    def put_nowait(self, item: Any) -> None:
        """
        Put item into queue without blocking.
        
        Args:
            item: Item to put in queue
            
        Raises:
            queue.Full: If queue is full
        """
        self._queue.put_nowait(item)
        with self._lock:
            self._put_count += 1
    
    def get_nowait(self) -> Any:
        """
        Get item from queue without blocking.
        
        Returns:
            Item from queue
            
        Raises:
            queue.Empty: If queue is empty
        """
        item = self._queue.get_nowait()
        with self._lock:
            self._get_count += 1
        return item
    
    def empty(self) -> bool:
        """
        Check if queue is empty.
        
        Returns:
            True if queue is empty
        """
        return self._queue.empty()
    
    def full(self) -> bool:
        """
        Check if queue is full.
        
        Returns:
            True if queue is full
        """
        return self._queue.full()
    
    def qsize(self) -> int:
        """
        Get approximate queue size.
        
        Returns:
            Approximate number of items in queue
        """
        return self._queue.qsize()
    
    def task_done(self) -> None:
        """Mark a task as done."""
        self._queue.task_done()
    
    def join(self) -> None:
        """Wait until all tasks are done."""
        self._queue.join()
    
    def get_stats(self) -> dict:
        """
        Get queue statistics.
        
        Returns:
            Dictionary with queue statistics
        """
        with self._lock:
            return {
                "size": self.qsize(),
                "empty": self.empty(),
                "full": self.full(),
                "put_count": self._put_count,
                "get_count": self._get_count,
                "pending_items": self._put_count - self._get_count
            }
    
    def clear(self) -> int:
        """
        Clear all items from queue.
        
        Returns:
            Number of items removed
        """
        removed_count = 0
        try:
            while True:
                self._queue.get_nowait()
                removed_count += 1
        except queue.Empty:
            pass
        
        with self._lock:
            self._get_count += removed_count
        
        return removed_count
    
    def peek(self) -> Optional[Any]:
        """
        Peek at next item without removing it.
        
        Returns:
            Next item or None if queue is empty
        """
        try:
            # This is a bit of a hack - we get the item and put it back
            item = self._queue.get_nowait()
            # Put it back at the front (this is not atomic, but best we can do with queue.Queue)
            temp_queue = queue.Queue(self._queue.maxsize)
            temp_queue.put_nowait(item)
            while not self._queue.empty():
                temp_queue.put_nowait(self._queue.get_nowait())
            
            # Replace the original queue
            self._queue = temp_queue
            return item
        except queue.Empty:
            return None
    
    def __len__(self) -> int:
        """Get queue size."""
        return self.qsize()
    
    def __bool__(self) -> bool:
        """Check if queue is not empty."""
        return not self.empty()


class ThreadSafeSet:
    """Thread-safe set implementation."""
    
    def __init__(self, initial_items: Optional[Set[Any]] = None):
        """
        Initialize thread-safe set.
        
        Args:
            initial_items: Optional initial items for the set
        """
        self._set: Set[Any] = set(initial_items) if initial_items else set()
        self._lock = threading.RLock()  # Use RLock for nested operations
    
    def add(self, item: Any) -> bool:
        """
        Add item to set.
        
        Args:
            item: Item to add
            
        Returns:
            True if item was added (wasn't already present)
        """
        with self._lock:
            if item not in self._set:
                self._set.add(item)
                return True
            return False
    
    def remove(self, item: Any) -> bool:
        """
        Remove item from set.
        
        Args:
            item: Item to remove
            
        Returns:
            True if item was removed (was present)
        """
        with self._lock:
            if item in self._set:
                self._set.remove(item)
                return True
            return False
    
    def discard(self, item: Any) -> None:
        """
        Remove item from set if present.
        
        Args:
            item: Item to discard
        """
        with self._lock:
            self._set.discard(item)
    
    def contains(self, item: Any) -> bool:
        """
        Check if item is in set.
        
        Args:
            item: Item to check
            
        Returns:
            True if item is in set
        """
        with self._lock:
            return item in self._set
    
    def __contains__(self, item: Any) -> bool:
        """Support 'in' operator."""
        return self.contains(item)
    
    def size(self) -> int:
        """
        Get set size.
        
        Returns:
            Number of items in set
        """
        with self._lock:
            return len(self._set)
    
    def __len__(self) -> int:
        """Get set size."""
        return self.size()
    
    def is_empty(self) -> bool:
        """
        Check if set is empty.
        
        Returns:
            True if set is empty
        """
        with self._lock:
            return len(self._set) == 0
    
    def clear(self) -> int:
        """
        Clear all items from set.
        
        Returns:
            Number of items removed
        """
        with self._lock:
            count = len(self._set)
            self._set.clear()
            return count
    
    def copy(self) -> Set[Any]:
        """
        Get a copy of the set.
        
        Returns:
            Copy of the internal set
        """
        with self._lock:
            return self._set.copy()
    
    def to_list(self) -> List[Any]:
        """
        Convert set to list.
        
        Returns:
            List containing all set items
        """
        with self._lock:
            return list(self._set)
    
    def update(self, items: Set[Any]) -> int:
        """
        Add multiple items to set.
        
        Args:
            items: Items to add
            
        Returns:
            Number of new items added
        """
        with self._lock:
            old_size = len(self._set)
            self._set.update(items)
            return len(self._set) - old_size
    
    def intersection(self, other: Set[Any]) -> Set[Any]:
        """
        Get intersection with another set.
        
        Args:
            other: Other set to intersect with
            
        Returns:
            Intersection of the two sets
        """
        with self._lock:
            return self._set.intersection(other)
    
    def union(self, other: Set[Any]) -> Set[Any]:
        """
        Get union with another set.
        
        Args:
            other: Other set to union with
            
        Returns:
            Union of the two sets
        """
        with self._lock:
            return self._set.union(other)
    
    def difference(self, other: Set[Any]) -> Set[Any]:
        """
        Get difference with another set.
        
        Args:
            other: Other set to get difference with
            
        Returns:
            Items in this set but not in other
        """
        with self._lock:
            return self._set.difference(other)
    
    def pop(self) -> Any:
        """
        Remove and return arbitrary item from set.
        
        Returns:
            Arbitrary item from set
            
        Raises:
            KeyError: If set is empty
        """
        with self._lock:
            if not self._set:
                raise KeyError("pop from empty set")
            return self._set.pop()
    
    def __iter__(self) -> Iterator[Any]:
        """
        Iterate over set items.
        
        Note: This creates a snapshot of the set for iteration.
        
        Returns:
            Iterator over set items
        """
        with self._lock:
            return iter(self._set.copy())
    
    def __str__(self) -> str:
        """String representation of set."""
        with self._lock:
            return f"ThreadSafeSet({self._set})"
    
    def __repr__(self) -> str:
        """Detailed string representation."""
        return f"ThreadSafeSet(size={self.size()})"


class ThreadSafeDeque:
    """Thread-safe double-ended queue implementation."""
    
    def __init__(self, maxlen: Optional[int] = None):
        """
        Initialize thread-safe deque.
        
        Args:
            maxlen: Maximum length of deque (None for unlimited)
        """
        self._deque = deque(maxlen=maxlen)
        self._lock = threading.Lock()
    
    def append(self, item: Any) -> None:
        """
        Add item to right end of deque.
        
        Args:
            item: Item to append
        """
        with self._lock:
            self._deque.append(item)
    
    def appendleft(self, item: Any) -> None:
        """
        Add item to left end of deque.
        
        Args:
            item: Item to append to left
        """
        with self._lock:
            self._deque.appendleft(item)
    
    def pop(self) -> Any:
        """
        Remove and return item from right end.
        
        Returns:
            Item from right end
            
        Raises:
            IndexError: If deque is empty
        """
        with self._lock:
            return self._deque.pop()
    
    def popleft(self) -> Any:
        """
        Remove and return item from left end.
        
        Returns:
            Item from left end
            
        Raises:
            IndexError: If deque is empty
        """
        with self._lock:
            return self._deque.popleft()
    
    def extend(self, items: List[Any]) -> None:
        """
        Extend deque with items on the right.
        
        Args:
            items: Items to extend with
        """
        with self._lock:
            self._deque.extend(items)
    
    def extendleft(self, items: List[Any]) -> None:
        """
        Extend deque with items on the left.
        
        Args:
            items: Items to extend with (will be reversed)
        """
        with self._lock:
            self._deque.extendleft(items)
    
    def clear(self) -> None:
        """Clear all items from deque."""
        with self._lock:
            self._deque.clear()
    
    def __len__(self) -> int:
        """Get deque length."""
        with self._lock:
            return len(self._deque)
    
    def __bool__(self) -> bool:
        """Check if deque is not empty."""
        with self._lock:
            return bool(self._deque)
    
    def copy(self) -> List[Any]:
        """
        Get a copy of deque as list.
        
        Returns:
            List copy of deque contents
        """
        with self._lock:
            return list(self._deque)