#!/usr/bin/env python
# coding:utf-8

import numpy as np
from typing import Dict, Any, List



class NumpyRingBuffer:
    """
    Fixed-size circular buffer using pre-allocated numpy array.
    Optimized for high-frequency data updates with minimal memory overhead.
    """
    
    # Define column indices for mapping
    COL_TIMESTAMP = 0
    COL_PRICE = 1
    COL_VOLUME = 2
    COL_PRICE_PCT = 3
    COL_VOL_PCT = 4
    
    NUM_FEATURES = 5

    #-----------------------------------------------------------------------------------------------      
    def __init__(self, capacity: int):
        self._capacity = capacity
        # Pre-allocate memory: (capacity, features)
        # using float64 to accommodate timestamp and high precision prices
        self._data = np.zeros((capacity, self.NUM_FEATURES), dtype=np.float64)
        self._index = 0
        self._size = 0
        self._is_full = False

    #-----------------------------------------------------------------------------------------------     
    def append(self, data_point: Dict[str, Any]):
        """
        Append a single data point. O(1) operation.
        """
        # Map dictionary to fixed schema
        # Default to 0.0 if missing to safeguard array type
        row = [
            float(data_point.get('timestamp', 0)),
            float(data_point.get('price', 0)),
            float(data_point.get('volume', 0)),
            float(data_point.get('price_percent_change', 0)),
            float(data_point.get('volume_percent_change', 0))
        ]
        
        # Insert at current index
        self._data[self._index] = row
        
        # Advance pointer
        self._index = (self._index + 1) % self._capacity
        
        # Update size tracking
        if self._size < self._capacity:
            self._size += 1
        else:
            self._is_full = True

    columns = ['timestamp', 'price', 'volume', 'price_percentage_change', 'volume_percent_change']

    #-----------------------------------------------------------------------------------------------     
    def get_latest(self, n: int = 1) -> List[Dict[str, Any]]:
        """
        Retrieve n latest records converted back to dictionaries.
        """
        if self._size == 0:
            return []
            
        count = min(n, self._size)
        
        # Calculate indices
        # If full, valid data wraps around.
        # Latest data is at self._index - 1, then backwards.
        
        indices = (np.arange(count) + self._index - count) % self._capacity
        rows = self._data[indices]
        
        result = []
        for row in rows:
            result.append({
                'timestamp': int(row[self.COL_TIMESTAMP]), # Convert back to int for TS
                'price': row[self.COL_PRICE],
                'volume': row[self.COL_VOLUME],
                'price_percent_change': row[self.COL_PRICE_PCT],
                'volume_percent_change': row[self.COL_VOL_PCT]
            })
            
        return result

    #-----------------------------------------------------------------------------------------------     
    def get_snapshot(self) -> np.ndarray:
        """
        Return a copy of the valid data as a continuous numpy array.
        Ordered by time (oldest -> newest).
        """
        if self._size == 0:
            return np.empty((0, self.NUM_FEATURES), dtype=np.float64)
            
        if not self._is_full:
            # Data is continuous from 0 to size
            return self._data[:self._size].copy()
            
        # Data is wrapped: [index:] + [:index]
        part1 = self._data[self._index:]
        part2 = self._data[:self._index]
        return np.concatenate((part1, part2))

    def resize(self, new_capacity: int):
        """
        Resize the buffer. 
        If shrinking, keeps the most recent data.
        """
        if new_capacity == self._capacity:
            return
            
        # Extract current ordered data
        if self._size == 0:
            current_ordered = np.empty((0, self.NUM_FEATURES))
        elif self._is_full:
            # Unwrap: [index:] + [:index]
            part1 = self._data[self._index:]
            part2 = self._data[:self._index]
            current_ordered = np.concatenate((part1, part2))
        else:
            # Just [0:size]
            current_ordered = self._data[:self._size]
            
        # Determine how many items to keep
        items_to_keep = min(len(current_ordered), new_capacity)
        
        # If we need to drop old data, take the slice from the end
        if items_to_keep < len(current_ordered):
            current_ordered = current_ordered[-items_to_keep:]
            
        # Allocate new buffer
        new_data = np.zeros((new_capacity, self.NUM_FEATURES), dtype=np.float64)
        
        # Fill new buffer
        # We put them at the start [0:items_to_keep]
        new_data[:items_to_keep] = current_ordered
        
        self._data = new_data
        self._capacity = new_capacity
        self._size = items_to_keep
        self._index = items_to_keep % new_capacity
        self._is_full = (self._size == new_capacity)

    #-----------------------------------------------------------------------------------------------     
    @property
    def size(self) -> int:
        return self._size

    #-----------------------------------------------------------------------------------------------             
    @property
    def capacity(self) -> int:
        return self._capacity