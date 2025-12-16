#!/usr/bin/env python
# coding:utf-8

from typing import List
import numpy as np



import logging
logger = logging.getLogger(__name__)

class TimeSeriesResampler:
    """
    Handles time-series windowing and grouping logic.
    Distinct from math or business logic.
    """

    #-----------------------------------------------------------------------------------------------   
    @staticmethod
    def resample_indices(timestamps: np.ndarray, window_seconds: int) -> List[tuple]:
        """
        Groups timestamps into fixed time windows.
        
        Args:
            timestamps: Sorted 1D numpy array of timestamps (floats/ints).
            window_seconds: Size of each window in seconds.
            
        Returns:
            List[tuple]: List of (indices_array, start_time, end_time)
            Using indices is more memory efficient than copying data chunks.
        """
        if len(timestamps) == 0:
            return []
            
        min_ts = np.min(timestamps)
        max_ts = np.max(timestamps)
        
        # Create window boundaries
        # Use arange to get start times
        window_starts = np.arange(min_ts, max_ts + window_seconds, window_seconds)
        
        chunk_indices = []
        
        for i in range(len(window_starts) - 1):
            window_start = window_starts[i]
            window_end = window_starts[i + 1]
            
            # Vectorized find:
            # start_idx = np.searchsorted(timestamps, window_start, side='left')
            # end_idx = np.searchsorted(timestamps, window_end, side='left')
            # indices = np.arange(start_idx, end_idx)
            
            # Let's assume input is sorted.
            start_idx = np.searchsorted(timestamps, window_start, side='left')
            end_idx = np.searchsorted(timestamps, window_end, side='left')
            
            if start_idx < end_idx:
                 chunk_indices.append((np.arange(start_idx, end_idx), window_start, window_end))
            
        return chunk_indices
