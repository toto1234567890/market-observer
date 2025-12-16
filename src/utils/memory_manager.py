#!/usr/bin/env python
# coding:utf-8

from typing import Dict, Any, Optional
from os import getpid as osGetpid
from gc import collect as gcCollect
from psutil import Process as psutilProcess

from .ring_buffer import NumpyRingBuffer
from .constant import calculate_max_data_points, DEFAULT_RETENTION_DAYS



class SimpleMemoryManager:
    """Basic memory management for data streaming with retention limits"""

    #-----------------------------------------------------------------------------------------------     
    def __init__(self, logger, max_memory_mb: int = 500, max_data_points: int = calculate_max_data_points(DEFAULT_RETENTION_DAYS)):
        self.logger = logger
        self.max_memory_mb = max_memory_mb
        self.max_data_points = max_data_points
        self.data_streams: Dict[str, NumpyRingBuffer] = {}

    #-----------------------------------------------------------------------------------------------     
    def add_data_point(self, symbol: str, data: Dict[str, Any]):
        """Add data point with memory limits"""
        if symbol not in self.data_streams:
            self.data_streams[symbol] = NumpyRingBuffer(capacity=self.max_data_points)
        
        self.data_streams[symbol].append(data)
        
        # Periodic memory check
        if self.data_streams[symbol].size % 100 == 0:  # Check every 100 points
            self._check_memory_limits()

    #-----------------------------------------------------------------------------------------------     
    def get_latest_data(self, symbol: str = None, all_data: bool = False) -> Optional[Any]:
        """Get latest data points"""
        
        if symbol is None:

            if all_data:
                # Return FULL state (Snapshot + History) for ALL symbols
                # Expected format: { Symbol: { ...latest_values, time_series: [...] } }
                result = {}
                for sym, stream in self.data_streams.items():
                    if stream.size > 0:
                        # Get latest point (for snapshot fields)
                        latest = stream.get_latest(1)[0]
                        # Get full history (for time_series)
                        history = stream.get_latest(stream.size)
                        
                        # Combine
                        # We copy latest to avoid mutating the dict returned by get_latest if it's cached/ref
                        data_object = latest.copy()
                        data_object['time_series'] = history
                        result[sym] = data_object
                return result

            else:
                # Return latest point for ALL symbols (Snapshot only)
                return {
                    sym: stream.get_latest(1)[0] if stream.size > 0 else None
                    for sym, stream in self.data_streams.items()
                    if stream.size > 0
                }
        
        if symbol not in self.data_streams or self.data_streams[symbol].size == 0:
            return None

        if all_data:
            # Single symbol full history
            if self.data_streams[symbol].size > 0:
                latest = self.data_streams[symbol].get_latest(1)[0]
                history = self.data_streams[symbol].get_latest(self.data_streams[symbol].size)
                data_object = latest.copy()
                data_object['time_series'] = history
                return data_object
            return None
        else:
            return self.data_streams[symbol].get_latest(1)[0]

    #-----------------------------------------------------------------------------------------------  
    def get_latest_arrays(self, symbol: str) -> Optional[Any]:
        """Get all data as numpy array"""
        if symbol not in self.data_streams or self.data_streams[symbol].size == 0:
            return None
        return self.data_streams[symbol].get_snapshot()

    #-----------------------------------------------------------------------------------------------    
    def _check_memory_limits(self):
        """Check and enforce memory limits"""
        current_memory = self._get_process_memory_mb()
        
        if current_memory > self.max_memory_mb:
            self.logger.warning(f"Memory usage {current_memory:.1f}MB exceeds limit {self.max_memory_mb}MB. Cleaning up.")
            
            # Reduce data retention by half to free memory
            for symbol in self.data_streams:
                buffer = self.data_streams[symbol]
                if buffer.capacity > 100:
                    new_capacity = max(50, buffer.capacity // 2)
                    buffer.resize(new_capacity)
            
            # Force garbage collection
            gcCollect()
            
    #-----------------------------------------------------------------------------------------------     
    def _get_process_memory_mb(self) -> float:
        """Get current process memory usage in MB"""
        try:
            process = psutilProcess(osGetpid())
            return process.memory_info().rss / 1024 / 1024
        except:
            # Fallback for systems without psutil
            # Estimate: num_symbols * capacity * size_of_float64 * num_features
            # 8 bytes * 5 features = 40 bytes per row
            total_items = sum(b.capacity for b in self.data_streams.values())
            return (total_items * 40) / 1024 / 1024

    #-----------------------------------------------------------------------------------------------         
    def cleanup(self):
        """Cleanup all data"""
        self.data_streams.clear()
        gcCollect()