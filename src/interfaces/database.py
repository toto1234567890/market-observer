#!/usr/bin/env python
# coding:utf-8

from abc import ABC, abstractmethod
from typing import List, Dict, Any


class IDatabase(ABC):
    """Interface for storage."""
    
    @abstractmethod
    async def initialize(self):
        """Setup tables/connection."""
        pass
        
    @abstractmethod
    async def save_bulk(self, data_list: List[Dict[str, Any]]):
        """Save a batch of records."""
        pass
    
    @abstractmethod
    async def save_aggregations(self, aggregations: Dict[str, Dict[str, Any]]):
        """Save time-windowed aggregations to appropriate tables."""
        pass

    @abstractmethod
    async def save_intermediate_stats(self, stats: Dict[str, Any], window: str):
        """Save intermediate calculated stats for a specific window."""
        pass

    @abstractmethod
    async def cleanup_old_data(self, retention_days: int):
        """Remove data older than retention_days."""
        pass

    @abstractmethod
    async def close(self):
        """Close connection."""
        pass