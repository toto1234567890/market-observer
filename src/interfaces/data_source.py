#!/usr/bin/env python
# coding:utf-8

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class IDataSource(ABC):
    """Interface for fetching stock data."""
    
    @abstractmethod
    async def fetch_initial_data(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Fetch initial data for symbols (full history).
        """
        pass

    @abstractmethod
    async def fetch_update_data(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Fetch update data for symbols (latest).
        """
        pass

    @abstractmethod
    async def listen(self, symbols: List[str], callback) -> None:
        """
        Start listening for data updates (Push model).
        
        Args:
            symbols: List of symbols to monitor
            callback: Async function to call with new data: async def callback(data: Dict[str, Any])
        """
        pass