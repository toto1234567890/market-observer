#!/usr/bin/env python
# coding:utf-8

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional


class IDataExchanger(ABC):
    """Interface for sharing data with external systems (Server/Push)."""
    
    @abstractmethod
    async def broadcast(self, data: Dict[str, Any]):
        """Push data to external listeners or update state."""
        pass
        
    @abstractmethod
    async def start_server(self):
        """Start any background server (e.g. FastAPI)."""
        pass