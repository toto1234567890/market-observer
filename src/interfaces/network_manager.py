#!/usr/bin/env python
# coding:utf-8

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional


class INetworkManager(ABC):
    """Interface for network operations (fetching, proxies)."""
    
    @abstractmethod
    async def fetch_text(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch raw text from a URL."""
        pass
        
    @abstractmethod
    async def fetch_json(self, url: str, params: Optional[Dict] = None) -> Any:
        """Fetch JSON from a URL."""
        pass
    
    @abstractmethod
    async def close(self):
        """Clean up resources."""
        pass