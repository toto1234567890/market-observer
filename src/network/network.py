#!/usr/bin/env python
# coding:utf-8

import asyncio
import aiohttp
from typing import Optional, Dict, Any, List

from src.interfaces.network_manager import INetworkManager
from src.helpers.proxy import ProxyManager



class AsyncNetworkManager(INetworkManager):
    """
    Implementation of INetworkManager using aiohttp.
    Supports basic proxy rotation (if provided) and retry logic.
    """

    #-----------------------------------------------------------------------------------------------    
    def __init__(self, proxies: Optional[List[str]] = None, timeout: int = 10, retries: int = 3):
        self.proxy_manager = ProxyManager(proxies)
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self.retries = retries
        self._session: Optional[aiohttp.ClientSession] = None

    #-----------------------------------------------------------------------------------------------        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            }
            self._session = aiohttp.ClientSession(timeout=self.timeout, headers=headers)
        return self._session

    #-----------------------------------------------------------------------------------------------    
    def _get_proxy(self) -> Optional[str]:
        return self.proxy_manager.get_proxy()

    #-----------------------------------------------------------------------------------------------
    async def fetch_text(self, url: str, params: Optional[Dict] = None) -> str:
        """Fetch raw text from URL with retries."""
        session = await self._get_session()
        last_exception = None
        
        for attempt in range(self.retries):
            proxy = self._get_proxy()
            try:
                async with session.get(url, params=params, proxy=proxy) as response:
                    response.raise_for_status()
                    return await response.text()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                # Exponential backoff: 0.5s, 1s, 2s...
                await asyncio.sleep(0.5 * (2 ** attempt))
        
        raise last_exception or Exception("Unknown error in fetch_text")

    #-----------------------------------------------------------------------------------------------
    async def fetch_json(self, url: str, params: Optional[Dict] = None) -> Any:
        """Fetch JSON from URL with retries."""
        session = await self._get_session()
        last_exception = None
        
        for attempt in range(self.retries):
            proxy = self._get_proxy()
            try:
                async with session.get(url, params=params, proxy=proxy) as response:
                    response.raise_for_status()
                    return await response.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exception = e
                await asyncio.sleep(0.5 * (2 ** attempt))
                
        raise last_exception or Exception("Unknown error in fetch_json")

    #-----------------------------------------------------------------------------------------------        
    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
