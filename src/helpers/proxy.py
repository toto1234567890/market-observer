#!/usr/bin/env python
# coding:utf-8

import random
from typing import List, Optional



class ProxyManager:
    """
    Helper class to manage and rotate proxies.
    """
    #-----------------------------------------------------------------------------------------------
    def __init__(self, proxies: Optional[List[str]] = None):
        self.proxies = proxies or []

    #-----------------------------------------------------------------------------------------------        
    def get_proxy(self) -> Optional[str]:
        """Return a random proxy from the list, or None if empty."""
        if not self.proxies:
            return None
        return random.choice(self.proxies)

    #-----------------------------------------------------------------------------------------------
    def add_proxy(self, proxy: str):
        self.proxies.append(proxy)
