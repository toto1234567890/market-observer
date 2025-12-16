#!/usr/bin/env python
# coding:utf-8

from typing import List
from datetime import datetime
from pytz import utc as pytzUtc

from .trading_calendar import get_calendar


import logging
logger = logging.getLogger(__name__)



class MarketScheduler:
    """
    Manages market schedules for multiple symbols by resolving their exchange calendars.
    Used to determine if fetching is necessary or if we can relax the polling loop.
    """

    #-----------------------------------------------------------------------------------------------       
    def __init__(self, symbols: List[str], logger: logging.Logger):
        self.logger = logger
        self.calendars = {} # {symbol: calendar_instance}
        self.map_symbols_to_calendars(symbols)

    #-----------------------------------------------------------------------------------------------   
    def map_symbols_to_calendars(self, symbols: List[str]):
        """
        Resolve and cache the trading calendar for each symbol.
        This avoids re-parsing suffixes or reloading calendars on every loop.
        """
        for symbol in symbols:
            # We use a unique instance name for logging purposes if needed
            cal = get_calendar(symbol, self.logger, instance_name="MarketScheduler", return_default=True)
            if cal:
                self.calendars[symbol] = cal
        
        self.logger.info(f"MarketScheduler: Mapped {len(symbols)} symbols to {len(set(self.calendars.values()))} unique calendars.")

    #-----------------------------------------------------------------------------------------------   
    def any_market_open(self) -> bool:
        """
        Check if ANY of the tracked markets are currently open.
        This is primarily used for loop pacing (optimization).
        
        Returns:
            bool: True if at least one market is open, False if all are closed.
        """
        now_utc = datetime.now(pytzUtc)
        
        # Optimization: We could group by calendar to avoid redundant checks
        # safely iterate unique calendars
        unique_cals = set(self.calendars.values())
        
        for cal in unique_cals:
            try:
                if cal.is_open_on_minute(now_utc):
                    return True
            except Exception as e:
                # If a calendar check fails, assume open to be safe
                self.logger.warning(f"Error checking calendar status: {e}")
                return True
                
        return False
