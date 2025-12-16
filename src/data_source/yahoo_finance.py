#!/usr/bin/env python
# coding:utf-8


from typing import List, Dict, Any
from asyncio import sleep as asyncioSleep, Semaphore as asyncioSemaphore, \
                    gather as asyncioGather, CancelledError as asyncioCancelledError

from src.interfaces.data_source import IDataSource
from src.interfaces.network_manager import INetworkManager
from src.utils.market_scheduler import MarketScheduler



class YahooFinanceSource(IDataSource):
    """
    Fetches stock data from Yahoo Finance Chart API (v8).
    Returns full time series with 5-minute candles including cumulated volumes.
    """
    
    BASE_URL = "https://query1.finance.yahoo.com/v7/finance/quote"
    
    
    #-----------------------------------------------------------------------------------------------
    def __init__(self, config, logger, network: INetworkManager):
        self.config = config
        self.logger = logger
        self.network = network
        self.market_scheduler = None
        self.last_timestamps = {}

    #-----------------------------------------------------------------------------------------------        
    async def fetch_initial_data(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Fetch initial historical data for all symbols to build baseline stats.
        
        Args:
            symbols: List of stock symbols to fetch.
            
        Returns:
            Dict[str, Any]: Dictionary mapping symbols to their historical data.
        """
        data = await self._fetch_batch(symbols, self._fetch_initial_symbol)
        
        # Sync last_timestamps to avoid processing old data as new in listener
        if data:
            for symbol, item in data.items():
                if item.get("timestamp"):
                    self.last_timestamps[symbol] = item["timestamp"]
        
        return data

    #-----------------------------------------------------------------------------------------------
    async def fetch_update_data(self, symbols: List[str]) -> Dict[str, Any]:
        """
        Fetch the latest data updates for all symbols.
        Typically fetches the last 1 day of data to ensure we catch recent movements.
        
        Args:
            symbols: List of stock symbols to fetch.
            
        Returns:
            Dict[str, Any]: Dictionary mapping symbols to their latest data.
        """
        return await self._fetch_batch(symbols, self._fetch_update_symbol)

    #-----------------------------------------------------------------------------------------------
    async def listen(self, symbols: List[str], callback) -> None:
        """
        Start listening for data updates (Push model).
        """
        self.market_scheduler = MarketScheduler(symbols, self.logger)
        self.logger.info("Starting YahooFinance data listener...")
        
        while True:
            try:
                # Market status check
                any_market_open = self.market_scheduler.any_market_open()
                
                if not any_market_open:
                    self.logger.info("Markets closed. Fetching updates to check for delayed data...")
                    sleep_time = 60 # Check less frequently when closed
                else:
                    self.logger.info(f"Fetching updates for {len(symbols)} symbols...")
                    sleep_time = self.config.UPDATE_INTERVAL_SECONDS

                # Sleep BEFORE fetching to respect the interval from the start
                await asyncioSleep(sleep_time)

                # Fetch data
                data = await self.fetch_update_data(symbols)
                
                # Check for fresh data (simple dedup logic)
                valid_data = {}
                if data:
                    for symbol, item in data.items():
                        current_ts = item.get('timestamp')
                        last_ts = self.last_timestamps.get(symbol)
                        
                        if last_ts is None or current_ts > last_ts:
                            valid_data[symbol] = item
                            self.last_timestamps[symbol] = current_ts

                if valid_data:
                    # PUSH data to the callback
                    await callback(valid_data)
                
            except asyncioCancelledError:
                self.logger.info("YahooFinance listener cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Error in YahooFinance listener: {e}")
                await asyncioSleep(10)

    #-----------------------------------------------------------------------------------------------
    async def _fetch_batch(self, symbols: List[str], fetch_func) -> Dict[str, Any]:
        """
        Helper to process a batch of symbols concurrently with rate limiting.
        """
        if not symbols:
            return {}
            
        results = {}
        semaphore = asyncioSemaphore(self.config.CONCURRENT_REQUESTS)
        
        async def limited_fetch(symbol):
            async with semaphore:                
                await asyncioSleep(0.01) 
                return await fetch_func(symbol)
        
        tasks = [limited_fetch(sym) for sym in symbols]
        symbol_results = await asyncioGather(*tasks, return_exceptions=True)
        
        for res in symbol_results:
            if isinstance(res, Exception):
                self.logger.error(f"Error fetching symbol: {res}")
            elif isinstance(res, dict) and res:
                results.update(res)
                
        self.logger.info(f"YahooFinance: Fetched {len(results)}/{len(symbols)} symbols successfully.")
        return results

    #-----------------------------------------------------------------------------------------------
    async def _fetch_initial_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch max retention history for a single symbol.
        Used during application startup.
        """
        return await self._fetch_yahoo_data(symbol, range_str=f"{self.config.RETENTION_DAYS}d")

    #-----------------------------------------------------------------------------------------------
    async def _fetch_update_symbol(self, symbol: str) -> Dict[str, Any]:
        """
        Fetch 1-day history for a single symbol to get the latest updates.
        """
        return await self._fetch_yahoo_data(symbol, range_str="1d")

    #-----------------------------------------------------------------------------------------------
    async def _fetch_yahoo_data(self, symbol: str, range_str: str) -> Dict[str, Any]:
        """
        Core logic to query Yahoo Finance API and parse the response.
        
        Args:
            symbol: Stock symbol.
            range_str: Time range string (e.g., '1d', '7d').
            
        Returns:
            Dict[str, Any]: Parsed data including time series and latest snapshot.
        """
        params = {
            "interval": "5m",
            "range": range_str,
            "includePrePost": "false"
        }
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        
        try:
            data = await self.network.fetch_json(url, params=params)
            quote_data = data.get("chart", {}).get("result", [{}])[0]
            
            if not quote_data or "timestamp" not in quote_data:
                self.logger.warning(f"No result in chart data for {symbol}")
                return {}

            meta = quote_data.get("meta", {})
            indicators = quote_data.get("indicators", {}).get("quote", [{}])[0]
            
            timestamps = quote_data.get("timestamp", [])
            closes = indicators.get("close", [])
            opens = indicators.get("open", [])
            highs = indicators.get("high", [])
            lows = indicators.get("low", [])
            volumes = indicators.get("volume", [])
            
            if not timestamps:
                return {}

            # 1. Validation: Alignment check
            # Ensure all arrays are same length to avoid zip truncation hiding missing data
            data_arrays = [closes, opens, highs, lows, volumes]
            if not all(len(arr) == len(timestamps) for arr in data_arrays):
                self.logger.warning(f"Data alignment error for {symbol}: Mismatched array lengths.")
                return {}
            
            # Build full time series data
            time_series = []
            prev_close = None
            prev_volume = None
            
            if meta.get("chartPreviousClose"):
                prev_close = meta.get("chartPreviousClose")
            
            # 2. Sorting & Iteration
            # Zip and sort by timestamp (x[0])
            # This is cleaner than index sorting
            raw_tuples = zip(timestamps, opens, highs, lows, closes, volumes)
            sorted_data = sorted(raw_tuples, key=lambda x: x[0])

            valid_points = 0
            
            for ts, op, hi, lo, cl, vol in sorted_data:
                 # Basic None check
                 if None in (ts, op, hi, lo, cl, vol):
                     self.logger.error(f"Invalid OHLCV datas received from yahoo finance : {symbol}: {ts}, {op}, {hi}, {lo}, {cl}, {vol}")
                     continue

                 # 3. Data Cleaning
                 if cl <= 0 or vol < 0:
                     self.logger.error(f"Skipping invalid point for {symbol}: {cl}, {vol}")
                     continue
                     
                 price_pct = 0.0
                 vol_pct = 0.0
                
                 if prev_close and prev_close != 0:
                     price_pct = (cl - prev_close) / prev_close
                
                 if prev_volume and prev_volume != 0:
                      vol_pct = (vol - prev_volume) / prev_volume
                
                 item = {
                    "symbol": symbol,
                    "timestamp": ts,
                    "open": op,
                    "high": hi,
                    "low": lo,
                    "close": cl,
                    "volume": vol, 
                    "price": cl,
                    "price_percent_change": price_pct,
                    "volume_percent_change": vol_pct
                }
                 time_series.append(item)
                 valid_points += 1
                
                 prev_close = cl
                 prev_volume = vol
            
            if not time_series:
                return {}
            
            # 4. Logging
            start_ts = time_series[0]['timestamp']
            end_ts = time_series[-1]['timestamp']
            self.logger.debug(f"Fetched {symbol}: {valid_points} valid points [{start_ts} -> {end_ts}]")
            
            latest = time_series[-1]
            return {
                symbol: {
                    "symbol": symbol,
                    "price": latest["close"],
                    "volume": latest["volume"],
                    "price_percent_change": latest["price_percent_change"],
                    "volume_percent_change": latest["volume_percent_change"],
                    "timestamp": latest["timestamp"],
                    "time_series": time_series
                }
            }
            
        except Exception as e:
            self.logger.error(f"Failed to fetch {symbol} ({url}): {e}")
            raise e
