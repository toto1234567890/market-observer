#!/usr/bin/env python
# coding:utf-8

from typing import Dict, List, Any, Optional
from time import time as timeTime
import numpy as np

from .core.statistics import CoreStatistics
from .core.financial import CoreFinancial
from .resampler import TimeSeriesResampler



import logging
logger = logging.getLogger(__name__)



class AnalysisFacade:
    """
    Orchestrates the analysis pipeline.
    1. Prepares Data (Dict -> Numpy)
    2. Resamples (Time Windows)
    3. Calculates (Core Functions)
    """

    #-----------------------------------------------------------------------------------------------    
    def __init__(self, config):
        self.config = config

    #-----------------------------------------------------------------------------------------------    
    def aggregate(self, data: Dict[str, Any], window_name: str, intermediate_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Aggregate real-time data for a single window.
        """
        results = {}
        window_seconds = self.config.WINDOWS_SECONDS_MAP.get(window_name)
        if not window_seconds:
             return {}

        for symbol, symbol_data in data.items():
            time_series = symbol_data.get("time_series", [])
            if len(time_series) == 0:
                continue

            try:
                # 1. Preparation
                ts_array, price_array, vol_array = self._prepare_arrays(time_series)
                if len(ts_array) == 0:
                    continue

                # 2. Filtering (Last window only for real-time aggregation)
                # We only want data belonging to the *current* window ending now
                now = timeTime()
                cutoff = now - window_seconds
                
                # Slicing
                mask = ts_array >= cutoff
                # If no data in current window
                if not np.any(mask):
                    continue
                    
                w_prices = price_array[mask]
                w_vols = vol_array[mask]
                w_ts = ts_array[mask]

                # 3. Calculation
                ohlcv = CoreFinancial.compute_ohlcv(w_prices, w_vols)
                
                # Correlation
                corr = CoreStatistics.calculate_correlation(w_prices, w_vols)
                
                # Anomaly
                avg_vol = intermediate_stats.get(symbol, {}).get("avg_volume_history", 1.0) if intermediate_stats else 1.0
                anomaly = CoreFinancial.calculate_anomaly_ratio(ohlcv["volume"], avg_vol)
                
                # Change
                pct_change = CoreFinancial.calculate_change_percent(ohlcv["close"], ohlcv["open"])

                # Package Result
                # Use deviation from average as volume_percent_change for activity sorting
                # Since we don't have 'previous window' in this context easily
                vol_pct_from_avg = (ohlcv["volume"] - avg_vol) / avg_vol if avg_vol > 0 else 0.0

                agg_data = {
                    "symbol": symbol,
                    "open": ohlcv["open"],
                    "close": ohlcv["close"],
                    "high": ohlcv["high"],
                    "low": ohlcv["low"],
                    "volume": ohlcv["volume"],
                    "avg_price": ohlcv["avg_price"],
                    "price_percent_change": pct_change,
                    "volume_percent_change": vol_pct_from_avg,
                    "price_volume_correlation": corr,
                    "volume_anomaly_ratio": anomaly,
                    "start_time": int(w_ts[0]),
                    "end_time": int(w_ts[-1]),
                    "data_points": len(w_prices)
                }
                
                results[symbol] = {window_name: agg_data}
                
            except Exception as e:
                logger.error(f"Error aggregating {symbol} {window_name}: {e}")
                continue

        return results

    #-----------------------------------------------------------------------------------------------    
    def aggregate_initial(self, data: Dict[str, Any], window_name: str, intermediate_stats: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Aggregate entire history into windowed candles.
        """
        results = {}
        window_seconds = self.config.WINDOWS_SECONDS_MAP.get(window_name)
        if not window_seconds:
            return {}

        for symbol, symbol_data in data.items():
            time_series = symbol_data.get("time_series", [])
            if len(time_series) == 0:
                continue
                
            try:
                # 1. Preparation
                ts_array, price_array, vol_array = self._prepare_arrays(time_series)
                
                # 2. Resampling
                # returns list of INDICES
                window_indices_list = TimeSeriesResampler.resample_indices(ts_array, window_seconds)
                
                candles = []
                avg_vol = intermediate_stats.get(symbol, {}).get("avg_volume_history", 1.0) if intermediate_stats else 1.0
                
                prev_close = None
                prev_volume = None
                
                # 3. Loop & Calculate
                for indices, w_start, w_end in window_indices_list:
                    if len(indices) == 0: continue
                    
                    w_prices = price_array[indices]
                    w_vols = vol_array[indices]
                    # w_ts = ts_array[indices] # Unused if we use w_start/w_end
                    
                    ohlcv = CoreFinancial.compute_ohlcv(w_prices, w_vols)
                    corr = CoreStatistics.calculate_correlation(w_prices, w_vols)
                    anomaly = CoreFinancial.calculate_anomaly_ratio(ohlcv["volume"], avg_vol)
                    
                    # Percent Change
                    pct_change = 0.0
                    if prev_close:
                        pct_change = CoreFinancial.calculate_change_percent(ohlcv["close"], prev_close)
                        
                    # Volume Change
                    vol_change = 0.0
                    if prev_volume:
                        vol_change = CoreFinancial.calculate_change_percent(ohlcv["volume"], prev_volume)
                    
                    candle = {
                        "symbol": symbol,
                        **ohlcv,
                        "price_percent_change": pct_change,
                        "volume_percent_change": vol_change,
                        "price_volume_correlation": corr,
                        "volume_anomaly_ratio": anomaly,
                        "start_time": int(w_start),
                        "end_time": int(w_end),
                        "data_points": len(indices)
                    }
                    
                    candles.append(candle)
                    prev_close = ohlcv["close"]
                    prev_volume = ohlcv["volume"]
                
                if candles:
                     results[symbol] = {window_name: candles}
                     
            except Exception as e:
                logger.error(f"Error initial aggregation {symbol}: {e}")
                continue
                
        return results

    #-----------------------------------------------------------------------------------------------    
    def calculate_stats_for_windows(self, data: Dict[str, Any], window_names: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Calculate baseline statistics (Mean, Std) for specified windows.
        """
        results = {}
        
        target_windows = []
        for wn in window_names:
            sec = self.config.WINDOWS_SECONDS_MAP.get(wn)
            if sec: target_windows.append((wn, sec))
            
        for symbol, symbol_data in data.items():
            time_series = symbol_data.get("time_series", [])
            if len(time_series) == 0: continue
            
            try:
                ts_array, _, vol_array = self._prepare_arrays(time_series)
                symbol_stats = {}
                
                for w_name, w_seconds in target_windows:
                    # Resample
                    indices_list = TimeSeriesResampler.resample_indices(ts_array, w_seconds)
                    
                    # Sum volumes per window
                    window_volumes = []
                    for idxs, _, _ in indices_list:
                         # Using numpy sum on the slice
                         window_volumes.append(np.sum(vol_array[idxs]))
                    
                    if not window_volumes:
                        continue
                        
                    w_vol_array = np.array(window_volumes)
                    mean, std = CoreStatistics.calculate_mean_std(w_vol_array)
                    
                    symbol_stats[w_name] = {
                        "avg_volume_history": mean,
                        "std_volume_history": std,
                        "data_points_history": len(window_volumes),
                        "last_history_timestamp": int(ts_array[-1])
                    }
                
                if symbol_stats:
                    results[symbol] = symbol_stats
                    
            except Exception as e:
                logger.error(f"Error stats {symbol}: {e}")
                
        return results

    #-----------------------------------------------------------------------------------------------
    @staticmethod
    def convert_to_numpy_matrix(data: List[Dict[str, Any]]) -> np.ndarray:
        """
        Convert list of dicts to numpy matrix (N, 3) [ts, price, vol].
        Useful for pre-processing data before aggregation.
        """
        if not data:
            return np.empty((0, 3), dtype=np.float64)
            
        # Ensure list is sorted by timestamp
        # Note: Modifies list in-place which is generally fine for historical data loading
        data.sort(key=lambda x: x.get('timestamp', 0))
        
        rows = []
        for d in data:
            ts = d.get('timestamp')
            p = d.get('price')
            v = d.get('volume')
            
            if ts is not None and p is not None and v is not None:
                rows.append([ts, p, v])
                
        if not rows:
             return np.empty((0, 3), dtype=np.float64)
             
        return np.array(rows, dtype=np.float64)

    #-----------------------------------------------------------------------------------------------    
    def _prepare_arrays(self, data: Any) -> Any:
        # Check if data is already a numpy array (from RingBuffer optimization)
        # Expected shape: (N, 5) -> [timestamp, price, volume, pct, vol_pct]
        if isinstance(data, np.ndarray):
            if data.size == 0:
                 return np.array([]), np.array([]), np.array([])
            
            # Extract columns directly: 0=ts, 1=price, 2=volume
            return data[:, 0], data[:, 1], data[:, 2]

        # Standard List[Dict] processing (Historical Data or non-opt path)
        if not data: return np.array([]), np.array([]), np.array([])
        
        # Sort by timestamp
        data.sort(key=lambda x: x.get('timestamp', 0))
        
        timestamps = []
        prices = []
        volumes = []
        
        # Robust extraction similar to original VolumeCalculator validation
        for d in data:
            ts = d.get('timestamp')
            p = d.get('price')
            v = d.get('volume')
            
            # Ensure strictly valid numbers
            if ts is not None and p is not None and v is not None:
                # Basic type/value check if needed, but assuming source provides numbers
                timestamps.append(ts)
                prices.append(p)
                volumes.append(v)
        
        return np.array(timestamps, dtype=np.float64), np.array(prices, dtype=np.float64), np.array(volumes, dtype=np.float64)
