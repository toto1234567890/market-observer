#!/usr/bin/env python
# coding:utf-8


from typing import Dict, Any
import numpy as np



class CoreFinancial:
    """
    Pure financial domain logic.
    Handles OHLCV calculations and market-specific metrics.
    """

    #----------------------------------------------------------------------------------------------- 
    @staticmethod
    def compute_ohlcv(prices: np.ndarray, volumes: np.ndarray) -> Dict[str, float]:
        """
        Compute OHLCV metrics from price and volume arrays.
        Assumes arrays are sorted by time.
        """
        if len(prices) == 0:
            return {
                "open": 0.0, "high": 0.0, "low": 0.0, "close": 0.0,
                "volume": 0.0, "avg_price": 0.0
            }

        return {
            "open": float(prices[0]),
            "high": float(np.max(prices)),
            "low": float(np.min(prices)),
            "close": float(prices[-1]),
            "volume": float(np.sum(volumes)),
            "avg_price": float(np.mean(prices))
        }

    #----------------------------------------------------------------------------------------------- 
    @staticmethod
    def calculate_change_percent(current_val: float, prev_val: float) -> float:
        """
        Calculate percentage change between two values.
        """
        if prev_val == 0:
            return 0.0
        return (current_val - prev_val) / prev_val

    #----------------------------------------------------------------------------------------------- 
    @staticmethod
    def calculate_anomaly_ratio(current_vol: float, historical_avg: float) -> float:
        """
        Calculate volume anomaly ratio (Current / Average).
        """
        if historical_avg <= 0:
            return 1.0 if current_vol == 0 else float(current_vol) # Fallback if avg is 0 but we have volume
            
        return current_vol / historical_avg
