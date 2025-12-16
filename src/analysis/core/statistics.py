#!/usr/bin/env python
# coding:utf-8

from typing import Optional, Tuple
import numpy as np



class CoreStatistics:
    """
    Pure mathematical statistics functions using Numpy.
    Stateless and side-effect free.
    """

    #-----------------------------------------------------------------------------------------------    
    @staticmethod
    def calculate_mean_std(values: np.ndarray) -> Tuple[float, float]:
        """
        Calculate mean and standard deviation.
        Returns (0.0, 0.0) if array is empty.
        """
        if len(values) == 0:
            return 0.0, 0.0
        
        mean = float(np.mean(values))
        std = float(np.std(values)) if len(values) > 1 else 0.0
        return mean, std

    #-----------------------------------------------------------------------------------------------    
    @staticmethod
    def calculate_correlation(series_a: np.ndarray, series_b: np.ndarray) -> float:
        """
        Calculate Pearson correlation coefficient between two arrays.
        Returns 0.0 if arrays are valid but constant (std=0) or empty.
        """
        if len(series_a) != len(series_b) or len(series_a) < 2:
            return 0.0
            
        # Check for zero variance to avoid RuntimeWarning/NaN
        if np.std(series_a) == 0 or np.std(series_b) == 0:
            return 0.0
            
        corr = np.corrcoef(series_a, series_b)[0, 1]
        
        if np.isnan(corr):
            return 0.0
            
        return float(corr)

    #-----------------------------------------------------------------------------------------------    
    @staticmethod
    def calculate_z_score(value: float, mean: float, std: float) -> float:
        """
        Calculate Z-Score (Standard Score).
        """
        if std == 0:
            return 0.0
        return (value - mean) / std
