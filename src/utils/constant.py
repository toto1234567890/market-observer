#!/usr/bin/env python
# coding:utf-8

"""
Constants for data retention and memory management.
Assuming standard trading week of ~5 days * 6.5 hours * 60 minutes = 1950 points.
Rounded up to 2000 for safety.
"""

"""
Constants and helper functions for data retention and memory management.
Assuming standard trading day of 6.5 hours * 60 minutes = 390 points.
Rounded up to 400 for safety.
"""

from math import ceil as mathCeil

DEFAULT_RETENTION_DAYS = 7

def calculate_max_data_points(days: int) -> int:
    """
    Calculate max data points based on retention days.
    """
    # approx 400 points per day (covering 6.5h market hours)
    return mathCeil(days * 400)
