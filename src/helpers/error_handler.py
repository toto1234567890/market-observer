#!/usr/bin/env python
# coding:utf-8


from typing import Callable, Any
from collections import deque
from asyncio import sleep as asyncioSleep, CancelledError as asyncioCancelledError, \
                    iscoroutinefunction as asyncioIscoroutinefunction, TimeoutError as asyncioTimeoutError

from traceback import format_exc as tracebackFormat_exc
from functools import wraps


import logging


class MarketObserverError(Exception):
    """Base exception for MarketObserver application"""
    pass

class ConfigurationError(MarketObserverError):
    """Configuration-related errors"""
    pass

class NetworkError(MarketObserverError):
    """Network-related errors"""
    pass

class DataSourceError(MarketObserverError):
    """Data source errors"""
    pass

class DatabaseError(MarketObserverError):
    """Database operation errors"""
    pass

class ValidationError(MarketObserverError):
    """Data validation errors"""
    pass


#-----------------------------------------------------------------------------------------------   
def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0, 
                      exceptions: tuple = (Exception,)):
    """Decorator for retrying async functions with exponential backoff"""
    def decorator(func: Callable):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            last_exception: Exception=None
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries - 1:
                        break
                    delay = base_delay * (2 ** attempt)
                    logging.getLogger(func.__module__).warning(
                        f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}: {e}. "
                        f"Retrying in {delay:.1f}s"
                    )
                    await asyncioSleep(delay)
            if last_exception:
                raise last_exception
            raise exceptions[0]("Max retries exceeded without captured exception")
        return wrapper
    return decorator


#-----------------------------------------------------------------------------------------------   
def handle_errors(logger: logging.Logger, re_raise: bool = True):
    """Decorator for comprehensive error handling"""
    def decorator(func: Callable):
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except asyncioCancelledError:
                raise  # Don't intercept cancellations
            except MarketObserverError as e:
                logger.error(f"Controlled error in {func.__name__}: {e}")
                if re_raise:
                    raise
                return None
            except Exception as e:
                logger.error(f"Unexpected error in {func.__name__}: {e}\n{tracebackFormat_exc()}")
                if re_raise:
                    raise DatabaseError(f"Operation failed: {e}") from e
                return None
        
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except MarketObserverError as e:
                logger.error(f"Controlled error in {func.__name__}: {e}")
                if re_raise:
                    raise
                return None
            except Exception as e:
                logger.error(f"Unexpected error in {func.__name__}: {e}\n{tracebackFormat_exc()}")
                if re_raise:
                    raise DatabaseError(f"Operation failed: {e}") from e
                return None
        
        return async_wrapper if asyncioIscoroutinefunction(func) else sync_wrapper
    return decorator


#-----------------------------------------------------------------------------------------------   


class ErrorHandler:
    """Centralized error handling with context management"""
    #-----------------------------------------------------------------------------------------------       
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.error_count = 0
        self.max_errors_before_restart = 10

    #-----------------------------------------------------------------------------------------------       
    async def execute_with_retry(self, operation: str, func: Callable, *args, 
                               max_retries: int = 3, **kwargs) -> Any:
        """Execute function with retry logic and proper error categorization"""
        for attempt in range(max_retries):
            try:
                result = await func(*args, **kwargs)
                self.error_count = max(0, self.error_count - 1)  # Recover from errors
                return result
                
            except asyncioTimeoutError as e:
                self.logger.warning(f"{operation} timed out (attempt {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    self.error_count += 1
                    raise NetworkError(f"{operation} timeout after {max_retries} attempts") from e
                await asyncioSleep(2 ** attempt)  # Exponential backoff
                
            except Exception as e:
                self.logger.error(f"{operation} failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    self.error_count += 1
                    if "network" in operation.lower() or "fetch" in operation.lower():
                        raise NetworkError(f"{operation} failed") from e
                    elif "database" in operation.lower() or "save" in operation.lower():
                        raise DatabaseError(f"{operation} failed") from e
                    else:
                        raise MarketObserverError(f"{operation} failed") from e
                await asyncioSleep(2 ** attempt)
        
        raise MarketObserverError(f"{operation} failed after {max_retries} attempts")

    #-----------------------------------------------------------------------------------------------       
    def reset_error_count(self):
        """Reset error counter"""
        self.error_count = 0