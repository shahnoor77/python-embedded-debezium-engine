"""
Retry utilities for handling transient failures
"""
import logging
import time
from functools import wraps
from typing import Callable, Any

logger = logging.getLogger(__name__)


def with_retry(max_attempts: int = 3, delay_seconds: float = 5.0, 
               backoff_multiplier: float = 2.0):
    """
    Decorator to retry a function on failure
    
    Args:
        max_attempts: Maximum number of retry attempts
        delay_seconds: Initial delay between retries in seconds
        backoff_multiplier: Multiplier for exponential backoff
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay_seconds
            
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    
                    if attempt == max_attempts:
                        logger.error(f"Function {func.__name__} failed after {max_attempts} attempts")
                        raise
                    
                    logger.warning(
                        f"Function {func.__name__} failed (attempt {attempt}/{max_attempts}): {e}. "
                        f"Retrying in {current_delay:.1f}s..."
                    )
                    
                    time.sleep(current_delay)
                    current_delay *= backoff_multiplier
            
            raise last_exception
        
        return wrapper
    return decorator