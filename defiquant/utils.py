from cachetools import TTLCache, cached
from defiquant.config import config

def get_cache():
    if config.CACHE_ENABLED:
        return TTLCache(maxsize=config.CACHE_MAXSIZE, ttl=config.CACHE_TTL)
    return None

def api_cache(func):
    def wrapper(*args, use_cache=True, **kwargs):
        cache = get_cache()
        if use_cache and cache:
            return cached(cache)(func)(*args, **kwargs)
        return func(*args, **kwargs)
    return wrapper
