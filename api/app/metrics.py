from prometheus_client import Counter

CACHE_HITS = Counter("products_api_cache_hits_total", "Product cache hits")
CACHE_MISSES = Counter("products_api_cache_misses_total", "Product cache misses")
