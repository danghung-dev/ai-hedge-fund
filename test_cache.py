#!/usr/bin/env python3
"""Test script for the cache functionality."""

import os
from pathlib import Path
from src.data.cache import get_cache, get_default_cache_path, save_cache, load_cache

def main():
    # Get the cache instance
    cache = get_cache()
    
    # Print the default cache path
    cache_path = get_default_cache_path()
    print(f"Cache path: {cache_path}")
    
    # Add some test data
    print("\nAdding test data to cache...")
    cache.set_prices("TEST", [
        {"time": "2023-01-01", "open": 100, "high": 105, "low": 95, "close": 102, "volume": 1000},
        {"time": "2023-01-02", "open": 102, "high": 107, "low": 101, "close": 105, "volume": 1200},
    ])
    
    cache.set_financial_metrics("TEST", [
        {"ticker": "TEST", "report_period": "2022-12-31", "period": "year", "currency": "USD", "price_to_earnings_ratio": 15.5},
        {"ticker": "TEST", "report_period": "2023-03-31", "period": "quarter", "currency": "USD", "price_to_earnings_ratio": 16.2},
    ])
    
    # No need to force save, it happens automatically
    
    # Check if file exists
    if os.path.exists(cache_path):
        file_size = os.path.getsize(cache_path)
        print(f"Cache file exists, size: {file_size} bytes")
    else:
        print("Cache file does not exist!")
    
    # Clear the cache
    print("\nClearing cache...")
    cache.clear()
    
    # Verify cache is empty
    print("\nChecking if cache is empty after clear...")
    prices = cache.get_prices("TEST")
    print(f"Prices after clear: {prices}")
    
    # Load from disk
    print("\nLoading cache from disk...")
    load_cache()
    
    # Verify data is loaded
    prices = cache.get_prices("TEST")
    metrics = cache.get_financial_metrics("TEST")
    
    print("\nData after loading from disk:")
    print(f"Prices: {prices}")
    print(f"Metrics: {metrics}")
    
    # Add new data - will be saved immediately
    print("\nAdding more test data...")
    cache.set_prices("TEST2", [
        {"time": "2023-02-01", "open": 200, "high": 205, "low": 195, "close": 202, "volume": 2000},
    ])
    
    # Verify it was saved by reloading
    print("\nVerifying data was saved immediately...")
    # Force a fresh load
    cache.clear()
    load_cache()
    
    # Check for the new data
    test2_prices = cache.get_prices("TEST2")
    print(f"TEST2 prices after reload: {test2_prices}")
    
    print("\nCache test completed!")

if __name__ == "__main__":
    main() 