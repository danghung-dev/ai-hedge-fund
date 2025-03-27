import os
import json
from pathlib import Path
from typing import Any

class Cache:
    """In-memory cache for API responses with immediate auto-save functionality."""

    def __init__(self):
        self._prices_cache: dict[str, list[dict[str, Any]]] = {}
        self._financial_metrics_cache: dict[str, list[dict[str, Any]]] = {}
        self._line_items_cache: dict[str, list[dict[str, Any]]] = {}
        self._insider_trades_cache: dict[str, list[dict[str, Any]]] = {}
        self._company_news_cache: dict[str, list[dict[str, Any]]] = {}

    def _merge_data(self, existing: list[dict] | None, new_data: list[dict], key_field: str) -> list[dict]:
        """Merge existing and new data, avoiding duplicates based on a key field and merging fields within same key."""
        if not existing:
            return new_data

        # Create a dictionary for O(1) lookup of existing items by key field
        existing_dict = {item[key_field]: item for item in existing}
        merged = []

        # Process each new item
        for new_item in new_data:
            key = new_item[key_field]
            
            if key in existing_dict:
                # If key exists, merge the fields
                existing_item = existing_dict[key]
                merged_item = existing_item.copy()  # Start with existing fields
                merged_item.update(new_item)  # Update/add new fields
                merged.append(merged_item)
            else:
                # If key doesn't exist, add as new item
                merged.append(new_item)

        # Add any existing items that weren't updated
        for key, item in existing_dict.items():
            if not any(new_item[key_field] == key for new_item in new_data):
                merged.append(item)

        return merged

    def get_prices(self, ticker: str) -> list[dict[str, Any]] | None:
        """Get cached price data if available."""
        return self._prices_cache.get(ticker)

    def set_prices(self, ticker: str, data: list[dict[str, Any]]):
        """Append new price data to cache and save immediately."""
        # Ensure all data is JSON serializable
        serializable_data = []
        for item in data:
            serializable_item = {
                'time': str(item['time']),  # Ensure time is string
                'open': float(item['open']),
                'close': float(item['close']),
                'high': float(item['high']),
                'low': float(item['low']),
                'volume': int(item['volume'])
            }
            serializable_data.append(serializable_item)
            
        self._prices_cache[ticker] = self._merge_data(self._prices_cache.get(ticker), serializable_data, key_field="time")
        # Save to disk immediately
        self.save_to_disk()

    def get_financial_metrics(self, ticker: str) -> list[dict[str, Any]]:
        """Get cached financial metrics if available."""
        return self._financial_metrics_cache.get(ticker)

    def set_financial_metrics(self, ticker: str, data: list[dict[str, Any]]):
        """Append new financial metrics to cache and save immediately."""
        self._financial_metrics_cache[ticker] = self._merge_data(self._financial_metrics_cache.get(ticker), data, key_field="report_period")
        # Save to disk immediately
        self.save_to_disk()

    def get_line_items(self, ticker: str) -> list[dict[str, Any]] | None:
        """Get cached line items if available."""
        return self._line_items_cache.get(ticker)

    def set_line_items(self, ticker: str, data: list[dict[str, Any]]):
        """Append new line items to cache and save immediately."""
        self._line_items_cache[ticker] = self._merge_data(self._line_items_cache.get(ticker), data, key_field="report_period")
        # Save to disk immediately
        self.save_to_disk()

    def get_insider_trades(self, ticker: str) -> list[dict[str, Any]] | None:
        """Get cached insider trades if available."""
        return self._insider_trades_cache.get(ticker)

    def set_insider_trades(self, ticker: str, data: list[dict[str, Any]]):
        """Append new insider trades to cache and save immediately."""
        self._insider_trades_cache[ticker] = self._merge_data(self._insider_trades_cache.get(ticker), data, key_field="filing_date")
        # Save to disk immediately
        self.save_to_disk()

    def get_company_news(self, ticker: str) -> list[dict[str, Any]] | None:
        """Get cached company news if available."""
        return self._company_news_cache.get(ticker)

    def set_company_news(self, ticker: str, data: list[dict[str, Any]]):
        """Append new company news to cache and save immediately."""
        self._company_news_cache[ticker] = self._merge_data(self._company_news_cache.get(ticker), data, key_field="date")
        # Save to disk immediately
        self.save_to_disk()

    def save_to_disk(self, filepath: str | None = None):
        """Save the entire cache to disk as a JSON file.
        
        Args:
            filepath: The path where to save the cache file. If None, uses the default location.
        """
        filepath = filepath or get_default_cache_path()
        
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        cache_data = {
            'prices': self._prices_cache,
            'financial_metrics': self._financial_metrics_cache,
            'line_items': self._line_items_cache,
            'insider_trades': self._insider_trades_cache,
            'company_news': self._company_news_cache,
        }
        
        try:
            # First write to a temporary file to avoid corrupting the cache file if there's an error
            temp_filepath = f"{filepath}.tmp"
            with open(temp_filepath, 'w') as f:
                json.dump(cache_data, f, indent=2)
            
            # Then rename the temporary file to the actual file
            os.replace(temp_filepath, filepath)
            print(f"Cache saved to: {filepath}")
        except Exception as e:
            print(f"Error saving cache to {filepath}: {e}")
    
    def load_from_disk(self, filepath: str | None = None):
        """Load cache data from a JSON file on disk.
        
        Args:
            filepath: The path to the cache file. If None, uses the default location.
            
        Returns:
            bool: True if the file was loaded successfully, False otherwise
        """
        filepath = filepath or get_default_cache_path()
        
        if not os.path.exists(filepath):
            print(f"Cache file not found at: {filepath}")
            return False
        
        try:
            with open(filepath, 'r') as f:
                cache_data = json.load(f)
            
            self._prices_cache = cache_data.get('prices', {})
            self._financial_metrics_cache = cache_data.get('financial_metrics', {})
            self._line_items_cache = cache_data.get('line_items', {})
            self._insider_trades_cache = cache_data.get('insider_trades', {})
            self._company_news_cache = cache_data.get('company_news', {})
            
            print(f"Cache loaded from: {filepath}")
            return True
        except (json.JSONDecodeError, KeyError, Exception) as e:
            print(f"Error loading cache from {filepath}: {e}")
            return False

    def clear(self):
        """Clear all cached data and save immediately."""
        self._prices_cache.clear()
        self._financial_metrics_cache.clear()
        self._line_items_cache.clear()
        self._insider_trades_cache.clear()
        self._company_news_cache.clear()
        # Save to disk immediately
        self.save_to_disk()


def get_default_cache_path() -> str:
    """Get the default path for cache storage."""
    # Check environment variable first
    if cache_path := os.environ.get("FINANCIAL_CACHE_PATH"):
        return cache_path
    
    # Use the .cache directory in the project workspace
    project_root = Path(__file__).resolve().parent.parent.parent
    cache_dir = project_root / ".cache"
    
    os.makedirs(cache_dir, exist_ok=True)
    return str(cache_dir / "financial_data_cache.json")


# Global cache instance
_cache = Cache()


def get_cache() -> Cache:
    """Get the global cache instance."""
    return _cache


# Helper functions to directly access cache persistence
def save_cache(filepath: str | None = None):
    """Save the global cache to disk.
    
    Args:
        filepath: Optional custom path for the cache file
    """
    _cache.save_to_disk(filepath)


def load_cache(filepath: str | None = None):
    """Load the global cache from disk.
    
    Args:
        filepath: Optional custom path for the cache file
        
    Returns:
        bool: True if load was successful, False otherwise
    """
    return _cache.load_from_disk(filepath)


def clear_cache():
    """Clear the global cache."""
    _cache.clear()


# Try to load cache at module initialization
try:
    load_cache()
except Exception as e:
    print(f"Warning: Failed to load cache: {e}")
