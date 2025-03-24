import os
import pandas as pd
import requests
from typing import List, Optional

from data.cache import get_cache
from data.models import (
    CompanyNews,
    CompanyNewsResponse,
    FinancialMetrics,
    FinancialMetricsResponse,
    Price,
    PriceResponse,
    LineItem,
    LineItemResponse,
    InsiderTrade,
    InsiderTradeResponse,
)

# Import both API implementations
from . import vnstock_api
from . import financialdatasets_api

# Determine which API to use based on environment variable
USE_VNSTOCK = os.environ.get('USE_VNSTOCK', 'true').lower() == 'true'

# Select the appropriate API implementation
_api = vnstock_api if USE_VNSTOCK else financialdatasets_api

# Global cache instance
_cache = get_cache()


def get_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch price data using the configured API."""
    return _api.get_prices(ticker, start_date, end_date)


def get_financial_metrics(
    ticker: str,
    end_date: str,
    period: str = "year" if USE_VNSTOCK else "ttm",
    limit: int = 10,
) -> pd.DataFrame:
    """Fetch financial metrics using the configured API."""
    return _api.get_financial_metrics(ticker, end_date, period, limit)


def get_market_cap(ticker: str, end_date: str) -> float:
    """Fetch market cap using the configured API."""
    return _api.get_market_cap(ticker, end_date)


def prices_to_df(prices: pd.DataFrame) -> pd.DataFrame:
    """Convert prices to DataFrame using the configured API."""
    return _api.prices_to_df(prices)


def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """Get price data using the configured API."""
    return _api.get_price_data(ticker, start_date, end_date)


def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "year" if USE_VNSTOCK else "ttm",
    limit: int = 10,
) -> list[LineItem]:
    """Fetch line items using the configured API."""
    return _api.search_line_items(ticker, line_items, end_date, period, limit)


def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade]:
    """Fetch insider trades using the configured API."""
    # Check cache first
    if cached_data := _cache.get_insider_trades(ticker):
        # Filter cached data by date range
        filtered_data = [InsiderTrade(**trade) for trade in cached_data 
                        if (start_date is None or (trade.get("transaction_date") or trade["filing_date"]) >= start_date)
                        and (trade.get("transaction_date") or trade["filing_date"]) <= end_date]
        filtered_data.sort(key=lambda x: x.transaction_date or x.filing_date, reverse=True)
        if filtered_data:
            return filtered_data

    # If not in cache, fetch from configured API
    trades = _api.get_insider_trades(ticker, end_date, start_date, limit)
    
    # Cache the results if we got any
    if trades:
        _cache.set_insider_trades(ticker, [trade.model_dump() for trade in trades])
    
    return trades


def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews]:
    """Fetch company news using the configured API."""
    # Check cache first
    if cached_data := _cache.get_company_news(ticker):
        # Filter cached data by date range
        filtered_data = [CompanyNews(**news) for news in cached_data 
                        if (start_date is None or news["date"] >= start_date)
                        and news["date"] <= end_date]
        filtered_data.sort(key=lambda x: x.date, reverse=True)
        if filtered_data:
            return filtered_data

    # If not in cache, fetch from configured API
    news = _api.get_company_news(ticker, end_date, start_date, limit)
    
    # Cache the results if we got any
    if news:
        _cache.set_company_news(ticker, [n.model_dump() for n in news])
    
    return news


# Additional vnstock-specific functions
def get_company_info(ticker: str) -> dict:
    """Fetch company information (vnstock only)."""
    if not USE_VNSTOCK:
        raise NotImplementedError("This function is only available with vnstock API")
    return _api.get_company_info(ticker)

def get_trading_data(tickers: List[str]) -> pd.DataFrame:
    """Fetch real-time trading data (vnstock only)."""
    if not USE_VNSTOCK:
        raise NotImplementedError("This function is only available with vnstock API")
    return _api.get_trading_data(tickers)

def get_listing() -> pd.DataFrame:
    """Fetch list of all listed companies (vnstock only)."""
    if not USE_VNSTOCK:
        raise NotImplementedError("This function is only available with vnstock API")
    return _api.get_listing()

def get_financial_statements(
    ticker: str,
    statement_type: str = "balance_sheet",
    period: str = "year"
) -> pd.DataFrame:
    """Fetch financial statements (vnstock only)."""
    if not USE_VNSTOCK:
        raise NotImplementedError("This function is only available with vnstock API")
    return _api.get_financial_statements(ticker, statement_type, period)
