import os
import pandas as pd
from datetime import datetime
from vnstock import Vnstock, Quote, Listing, Trading, Company, Finance
from data.models import FinancialMetrics, FinancialMetricsResponse, LineItem, LineItemResponse, InsiderTrade, CompanyNews

# Initialize Vnstock instance with source from environment variable
VNSTOCK_SOURCE = os.environ.get('VNSTOCK_SOURCE', 'TCBS')
_vnstock = Vnstock()

def get_prices(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch historical price data for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: DataFrame containing price data
    """
    try:
        # Initialize stock with configured source
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        # Get historical data
        df = stock.quote.history(
            start=start_date,
            end=end_date,
            interval='1D'
        )
        
        # Rename columns to match our schema
        df = df.rename(columns={
            'time': 'Date',
            'open': 'open',
            'high': 'high',
            'low': 'low',
            'close': 'close',
            'volume': 'volume'
        })
        
        # Set Date as index
        df['Date'] = pd.to_datetime(df['Date'])
        df.set_index('Date', inplace=True)
        
        # Ensure numeric columns
        numeric_cols = ['open', 'close', 'high', 'low', 'volume']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        df.sort_index(inplace=True)
        return df
        
    except Exception as e:
        raise Exception(f"Error fetching price data for {ticker}: {str(e)}")

def get_financial_metrics(ticker: str, end_date: str, period: str = "year", limit: int = 10) -> pd.DataFrame:
    """
    Fetch financial metrics for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        end_date (str): End date in YYYY-MM-DD format
        period (str): 'year' or 'quarter'
        limit (int): Number of periods to return
        
    Returns:
        pd.DataFrame: DataFrame containing financial metrics
    """
    try:
        print(f'Getting financial metrics for {ticker} from {VNSTOCK_SOURCE}')
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        # Get financial ratios
        print(f'Fetching financial ratios...')
        df = stock.finance.ratio(period=period, lang='en', dropna=True)
        print(f'Raw financial ratios data shape: {df.shape}')
        
        if df.empty:
            print(f'No financial ratios data available for {ticker}')
            return pd.DataFrame()
            
        # Get additional metrics from income statement for revenue growth
        try:
            income_stmt = stock.finance.income_statement(period=period, lang='en', dropna=True)
            if not income_stmt.empty:
                df['net_revenue'] = income_stmt['net_revenue']
                # Calculate revenue growth
                df['revenue_growth'] = df['net_revenue'].pct_change(-1)  # Previous period growth
        except Exception as e:
            print(f'Error fetching income statement data: {str(e)}')
            
        # Get market cap from company overview
        try:
            overview = stock.company.overview()
            if not overview.empty:
                df['market_cap'] = overview.get('market_cap', None)
        except Exception as e:
            print(f'Error fetching company overview data: {str(e)}')
        
        # Filter by date and limit
        df.index = pd.to_datetime(df.index)
        df = df[df.index <= end_date]
        if df.empty:
            print(f'No financial ratios data available for {ticker} before {end_date}')
            return pd.DataFrame()
            
        df = df.sort_index(ascending=False).head(limit)
        print(f'Filtered financial ratios data shape: {df.shape}')
        
        # Convert DataFrame rows to FinancialMetrics objects
        financial_metrics = []
        for idx, row in df.iterrows():
            try:
                metrics = FinancialMetrics(
                    ticker=ticker,
                    report_period=idx.strftime('%Y-%m-%d'),
                    period=period,
                    currency='VND',
                    # Map vnstock fields to FinancialMetrics fields with proper error handling
                    market_cap=row.get('market_cap'),
                    price_to_earnings_ratio=_safe_get(row, 'price_to_earning'),
                    price_to_book_ratio=_safe_get(row, 'price_to_book'),
                    enterprise_value_to_ebitda_ratio=_safe_get(row, 'value_before_ebitda'),
                    return_on_equity=_safe_get(row, 'roe'),
                    return_on_assets=_safe_get(row, 'roa'),
                    earnings_per_share=_safe_get(row, 'earning_per_share'),
                    book_value_per_share=_safe_get(row, 'book_value_per_share'),
                    current_ratio=_safe_get(row, 'current_payment'),
                    quick_ratio=_safe_get(row, 'quick_payment'),
                    debt_to_equity=_safe_get(row, 'debt_on_equity'),
                    debt_to_assets=_safe_get(row, 'debt_on_asset'),
                    operating_margin=_safe_get(row, 'operating_profit_margin'),
                    gross_margin=_safe_get(row, 'gross_profit_margin'),
                    net_margin=_safe_get(row, 'post_tax_margin'),
                    revenue_growth=_safe_get(row, 'revenue_growth'),
                    earnings_growth=_safe_get(row, 'eps_change'),
                    # Calculate additional metrics where possible
                    enterprise_value=None,  # Not available in vnstock
                    price_to_sales_ratio=_safe_get(row, 'price_to_sale'),
                    enterprise_value_to_revenue_ratio=None,  # Not available in vnstock
                    free_cash_flow_yield=None,  # Not available in vnstock
                    peg_ratio=None,  # Not available in vnstock
                    return_on_invested_capital=_safe_get(row, 'roic'),
                    asset_turnover=_safe_get(row, 'asset_turnover'),
                    inventory_turnover=_safe_get(row, 'inventory_turnover'),
                    receivables_turnover=_safe_get(row, 'receivable_turnover'),
                    days_sales_outstanding=_safe_get(row, 'days_receivable'),
                    operating_cycle=None,  # Not available in vnstock
                    working_capital_turnover=None,  # Not available in vnstock
                    cash_ratio=_safe_get(row, 'cash_ratio'),
                    operating_cash_flow_ratio=None,  # Not available in vnstock
                    interest_coverage=_safe_get(row, 'ebit_on_interest'),
                    book_value_growth=_safe_get(row, 'book_value_per_share_change'),
                    earnings_per_share_growth=_safe_get(row, 'eps_change'),
                    free_cash_flow_growth=None,  # Not available in vnstock
                    operating_income_growth=None,  # Not available in vnstock
                    ebitda_growth=None,  # Not available in vnstock
                    payout_ratio=_safe_get(row, 'dividend_yield'),
                    free_cash_flow_per_share=None  # Not available in vnstock
                )
                financial_metrics.append(metrics)
            except Exception as e:
                print(f'Error processing row for {ticker} at {idx}: {str(e)}')
                print(f'Row data: {row.to_dict()}')
                continue
        
        if not financial_metrics:
            print(f'No valid financial metrics could be processed for {ticker}')
            return pd.DataFrame()
            
        # Create FinancialMetricsResponse
        response = FinancialMetricsResponse(financial_metrics=financial_metrics)
        
        # Convert to DataFrame and return
        result_df = pd.DataFrame([m.model_dump() for m in response.financial_metrics])
        print(f'Processed {len(result_df)} financial metrics rows for {ticker}')
        return result_df
        
    except Exception as e:
        print(f'Error fetching financial metrics for {ticker}: {str(e)}')
        return pd.DataFrame()

def _safe_get(row: pd.Series, field: str) -> float | None:
    """Helper function to safely get a field value from a pandas Series."""
    try:
        value = row.get(field)
        return float(value) if pd.notna(value) else None
    except (ValueError, TypeError):
        return None

def get_company_info(ticker: str) -> dict:
    """
    Fetch company information for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        
    Returns:
        dict: Dictionary containing company information
    """
    try:
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        company_info = stock.company.overview()
        return company_info
        
    except Exception as e:
        raise Exception(f"Error fetching company info for {ticker}: {str(e)}")

def get_market_cap(ticker: str, end_date: str) -> float:
    """
    Fetch market cap for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        float: Market capitalization value
    """
    try:
        # Get financial metrics which include market cap
        metrics = get_financial_metrics(ticker, end_date, limit=1)
        if 'market_cap' in metrics.columns:
            return metrics['market_cap'].iloc[0]
        return None
        
    except Exception as e:
        raise Exception(f"Error fetching market cap for {ticker}: {str(e)}")

def get_trading_data(tickers: list[str]) -> pd.DataFrame:
    """
    Fetch real-time trading data for given tickers using vnstock.
    
    Args:
        tickers (list[str]): List of stock symbols
        
    Returns:
        pd.DataFrame: DataFrame containing trading data
    """
    try:
        trading = Trading(source=VNSTOCK_SOURCE)
        df = trading.price_board(tickers)
        return df
        
    except Exception as e:
        raise Exception(f"Error fetching trading data: {str(e)}")

def get_listing() -> pd.DataFrame:
    """
    Fetch list of all listed companies using vnstock.
    
    Returns:
        pd.DataFrame: DataFrame containing listed companies
    """
    try:
        listing = Listing()
        df = listing.all_symbols()
        return df
        
    except Exception as e:
        raise Exception(f"Error fetching listing data: {str(e)}")

def get_financial_statements(ticker: str, statement_type: str = "balance_sheet", period: str = "year") -> pd.DataFrame:
    """
    Fetch financial statements for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        statement_type (str): One of 'balance_sheet', 'income_statement', 'cash_flow'
        period (str): 'year' or 'quarter'
        
    Returns:
        pd.DataFrame: DataFrame containing financial statement data
    """
    try:
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        if statement_type == "balance_sheet":
            df = stock.finance.balance_sheet(period=period, lang='en', dropna=True)
        elif statement_type == "income_statement":
            df = stock.finance.income_statement(period=period, lang='en', dropna=True)
        elif statement_type == "cash_flow":
            df = stock.finance.cash_flow(period=period, dropna=True)
        else:
            raise ValueError(f"Invalid statement type: {statement_type}")
            
        return df
        
    except Exception as e:
        raise Exception(f"Error fetching {statement_type} for {ticker}: {str(e)}")

# Helper function to convert prices to DataFrame (maintaining compatibility)
def prices_to_df(prices: pd.DataFrame) -> pd.DataFrame:
    """
    Convert prices DataFrame to match the expected schema.
    
    Args:
        prices (pd.DataFrame): DataFrame from get_prices
        
    Returns:
        pd.DataFrame: Formatted DataFrame
    """
    return prices

# Maintain compatibility with existing code
def get_price_data(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Get price data maintaining compatibility with existing code.
    
    Args:
        ticker (str): Stock symbol
        start_date (str): Start date in YYYY-MM-DD format
        end_date (str): End date in YYYY-MM-DD format
        
    Returns:
        pd.DataFrame: DataFrame containing price data
    """
    return get_prices(ticker, start_date, end_date)

def search_line_items(
    ticker: str,
    line_items: list[str],
    end_date: str,
    period: str = "year",
    limit: int = 10,
) -> list[LineItem]:
    """
    Fetch line items from financial statements using vnstock.
    
    Args:
        ticker (str): Stock symbol
        line_items (list[str]): List of line items to search for
        end_date (str): End date in YYYY-MM-DD format
        period (str): 'year' or 'quarter'
        limit (int): Number of periods to return
        
    Returns:
        list[LineItem]: List of line items found
    """
    try:
        print(f"Fetching financial data for {ticker} from {VNSTOCK_SOURCE}")
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        # Map common line items to their vnstock field names
        field_mapping = {
            # Income Statement
            "revenue": "net_revenue",
            "net_income": "profit_after_tax",
            "earnings_per_share": "basic_earnings_per_share",
            "operating_income": "operating_profit",
            "gross_profit": "gross_profit",
            "interest_income": "interest_income",
            "interest_expense": "interest_expense",
            "pretax_income": "profit_before_tax",
            "income_tax": "corporate_income_tax",
            
            # Balance Sheet
            "total_assets": "total_assets",
            "total_liabilities": "total_liabilities",
            "shareholders_equity": "owner_equity",
            "cash_and_equivalents": "cash",
            "current_assets": "current_assets",
            "current_liabilities": "current_liabilities",
            "accounts_receivable": "short_term_receivable",
            "inventory": "inventory",
            "long_term_investments": "long_term_investment",
            "fixed_assets": "fixed_assets",
            "intangible_assets": "intangible_assets",
            "short_term_debt": "short_term_debt",
            "long_term_debt": "long_term_debt",
            
            # Cash Flow
            "operating_cash_flow": "net_cash_from_operating_activities",
            "investing_cash_flow": "net_cash_from_investing_activities",
            "financing_cash_flow": "net_cash_from_financing_activities",
            "capital_expenditure": "capital_expenditure",
            "depreciation_and_amortization": "depreciation"
        }
        
        # Get balance sheet data
        print(f"Fetching balance sheet data...")
        balance_sheet = stock.finance.balance_sheet(period=period, lang='en', dropna=True)
        print(f"Balance sheet data shape: {balance_sheet.shape if not balance_sheet.empty else '(empty)'}")
        
        # Get income statement data
        print(f"Fetching income statement data...")
        income_stmt = stock.finance.income_statement(period=period, lang='en', dropna=True)
        print(f"Income statement data shape: {income_stmt.shape if not income_stmt.empty else '(empty)'}")
        
        # Get cash flow data
        print(f"Fetching cash flow data...")
        cash_flow = stock.finance.cash_flow(period=period, dropna=True)
        print(f"Cash flow data shape: {cash_flow.shape if not cash_flow.empty else '(empty)'}")
        
        # Combine all statements into one DataFrame
        combined_data = pd.DataFrame()
        
        # Process each statement and add to combined data
        if not balance_sheet.empty:
            balance_sheet.index = pd.to_datetime(balance_sheet.index)
            combined_data = pd.concat([combined_data, balance_sheet], axis=1)
            
        if not income_stmt.empty:
            income_stmt.index = pd.to_datetime(income_stmt.index)
            combined_data = pd.concat([combined_data, income_stmt], axis=1)
            
        if not cash_flow.empty:
            cash_flow.index = pd.to_datetime(cash_flow.index)
            combined_data = pd.concat([combined_data, cash_flow], axis=1)
        
        if combined_data.empty:
            print(f"No financial data available for {ticker}")
            return []
            
        print(f"Combined data columns: {combined_data.columns.tolist()}")
        
        # Filter by date and limit
        combined_data = combined_data[combined_data.index <= end_date]
        if combined_data.empty:
            print(f"No financial data available for {ticker} before {end_date}")
            return []
            
        combined_data = combined_data.sort_index(ascending=False).head(limit)
        print(f"Filtered combined data shape: {combined_data.shape}")
        
        # Create LineItem objects
        result_items = []
        for idx, row in combined_data.iterrows():
            # Create a dictionary to store the line item values
            line_item_data = {
                "ticker": ticker,
                "report_period": idx.strftime('%Y-%m-%d'),
                "period": period,
                "currency": "VND"
            }
            
            # Add requested line items if available
            for item in line_items:
                if item in field_mapping:
                    # Direct mapping available
                    field = field_mapping[item]
                    value = _safe_get(row, field)
                    line_item_data[item] = value
                else:
                    # Calculate derived fields
                    if item == "free_cash_flow":
                        operating_cf = _safe_get(row, "net_cash_from_operating_activities")
                        capex = _safe_get(row, "capital_expenditure")
                        if operating_cf is not None and capex is not None:
                            line_item_data[item] = operating_cf - capex
                    elif item == "operating_margin":
                        revenue = _safe_get(row, "net_revenue")
                        operating_profit = _safe_get(row, "operating_profit")
                        if revenue is not None and operating_profit is not None and revenue != 0:
                            line_item_data[item] = operating_profit / revenue
                    elif item == "gross_margin":
                        revenue = _safe_get(row, "net_revenue")
                        gross_profit = _safe_get(row, "gross_profit")
                        if revenue is not None and gross_profit is not None and revenue != 0:
                            line_item_data[item] = gross_profit / revenue
                    elif item == "ebit":
                        operating_profit = _safe_get(row, "operating_profit")
                        if operating_profit is not None:
                            line_item_data[item] = operating_profit
                    elif item == "ebitda":
                        operating_profit = _safe_get(row, "operating_profit")
                        depreciation = _safe_get(row, "depreciation")
                        if operating_profit is not None:
                            line_item_data[item] = operating_profit + (depreciation or 0)
                    elif item == "working_capital":
                        current_assets = _safe_get(row, "current_assets")
                        current_liabilities = _safe_get(row, "current_liabilities")
                        if current_assets is not None and current_liabilities is not None:
                            line_item_data[item] = current_assets - current_liabilities
                    elif item == "total_debt":
                        short_term = _safe_get(row, "short_term_debt")
                        long_term = _safe_get(row, "long_term_debt")
                        if short_term is not None or long_term is not None:
                            line_item_data[item] = (short_term or 0) + (long_term or 0)
                    elif item == "net_debt":
                        total_debt = line_item_data.get("total_debt")
                        cash = _safe_get(row, "cash")
                        if total_debt is not None and cash is not None:
                            line_item_data[item] = total_debt - cash
            
            # Create LineItem object
            try:
                line_item = LineItem(**line_item_data)
                result_items.append(line_item)
            except Exception as e:
                print(f"Error creating LineItem for {ticker} at {idx}: {str(e)}")
                print(f"Line item data: {line_item_data}")
                continue
        
        print(f"Created {len(result_items)} LineItem objects")
        return result_items
        
    except Exception as e:
        print(f"Error fetching line items for {ticker}: {str(e)}")
        return []

def get_insider_trades(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[InsiderTrade]:
    """
    Fetch insider trades for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        end_date (str): End date in YYYY-MM-DD format
        start_date (str | None): Start date in YYYY-MM-DD format, optional
        limit (int): Maximum number of trades to return
        
    Returns:
        list[InsiderTrade]: List of insider trades
    """
    try:
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        # Get insider trades data
        df = stock.ownership.insider_trading()
        
        # Convert dates to datetime for filtering
        df['transaction_date'] = pd.to_datetime(df['transaction_date'])
        
        # Filter by date range
        df = df[df['transaction_date'] <= end_date]
        if start_date:
            df = df[df['transaction_date'] >= start_date]
            
        # Sort by date descending and limit results
        df = df.sort_values('transaction_date', ascending=False).head(limit)
        
        # Convert to InsiderTrade objects
        insider_trades = []
        for _, row in df.iterrows():
            trade = InsiderTrade(
                ticker=ticker,
                issuer=None,  # Not provided by vnstock
                name=row.get('owner_name'),
                title=row.get('position'),
                is_board_director=None,  # Not directly provided
                transaction_date=row['transaction_date'].strftime('%Y-%m-%d'),
                transaction_shares=row.get('volume_change'),
                transaction_price_per_share=row.get('price'),
                transaction_value=row.get('value'),
                shares_owned_before_transaction=row.get('volume_initial'),
                shares_owned_after_transaction=row.get('volume_final'),
                security_title=None,  # Not provided by vnstock
                filing_date=row['transaction_date'].strftime('%Y-%m-%d')  # Use transaction date as filing date
            )
            insider_trades.append(trade)
            
        return insider_trades
        
    except Exception as e:
        raise Exception(f"Error fetching insider trades for {ticker}: {str(e)}")

def get_company_news(
    ticker: str,
    end_date: str,
    start_date: str | None = None,
    limit: int = 1000,
) -> list[CompanyNews]:
    """
    Fetch company news for a given ticker using vnstock.
    
    Args:
        ticker (str): Stock symbol
        end_date (str): End date in YYYY-MM-DD format
        start_date (str | None): Start date in YYYY-MM-DD format, optional
        limit (int): Maximum number of news items to return
        
    Returns:
        list[CompanyNews]: List of company news
    """
    try:
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        # Get company news data
        df = stock.company.news()
        
        # Convert dates to datetime for filtering
        df['date'] = pd.to_datetime(df['date'])
        
        # Filter by date range
        df = df[df['date'] <= end_date]
        if start_date:
            df = df[df['date'] >= start_date]
            
        # Sort by date descending and limit results
        df = df.sort_values('date', ascending=False).head(limit)
        
        # Convert to CompanyNews objects
        news_items = []
        for _, row in df.iterrows():
            news = CompanyNews(
                ticker=ticker,
                date=row['date'].strftime('%Y-%m-%d'),
                title=row.get('title'),
                content=row.get('content'),
                source=row.get('source'),
                url=row.get('link')
            )
            news_items.append(news)
            
        return news_items
        
    except Exception as e:
        raise Exception(f"Error fetching company news for {ticker}: {str(e)}") 