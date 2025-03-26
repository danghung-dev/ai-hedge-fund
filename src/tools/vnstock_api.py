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
        
        # Check if the dataframe is empty or None
        if df is None or df.empty:
            print(f"No price data found for {ticker} between {start_date} and {end_date}")
            return pd.DataFrame()
        
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
        print(f"Price data for {ticker} shape: {df.shape}")
        print(f"Price data for {ticker} columns: {df.columns}")
        
        # Show full price data
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
            print(f"Full price data for {ticker}:\n{df}")
            
        return df
        
    except Exception as e:
        print(f"Error fetching price data for {ticker}: {str(e)}")
        return pd.DataFrame()

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
        # if period is not year or quarter, set it to year
        if period not in ['year', 'quarter']:
            period = 'year'
            
        print(f'Getting financial metrics for {ticker} from {VNSTOCK_SOURCE}')
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        # Get financial ratios
        print(f'Fetching financial ratios...')
        df = stock.finance.ratio(period=period, lang='en', dropna=True)
        print(f'Raw financial ratios data shape: {df.shape}')
        
        if df.empty:
            print(f'No financial ratios data available for {ticker}')
            return pd.DataFrame()
            
        # Get income statement for additional metrics
        try:
            income_stmt = stock.finance.income_statement(period=period, lang='en', dropna=True)
            print(f"Income statement header: {income_stmt.columns}")
            
            if not income_stmt.empty:
                # Using pd.option_context to show complete data
                with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                    print(f"Income statement data:\n{income_stmt.head().to_string()}")
                
                # Map income statement metrics to our model fields
                # Updated based on the guide and our search_line_items function
                income_mapping = {
                    'revenue': 'revenue', 
                    'share_holder_income': 'net_income',  # Updated based on guide
                    'operation_profit': 'operating_income',
                    'gross_profit': 'gross_profit',
                    'year_revenue_growth': 'revenue_growth',
                    'year_operation_profit_growth': 'operating_income_growth',
                    'year_share_holder_income_growth': 'earnings_growth',  # Updated based on guide
                    'ebitda': 'ebitda'
                }
                
                # Add income statement metrics to df
                for income_col, metric_field in income_mapping.items():
                    if income_col in income_stmt.columns and metric_field:
                        df[metric_field] = income_stmt[income_col]
                
                # Calculate operating margin as (operation_profit / revenue) * 100
                if 'operation_profit' in income_stmt.columns and 'revenue' in income_stmt.columns:
                    df['operating_margin'] = (income_stmt['operation_profit'] / income_stmt['revenue']) * 100
                    print(f"Calculated operating_margin for {ticker}")
                
                # Calculate depreciation_and_amortization
                if 'ebitda' in income_stmt.columns and 'operation_profit' in income_stmt.columns:
                    df['depreciation_and_amortization'] = income_stmt['ebitda'] - income_stmt['operation_profit']
                    print(f"Calculated depreciation_and_amortization for {ticker}")
        except Exception as e:
            print(f'Error fetching income statement data: {str(e)}')
            
        # Get cash flow statement for capex and other metrics
        try:
            cash_flow = stock.finance.cash_flow(period=period, lang='en', dropna=True)
            print(f"Cash flow header: {cash_flow.columns}")
            
            if not cash_flow.empty:
                # Using pd.option_context to show complete data
                with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                    print(f"Cash flow data:\n{cash_flow.head().to_string()}")
                
                # Map cash flow metrics to our model fields
                cash_flow_mapping = {
                    'invest_cost': 'capital_expenditure',  # From guide
                    'from_financial': 'issuance_or_purchase_of_equity_shares',  # From guide
                    'from_sale': 'operating_cash_flow',
                    'from_invest': 'investing_cash_flow',
                    'free_cash_flow': 'free_cash_flow'
                }
                
                # Add cash flow metrics to df
                for cf_col, metric_field in cash_flow_mapping.items():
                    if cf_col in cash_flow.columns and metric_field:
                        df[metric_field] = cash_flow[cf_col]
        except Exception as e:
            print(f'Error fetching cash flow data: {str(e)}')
            
        # Get balance sheet for asset and liability data
        try:
            balance_sheet = stock.finance.balance_sheet(period=period, lang='en', dropna=True)
            print(f"Balance sheet header: {balance_sheet.columns}")
            
            if not balance_sheet.empty:
                # Using pd.option_context to show complete data
                with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                    print(f"Balance sheet data:\n{balance_sheet.head().to_string()}")
                
                # Map balance sheet metrics to our model fields
                balance_sheet_mapping = {
                    'current_asset': 'short_asset',
                    'asset': 'total_assets',  # From guide
                    'debt': 'total_liabilities',  # From guide
                    'equity': 'shareholders_equity',
                    'cash': 'cash_and_equivalents',
                    'inventory': 'inventory',
                    'short_asset': 'current_assets',
                    'short_debt': 'current_liabilities'
                }
                
                # Add balance sheet metrics to df
                for bs_col, metric_field in balance_sheet_mapping.items():
                    if bs_col in balance_sheet.columns and metric_field:
                        df[metric_field] = balance_sheet[bs_col]
        except Exception as e:
            print(f'Error fetching balance sheet data: {str(e)}')
            
        # Get dividends data
        try:
            dividend_df = stock.company.dividends()
            if not dividend_df.empty:
                # Using pd.option_context to show complete data
                with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                    print(f"Dividend data:\n{dividend_df.to_string()}")
                
                # Get outstanding shares from company overview
                overview = stock.company.overview()
                outstanding_shares = overview['outstanding_share'].iloc[0] * 1000000 if 'outstanding_share' in overview.columns else 0
                
                if outstanding_shares > 0:
                    # Add outstanding_shares to df for all periods
                    df['outstanding_shares'] = outstanding_shares
                    print(f"Added outstanding_shares: {outstanding_shares}")
                    
                    # Calculate dividend payout for each year
                    if 'cash_dividend_percentage' in dividend_df.columns and 'cash_year' in dividend_df.columns:
                        # Group by cash_year and sum dividend percentages
                        par_value = 10000  # VND
                        dividend_by_year = dividend_df.groupby('cash_year')['cash_dividend_percentage'].sum()
                        
                        # Calculate actual dividend payout
                        dividend_payout = dividend_by_year.apply(lambda x: x * outstanding_shares * par_value)
                        print(f"Calculated dividend payouts by year: {dividend_payout}")
                        
                        # Add to df by matching years
                        for year, dividend_value in dividend_payout.items():
                            # Match with df index years and set dividends_and_other_cash_distributions
                            year_mask = df.index.year == year
                            if any(year_mask):
                                df.loc[year_mask, 'dividends_and_other_cash_distributions'] = -dividend_value  # Negative for cash outflow
                                print(f"Added dividend for year {year}: {-dividend_value}")
        except Exception as e:
            print(f'Error fetching dividend data: {str(e)}')
            
        # Get market cap from company overview if not already done
        try:
            if 'outstanding_shares' not in df.columns:
                overview = stock.company.overview()
                # Display all overview data
                with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                    print(f"Company overview data:\n{overview.to_string()}")
                
                if not overview.empty and 'market_cap' in overview.columns:
                    df['market_cap'] = overview['market_cap'].iloc[0]
                    print(f"Added market_cap: {overview['market_cap'].iloc[0]}")
                
                if not overview.empty and 'outstanding_share' in overview.columns:
                    df['outstanding_shares'] = overview['outstanding_share'].iloc[0] * 1000000
                    print(f"Added outstanding_shares: {overview['outstanding_share'].iloc[0] * 1000000}")
        except Exception as e:
            print(f'Error fetching company overview data: {str(e)}')
        
        # Filter by date and limit
        df.index = pd.to_datetime(df.index)
        df = df[df.index <= end_date]
        if df.empty:
            print(f'No financial ratios data available for {ticker} before {end_date}')
            return pd.DataFrame()
            
        # Sort by date descending
        df = df.sort_index(ascending=False)
        
        # Show all data before limiting
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
            print(f'Full financial metrics data:\n{df.to_string()}')
            
        # Apply limit if specified
        if limit and limit > 0:
            df = df.head(limit)
            
        print(f'Filtered financial metrics data shape: {df.shape}')
        
        # Convert DataFrame rows to FinancialMetrics objects
        financial_metrics = []
        for idx, row in df.iterrows():
            try:
                metrics = FinancialMetrics(
                    ticker=ticker,
                    report_period=idx.strftime('%Y-%m-%d'),
                    period=period,
                    currency='VND',
                    # Map fields to FinancialMetrics with proper error handling
                    # Direct mappings from the updated field names
                    market_cap=_safe_get(row, 'market_cap'),
                    total_assets=_safe_get(row, 'total_assets'),
                    total_liabilities=_safe_get(row, 'total_liabilities'),
                    shareholders_equity=_safe_get(row, 'shareholders_equity'),
                    cash_and_equivalents=_safe_get(row, 'cash_and_equivalents'),
                    inventory=_safe_get(row, 'inventory'),
                    current_assets=_safe_get(row, 'current_assets'),
                    current_liabilities=_safe_get(row, 'current_liabilities'),
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
                    operating_margin=_safe_get(row, 'operating_margin'),
                    gross_margin=_safe_get(row, 'gross_profit_margin'),
                    net_margin=_safe_get(row, 'post_tax_margin'),
                    revenue_growth=_safe_get(row, 'revenue_growth'),
                    earnings_growth=_safe_get(row, 'earnings_growth'),
                    operating_income_growth=_safe_get(row, 'operating_income_growth'),
                    capital_expenditure=_safe_get(row, 'capital_expenditure'),
                    depreciation_and_amortization=_safe_get(row, 'depreciation_and_amortization'),
                    net_income=_safe_get(row, 'net_income'),
                    outstanding_shares=_safe_get(row, 'outstanding_shares'),
                    dividends_and_other_cash_distributions=_safe_get(row, 'dividends_and_other_cash_distributions'),
                    issuance_or_purchase_of_equity_shares=_safe_get(row, 'issuance_or_purchase_of_equity_shares'),
                    operating_cash_flow=_safe_get(row, 'operating_cash_flow'),
                    investing_cash_flow=_safe_get(row, 'investing_cash_flow'),
                    free_cash_flow=_safe_get(row, 'free_cash_flow'),
                    # Additional metrics to keep existing functionality
                    price_to_sales_ratio=_safe_get(row, 'price_to_sale'),
                    return_on_invested_capital=_safe_get(row, 'roic'),
                    asset_turnover=_safe_get(row, 'asset_turnover'),
                    inventory_turnover=_safe_get(row, 'inventory_turnover'),
                    receivables_turnover=_safe_get(row, 'receivable_turnover'),
                    days_sales_outstanding=_safe_get(row, 'days_receivable'),
                    cash_ratio=_safe_get(row, 'cash_ratio'),
                    interest_coverage=_safe_get(row, 'ebit_on_interest'),
                    book_value_growth=_safe_get(row, 'book_value_per_share_change'),
                    earnings_per_share_growth=_safe_get(row, 'eps_change'),
                    # Fields not directly available in vnstock
                    enterprise_value=None,
                    enterprise_value_to_revenue_ratio=None,
                    free_cash_flow_yield=None,
                    peg_ratio=None,
                    operating_cycle=None,
                    working_capital_turnover=None,
                    operating_cash_flow_ratio=None,
                    free_cash_flow_growth=None,
                    ebitda_growth=None,
                    payout_ratio=_safe_get(row, 'dividend_yield'),
                    free_cash_flow_per_share=None
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
        # if period is not year or quarter, set it to year
        if period not in ['year', 'quarter']:
            period = 'year'        
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        if statement_type == "balance_sheet":
            df = stock.finance.balance_sheet(period=period, lang='en', dropna=True)
        elif statement_type == "income_statement":
            df = stock.finance.income_statement(period=period, lang='en', dropna=True)
        elif statement_type == "cash_flow":
            df = stock.finance.cash_flow(period=period, dropna=True)
        else:
            raise ValueError(f"Invalid statement type: {statement_type}")
        
        # Display the full statement data
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
            print(f"Full {statement_type} data for {ticker}:\n{df}")
            
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
        # if period is not year or quarter, set it to year
        if period not in ['year', 'quarter']:
            period = 'year'        
        print(f"Fetching financial data for {ticker} from {VNSTOCK_SOURCE}")
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
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
        cash_flow = stock.finance.cash_flow(period=period, lang='en', dropna=True)
        print(f"Cash flow data shape: {cash_flow.shape if not cash_flow.empty else '(empty)'}")
        
        # Get company overview for share data
        print(f"Fetching company overview...")
        overview = stock.company.overview()
        
        # Using pd.option_context to display all overview data without truncation
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
            print(f"Company overview data:\n{overview.to_string()}")
            
        # Get outstanding shares (convert from millions to actual shares)
        outstanding_shares = overview['outstanding_share'].iloc[0] * 1000000 if 'outstanding_share' in overview.columns else 0
        
        # Get dividend data if needed
        dividend_data = None
        if "dividends_and_other_cash_distributions" in line_items:
            try:
                print(f"Fetching dividend data...")
                dividend_df = stock.company.dividends()
                if not dividend_df.empty:
                    # Display full dividend data
                    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
                        print(f"Dividend data:\n{dividend_df.to_string()}")
                        
                    # Assuming par value is 10,000 VND (as shown in the guide)
                    par_value = 10000  # VND
                    
                    # Group by cash_year and sum cash_dividend_percentage
                    dividend_data = dividend_df.groupby('cash_year')['cash_dividend_percentage'].sum()
                    # Calculate total dividend payout per year
                    dividend_data = dividend_data.apply(lambda x: x * outstanding_shares * par_value)
                    print(f"Calculated dividend payouts by year: {dividend_data}")
            except Exception as e:
                print(f"Error fetching dividend data: {str(e)}")
        
        # Map line items to their vnstock field names according to the guide
        field_mapping = {
            # Balance Sheet
            "current_assets": "short_asset",
            "current_liabilities": "short_debt",
            "total_assets": "asset",
            "total_liabilities": "debt",
            "shareholders_equity": "equity",
            "cash_and_equivalents": "cash",
            "accounts_receivable": "short_receivable",
            "inventory": "inventory",
            "long_term_investments": "short_invest",  # May be incorrect but closest field
            "fixed_assets": "fixed_asset",
            "intangible_assets": None,  # Not directly available in the headers
            "short_term_debt": "short_debt",
            "long_term_debt": "long_debt",
            
            # Income Statement
            "revenue": "revenue",
            "operating_margin": None,  # Will calculate as (operation_profit / revenue) * 100
            "net_income": "post_tax_profit",
            "earnings_per_share": "earning_per_share",  # This is in the ratio data
            "operating_income": "operation_profit",
            "gross_profit": "gross_profit",
            "interest_income": None,  # Not directly available in the headers
            "interest_expense": "interest_expense",
            "pretax_income": "pre_tax_profit",
            "income_tax": None,  # Not directly available in the headers
            "net_income": "share_holder_income",  # Updated based on guide
            
            # Cash Flow
            "operating_cash_flow": "from_sale",
            "investing_cash_flow": "from_invest",
            "financing_cash_flow": "from_financial",
            'free_cash_flow': 'free_cash_flow',
            "capital_expenditure": "invest_cost",  # Negative value represents outflow
            "issuance_or_purchase_of_equity_shares": "from_financial",
            
            # Need to be calculated
            "outstanding_shares": None,  # Will get from overview directly
            "depreciation_and_amortization": None,  # Will calculate from ebitda - operation_profit
            "dividends_and_other_cash_distributions": None  # Will calculate from dividend data
        }
        
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
            
        # Sort by date descending and get all rows
        combined_data = combined_data.sort_index(ascending=False)
        if limit and limit > 0:
            combined_data = combined_data.head(limit)
        
        # Set pandas display options to show all data
        with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', 1000):
            print(f"Filtered combined data:\n{combined_data.to_string()}")
        
        # Create LineItem objects
        result_items = []
        for idx, row in combined_data.iterrows():
            year = idx.year
            
            # Create a dictionary to store the line item values
            line_item_data = {
                "ticker": ticker,
                "report_period": idx.strftime('%Y-%m-%d'),
                "period": period,
                "currency": "VND",
                "outstanding_shares": outstanding_shares
            }
            
            # Add requested line items if available
            for item in line_items:
                if item in field_mapping:
                    field = field_mapping[item]
                    
                    if field is not None:
                        # Direct mapping available
                        value = _safe_get(row, field)
                        if value is not None:
                            # Convert to actual value (not in millions)
                            line_item_data[item] = value * 1000000
                            
                    elif item == "outstanding_shares":
                        # Already set from overview data
                        line_item_data[item] = outstanding_shares
                        
                    elif item == "depreciation_and_amortization":
                        # Calculate as ebitda - operation_profit
                        ebitda = _safe_get(row, "ebitda")
                        operation_profit = _safe_get(row, "operation_profit")
                        if ebitda is not None and operation_profit is not None:
                            depreciation = (ebitda - operation_profit) * 1000000
                            line_item_data[item] = depreciation
                            print(f"Calculated depreciation_and_amortization for {ticker} at {idx}: {depreciation}")
                            
                    elif item == "operating_margin":
                        # Calculate operating_margin as (operation_profit / revenue) * 100
                        operation_profit = _safe_get(row, "operation_profit")
                        revenue = _safe_get(row, "revenue")
                        if operation_profit is not None and revenue is not None and revenue != 0:
                            operating_margin = (operation_profit / revenue) * 100
                            line_item_data[item] = operating_margin
                            print(f"Calculated operating_margin for {ticker} at {idx}: {operating_margin}%")
                            
                    elif item == "dividends_and_other_cash_distributions" and dividend_data is not None:
                        # Get dividend value for this year
                        if year in dividend_data.index:
                            # Negative value represents cash outflow
                            dividend_value = -dividend_data[year]
                            line_item_data[item] = dividend_value
                            print(f"Calculated dividends for {ticker} at {idx}: {dividend_value}")
                        else:
                            line_item_data[item] = 0
                            print(f"No dividend data for {ticker} at {idx} (year {year})")
            
            # Create LineItem object
            try:
                line_item = LineItem(**line_item_data)
                result_items.append(line_item)
            except Exception as e:
                print(f"Error creating LineItem for {ticker} at {idx}: {str(e)}")
                print(f"Line item data: {line_item_data}")
                continue
        
        print(f"Created {len(result_items)} LineItem objects")
        
        # Show complete output of the first few line items
        if result_items:
            print(f"Sample of line items:")
            for i, item in enumerate(result_items[:3]):  # Show first 3 items
                print(f"Item {i+1}:")
                item_dict = item.model_dump()
                for k, v in item_dict.items():
                    print(f"  {k}: {v}")
        
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