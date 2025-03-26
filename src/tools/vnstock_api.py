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
        # Validate period parameter
        if period not in ['year', 'quarter']:
            period = 'year'
            
        print(f'Getting financial metrics for {ticker} from {VNSTOCK_SOURCE}')
        stock = _vnstock.stock(symbol=ticker, source=VNSTOCK_SOURCE)
        
        # Fetch all required data sources
        print(f'Fetching financial ratios...')
        ratio_df = stock.finance.ratio(period=period, lang='en', dropna=True)
        
        print(f'Fetching income statement...')
        income_stmt_df = stock.finance.income_statement(period=period, lang='en', dropna=True)
        
        print(f'Fetching balance sheet...')
        balance_sheet_df = stock.finance.balance_sheet(period=period, lang='en')
        
        print(f'Fetching cash flow...')
        cash_flow_df = stock.finance.cash_flow(period=period, lang='en')
        
        print(f'Fetching company overview...')
        overview_df = stock.company.overview()
        
        print(f'Fetching dividends...')
        dividends_df = stock.company.dividends()
        
        # Check if ratio dataframe is empty
        if ratio_df.empty:
            print(f'No financial ratios data available for {ticker}')
            return pd.DataFrame()
        
        # Convert all date indexes to datetime for consistent processing
        ratio_df.index = pd.to_datetime(ratio_df.index)
        if not income_stmt_df.empty:
            income_stmt_df.index = pd.to_datetime(income_stmt_df.index)
        if not balance_sheet_df.empty:
            balance_sheet_df.index = pd.to_datetime(balance_sheet_df.index)
        if not cash_flow_df.empty:
            cash_flow_df.index = pd.to_datetime(cash_flow_df.index)
        
        # Filter data by end_date
        ratio_df = ratio_df[ratio_df.index <= end_date]
        if not income_stmt_df.empty:
            income_stmt_df = income_stmt_df[income_stmt_df.index <= end_date]
        if not balance_sheet_df.empty:
            balance_sheet_df = balance_sheet_df[balance_sheet_df.index <= end_date]
        if not cash_flow_df.empty:
            cash_flow_df = cash_flow_df[cash_flow_df.index <= end_date]
        
        if ratio_df.empty:
            print(f'No financial ratios data available for {ticker} before {end_date}')
            return pd.DataFrame()
        
        # Sort by date descending
        ratio_df = ratio_df.sort_index(ascending=False)
        if not income_stmt_df.empty:
            income_stmt_df = income_stmt_df.sort_index(ascending=False)
        if not balance_sheet_df.empty:
            balance_sheet_df = balance_sheet_df.sort_index(ascending=False)
        if not cash_flow_df.empty:
            cash_flow_df = cash_flow_df.sort_index(ascending=False)
        
        # Apply limit
        if limit and limit > 0:
            ratio_df = ratio_df.head(limit)
        
        # Extract outstanding shares from overview for calculations
        outstanding_shares = None
        if not overview_df.empty and 'outstanding_share' in overview_df.columns:
            outstanding_shares = overview_df['outstanding_share'].iloc[0] * 1e6  # Convert from millions to units
            print(f"Outstanding shares: {outstanding_shares}")
        
        # Create financial metrics list
        financial_metrics = []
        
        # Process each period
        for idx, ratio_row in ratio_df.iterrows():
            period_date = idx.strftime('%Y-%m-%d')
            year = idx.year
            
            # Get corresponding rows from other dataframes
            income_row = income_stmt_df.loc[idx] if not income_stmt_df.empty and idx in income_stmt_df.index else pd.Series()
            balance_row = balance_sheet_df.loc[idx] if not balance_sheet_df.empty and idx in balance_sheet_df.index else pd.Series()
            cash_flow_row = cash_flow_df.loc[idx] if not cash_flow_df.empty and idx in cash_flow_df.index else pd.Series()
            
            # For debugging
            print(f"\nProcessing period: {period_date}")
            
            # Calculate market cap if stock price is available
            market_cap = None
            if pd.notna(ratio_row.get('price_to_earning')) and pd.notna(ratio_row.get('earning_per_share')) and outstanding_shares:
                market_cap = ratio_row['price_to_earning'] * (ratio_row['earning_per_share'] * outstanding_shares / 1e9)  # Bn. VND
                print(f"Calculated market cap: {market_cap} Bn VND")
            elif not overview_df.empty and 'market_cap' in overview_df.columns:
                market_cap = overview_df['market_cap'].iloc[0]
                print(f"Using market cap from overview: {market_cap}")
            
            # Calculate Enterprise Value: Market Cap + Debt - Cash
            enterprise_value = None
            if market_cap and pd.notna(balance_row.get('debt')) and pd.notna(balance_row.get('cash')):
                total_debt = balance_row['debt'] / 1e9  # Convert to Bn. VND
                cash = balance_row['cash'] / 1e9  # Convert to Bn. VND
                enterprise_value = market_cap + total_debt - cash
                print(f"Calculated enterprise value: {enterprise_value} Bn VND")
            
            # Calculate revenue for ratio calculations
            revenue = None
            if pd.notna(income_row.get('revenue')):
                revenue = income_row['revenue'] / 1e9  # Convert to Bn. VND
            
            # Calculate EBITDA for ratios
            ebitda = None
            if pd.notna(income_row.get('ebitda')):
                ebitda = income_row['ebitda'] / 1e9  # Convert to Bn. VND
            
            # Calculate Free Cash Flow for yield
            free_cash_flow = None
            if pd.notna(cash_flow_row.get('free_cash_flow')):
                free_cash_flow = cash_flow_row['free_cash_flow'] / 1e9  # Convert to Bn. VND
            
            # Calculate additional ratios
            price_to_sales_ratio = market_cap / revenue if market_cap and revenue else None
            ev_to_ebitda_ratio = enterprise_value / ebitda if enterprise_value and ebitda else None
            ev_to_revenue_ratio = enterprise_value / revenue if enterprise_value and revenue else None
            free_cash_flow_yield = free_cash_flow / market_cap if market_cap and pd.notna(free_cash_flow) else None
            
            # Calculate inventory turnover
            inventory_turnover = None
            if pd.notna(income_row.get('cost_of_good_sold')) and pd.notna(balance_row.get('inventory')):
                cogs = income_row['cost_of_good_sold'] / 1e9  # Convert to Bn. VND
                inventory = balance_row['inventory'] / 1e9  # Convert to Bn. VND
                inventory_turnover = cogs / inventory if inventory else None
            
            # Calculate receivables turnover
            receivables_turnover = None
            if revenue and pd.notna(balance_row.get('short_receivable')):
                receivables = balance_row['short_receivable'] / 1e9  # Convert to Bn. VND
                receivables_turnover = revenue / receivables if receivables else None
            
            # Calculate cash ratio
            cash_ratio = None
            if pd.notna(balance_row.get('cash')) and pd.notna(balance_row.get('short_debt')):
                cash = balance_row['cash'] / 1e9  # Convert to Bn. VND
                current_liabilities = balance_row['short_debt'] / 1e9  # Convert to Bn. VND
                cash_ratio = cash / current_liabilities if current_liabilities else None
            
            # Calculate operating cash flow ratio
            operating_cash_flow_ratio = None
            if pd.notna(cash_flow_row.get('from_sale')) and pd.notna(balance_row.get('short_debt')):
                operating_cash_flow = cash_flow_row['from_sale'] / 1e9  # Convert to Bn. VND
                current_liabilities = balance_row['short_debt'] / 1e9  # Convert to Bn. VND
                operating_cash_flow_ratio = operating_cash_flow / current_liabilities if current_liabilities else None
            
            # Calculate operating cycle
            operating_cycle = None
            if pd.notna(ratio_row.get('days_receivable')) and pd.notna(ratio_row.get('days_inventory')):
                operating_cycle = ratio_row['days_receivable'] + ratio_row['days_inventory']
            
            # Calculate payout ratio
            payout_ratio = None
            if not dividends_df.empty and pd.notna(income_row.get('post_tax_profit')):
                # Filter dividends for this year
                year_dividends = dividends_df[dividends_df['cash_year'] == year]
                if not year_dividends.empty:
                    # Sum all dividend percentages for the year
                    dividend_percentage = year_dividends['cash_dividend_percentage'].sum()
                    # Par value usually 10,000 VND
                    par_value = 10000
                    total_dividend = dividend_percentage * outstanding_shares * par_value / 1e9
                    net_income = income_row['post_tax_profit'] / 1e9  # Convert to Bn. VND
                    payout_ratio = total_dividend / net_income if net_income else None
                    print(f"Calculated payout ratio: {payout_ratio}")
            
            # Calculate free cash flow per share
            free_cash_flow_per_share = None
            if pd.notna(cash_flow_row.get('free_cash_flow')) and outstanding_shares:
                free_cash_flow_per_share = cash_flow_row['free_cash_flow'] / outstanding_shares
            
            # Create FinancialMetrics object
            metrics = FinancialMetrics(
                ticker=ticker,
                report_period=period_date,
                period=period,
                currency="VND",
                # Market & Valuation metrics
                market_cap=market_cap,
                enterprise_value=enterprise_value,
                price_to_earnings_ratio=_safe_get(ratio_row, 'price_to_earning'),
                price_to_book_ratio=_safe_get(ratio_row, 'price_to_book'),
                price_to_sales_ratio=price_to_sales_ratio,
                enterprise_value_to_ebitda_ratio=ev_to_ebitda_ratio,
                enterprise_value_to_revenue_ratio=ev_to_revenue_ratio,
                free_cash_flow_yield=free_cash_flow_yield,
                peg_ratio=None,  # Not directly available
                
                # Profitability metrics
                gross_margin=_safe_get(ratio_row, 'gross_profit_margin'),
                operating_margin=_safe_get(ratio_row, 'operating_profit_margin'),
                net_margin=_safe_get(ratio_row, 'post_tax_margin'),
                return_on_equity=_safe_get(ratio_row, 'roe'),
                return_on_assets=_safe_get(ratio_row, 'roa'),
                return_on_invested_capital=_safe_get(ratio_row, 'roic'),
                
                # Efficiency metrics
                asset_turnover=_safe_get(ratio_row, 'revenue_on_asset'),
                inventory_turnover=inventory_turnover,
                receivables_turnover=receivables_turnover,
                days_sales_outstanding=_safe_get(ratio_row, 'days_receivable'),
                operating_cycle=operating_cycle,
                working_capital_turnover=_safe_get(ratio_row, 'revenue_on_work_capital'),
                
                # Liquidity metrics
                current_ratio=_safe_get(ratio_row, 'current_payment'),
                quick_ratio=_safe_get(ratio_row, 'quick_payment'),
                cash_ratio=cash_ratio,
                operating_cash_flow_ratio=operating_cash_flow_ratio,
                
                # Solvency metrics
                debt_to_equity=_safe_get(ratio_row, 'debt_on_equity'),
                debt_to_assets=_safe_get(ratio_row, 'debt_on_asset'),
                interest_coverage=_safe_get(ratio_row, 'ebit_on_interest'),
                
                # Growth metrics
                revenue_growth=_safe_get(income_row, 'year_revenue_growth'),
                earnings_growth=_safe_get(income_row, 'year_share_holder_income_growth'),
                book_value_growth=_safe_get(ratio_row, 'book_value_per_share_change'),
                earnings_per_share_growth=_safe_get(ratio_row, 'eps_change'),
                free_cash_flow_growth=None,  # Not directly available
                operating_income_growth=_safe_get(income_row, 'year_operation_profit_growth'),
                ebitda_growth=None,  # Not directly available
                
                # Per share metrics
                payout_ratio=payout_ratio,
                earnings_per_share=_safe_get(ratio_row, 'earning_per_share') / 1000.0 if pd.notna(_safe_get(ratio_row, 'earning_per_share')) else None,  # Convert to thousands
                book_value_per_share=_safe_get(ratio_row, 'book_value_per_share') / 1000.0 if pd.notna(_safe_get(ratio_row, 'book_value_per_share')) else None,  # Convert to thousands
                free_cash_flow_per_share=free_cash_flow_per_share
            )
            financial_metrics.append(metrics)
        
        print(f"Generated {len(financial_metrics)} financial metrics entries for {ticker}")
        
        # Create response and convert to DataFrame
        response = FinancialMetricsResponse(financial_metrics=financial_metrics)
        result_df = pd.DataFrame([m.model_dump() for m in response.financial_metrics])
        
        return result_df
        
    except Exception as e:
        print(f"Error fetching financial metrics for {ticker}: {str(e)}")
        import traceback
        traceback.print_exc()
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
        
        # Fetch ratio data
        print(f"Fetching ratio data...")
        ratio_df = stock.finance.ratio(period=period, lang='en', dropna=True)
        print(f"Ratio data shape: {ratio_df.shape if not ratio_df.empty else '(empty)'}")
        
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
        
        # Extended map of line items to their vnstock field names
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
            "long_term_investments": "short_invest",
            "fixed_assets": "fixed_asset",
            "intangible_assets": None,
            "short_term_debt": "short_debt",
            "long_term_debt": "long_debt",
            
            # Income Statement
            "revenue": "revenue",
            "operating_margin": None,  # Will calculate
            "net_income": "post_tax_profit",
            "operating_income": "operation_profit",
            "gross_profit": "gross_profit",
            "interest_income": None,
            "interest_expense": "interest_expense",
            "pretax_income": "pre_tax_profit",
            "income_tax": None,
            
            # Cash Flow
            "operating_cash_flow": "from_sale",
            "investing_cash_flow": "from_invest",
            "financing_cash_flow": "from_financial",
            "free_cash_flow": "free_cash_flow",
            "capital_expenditure": "invest_cost",
            "issuance_or_purchase_of_equity_shares": "from_financial",
            
            # Ratio Data (new mappings)
            "price_to_earnings_ratio": "price_to_earning",
            "price_to_book_ratio": "price_to_book",
            "price_to_sales_ratio": "price_to_sale",
            "enterprise_value_to_ebitda_ratio": "value_before_ebitda",
            "gross_margin": "gross_profit_margin",
            "net_margin": "post_tax_margin",
            "return_on_equity": "roe",
            "return_on_assets": "roa",
            "return_on_invested_capital": "roic",
            "asset_turnover": "revenue_on_asset",
            "days_sales_outstanding": "days_receivable",
            "working_capital_turnover": "revenue_on_work_capital",
            "current_ratio": "current_payment",
            "quick_ratio": "quick_payment",
            "debt_to_equity": "debt_on_equity",
            "debt_to_assets": "debt_on_asset",
            "interest_coverage": "ebit_on_interest",
            "book_value_growth": "book_value_per_share_change",
            "earnings_per_share_growth": "eps_change",
            "earnings_per_share": "earning_per_share",
            "book_value_per_share": "book_value_per_share",
            
            # Need to be calculated
            "outstanding_shares": None,
            "depreciation_and_amortization": None,
            "dividends_and_other_cash_distributions": None
        }
        
        # Combine all statements into one DataFrame
        combined_data = pd.DataFrame()
        
        # Process each statement and add to combined data
        if not ratio_df.empty:
            ratio_df.index = pd.to_datetime(ratio_df.index)
            combined_data = pd.concat([combined_data, ratio_df], axis=1)
            
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
                            # Handle specific conversions
                            if item in ["earnings_per_share", "book_value_per_share"]:
                                # These are already in the right units in ratio_df
                                line_item_data[item] = value 
                            elif item in ["price_to_earnings_ratio", "price_to_book_ratio", "price_to_sales_ratio", 
                                          "enterprise_value_to_ebitda_ratio", "gross_margin", "net_margin", "return_on_equity",
                                          "return_on_assets", "return_on_invested_capital", "asset_turnover", 
                                          "days_sales_outstanding", "working_capital_turnover", "current_ratio", 
                                          "quick_ratio", "debt_to_equity", "debt_to_assets", "interest_coverage", 
                                          "book_value_growth", "earnings_per_share_growth"]:
                                # Ratios and percentages don't need to be converted
                                line_item_data[item] = value
                            else:
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
                        # First try to get from ratio data directly
                        op_margin = _safe_get(row, "operating_profit_margin")
                        if op_margin is not None:
                            line_item_data[item] = op_margin
                            print(f"Got operating_margin from ratio data for {ticker} at {idx}: {op_margin}%")
                        else:
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
                
                # Calculate additional derived metrics that may not be directly mapped
                if item == "free_cash_flow_per_share" and outstanding_shares > 0:
                    fcf = _safe_get(row, "free_cash_flow")
                    if fcf is not None:
                        line_item_data[item] = fcf * 1000000 / outstanding_shares
                        print(f"Calculated free_cash_flow_per_share for {ticker} at {idx}: {line_item_data[item]}")
                
                # Calculate inventory_turnover if needed
                if item == "inventory_turnover":
                    cogs = _safe_get(row, "cost_of_good_sold")
                    inventory = _safe_get(row, "inventory")
                    if cogs is not None and inventory is not None and inventory > 0:
                        line_item_data[item] = cogs / inventory
                        print(f"Calculated inventory_turnover for {ticker} at {idx}: {line_item_data[item]}")
                
                # Calculate receivables_turnover if needed
                if item == "receivables_turnover":
                    revenue = _safe_get(row, "revenue")
                    receivables = _safe_get(row, "short_receivable")
                    if revenue is not None and receivables is not None and receivables > 0:
                        line_item_data[item] = revenue / receivables
                        print(f"Calculated receivables_turnover for {ticker} at {idx}: {line_item_data[item]}")
                
                # Calculate operating_cycle if needed
                if item == "operating_cycle":
                    days_receivable = _safe_get(row, "days_receivable")
                    days_inventory = _safe_get(row, "days_inventory")
                    if days_receivable is not None and days_inventory is not None:
                        line_item_data[item] = days_receivable + days_inventory
                        print(f"Calculated operating_cycle for {ticker} at {idx}: {line_item_data[item]}")
            
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