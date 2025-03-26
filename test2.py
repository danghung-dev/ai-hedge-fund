from vnstock import Vnstock
import pandas as pd
# Set display options to show all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

stock = Vnstock().stock(symbol='VCI', source='VCI')
stock_head = stock.finance.ratio(period='year', lang='en', dropna=True)

# Display detailed information about the DataFrame
print("\nDetailed DataFrame Information:")
stock_head.info()

# Also print the actual data
print("\nFirst few rows of the data:")
print(stock_head.head())

# # Initialize the stock object with the desired symbol and source
# stock = Vnstock().stock(symbol='VNM', source='TCBS')
# company = stock.company

# ratio = stock.finance.ratio(period='quarter', lang='vi').head()
# ratio = stock.finance.ratio(period='year', lang='vi', dropna=True)
# print(f"ratio header: {ratio.columns}")
# # print head 5 rows
# print(ratio.head(5))

# # Get financial data
# print("Fetching financial data...")
# balance_sheet = stock.finance.balance_sheet(period='year', lang='en', dropna=True)
# income_stmt = stock.finance.income_statement(period='year', lang='en', dropna=True)
# cash_flow = stock.finance.cash_flow(period='year', lang='en', dropna=True)
# overview = company.overview()
# dividends = company.dividends()

# # Setting display options to show all data
# pd.set_option('display.max_columns', None)
# pd.set_option('display.width', 1000)

# # Print overview data with all columns
# print("\nOVERVIEW DATA:")
# print(f"Columns: {overview.columns.tolist()}")
# print(overview.to_string())

# # Print balance sheet data
# print("\nBALANCE SHEET DATA:")
# print(f"Columns: {balance_sheet.columns.tolist()}")
# print(balance_sheet.head().to_string())

# # Print income statement data
# print("\nINCOME STATEMENT DATA:")
# print(f"Columns: {income_stmt.columns.tolist()}")
# print(income_stmt.head().to_string())

# # Print cash flow data
# print("\nCASH FLOW DATA:")
# print(f"Columns: {cash_flow.columns.tolist()}")
# print(cash_flow.head().to_string())

# # Print dividend data
# print("\nDIVIDEND DATA:")
# print(f"Columns: {dividends.columns.tolist()}")
# print(dividends.to_string())

# # Field mappings based on guide
# field_mappings = {
#     "capital_expenditure": "invest_cost",  # From cash flow
#     "net_income": "share_holder_income",   # From income statement
#     "total_assets": "asset",               # From balance sheet
#     "total_liabilities": "debt",           # From balance sheet
#     "issuance_or_purchase_of_equity_shares": "from_financial",  # From cash flow
# }

# # Test calculations for the specific fields
# print("\nCALCULATED METRICS:")

# # Test outstanding_shares
# if 'outstanding_share' in overview.columns:
#     outstanding_shares = overview['outstanding_share'].iloc[0] * 1000000
#     print(f"outstanding_shares: {outstanding_shares}")
# else:
#     print("outstanding_shares field not found in overview data")

# # Test depreciation_and_amortization
# if 'ebitda' in income_stmt.columns and 'operation_profit' in income_stmt.columns:
#     depreciation = income_stmt['ebitda'] - income_stmt['operation_profit']
#     print(f"depreciation_and_amortization:\n{depreciation * 1000000}")
# else:
#     print("Required fields for depreciation_and_amortization calculation not found")

# # Test dividends_and_other_cash_distributions
# if not dividends.empty and 'cash_dividend_percentage' in dividends.columns and 'cash_year' in dividends.columns:
#     # Group by cash_year and sum dividend percentages
#     dividend_by_year = dividends.groupby('cash_year')['cash_dividend_percentage'].sum()
    
#     # Calculate actual dividend payout (assuming par value of 10,000)
#     par_value = 10000
#     dividend_payout = dividend_by_year.apply(lambda x: x * outstanding_shares * par_value)
#     print(f"dividends_and_other_cash_distributions by year:\n{dividend_payout}")
# else:
#     print("Required fields for dividends_and_other_cash_distributions calculation not found")

# # Print direct mappings for other fields
# print("\nDIRECT MAPPED FIELDS (first row of data):")
# for field, mapping in field_mappings.items():
#     # Check which dataframe contains the mapping
#     if mapping in balance_sheet.columns:
#         value = balance_sheet[mapping].iloc[0] * 1000000 if not balance_sheet.empty else None
#         print(f"{field}: {value} (from balance sheet)")
#     elif mapping in income_stmt.columns:
#         value = income_stmt[mapping].iloc[0] * 1000000 if not income_stmt.empty else None
#         print(f"{field}: {value} (from income statement)")
#     elif mapping in cash_flow.columns:
#         value = cash_flow[mapping].iloc[0] * 1000000 if not cash_flow.empty else None
#         print(f"{field}: {value} (from cash flow)")
#     else:
#         print(f"{field}: mapping '{mapping}' not found in any statement")




