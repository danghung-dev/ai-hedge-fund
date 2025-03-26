from vnstock import Vnstock
import json
import pandas as pd
# Function to convert an object to a dictionary, including nested objects
def object_to_dict(obj):
    if hasattr(obj, '__dict__'):
        return {key: object_to_dict(value) for key, value in obj.__dict__.items()}
    return obj

# Set display options to show all columns
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# Initialize the stock object with the desired symbol and source
stock = Vnstock().stock(symbol='NVL', source='VCI')
company = stock.company

dividends = company.dividends()
print("\nDIVIDENDS INFO:")
dividends.info()

shareholders = company.shareholders()
print("\nSHAREHOLDERS INFO:")
shareholders.info()

officers = company.officers()
print("\nOFFICERS INFO:")
officers.info()

events = company.events()
print("\nEVENTS INFO:")
events.info()

news = company.news()


# ratio_summary = company.ratio_summary()
# print("\nRATIO SUMMARY INFO:")
# ratio_summary.info()

# trading_stats = company.trading_stats()
# print("\nTRADING STATS INFO:")
# trading_stats.info()

print("\nNEWS INFO:")
news.info()

overview = company.overview()
print("\nOVERVIEW INFO:")
overview.info()

# profile = company.profile()
# print("\nPROFILE INFO:")
# profile.info()



insider_deals = company.insider_deals()
print("\nINSIDER DEALS INFO:")
insider_deals.info()

ratio = stock.finance.ratio(period='year', lang='en', dropna=True)
print("\nRATIO INFO:")
ratio.info()

income_stmt = stock.finance.income_statement(period='year', lang='en', dropna=True)
print("\nINCOME STATEMENT INFO:")
income_stmt.info()

balance_sheet = stock.finance.balance_sheet(period='year', lang='en')
print("\nBALANCE SHEET INFO:")
balance_sheet.info()

cash_flow = stock.finance.cash_flow(period='year', lang='en')
print("\nCASH FLOW INFO:")
cash_flow.info()


# Logging data
 

SHAREHOLDERS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 31 entries, 0 to 30
Data columns (total 5 columns):
 #   Column             Non-Null Count  Dtype  
---  ------             --------------  -----  
 0   id                 31 non-null     object 
 1   share_holder       31 non-null     object 
 2   quantity           31 non-null     int64  
 3   share_own_percent  31 non-null     float64
 4   update_date        31 non-null     object 
dtypes: float64(1), int64(1), object(3)
memory usage: 1.3+ KB

OFFICERS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 9 entries, 0 to 8
Data columns (total 7 columns):
 #   Column               Non-Null Count  Dtype  
---  ------               --------------  -----  
 0   id                   9 non-null      object 
 1   officer_name         9 non-null      object 
 2   officer_position     9 non-null      object 
 3   position_short_name  9 non-null      object 
 4   update_date          9 non-null      object 
 5   officer_own_percent  9 non-null      float64
 6   quantity             9 non-null      int64  
dtypes: float64(1), int64(1), object(5)
memory usage: 636.0+ bytes

EVENTS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 37 entries, 0 to 36
Data columns (total 13 columns):
 #   Column               Non-Null Count  Dtype  
---  ------               --------------  -----  
 0   id                   37 non-null     object 
 1   event_title          37 non-null     object 
 2   en__event_title      37 non-null     object 
 3   public_date          37 non-null     object 
 4   issue_date           37 non-null     object 
 5   source_url           37 non-null     object 
 6   event_list_code      37 non-null     object 
 7   ratio                35 non-null     float64
 8   value                35 non-null     float64
 9   record_date          37 non-null     object 
 10  exright_date         37 non-null     object 
 11  event_list_name      37 non-null     object 
 12  en__event_list_name  37 non-null     object 
dtypes: float64(2), object(11)
memory usage: 3.9+ KB

NEWS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 10 entries, 0 to 9
Data columns (total 18 columns):
 #   Column              Non-Null Count  Dtype  
---  ------              --------------  -----  
 0   id                  10 non-null     object 
 1   news_title          10 non-null     object 
 2   news_sub_title      10 non-null     object 
 3   friendly_sub_title  10 non-null     object 
 4   news_image_url      10 non-null     object 
 5   news_source_link    10 non-null     object 
 6   created_at          0 non-null      object 
 7   public_date         10 non-null     int64  
 8   updated_at          0 non-null      object 
 9   lang_code           10 non-null     object 
 10  news_id             10 non-null     object 
 11  news_short_content  10 non-null     object 
 12  news_full_content   10 non-null     object 
 13  close_price         9 non-null      float64
 14  ref_price           9 non-null      float64
 15  floor               9 non-null      float64
 16  ceiling             9 non-null      float64
 17  price_change_pct    9 non-null      float64
dtypes: float64(5), int64(1), object(12)
memory usage: 1.5+ KB

OVERVIEW INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1 entries, 0 to 0
Data columns (total 10 columns):
 #   Column                       Non-Null Count  Dtype 
---  ------                       --------------  ----- 
 0   symbol                       1 non-null      object
 1   id                           1 non-null      object
 2   issue_share                  1 non-null      int64 
 3   history                      1 non-null      object
 4   company_profile              1 non-null      object
 5   icb_name3                    1 non-null      object
 6   icb_name2                    1 non-null      object
 7   icb_name4                    1 non-null      object
 8   financial_ratio_issue_share  1 non-null      int64 
 9   charter_capital              1 non-null      int64 
dtypes: int64(3), object(7)
memory usage: 212.0+ bytes

RATIO INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 12 entries, 0 to 11
Data columns (total 36 columns):
 #   Column                                                       Non-Null Count  Dtype  
---  ------                                                       --------------  -----  
 0   (Meta, ticker)                                               12 non-null     object 
 1   (Meta, yearReport)                                           12 non-null     int64  
 2   (Meta, lengthReport)                                         12 non-null     int64  
 3   (Chỉ tiêu cơ cấu nguồn vốn, (ST+LT borrowings)/Equity)       12 non-null     float64
 4   (Chỉ tiêu cơ cấu nguồn vốn, Debt/Equity)                     12 non-null     float64
 5   (Chỉ tiêu cơ cấu nguồn vốn, Fixed Asset-To-Equity)           12 non-null     float64
 6   (Chỉ tiêu cơ cấu nguồn vốn, Owners' Equity/Charter Capital)  12 non-null     float64
 7   (Chỉ tiêu hiệu quả hoạt động, Asset Turnover)                12 non-null     float64
 8   (Chỉ tiêu hiệu quả hoạt động, Fixed Asset Turnover)          12 non-null     float64
 9   (Chỉ tiêu hiệu quả hoạt động, Days Sales Outstanding)        12 non-null     float64
 10  (Chỉ tiêu hiệu quả hoạt động, Days Inventory Outstanding)    12 non-null     float64
 11  (Chỉ tiêu hiệu quả hoạt động, Days Payable Outstanding)      12 non-null     float64
 12  (Chỉ tiêu hiệu quả hoạt động, Cash Cycle)                    12 non-null     float64
 13  (Chỉ tiêu hiệu quả hoạt động, Inventory Turnover)            12 non-null     float64
 14  (Chỉ tiêu khả năng sinh lợi, EBIT Margin (%))                12 non-null     float64
 15  (Chỉ tiêu khả năng sinh lợi, Gross Profit Margin (%))        12 non-null     float64
 16  (Chỉ tiêu khả năng sinh lợi, Net Profit Margin (%))          12 non-null     float64
 17  (Chỉ tiêu khả năng sinh lợi, ROE (%))                        12 non-null     float64
 18  (Chỉ tiêu khả năng sinh lợi, ROIC (%))                       12 non-null     float64
 19  (Chỉ tiêu khả năng sinh lợi, ROA (%))                        12 non-null     float64
 20  (Chỉ tiêu khả năng sinh lợi, EBITDA (Bn. VND))               12 non-null     int64  
 21  (Chỉ tiêu khả năng sinh lợi, EBIT (Bn. VND))                 12 non-null     int64  
 22  (Chỉ tiêu thanh khoản, Current Ratio)                        12 non-null     float64
 23  (Chỉ tiêu thanh khoản, Cash Ratio)                           12 non-null     float64
 24  (Chỉ tiêu thanh khoản, Quick Ratio)                          12 non-null     float64
 25  (Chỉ tiêu thanh khoản, Interest Coverage)                    12 non-null     float64
 26  (Chỉ tiêu thanh khoản, Financial Leverage)                   12 non-null     float64
 27  (Chỉ tiêu định giá, Market Capital (Bn. VND))                12 non-null     int64  
 28  (Chỉ tiêu định giá, Outstanding Share (Mil. Shares))         12 non-null     int64  
 29  (Chỉ tiêu định giá, P/E)                                     12 non-null     float64
 30  (Chỉ tiêu định giá, P/B)                                     12 non-null     float64
 31  (Chỉ tiêu định giá, P/S)                                     12 non-null     float64
 32  (Chỉ tiêu định giá, P/Cash Flow)                             12 non-null     float64
 33  (Chỉ tiêu định giá, EPS (VND))                               12 non-null     float64
 34  (Chỉ tiêu định giá, BVPS (VND))                              12 non-null     float64
 35  (Chỉ tiêu định giá, EV/EBITDA)                               12 non-null     float64
dtypes: float64(29), int64(6), object(1)
memory usage: 3.5+ KB

INCOME STATEMENT INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 12 entries, 0 to 11
Data columns (total 28 columns):
 #   Column                                 Non-Null Count  Dtype  
---  ------                                 --------------  -----  
 0   ticker                                 12 non-null     object 
 1   yearReport                             12 non-null     int64  
 2   Revenue YoY (%)                        12 non-null     float64
 3   Revenue (Bn. VND)                      12 non-null     int64  
 4   Attribute to parent company (Bn. VND)  12 non-null     int64  
 5   Attribute to parent company YoY (%)    12 non-null     float64
 6   Financial Income                       12 non-null     int64  
 7   Interest Expenses                      12 non-null     int64  
 8   Sales                                  12 non-null     int64  
 9   Sales deductions                       12 non-null     int64  
 10  Net Sales                              12 non-null     int64  
 11  Cost of Sales                          12 non-null     int64  
 12  Gross Profit                           12 non-null     int64  
 13  Financial Expenses                     12 non-null     int64  
 14  Gain/(loss) from joint ventures        12 non-null     int64  
 15  Selling Expenses                       12 non-null     int64  
 16  General & Admin Expenses               12 non-null     int64  
 17  Operating Profit/Loss                  12 non-null     int64  
 18  Other income                           12 non-null     int64  
 19  Net income from associated companies   12 non-null     int64  
 20  Other Income/Expenses                  12 non-null     int64  
 21  Net other income/expenses              12 non-null     int64  
 22  Profit before tax                      12 non-null     int64  
 23  Business income tax - current          12 non-null     int64  
 24  Business income tax - deferred         12 non-null     int64  
 25  Net Profit For the Year                12 non-null     int64  
 26  Minority Interest                      12 non-null     int64  
 27  Attributable to parent company         12 non-null     int64  
dtypes: float64(2), int64(25), object(1)
memory usage: 2.8+ KB

BALANCE SHEET INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 12 entries, 0 to 11
Data columns (total 38 columns):
 #   Column                                  Non-Null Count  Dtype 
---  ------                                  --------------  ----- 
 0   ticker                                  12 non-null     object
 1   yearReport                              12 non-null     int64 
 2   CURRENT ASSETS (Bn. VND)                12 non-null     int64 
 3   Cash and cash equivalents (Bn. VND)     12 non-null     int64 
 4   Short-term investments (Bn. VND)        12 non-null     int64 
 5   Accounts receivable (Bn. VND)           12 non-null     int64 
 6   Net Inventories                         12 non-null     int64 
 7   Other current assets                    12 non-null     int64 
 8   LONG-TERM ASSETS (Bn. VND)              12 non-null     int64 
 9   Long-term loans receivables (Bn. VND)   12 non-null     int64 
 10  Fixed assets (Bn. VND)                  12 non-null     int64 
 11  Investment in properties                12 non-null     int64 
 12  Long-term investments (Bn. VND)         12 non-null     int64 
 13  Goodwill                                12 non-null     int64 
 14  Other non-current assets                12 non-null     int64 
 15  TOTAL ASSETS (Bn. VND)                  12 non-null     int64 
 16  LIABILITIES (Bn. VND)                   12 non-null     int64 
 17  Current liabilities (Bn. VND)           12 non-null     int64 
 18  Long-term liabilities (Bn. VND)         12 non-null     int64 
 19  OWNER'S EQUITY(Bn.VND)                  12 non-null     int64 
 20  Capital and reserves (Bn. VND)          12 non-null     int64 
 21  Undistributed earnings (Bn. VND)        12 non-null     int64 
 22  MINORITY INTERESTS                      12 non-null     int64 
 23  TOTAL RESOURCES (Bn. VND)               12 non-null     int64 
 24  Prepayments to suppliers (Bn. VND)      12 non-null     int64 
 25  Short-term loans receivables (Bn. VND)  12 non-null     int64 
 26  Inventories, Net (Bn. VND)              12 non-null     int64 
 27  Other current assets (Bn. VND)          12 non-null     int64 
 28  Common shares (Bn. VND)                 12 non-null     int64 
 29  Paid-in capital (Bn. VND)               12 non-null     int64 
 30  Long-term borrowings (Bn. VND)          12 non-null     int64 
 31  Advances from customers (Bn. VND)       12 non-null     int64 
 32  Short-term borrowings (Bn. VND)         12 non-null     int64 
 33  Good will (Bn. VND)                     12 non-null     int64 
 34  Long-term prepayments (Bn. VND)         12 non-null     int64 
 35  Other long-term assets (Bn. VND)        12 non-null     int64 
 36  Other long-term receivables (Bn. VND)   12 non-null     int64 
 37  Long-term trade receivables (Bn. VND)   12 non-null     int64 
dtypes: int64(37), object(1)
memory usage: 3.7+ KB

CASH FLOW INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 12 entries, 0 to 11
Data columns (total 36 columns):
 #   Column                                                                   Non-Null Count  Dtype 
---  ------                                                                   --------------  ----- 
 0   ticker                                                                   12 non-null     object
 1   yearReport                                                               12 non-null     int64 
 2   Net Profit/Loss before tax                                               12 non-null     int64 
 3   Depreciation and Amortisation                                            12 non-null     int64 
 4   Provision for credit losses                                              12 non-null     int64 
 5   Unrealized foreign exchange gain/loss                                    12 non-null     int64 
 6   Profit/Loss from investing activities                                    12 non-null     int64 
 7   Interest Expense                                                         12 non-null     int64 
 8   Operating profit before changes in working capital                       12 non-null     int64 
 9   Increase/Decrease in receivables                                         12 non-null     int64 
 10  Increase/Decrease in inventories                                         12 non-null     int64 
 11  Increase/Decrease in payables                                            12 non-null     int64 
 12  Increase/Decrease in prepaid expenses                                    12 non-null     int64 
 13  Interest paid                                                            12 non-null     int64 
 14  Business Income Tax paid                                                 12 non-null     int64 
 15  Other receipts from operating activities                                 12 non-null     int64 
 16  Other payments on operating activities                                   12 non-null     int64 
 17  Net cash inflows/outflows from operating activities                      12 non-null     int64 
 18  Purchase of fixed assets                                                 12 non-null     int64 
 19  Proceeds from disposal of fixed assets                                   12 non-null     int64 
 20  Loans granted, purchases of debt instruments (Bn. VND)                   12 non-null     int64 
 21  Collection of loans, proceeds from sales of debts instruments (Bn. VND)  12 non-null     int64 
 22  Investment in other entities                                             12 non-null     int64 
 23  Proceeds from divestment in other entities                               12 non-null     int64 
 24  Gain on Dividend                                                         12 non-null     int64 
 25  Net Cash Flows from Investing Activities                                 12 non-null     int64 
 26  Increase in charter captial                                              12 non-null     int64 
 27  Payments for share repurchases                                           12 non-null     int64 
 28  Proceeds from borrowings                                                 12 non-null     int64 
 29  Repayment of borrowings                                                  12 non-null     int64 
 30  Dividends paid                                                           12 non-null     int64 
 31  Cash flows from financial activities                                     12 non-null     int64 
 32  Net increase/decrease in cash and cash equivalents                       12 non-null     int64 
 33  Cash and cash equivalents                                                12 non-null     int64 
 34  Foreign exchange differences Adjustment                                  12 non-null     int64 
 35  Cash and Cash Equivalents at the end of period                           12 non-null     int64 
dtypes: int64(35), object(1)
memory usage: 3.5+ KB




# Giả sử các DataFrame đã được tải từ vnstock như trong truy vấn của bạn
# cash_flow, income_stmt, balance_sheet, overview, dividends

# 1. capital_expenditure
capital_expenditure = cash_flow['invest_cost']

# 2. depreciation_and_amortization
depreciation_and_amortization = income_stmt['ebitda'] - income_stmt['operation_profit']

# 3. net_income
net_income = income_stmt['share_holder_income']

# 4. outstanding_shares
outstanding_shares = overview['outstanding_share']

# 5. total_assets
total_assets = balance_sheet['asset']

# 6. total_liabilities
total_liabilities = balance_sheet['debt']


# 8. issuance_or_purchase_of_equity_shares
issuance_or_purchase_of_equity_shares = cash_flow['from_financial']

# In kết quả (ví dụ)
print("capital_expenditure:", capital_expenditure)
print("depreciation_and_amortization:", depreciation_and_amortization)
print("net_income:", net_income)
print("outstanding_shares:", outstanding_shares)
print("total_assets:", total_assets)
print("total_liabilities:", total_liabilities)
print("issuance_or_purchase_of_equity_shares:", issuance_or_purchase_of_equity_shares)


from vnstock import Vnstock
import pandas as pd

# Khởi tạo stock object cho SAB
stock = Vnstock().stock(symbol='SAB', source='TCBS')
company = stock.company

# Lấy dữ liệu dividends
dividends = company.dividends()

# Lấy outstanding_shares từ overview của SAB
overview = company.overview()
outstanding_shares = overview['outstanding_share'].iloc[0]  # Lấy giá trị đầu tiên (triệu cổ phiếu)

# Chuyển đổi outstanding_shares từ triệu thành số thực tế (vì dữ liệu thường ở đơn vị triệu)
outstanding_shares = outstanding_shares * 1_000_000  # Chuyển sang đơn vị cổ phiếu

# Giả định mệnh giá cổ phiếu là 10,000 VND
par_value = 10000  # VND

# Tính tổng tỷ lệ cổ tức theo cash_year
dividends_by_year = dividends.groupby('cash_year')['cash_dividend_percentage'].sum().reset_index()

# Tính dividends_and_other_cash_distributions (đơn vị: VND)
dividends_by_year['dividends_and_other_cash_distributions'] = (
    dividends_by_year['cash_dividend_percentage'] * outstanding_shares * par_value
)

# Đổi tên cột cho rõ ràng
dividends_by_year.columns = ['year', 'total_dividend_percentage', 'dividends_and_other_cash_distributions']
