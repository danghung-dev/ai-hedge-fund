
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
stock = Vnstock().stock(symbol='NVL', source='TCBS')
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

# LOG data
DIVIDENDS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1 entries, 0 to 0
Data columns (total 4 columns):
 #   Column                    Non-Null Count  Dtype  
---  ------                    --------------  -----  
 0   exercise_date             1 non-null      object 
 1   cash_year                 1 non-null      int64  
 2   cash_dividend_percentage  1 non-null      float64
 3   issue_method              1 non-null      object 
dtypes: float64(1), int64(1), object(2)
memory usage: 164.0+ bytes

SHAREHOLDERS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 10 entries, 0 to 9
Data columns (total 2 columns):
 #   Column             Non-Null Count  Dtype  
---  ------             --------------  -----  
 0   share_holder       10 non-null     object 
 1   share_own_percent  10 non-null     float64
dtypes: float64(1), object(1)
memory usage: 292.0+ bytes

OFFICERS INFO:
<class 'pandas.core.frame.DataFrame'>
Index: 20 entries, 1 to 19
Data columns (total 3 columns):
 #   Column               Non-Null Count  Dtype  
---  ------               --------------  -----  
 0   officer_name         20 non-null     object 
 1   officer_position     8 non-null      object 
 2   officer_own_percent  20 non-null     float64
dtypes: float64(1), object(2)
memory usage: 640.0+ bytes

EVENTS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 15 entries, 0 to 14
Data columns (total 14 columns):
 #   Column                 Non-Null Count  Dtype  
---  ------                 --------------  -----  
 0   rsi                    15 non-null     float64
 1   rs                     15 non-null     float64
 2   id                     15 non-null     int64  
 3   price                  13 non-null     float64
 4   price_change           13 non-null     float64
 5   price_change_ratio     13 non-null     float64
 6   price_change_ratio_1m  13 non-null     float64
 7   event_name             15 non-null     object 
 8   event_code             15 non-null     object 
 9   notify_date            15 non-null     object 
 10  exer_date              15 non-null     object 
 11  reg_final_date         15 non-null     object 
 12  exer_right_date        15 non-null     object 
 13  event_desc             15 non-null     object 
dtypes: float64(6), int64(1), object(7)
memory usage: 1.8+ KB

NEWS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 15 entries, 0 to 14
Data columns (total 10 columns):
 #   Column                 Non-Null Count  Dtype  
---  ------                 --------------  -----  
 0   rsi                    15 non-null     float64
 1   rs                     15 non-null     float64
 2   price                  13 non-null     float64
 3   price_change           13 non-null     float64
 4   price_change_ratio     13 non-null     float64
 5   price_change_ratio_1m  13 non-null     float64
 6   id                     15 non-null     int64  
 7   title                  15 non-null     object 
 8   source                 15 non-null     object 
 9   publish_date           15 non-null     object 
dtypes: float64(6), int64(1), object(3)
memory usage: 1.3+ KB

OVERVIEW INFO:
<class 'pandas.core.frame.DataFrame'>
Index: 1 entries, 0 to 0
Data columns (total 18 columns):
 #   Column             Non-Null Count  Dtype  
---  ------             --------------  -----  
 0   symbol             1 non-null      object 
 1   exchange           1 non-null      object 
 2   industry           1 non-null      object 
 3   company_type       1 non-null      object 
 4   no_shareholders    1 non-null      int64  
 5   foreign_percent    1 non-null      float64
 6   outstanding_share  1 non-null      float64
 7   issue_share        1 non-null      float64
 8   established_year   1 non-null      object 
 9   no_employees       1 non-null      int64  
 10  stock_rating       1 non-null      float64
 11  delta_in_week      1 non-null      float64
 12  delta_in_month     1 non-null      float64
 13  delta_in_year      1 non-null      float64
 14  short_name         1 non-null      object 
 15  website            1 non-null      object 
 16  industry_id        1 non-null      int64  
 17  industry_id_v2     1 non-null      object 
dtypes: float64(7), int64(3), object(8)
memory usage: 152.0+ bytes

PROFILE INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 1 entries, 0 to 0
Data columns (total 8 columns):
 #   Column               Non-Null Count  Dtype 
---  ------               --------------  ----- 
 0   symbol               1 non-null      object
 1   company_name         1 non-null      object
 2   company_profile      1 non-null      object
 3   history_dev          1 non-null      object
 4   company_promise      1 non-null      object
 5   business_risk        1 non-null      object
 6   key_developments     1 non-null      object
 7   business_strategies  1 non-null      object
dtypes: object(8)
memory usage: 196.0+ bytes

INSIDER DEALS INFO:
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 20 entries, 0 to 19
Data columns (total 6 columns):
 #   Column              Non-Null Count  Dtype         
---  ------              --------------  -----         
 0   deal_announce_date  20 non-null     datetime64[ns]
 1   deal_method         20 non-null     object        
 2   deal_action         20 non-null     object        
 3   deal_quantity       20 non-null     float64       
 4   deal_price          0 non-null      object        
 5   deal_ratio          0 non-null      object        
dtypes: datetime64[ns](1), float64(1), object(4)
memory usage: 1.1+ KB
2025-03-26 10:33:39 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports
2025-03-26 10:33:39 - vnstock.common.data.data_explorer - INFO - Tham số dropna không được hỗ trợ cho nguồn dữ liệu TCBS

RATIO INFO:
<class 'pandas.core.frame.DataFrame'>
Index: 7 entries, 2024 to 2018
Data columns (total 38 columns):
 #   Column                       Non-Null Count  Dtype  
---  ------                       --------------  -----  
 0   price_to_earning             7 non-null      float64
 1   price_to_book                7 non-null      float64
 2   value_before_ebitda          7 non-null      float64
 3   roe                          7 non-null      float64
 4   roa                          7 non-null      float64
 5   days_receivable              7 non-null      int64  
 6   days_inventory               7 non-null      int64  
 7   days_payable                 7 non-null      int64  
 8   ebit_on_interest             7 non-null      float64
 9   earning_per_share            7 non-null      int64  
 10  book_value_per_share         7 non-null      int64  
 11  equity_on_total_asset        7 non-null      float64
 12  equity_on_liability          7 non-null      float64
 13  current_payment              7 non-null      float64
 14  quick_payment                7 non-null      float64
 15  eps_change                   7 non-null      float64
 16  ebitda_on_stock              7 non-null      int64  
 17  gross_profit_margin          7 non-null      float64
 18  operating_profit_margin      5 non-null      float64
 19  post_tax_margin              6 non-null      float64
 20  debt_on_equity               7 non-null      float64
 21  debt_on_asset                7 non-null      float64
 22  debt_on_ebitda               7 non-null      float64
 23  short_on_long_debt           7 non-null      float64
 24  asset_on_equity              7 non-null      float64
 25  capital_balance              7 non-null      int64  
 26  cash_on_equity               7 non-null      float64
 27  cash_on_capitalize           7 non-null      float64
 28  cash_circulation             7 non-null      int64  
 29  revenue_on_work_capital      7 non-null      float64
 30  capex_on_fixed_asset         7 non-null      float64
 31  revenue_on_asset             7 non-null      float64
 32  post_tax_on_pre_tax          6 non-null      float64
 33  ebit_on_revenue              5 non-null      float64
 34  pre_tax_on_ebit              7 non-null      float64
 35  payable_on_equity            7 non-null      float64
 36  ebitda_on_stock_change       7 non-null      float64
 37  book_value_per_share_change  7 non-null      float64
dtypes: float64(30), int64(8)
memory usage: 2.1+ KB
2025-03-26 10:33:39 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports
2025-03-26 10:33:39 - vnstock.common.data.data_explorer - INFO - Tham số dropna không được hỗ trợ cho nguồn dữ liệu TCBS

INCOME STATEMENT INFO:
<class 'pandas.core.frame.DataFrame'>
Index: 10 entries, 2024 to 2013
Data columns (total 13 columns):
 #   Column                           Non-Null Count  Dtype  
---  ------                           --------------  -----  
 0   revenue                          10 non-null     int64  
 1   year_revenue_growth              8 non-null      float64
 2   cost_of_good_sold                10 non-null     int64  
 3   gross_profit                     10 non-null     int64  
 4   operation_expense                10 non-null     int64  
 5   operation_profit                 10 non-null     int64  
 6   year_operation_profit_growth     6 non-null      float64
 7   interest_expense                 10 non-null     int64  
 8   pre_tax_profit                   10 non-null     int64  
 9   post_tax_profit                  10 non-null     int64  
 10  share_holder_income              10 non-null     int64  
 11  year_share_holder_income_growth  7 non-null      float64
 12  ebitda                           10 non-null     int64  
dtypes: float64(3), int64(10)
memory usage: 1.1+ KB
2025-03-26 10:33:39 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports

BALANCE SHEET INFO:
<class 'pandas.core.frame.DataFrame'>
Index: 10 entries, 2024 to 2013
Data columns (total 17 columns):
 #   Column                     Non-Null Count  Dtype  
---  ------                     --------------  -----  
 0   short_asset                10 non-null     int64  
 1   cash                       10 non-null     int64  
 2   short_invest               10 non-null     int64  
 3   short_receivable           10 non-null     int64  
 4   inventory                  10 non-null     int64  
 5   long_asset                 10 non-null     int64  
 6   fixed_asset                10 non-null     int64  
 7   asset                      10 non-null     int64  
 8   debt                       10 non-null     int64  
 9   short_debt                 10 non-null     int64  
 10  long_debt                  10 non-null     int64  
 11  equity                     10 non-null     int64  
 12  capital                    10 non-null     int64  
 13  other_debt                 10 non-null     int64  
 14  un_distributed_income      7 non-null      float64
 15  minor_share_holder_profit  10 non-null     int64  
 16  payable                    10 non-null     int64  
dtypes: float64(1), int64(16)
memory usage: 1.4+ KB
2025-03-26 10:33:40 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports

CASH FLOW INFO:
<class 'pandas.core.frame.DataFrame'>
Index: 12 entries, 2024 to 2013
Data columns (total 5 columns):
 #   Column          Non-Null Count  Dtype  
---  ------          --------------  -----  
 0   invest_cost     12 non-null     int64  
 1   from_invest     12 non-null     int64  
 2   from_financial  12 non-null     int64  
 3   from_sale       12 non-null     int64  
 4   free_cash_flow  10 non-null     float64
dtypes: float64(1), int64(4)
memory usage: 576.0+ bytes


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
