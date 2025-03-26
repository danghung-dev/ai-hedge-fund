from vnstock import Vnstock
import json
import pandas as pd
# Function to convert an object to a dictionary, including nested objects
def object_to_dict(obj):
    if hasattr(obj, '__dict__'):
        return {key: object_to_dict(value) for key, value in obj.__dict__.items()}
    return obj

# Initialize the stock object with the desired symbol and source
stock = Vnstock().stock(symbol='NVL', source='TCBS')
company = stock.company
shareholders = company.shareholders()
print(f"shareholders header: {shareholders.columns}")
officers = company.officers()
print(f"officers header: {officers.columns}")

events = company.events()
print(f"events header: {events.columns}")

news = company.news()
print(f"news header: {news.columns}")


overview = company.overview()
print(f"overview header: {overview.columns}")
profile = company.profile()
print(f"profile header: {profile.columns}")
dividends = company.dividends()
print(f"dividends header: {dividends.columns}")
insider_deals = company.insider_deals()
print(f"insider_deals header: {insider_deals.columns}")


ratio = stock.finance.ratio(period='year', lang='en', dropna=True)
print(f"ratio header: {ratio.columns}")
income_stmt = stock.finance.income_statement(period='year', lang='en', dropna=True)
print(f"Income statement header: {income_stmt.columns}")

balance_sheet = stock.finance.balance_sheet(period='year', lang='vi')
print(f"balance_sheet header: {balance_sheet.columns}")

cash_flow = stock.finance.cash_flow(period='year', lang='vi')
print(f"cash_flow header: {cash_flow.columns}")




shareholders header: Index(['share_holder', 'share_own_percent'], dtype='object')
officers header: Index(['officer_name', 'officer_position', 'officer_own_percent'], dtype='object')
events header: Index(['rsi', 'rs', 'id', 'price', 'price_change', 'price_change_ratio',
       'price_change_ratio_1m', 'event_name', 'event_code', 'notify_date',
       'exer_date', 'reg_final_date', 'exer_right_date', 'event_desc'],
      dtype='object')
news header: Index(['rsi', 'rs', 'price', 'price_change', 'price_change_ratio',
       'price_change_ratio_1m', 'id', 'title', 'source', 'publish_date'],
      dtype='object')
overview header: Index(['symbol', 'exchange', 'industry', 'company_type', 'no_shareholders',
       'foreign_percent', 'outstanding_share', 'issue_share',
       'established_year', 'no_employees', 'stock_rating', 'delta_in_week',
       'delta_in_month', 'delta_in_year', 'short_name', 'website',
       'industry_id', 'industry_id_v2'],
      dtype='object')
profile header: Index(['symbol', 'company_name', 'company_profile', 'history_dev',
       'company_promise', 'business_risk', 'key_developments',
       'business_strategies'],
      dtype='object')
dividends header: Index(['exercise_date', 'cash_year', 'cash_dividend_percentage',
       'issue_method'],
      dtype='object')
insider_deals header: Index(['deal_announce_date', 'deal_method', 'deal_action', 'deal_quantity',
       'deal_price', 'deal_ratio'],
      dtype='object')
2025-03-25 17:59:19 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports
2025-03-25 17:59:19 - vnstock.common.data.data_explorer - INFO - Tham số dropna không được hỗ trợ cho nguồn dữ liệu TCBS
ratio header: Index(['price_to_earning', 'price_to_book', 'value_before_ebitda', 'roe',
       'roa', 'days_receivable', 'days_inventory', 'days_payable',
       'ebit_on_interest', 'earning_per_share', 'book_value_per_share',
       'equity_on_total_asset', 'equity_on_liability', 'current_payment',
       'quick_payment', 'eps_change', 'ebitda_on_stock', 'gross_profit_margin',
       'operating_profit_margin', 'post_tax_margin', 'debt_on_equity',
       'debt_on_asset', 'debt_on_ebitda', 'short_on_long_debt',
       'asset_on_equity', 'capital_balance', 'cash_on_equity',
       'cash_on_capitalize', 'cash_circulation', 'revenue_on_work_capital',
       'capex_on_fixed_asset', 'revenue_on_asset', 'post_tax_on_pre_tax',
       'ebit_on_revenue', 'pre_tax_on_ebit', 'payable_on_equity',
       'ebitda_on_stock_change', 'book_value_per_share_change'],
      dtype='object')
2025-03-25 17:59:22 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports
2025-03-25 17:59:22 - vnstock.common.data.data_explorer - INFO - Tham số dropna không được hỗ trợ cho nguồn dữ liệu TCBS
Income statement header: Index(['revenue', 'year_revenue_growth', 'cost_of_good_sold', 'gross_profit',
       'operation_expense', 'operation_profit', 'year_operation_profit_growth',
       'interest_expense', 'pre_tax_profit', 'post_tax_profit',
       'share_holder_income', 'year_share_holder_income_growth', 'ebitda'],
      dtype='object')
2025-03-25 17:59:22 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports
balance_sheet header: Index(['short_asset', 'cash', 'short_invest', 'short_receivable', 'inventory',
       'long_asset', 'fixed_asset', 'asset', 'debt', 'short_debt', 'long_debt',
       'equity', 'capital', 'other_debt', 'un_distributed_income',
       'minor_share_holder_profit', 'payable'],
      dtype='object')
2025-03-25 17:59:22 - vnstock.common.data.data_explorer - INFO - TCBS only supports Vietnamese reports
cash_flow header: Index(['invest_cost', 'from_invest', 'from_financial', 'from_sale',
       'free_cash_flow'],
      dtype='object')




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
