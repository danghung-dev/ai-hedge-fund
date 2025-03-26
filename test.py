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
# print data sample 5 rows 
print(f"overview data: {overview.head()}")
profile = company.profile()
print(f"profile header: {profile.columns}")
dividends = company.dividends()
print(f"dividends header: {dividends.columns}")
# print data sample 5 rows 
print(f"dividends data: {dividends.head()}")
insider_deals = company.insider_deals()
print(f"insider_deals header: {insider_deals.columns}")
# print data sample 5 rows 
print(f"insider_deals data: {insider_deals.head()}")


ratio = stock.finance.ratio(period='year', lang='en', dropna=True)
print(f"ratio header: {ratio.columns}")
income_stmt = stock.finance.income_statement(period='year', lang='en', dropna=True)
print(f"Income statement header: {income_stmt.columns}")

balance_sheet = stock.finance.balance_sheet(period='year', lang='vi')
print(f"balance_sheet header: {balance_sheet.columns}")

cash_flow = stock.finance.cash_flow(period='year', lang='vi')
print(f"cash_flow header: {cash_flow.columns}")


