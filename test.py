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

print("\nNEWS INFO:")
news.info()

overview = company.overview()
print("\nOVERVIEW INFO:")
overview.info()

profile = company.profile()
print("\nPROFILE INFO:")
profile.info()


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


