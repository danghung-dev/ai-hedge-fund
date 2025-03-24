from vnstock import Vnstock
import json
import pandas as pd
# Function to convert an object to a dictionary, including nested objects
def object_to_dict(obj):
    if hasattr(obj, '__dict__'):
        return {key: object_to_dict(value) for key, value in obj.__dict__.items()}
    return obj

# Initialize the stock object with the desired symbol and source
company = Vnstock().stock(symbol='NVL', source='TCBS')

ratio = company.finance.ratio(period='year', lang='en', dropna=True)


# Set pandas options to display all rows and columns
pd.set_option('display.max_rows', None)  # Show all rows
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.expand_frame_repr', False)  # Prevent line breaks in DataFrame display

# Print the ratio DataFrame
print("Financial Ratios:")
print(ratio)