import pandas as pd
import requests

x = requests.get(
    'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')

print('Status code:', x.status_code)

df_loan = pd.read_json(x.text)
print(df_loan.head())