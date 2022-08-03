import pandas as pd
import requests

x = requests.get(
    'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')

print('Status code:', x.status_code)


df_loan = pd.read_json(
    'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')
