import pandas as pd

df_branch = pd.read_json('cdw_sapp_branch.json', lines=True)
df_credit = pd.read_json('cdw_sapp_credit.json', lines=True)
df_customer = pd.read_json('cdw_sapp_custmer.json', lines=True)
df_loan = pd.read_json(
    'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')

# print(df_branch.head())
# print(df_credit.head())
# print(df_customer.head())
# print(df_loan.head())
# df_branch.info()
df_credit.info()
df_customer.info()
df_loan.info()