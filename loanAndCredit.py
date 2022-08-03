import pandas as pd


df_cust_raw = pd.read_json('cdw_sapp_custmer.json', lines=True)
df_cust_raw['FULL_STREET_ADDRESS'] = df_cust_raw['APT_NO'].map(
    str) + ' ' + df_cust_raw['STREET_NAME'].map(str)

custCols = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME',
            'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED']
df_cust_formatted = df_cust_raw[custCols]


print(df_cust_formatted.head())

df_branch = pd.read_json('cdw_sapp_branch.json', lines=True)
df_credit = pd.read_json('cdw_sapp_credit.json', lines=True)


# print(df_branch.head())
# print(df_credit.head())
# print(df_customer.head())
# print(df_loan.head())
# df_branch.info()
# df_credit.info()
# df_customer.info()
# df_loan.info()
