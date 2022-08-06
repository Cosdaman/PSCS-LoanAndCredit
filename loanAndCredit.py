import pandas as pd


# CUSTOMER DATA
# read data
df_cust_raw = pd.read_json('cdw_sapp_custmer.json', lines=True)

# format columns
df_cust_raw['FIRST_NAME'] = df_cust_raw['FIRST_NAME'].map(str)
df_cust_raw['FIRST_NAME'] = df_cust_raw['FIRST_NAME'].str.title()

df_cust_raw['MIDDLE_NAME'] = df_cust_raw['MIDDLE_NAME'].map(str)
df_cust_raw['MIDDLE_NAME'] = df_cust_raw['MIDDLE_NAME'].str.lower()

df_cust_raw['LAST_NAME'] = df_cust_raw['LAST_NAME'].map(str)
df_cust_raw['LAST_NAME'] = df_cust_raw['LAST_NAME'].str.title()

df_cust_raw['FULL_STREET_ADDRESS'] = df_cust_raw['APT_NO'].map(
    str) + ' ' + df_cust_raw['STREET_NAME'].map(str)

df_cust_raw['CUST_PHONE'] = df_cust_raw['CUST_PHONE'].map(str)
df_cust_raw['CUST_PHONE'] = df_cust_raw['CUST_PHONE'].str[:3] + \
    '-'+df_cust_raw['CUST_PHONE'].str[3:]

# arrange columns
custCols = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME',
            'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED']

df_cust_formatted = df_cust_raw[custCols]

# BRANCH DATA
df_branch_raw = pd.read_json('cdw_sapp_branch.json', lines=True)

# FORMAT BRANCH DATA
df_branch_raw['BRANCH_PHONE'] = df_branch_raw['BRANCH_PHONE'].map(str)
df_branch_raw['BRANCH_PHONE'] = "("+df_branch_raw['BRANCH_PHONE'].str[:3]+")" + \
    df_branch_raw['BRANCH_PHONE'].str[3:6] + \
    "-"+df_branch_raw['BRANCH_PHONE'].str[6:]

# CREDIT DATA
df_credit_raw = pd.read_json('cdw_sapp_credit.json', lines=True)

# FORMAT DATA

df_credit_raw['MONTH'] = df_credit_raw['MONTH'].map(str)
df_credit_raw['MONTH'] = df_credit_raw['MONTH'].str.zfill(2)

df_credit_raw['DAY'] = df_credit_raw['DAY'].map(str)
df_credit_raw['DAY'] = df_credit_raw['DAY'].str.zfill(2)

df_credit_raw['TIMEID'] = df_credit_raw['YEAR'].map(
    str) + df_credit_raw['MONTH'].map(str) + df_credit_raw['DAY'].map(str)

creditCols = ['CREDIT_CARD_NO', 'TIMEID', 'CUST_SSN', 'BRANCH_CODE',
              'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'TRANSACTION_ID']

df_credit_formatted = df_credit_raw[creditCols]

