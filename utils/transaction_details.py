
def zipcodeTransactions(spark, dbuser, dbpass):
    cust_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                      user=dbuser,
                                                      password=dbpass,
                                                      url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                      dbtable="creditcard_capstone.CDW_SAPP_CUSTOMER").load()

    cc_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                    user=dbuser,
                                                    password=dbpass,
                                                    url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                    dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()

    cust_pandas_df = cust_spark_df.toPandas()
    cc_pandas_df = cc_spark_df.toPandas()

    zipcode = input("Enter a zipcode: ")
    print("Enter a month in the format MM.")
    print("For example, January would be input as 01, February as 02, etc.")
    month = input("Enter a month: ")
    print("Enter a year in the format YYYY.")
    year = input("Enter a year: ")

    ssn_df = cust_pandas_df[cust_pandas_df['CUST_ZIP'] == int(zipcode)]
    ssn = ssn_df['SSN'].to_numpy()
    trans_filtered = cc_pandas_df[cc_pandas_df.CUST_SSN.isin(ssn)]
    trans_filtered['YEAR'] = trans_filtered['TIMEID'].str[:4]
    trans_filtered['MONTH'] = trans_filtered['TIMEID'].str[4:6]
    trans_filtered['DAY'] = trans_filtered['TIMEID'].str[6:]
    trans_filtered = trans_filtered[trans_filtered.MONTH.isin([month])]
    trans_filtered = trans_filtered[trans_filtered.YEAR.isin([year])]
    trans_filtered = trans_filtered.sort_values(by=['DAY'])

    print(trans_filtered)
    input('Press enter to continue...')
    return 0


def typeTransactions(spark, dbuser, dbpass):

    cc_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                    user=dbuser,
                                                    password=dbpass,
                                                    url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                    dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()

    cc_pandas_df = cc_spark_df.toPandas()
    types = cc_pandas_df['TRANSACTION_TYPE'].unique()

    print("Select a transaction type from the following list:")
    for i in types:
        print("    "+i)

    search_type = input("Transaction type to search for: ")
    trans_filtered = cc_pandas_df[cc_pandas_df.TRANSACTION_TYPE.isin(
        [search_type.title()])]
    print(trans_filtered)

    input('Press enter to continue...')
    return 0


def stateTransactions(spark, dbuser, dbpass):

    cc_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                    user=dbuser,
                                                    password=dbpass,
                                                    url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                    dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()

    branch_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                        user=dbuser,
                                                        password=dbpass,
                                                        url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                        dbtable="creditcard_capstone.CDW_SAPP_BRANCH").load()

    cc_pandas_df = cc_spark_df.toPandas()
    branch_pandas_df = branch_spark_df.toPandas()

    print("Choose a state to view data for transactions.")
    print("Please format your choice of state in its two letter abbreviations.")
    print("Example: Washington would be entered as 'WA'. ")
    state = input("State data to view: ")
    state = state.upper()

    branch_code_df = branch_pandas_df[branch_pandas_df['BRANCH_STATE'] == state]
    branch_codes = branch_code_df['BRANCH_CODE'].to_numpy()
    trans_filtered = cc_pandas_df[cc_pandas_df.BRANCH_CODE.isin(branch_codes)]
    count = trans_filtered['TRANSACTION_ID'].count()
    sum = trans_filtered['TRANSACTION_VALUE'].sum()

    print(f"The transaction data for the state of {state} is as follows:")
    print(f"Number of Transactions: {count}")
    print(f"Total value of transactions: {sum}")

    input('Press enter to continue...')
    return 0
