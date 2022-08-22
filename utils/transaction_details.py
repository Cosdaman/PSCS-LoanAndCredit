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
    input('Press enter to continue...')
    return 0


def stateTransactions(spark, dbuser, dbpass):
    input('Press enter to continue...')
    return 0
