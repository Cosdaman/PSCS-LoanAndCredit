def checkCustDetails(spark, dbuser, dbpass):
    cust_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                      user=dbuser,
                                                      password=dbpass,
                                                      url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                      dbtable="creditcard_capstone.CDW_SAPP_CUSTOMER").load()

    cust_pandas_df = cust_spark_df.toPandas()
    print("Enter customer SSN to check details...")
    ssn = input("SSN: ")
    res = cust_pandas_df[cust_pandas_df['SSN'] == int(ssn)]
    if(res.empty):
        print("Error:")
        print('One or more of your inputs were invalid or the data does not exist in the database, please try again.')
    else:
        print(res)
    input('Press enter to continue...')
    return 0


def modifyCustDetails(spark, dbuser, dbpass):
    import mysql.connector
    db_connection = mysql.connector.connect(user=dbuser, password=dbpass)
    db_cursor = db_connection.cursor()
    print("Please enter the SSN of the user you wish to modify.")
    ssn = input("SSN: ")

    query = f"(select * from cdw_sapp_customer where ssn = {ssn}) as customer"

    df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                           user=dbuser,
                                           password=dbpass,
                                           url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                           dbtable=query).load()

    if(len(df.head(1)) == 0):
        print("Error:")
        print('One or more of your inputs were invalid or the data does not exist in the database, please try again.')
        input('Press enter to continue...')
        return 0

    df.show()

    print('Which data would you like to change?')
    print('Enter the data category exactly as presented: ')
    column = input("")
    print('What would you like the new data to be?')
    new_val = input("New data: ")

    useDB = "USE creditcard_capstone;"
    editDB = f"UPDATE cdw_sapp_customer SET {column}='{new_val}' WHERE SSN={ssn};"
    db_cursor.execute(useDB)
    db_cursor.execute(editDB)
    db_connection.commit()

    print('Data entry changed.')
    query = f"(select * from cdw_sapp_customer where SSN={ssn}) as customer"

    df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                           user=dbuser,
                                           password=dbpass,
                                           url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                           dbtable=query).load()

    df.show()
    input('Press enter to continue...')
    return 0


def monthlyBill(spark, dbuser, dbpass):

    cc_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                    user=dbuser,
                                                    password=dbpass,
                                                    url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                    dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()

    cc_pandas_df = cc_spark_df.toPandas()
    print('Input details in order to generate the bill for.')
    cc_no = input("Credit Card Number: ")
    year = input("Year: ")
    month = input("Month: ")

    cc_filtered = cc_pandas_df[cc_pandas_df['CUST_CC_NO'] == cc_no]
    cc_filtered['YEAR'] = cc_filtered['TIMEID'].str[:4]
    cc_filtered['MONTH'] = cc_filtered['TIMEID'].str[4:6]
    cc_filtered = cc_filtered[cc_filtered.MONTH.isin([month])]
    cc_filtered = cc_filtered[cc_filtered.YEAR.isin([year])]

    if(cc_filtered.empty):
        print("Error:")
        print('One or more of your inputs were invalid or the data does not exist in the database, please try again.')
        input('Press enter to continue...')
        return 0

    print(f"Statement for {cc_no} for the month of {month}, year {year}")
    print(cc_filtered.to_string())
    sum = cc_filtered['TRANSACTION_VALUE'].sum()
    print(f"Total value of transactions: {sum}")

    input('Press enter to continue...')
    return 0


def custTransactionsTwoDates(spark, dbuser, dbpass):

    cc_spark_df = spark.read.format("jdbc").options(driver="com.mysql.cj.jdbc.Driver",
                                                    user=dbuser,
                                                    password=dbpass,
                                                    url="jdbc:mysql://localhost:3306/creditcard_capstone",
                                                    dbtable="creditcard_capstone.CDW_SAPP_CREDIT_CARD").load()

    cc_pandas_df = cc_spark_df.toPandas()
    print("Input details in order to view transactions between two time periods.")
    cc_no = input("Credit Card Number: ")
    print("Input the date in the following format: YYYYMMDD.")
    date1 = input("Date 1: ")
    date2 = input("Date 2: ")

    if(date1 > date2):
        date1, date2 = date2, date1

    cc_filtered = cc_pandas_df[cc_pandas_df['CUST_CC_NO'] == cc_no]
    cc_filtered = cc_filtered[cc_filtered['TIMEID'].between(date1, date2)]

    if(cc_filtered.empty):
        print("Error:")
        print('One or more of your inputs were invalid or the data does not exist in the database, please try again.')
    else:
        print(cc_filtered.sort_values('TIMEID').to_string())

    input('Press enter to continue...')
    return 0
