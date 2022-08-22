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
    print(res)

    return 0


def modifyCustDetails():
    return 0


def monthlyBill():
    return 0


def custTransactionsTwoDates():
    return 0
