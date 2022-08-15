import mysql.connector
import pandas as pd
from pyspark.sql import SparkSession
from dotenv import dotenv_values
import requests


def loadData():
    print('Beginning data import from files and API...')

    config = dotenv_values(".env")
    dbuser = config["dbuser"]
    dbpass = config["dbpass"]

    # Create PySpark SparkSession
    spark = SparkSession.builder \
        .master("local[1]") \
        .appName("LoadCredit") \
        .getOrCreate()

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

    df_cust_raw['LAST_UPDATED'] = pd.to_datetime(df_cust_raw['LAST_UPDATED'])

    # arrange columns
    custCols = ['SSN', 'FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME',
                'CREDIT_CARD_NO', 'FULL_STREET_ADDRESS', 'CUST_CITY', 'CUST_STATE', 'CUST_COUNTRY', 'CUST_ZIP', 'CUST_PHONE', 'CUST_EMAIL', 'LAST_UPDATED']

    df_cust_formatted = df_cust_raw[custCols]

    # BRANCH DATA
    df_branch_raw = pd.read_json('cdw_sapp_branch.json', lines=True)

    # FORMAT BRANCH DATA
    df_branch_raw['BRANCH_PHONE'] = df_branch_raw['BRANCH_PHONE'].map(str)
    df_branch_raw['BRANCH_PHONE'] = "("+df_branch_raw['BRANCH_PHONE'].str[:3]+")" + \
        df_branch_raw['BRANCH_PHONE'].str[3:6] + \
        "-"+df_branch_raw['BRANCH_PHONE'].str[6:]

    df_branch_raw['LAST_UPDATED'] = pd.to_datetime(
        df_branch_raw['LAST_UPDATED'])

    df_branch_formatted = df_branch_raw.copy(deep=True)

    # CREDIT DATA
    df_credit_raw = pd.read_json('cdw_sapp_credit.json', lines=True)

    # FORMAT DATA

    df_credit_raw['MONTH'] = df_credit_raw['MONTH'].map(str)
    df_credit_raw['MONTH'] = df_credit_raw['MONTH'].str.zfill(2)

    df_credit_raw['DAY'] = df_credit_raw['DAY'].map(str)
    df_credit_raw['DAY'] = df_credit_raw['DAY'].str.zfill(2)

    df_credit_raw['TIMEID'] = df_credit_raw['YEAR'].map(
        str) + df_credit_raw['MONTH'].map(str) + df_credit_raw['DAY'].map(str)

    df_credit_raw.rename(
        columns={"CREDIT_CARD_NO": "CUST_CC_NO"}, inplace=True)

    # arrange columns
    creditCols = ['CUST_CC_NO', 'TIMEID', 'CUST_SSN', 'BRANCH_CODE',
                  'TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'TRANSACTION_ID']

    df_credit_formatted = df_credit_raw[creditCols]

    x = requests.get(
        'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')

    df_loan = pd.read_json(x.text)

    x.close()

    sparkDF_Cust = spark.createDataFrame(df_cust_formatted)
    sparkDF_Branch = spark.createDataFrame(df_branch_formatted)
    sparkDF_Credit = spark.createDataFrame(df_credit_formatted)
    sparkDF_Loan = spark.createDataFrame(df_loan)

    db_connection = mysql.connector.connect(user=dbuser, password=dbpass)

    dropDB = "DROP DATABASE IF EXISTS creditcard_capstone;"
    createDB = "CREATE DATABASE creditcard_capstone;"

    db_cursor = db_connection.cursor()
    db_cursor.execute(dropDB)
    db_cursor.execute(createDB)

    sparkDF_Cust.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER") \
        .option("user", dbuser) \
        .option("password", dbpass) \
        .save()

    sparkDF_Credit.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
        .option("user", dbuser) \
        .option("password", dbpass) \
        .save()

    sparkDF_Branch.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
        .option("user", dbuser) \
        .option("password", dbpass) \
        .save()

    sparkDF_Loan.write.format("jdbc") \
        .mode("overwrite") \
        .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
        .option("dbtable", "creditcard_capstone.CDW_SAPP_LOAN_APPLICATION") \
        .option("user", 'root') \
        .option("password", 'root') \
        .save()

    print("Converting SQL data types...")

    useDB = "USE creditcard_capstone;"
    db_cursor.execute(useDB)

    alterTables = [
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN SSN INT;",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN FIRST_NAME VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN MIDDLE_NAME VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN LAST_NAME VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CREDIT_CARD_NO VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN FULL_STREET_ADDRESS VARCHAR(100);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CUST_CITY VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CUST_STATE VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CUST_COUNTRY VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CUST_ZIP INT;",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CUST_PHONE VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CUSTOMER MODIFY COLUMN CUST_EMAIL VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_CODE INT;",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_NAME VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_STREET VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_CITY VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_STATE VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_ZIP INT;",
        "ALTER TABLE CDW_SAPP_BRANCH MODIFY COLUMN BRANCH_PHONE VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN CUST_CC_NO VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN TIMEID VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN CUST_SSN INT;",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN BRANCH_CODE INT;",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN TRANSACTION_TYPE VARCHAR(50);",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN TRANSACTION_VALUE DOUBLE;",
        "ALTER TABLE CDW_SAPP_CREDIT_CARD MODIFY COLUMN TRANSACTION_ID INT;",
    ]

    for i in alterTables:
        db_cursor.execute(i)

    spark.stop()
    print("Credit data import complete.")
