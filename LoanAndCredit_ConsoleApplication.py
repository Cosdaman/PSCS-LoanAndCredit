from utils.transaction_details import zipcodeTransactions
import os
from art import *
import loadCreditAndLoan
import requests
from pyspark.sql import SparkSession
from utils import customer_details
from utils import transaction_details
from dotenv import dotenv_values
import pandas as pd

pd.options.mode.chained_assignment = None
config = dotenv_values(".env")
dbuser = config["dbuser"]
dbpass = config["dbpass"]


def cls():
    os.system('cls' if os.name == 'nt' else 'clear')


def mainMenu():
    cls()
    print(Art)
    print("--Main Menu--")
    print("Hello, please select from the following options:")
    print('1 - View Transactions')
    print('2 - View/Edit Customer data')
    print('3 - Load Data into Database')
    print('4 - View API Status Code')
    print('0 - Exit')
    choice = input("Choice: ")
    return choice


def mainMenuTree(mChoice):
    while mChoice != '0':
        match mChoice:
            case "1":
                tChoice = transactionsMenu()
                transactionsTree(tChoice)
                mChoice = mainMenu()

            case "2":
                cChoice = customerMenu()
                customerTree(cChoice)
                mChoice = mainMenu()

            case "3":
                loadCreditAndLoan.loadData(spark, dbuser, dbpass)
                mChoice = mainMenu()

            case "4":
                statusCodeApi()
                mChoice = mainMenu()

            case "0":
                return 0

            case _:
                print("Invalid choice, please pick valid options from the menu.")
                mChoice = mainMenu()


def transactionsMenu():
    cls()
    print("--Transactions Menu--")
    print("Choose to display total transactions from the following options:")
    print("1 - Month, Year, and Zip Code")
    print("2 - Transaction Type")
    print("3 - State")
    print("0 - Return to Main Menu")
    choice = input("Choice: ")
    return choice


def transactionsTree(tChoice):
    while tChoice != '0':
        match tChoice:
            case '1':
                transaction_details.zipcodeTransactions(spark, dbuser, dbpass)
                tChoice = transactionsMenu()

            case '2':
                print('Transaction Type')
                # ask transaction type
                tChoice = transactionsMenu()

            case '3':
                print('Branches in a certain state')
                # ask for state
                tChoice = transactionsMenu()

            case "0":
                return 0

            case _:
                print(
                    "Invalid choice, please pick valid options from the menu.")
                tChoice = transactionsMenu()


def customerMenu():
    cls()
    print("--Customer Menu--")
    print("Choose from the following options:")
    print("1 - Check details of a customer")
    print("2 - Modify details of a customer")
    print("3 - Generate monthly bill for a credit card number")
    print("4 - Display transactions of a customer between two dates")
    print("0 - Return to Main Menu")
    choice = input("Choice: ")
    return choice


def customerTree(cChoice):
    while cChoice != '0':
        match cChoice:
            case '1':
                customer_details.checkCustDetails(spark, dbuser, dbpass)
                cChoice = customerMenu()

            case '2':
                # ask ssn
                print('modify details')
                cChoice = customerMenu()

            case '3':
                # ask creditcard number, month, year
                print('generate monthly bill')
                cChoice = customerMenu()

            case '4':
                # ask first date and second date, show format
                print('display transactions')
                cChoice = customerMenu()

            case "0":
                return 0

            case _:
                print(
                    "Invalid choice, please pick valid options from the menu.")
                cChoice = customerMenu()


def statusCodeApi():
    x = requests.get(
        'https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json')

    print('Status code:', x.status_code)
    x.close()


intro = "LOAN      AND     CREDIT"

Art = text2art(intro, font='doom')
print(Art)
print("Loading...")

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("Kevin Ang Console App") \
    .getOrCreate()

print("Loaded.")
mChoice = mainMenu()
mainMenuTree(mChoice)

print("Goodbye.")
