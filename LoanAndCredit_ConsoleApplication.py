from art import *
import loadCreditAndLoan
import requests


def mainMenu():
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
                loadCreditAndLoan.loadData()
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
                print('Date and Zipcode')
                # ask month, year, zipcode
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
                print('check details')
                # ask ssn
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

mChoice = mainMenu()
mainMenuTree(mChoice)

print("Goodbye.")