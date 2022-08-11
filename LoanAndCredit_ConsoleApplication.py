from art import *
import loadCredit


def mainMenu():
    print("--Main Menu--")
    print("Hello, please select the data you wish to view from the following options:")
    print('1 - Transactions')
    print('2 - Customers')
    print('3 - Load Data into Database')
    print('0 - Exit')
    choice = input("Choice: ")
    return choice


def transactionsMenu():
    print("--Transactions Menu--")
    print("Choose to display total transactions from the following options:")
    print("1 - Date and Zip Code")
    print("2 - Transaction Type")
    print("3 - State")
    choice = input("Choice: ")
    return choice


def transactionsChoice():
    return 0


intro = "LOAN      AND     CREDIT"
Art = text2art(intro, font='doom')
print(Art)

mChoice = mainMenu()

while mChoice != '0':
    match mChoice:
        case "1":
            tchoice = transactionsMenu()
            while tchoice != '0':
                match tchoice:
                    case '1':
                        print('date and zipcode')
                        tchoice = transactionsMenu()

                    case _:
                        print(
                            "Invalid choice, please pick valid options from the menu.")
                        tchoice = transactionsMenu()

        case "2":
            print("customers")
            mChoice = mainMenu()

        case "3":
            loadCredit.loadData()
            mChoice = mainMenu()

        case "0":
            print("exit")

        case _:
            print("Invalid choice, please pick valid options from the menu.")
            choice = mainMenu()

print("Goodbye.")
