from art import *
import loadCredit


def mainMenu():
    print("Hello, please select the data you wish to view from the following options:")
    print('1 - Transactions')
    print('2 - Customers')
    print('0 - Exit')


intro = "LOAN      AND     CREDIT"
Art = text2art(intro, font='doom')
print(Art)

mainMenu()
catChoice = input("Choice: ")

while catChoice != '0':
    match catChoice:
        case "1":
            print("transactions")
            catChoice = input("Choice: ")

        case "2":
            print("customers")
            catChoice = input("Choice: ")

        case "3":
            loadCredit.loadData()
            mainMenu()
            catChoice = input("Choice: ")

        case "0":
            print("exit")

        case _:
            print("Invalid choice, please pick valid options from the menu.")
            mainMenu()
            catChoice = input("Choice: ")

print("Goodbye.")
