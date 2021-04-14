# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import sqlite3
import time

connection = None
cursor = None

def connect(path):
    global connection, cursor

    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute(' PRAGMA foreign_key = ON; ')

    connection.commit()

    return

def run_task_5():
    global connection, cursor

    print('=============Initializing Task 5: SQLite Query================')
    neighbourhood = input('Please input a neighbourhood to calculate average rental cost/night: ')
    print('\nStart calculating average process time for the query ...')

    total = 0

    start = time.time()
    cursor.execute("SELECT AVG(price) FROM listings WHERE neighbourhood = :neighbourhood;", {"neighbourhood":neighbourhood})
    end = time.time()
    total += (end - start)
    print('The average query time of T5SQL takes {}s'.format(total))
    
    row = cursor.fetchall()
    print('\nHere is the average rental cost/night for a given neighbourhood : ' + str(row[0][0]))

    connection.commit()

    return

def main():
    global connection, cursor

    path = './A5.db'
    connect(path)

    run_task_5()

    connection.close()
    print('Exiting the system...')

    return

if __name__ == "__main__":
    main()