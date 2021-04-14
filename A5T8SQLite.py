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

def run_task_8():
    global connection, cursor

    print('=============Initializing Task 8: SQLite Query================')
    listing_id = input('Please input a listing_id to  find the host_name, rental_price and the most recent review for that listing: ')
    print('\nStart calculating average process time for the query ...')

    total = 0

    start = time.time()
    cursor.execute('''SELECT l.host_name, l.price, r.comments FROM listings l, reviews r 
                      WHERE l.id = :listing_id AND l.id = r.listing_id AND r.date = (
                          SELECT MAX(r.date) FROM listings l, reviews r 
                          WHERE l.id = :listing_id AND l.id = r.listing_id);''', {"listing_id":listing_id})
    end = time.time()
    total += (end - start)
    print('The average query time of T8SQL takes {}s'.format(total))
    
    rows = cursor.fetchall()
    print('\nHere is the host_name, rental_price and the most recent review for a given listing:\n')
    # print(rows[0])
    for row in rows:
        print("Host name is: " + str(row[0]))
        print("Rental price is: " + str(row[1]))
        print("Most recent review is: " + str(row[2]))

    connection.commit()

    return

def main():
    global connection, cursor

    path = './A5.db'
    connect(path)

    run_task_8()

    connection.close()
    print('\nExiting the system...')

    return

if __name__ == "__main__":
    main()