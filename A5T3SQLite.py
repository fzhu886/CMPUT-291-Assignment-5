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

def run_task_3():
    global connection, cursor

    print('=============Initializing Task 3: SQLite Query================')
    print('\nStart calculating average process time for the query ...')

    total = 0
 
    start = time.time()
    cursor.execute('''
                      SELECT host_id, COUNT(id) FROM listings
                      GROUP BY host_id
                      ORDER BY COUNT(id) DESC
                      LIMIT 10;
                   ''')
    end = time.time()
    total += (end - start)
    print('The average query time of T3SQL takes {}s'.format(total))

    print('\nHere are the top 10 hosts who have the most listings (by host_id):\n')
    rows = cursor.fetchall()
    for row in rows:
       print(row[0],'listings: {}'.format(row[1]))   

    connection.commit()

    return

def main():
    global connection, cursor

    path = './A5.db'
    connect(path)

    run_task_3()

    connection.close()
    print('Exiting the system...')

    return

if __name__ == "__main__":
    main()
