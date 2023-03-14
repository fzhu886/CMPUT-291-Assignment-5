# All the codes below are made by Fan Zhu and Harshit Venket Subramanian
import random
import sqlite3
import csv
with open('YVR_Airbnb_listings_summary.csv', 'r') as file:
     reader = csv.reader(file)
     item_list = []

     for row in reader:
        item_list.append(row)

f = open('YVR_Airbnb_reviews.csv', 'r')
countries = csv.reader(f)
country_list = []
for row in countries:
    country_list.append(row)


connection = None
cursor = None

def connect(path):
    global connection, cursor

    connection = sqlite3.connect(path)
    cursor = connection.cursor()
    cursor.execute(' PRAGMA foreign_key = ON; ')

    connection.commit()

    return

def create_table():
    global connection, cursor

    cursor.execute('''DROP TABLE IF EXISTS listings;''')
    cursor.execute('''
                      CREATE TABLE listings (
                           id INTEGER,
                           name TEXT,
                           host_id INTEGER,
                           host_name TEXT,
                           neighbourhood TEXT,
                           room_type TEXT,
                           price INTEGER,
                           minimum INTEGER,
                           availability_365 INTEGER,
                           PRIMARY KEY(id)
                       );
                   ''')
    cursor.execute('''DROP TABLE IF EXISTS reviews;''')
    cursor.execute('''
                      CREATE TABLE reviews (
                           listing_id INTEGER,
                           id INTEGER,
                           date TEXT,
                           reviewer_id INTEGER,
                           reviewer_name TEXT,
                           comments TEXT,
                           PRIMARY KEY(id)
                       );
                   ''')


    connection.commit()

    return

def insert_data(item_list, country_list):
    global connection, cursor
 
    index = 0
    data = (item_list[index][0], item_list[index][1], item_list[index][2], item_list[index][3], item_list[index][4], item_list[index][5], item_list[index][6], item_list[index][7], item_list[index][8])
    print(data)

    for index in range(1, len(item_list)):
        data = (item_list[index][0], item_list[index][1], item_list[index][2], item_list[index][3], item_list[index][4], item_list[index][5], item_list[index][6], item_list[index][7], item_list[index][8])
        cursor.execute('''
                     INSERT INTO listings(id,
                           name,
                           host_id,
                           host_name,
                           neighbourhood,
                           room_type,
                           price,
                           minimum,
                           availability_365)
                     VALUES(?,?,?,?,?,?,?,?,?)
                     ;
                ''', data)

    for index in range(1, len(country_list)):
        data = (country_list[index][0], country_list[index][1], country_list[index][2], country_list[index][3], country_list[index][4], country_list[index][5])
        cursor.execute('''
                     INSERT INTO reviews(listing_id,
                           id,
                           date,
                           reviewer_id,
                           reviewer_name,
                           comments)
                     VALUES(?,?,?,?,?,?)
                     ;
                ''', data)


    connection.commit()

    return

def main():
    global connection, cursor

    path = r'./A5.db'
    connect(path)

    create_table()
    
    insert_data(item_list, country_list)

    connection.close()
    print('Exiting the system...')

    return

if __name__ == "__main__":
    main()
