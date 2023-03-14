# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import pymongo
import json
import time

def run_task_5():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["A5db"]

    print('=============Initializing Task 8: SQLite Query================')
    listing_id = input('Please input a listing_id to  find the host_name, rental_price and the most recent review for that listing: ')
    print('\nStart calculating average process time for the query ...')

    total = 0

    start = time.time()

    # get collection
    listings_collection = database.get_collection("listings")
    doc = listings_collection.aggregate(
        [

            {
            "$match" : 
                {
                    "reviews.listing_id" : {"$eq" : int(listing_id)}
                }
            },
            {
                "$unwind" : "$reviews"
            },
            {
                "$group" : {
                    "_id" : {
                    "host_name" : "$host_name",
                    "price" : "$price",
                    "review" : "$reviews.comments"
                    },
                    "max_date" : { "$max" : "$reviews.date"}
                } 
            },
            {
                "$sort": {
                    "max_date" : -1
                }
            },
            {
                "$limit" : 1
            }
        ])
    end = time.time()
    total += (end - start)

    print('The average query time of T8MongoDB takes {}s'.format(total))
    print('\nHere is the host_name, rental_price and the most recent review for a given listing:\n')

    for x in doc:
        print("Host name is: " + x['_id']['host_name'])
        print("Rental price is: " + str(x['_id']['price']))
        print("Most recent review is: " + x['_id']['review'])

    return

def main():
    run_task_5()
    print('\nExiting the system...')

    return

if __name__ == "__main__":
    main()
