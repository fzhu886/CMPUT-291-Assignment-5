# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import pymongo
import pprint
import json
import warnings
import time

def run_task_5():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["A5db"]

    print('=============Initializing Task 5: MongoDB Query================')
    neighbourhood = input('Please input a neighbourhood to calculate average rental cost/night: ')

    print('\nStart calculating average process time for the query ...')

    total = 0

    start = time.time()

    # get collection
    listings_collection = database.get_collection("listings")
    doc = listings_collection.aggregate([
        {
            "$match" : 
                     {"neighbourhood" : {"$eq" : neighbourhood} }
        },
        {
            "$group" : { "_id" : 0,
                         "average_price" : { "$avg" : "$price"}} 
        }
    ])
    end = time.time()
    total += (end - start)

    print('The average query time of T5MongoDB takes {}s'.format(total))
    print('\nHere is the average rental cost/night for a given neighbourhood : ', end = "")
    for x in doc:
        print(x['average_price'])

    return

def main():
    run_task_5()
    print('Exiting the system...')

    return

if __name__ == "__main__":
    main()
