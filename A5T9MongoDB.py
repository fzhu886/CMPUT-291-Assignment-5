# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import pymongo
import json
import time

def run_task_5():
    client = pymongo.MongoClient('mongodb://localhost:27017/')
    database = client["A5db"]

    print('=============Initializing Task 9: SQLite Query================')
    keywords = input('Please input some keywords so as to find the top 3 results: ')
    print('\nStart calculating average process time for the query ...')

    total = 0

    start = time.time()

    # get collection
    listings_collection = database.get_collection("listings")
    doc = listings_collection

    for word in keywords.split(","):
    	print(word.strip())


    # .find(
    # { $and : [
    #           {'comments': {'$regex': '\b' + "this" +'\b'}},
    #           {'comments': {'$regex': '\b' + "was" +'\b'}},
    #           {'comments': {'$regex': '\b' + "beautiful" +'\b'}},
    #      ]
    # });
    end = time.time()
    total += (end - start)

    print('The average query time of T9MongoDB takes {}s'.format(total))
    print('\nHere are the top 3 listings which have reviews most similar to the given set of keywords:\n')

    return

def main():
    run_task_5()
    print('\nExiting the system...')

    return

if __name__ == "__main__":
    main()
