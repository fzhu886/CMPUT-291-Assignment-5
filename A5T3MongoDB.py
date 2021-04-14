# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import pymongo
import time

def run_Task_3(mydb):

    collist = mydb.list_collection_names()
    if not "listings" in collist:
       raise Exception('Error! Listings not in A5db.')      
    listings = mydb["listings"]
    
    # Calculate the process time of the query

    total = 0
    start = time.time()
    host_id_list = listings.aggregate([{"$group": {"_id": "$host_id", "num_listings": {"$sum": 1}}},{"$sort": {'num_listings': -1}},{"$limit":10}])
    end = time.time()
    total += (end - start)
    print('\nThe average query time of T3MongoDB takes {}s'.format(total))


    print('\nHere are the listings that the hosts own:\n')
    for index in host_id_list:
       print(index['_id'],' listings: {}'.format(index['num_listings']))

    return
 

def main():
   myclient = pymongo.MongoClient('mongodb://localhost:27017/')
   dblist = myclient.list_database_names()

   if not "A5db" in dblist:
      raise Exception('Error! A5db not created.')

   mydb = myclient["A5db"]

   print("=============Initializing Task 3: MongoDB Query=====================")
   print("\nCalculating the average time to run this MongoDB query ...")
   
   run_Task_3(mydb)

   
   print('All the host_ids have been listed.\n')
   print('Exiting the system...')

   return


if __name__ == '__main__':
     main()
