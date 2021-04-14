# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import pymongo
import time

def run_Task_4(mydb):

    collist = mydb.list_collection_names()
    if not "listings" in collist:
       raise Exception('Error! Listings not in A5db.')      
    listings = mydb["listings"]
    
    # Calculate the process time of the query

    total = 0
    start = time.time()

    host_id_list = listings.aggregate(
                   [

                     {'$match': 
                          {
                             "reviews": []
                          }
                     },
                     {
                        "$sort":{"id":-1}
                     },
                     {
                        "$limit":10
                     }
                    ])

    end = time.time()
    total += (end - start)
    print('\nThe average query time of T4MongoDB takes {}s'.format(total))


    print('\nHere are the properties that has not received any review:\n')
    for item in host_id_list:
       print(item['id'])

    return
 

def main():
   myclient = pymongo.MongoClient('mongodb://localhost:27017/')
   dblist = myclient.list_database_names()

   if not "A5db" in dblist:
      raise Exception('Error! A5db not created.')

   mydb = myclient["A5db"]

   print("=============Initializing Task 4: MongoDB Query=====================")
   print("\nCalculating the average time to run this MongoDB query ...")
   
   run_Task_4(mydb)

   
   print('All the listing_ids have been listed.\n')
   print('Exiting the system...')

   return


if __name__ == '__main__':
     main()
