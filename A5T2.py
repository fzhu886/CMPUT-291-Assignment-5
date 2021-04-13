# All the codes below are made by Fan Zhu and Harshit Venket Subramanian

import pymongo
import csv
with open('YVR_Airbnb_listings_summary.csv', 'r') as file:
     reader = csv.reader(file)
     item_list = []

     for row in reader:
        if row[0].isdigit() and row[2].isdigit():
           num = int(row[0])
           row[0] = num
           row[2] = int(row[2])
        item_list.append(row)

f = open('YVR_Airbnb_reviews.csv', 'r')
countries = csv.reader(f)
country_list = []
for row in countries:
    if row[0].isdigit() and row[1].isdigit():
       num = int(row[0])
       row[0] = num
       row[1] = int(row[1])
    country_list.append(row)

def create_collection(mydb, item_list, country_list):

    collist = mydb.list_collection_names()
    if "listings" in collist:
       mydb["listings"].drop()      
    listings = mydb["listings"]
    entry_list = []
    
    for index1 in range(1, len(item_list)):
       review_list = []
       for index2 in range(1, len(country_list)):
           if country_list[index2][0] == item_list[index1][0]:
              review_dict = {}
              review_dict["listing_id"] = country_list[index2][0] 
              review_dict["id"] = country_list[index2][1]
              review_dict["date"] = country_list[index2][2]
              review_dict["reviewer_id"] = country_list[index2][3]
              review_dict["reviewer_name"] = country_list[index2][4]
              review_dict["comments"] = country_list[index2][5]
              review_list.append(review_dict)

       mydict = {}       
       mydict["id"] = item_list[index1][0]
       mydict["name"] = item_list[index1][1]
       mydict["host_id"] = item_list[index1][2]
       mydict["host_name"] = item_list[index1][3]
       mydict["neighbourhood"] = item_list[index1][4]
       mydict["room_type"] = item_list[index1][5]
       mydict["price"] = item_list[index1][6]
       mydict["minimum"] = item_list[index1][7]
       mydict["availability_365"] = item_list[index1][8]
       mydict["reviews"] = review_list
       entry_list.append(mydict)
    
    listings.insert_many(entry_list)

    return
 

def main():
   myclient = pymongo.MongoClient('mongodb://localhost:27017/')
   
   mydb = myclient["A5db"]

   print("Creating mongoDB: A5db...")
   print("The program generally takes around 3 mins...")
   
   create_collection(mydb, item_list, country_list)

   dblist = myclient.list_database_names()

   if not "A5db" in dblist:
      raise Exception('Error! A5db not created.')

   print('Database A5db has been created.')
   print('Exiting the system...')

   return


if __name__ == '__main__':
     main()
