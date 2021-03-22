# Pymongo-Queries
#Contains most essential pymongo queries

# How to retrieve data from mongodb as a dataframe

try:
    client = pymongo.MongoClient(URL)
    db = client[Collection_name]
    col = db[Document_name]
    df= pd.DataFrame(col.find({})
    print('Done')
except(pymongo.errors.ConnectionFailure) as e:
    print('Could not connect server', e)
    
# How to update data in mongodb

from tqdm import tqdm_notebook as tqdm
tqdm().pandas()
data_dict = df.to_dict("records")
for row in tqdm(data_dict):
    if row['new_customer_id'] in lts:
        col.update({},{'$rename' : {"new_customer_id":"id_customer"}},multi=True)
        
# How to insert data in mongodb

try:
    client = pymongo.MongoClient(URL)
    db = client[Collection_name]
    col = db[Document_name]
    data_dict = df.to_dict("records")
    col.insert(data_dict)
    print('Done')
except(pymongo.errors.ConnectionFailure) as e:
    print('Could not connect server', e)
    
# How to get data from mongodb within a specific duration of updation/creation

%%time
try:
    client = pymongo.MongoClient(URL)
    db = client[Collection_name]
    col = db[Document_name]
    df= pd.DataFrame(col.find({'created_at': {'$gte': (datetime.datetime.today() - datetime.timedelta(days = 60)), '$lt': datetime.datetime.today()}))
    print('Done')
except(pymongo.errors.ConnectionFailure) as e:
    print('Could not connect server', e)
                      
# How to update the values inside the mongodb document
                               
for row in tqdm(data_dict):
    try:
        if row[column_name] not in df[column_name]:
            col.insert_one(row)
        else:
            print('update')
            col.update_one({"id": row["id"]}, 
                                  {"$set": {"name": row['name'], 
                                            "email": row['email'],
                                            "phone_no": row['phone_no'],
                                            "address": row['address'],
                                            "created_at": row['created_at'],
                                            "updated_at": row['updated_at']}}, upsert=True)
    except(pymongo.errors.DuplicateKeyError):
        print('Error Occured:DuplicateKeyError')
