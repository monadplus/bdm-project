from pymongo import MongoClient
from urllib.parse import quote_plus

class CustomMongoClient():
    def __init__(self, user='test', password='1234',host='localhost'):
        uri = "mongodb://%s:%s@%s" % (quote_plus(user), quote_plus(password), host)
        self.client = MongoClient(uri)

    def dropDB(self, db='test'):
        client = self.client
        client.drop_database(db)

    def store(self, objs, collectionName, dbName = 'test'):
        """objs: [jsonObject].
        """
        client = self.client
        db = client[dbName]
        collection = db[collectionName]
        collection.insert_many(objs)

    def statistics(self, dbName='test'):
        client = self.client
        db = client[dbName]
        print('Collections names: {}'.format(db.collection_names()))
        for collection_name in db.collection_names():
            print('{}: {}'.format(collection_name, db[collection_name].count_documents({})))
