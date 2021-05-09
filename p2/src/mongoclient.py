from pymongo import MongoClient
from urllib.parse import quote_plus

class CustomMongoClient():

    client: MongoClient

    def __init__(self, user: str = 'test', password:str='1234',host:str='localhost'):
        uri = "mongodb://%s:%s@%s" % (quote_plus(user), quote_plus(password), host)
        self.client = MongoClient(uri)

    def dropDB(self, db:str='test') -> None:
        client = self.client
        client.drop_database(db)

    def store(self, objs, collectionName: str, dbName: str = 'test') -> None:
        """objs: [jsonObject].
        """
        client = self.client
        db = client[dbName]
        collection = db[collectionName]
        collection.insert_many(objs)

    def statistics(self, dbName:str = 'test') -> None:
        client = self.client
        db = client[dbName]
        print('Collections names: {}'.format(db.collection_names()))
        for collection_name in db.collection_names():
            print('{}: {}'.format(collection_name, db[collection_name].count_documents({})))
