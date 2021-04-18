from pymongo import MongoClient
from urllib.parse import quote_plus

def getMongoClient():
    user = 'test'
    password = '1234'
    host = 'localhost'
    uri = "mongodb://%s:%s@%s" % (quote_plus(user), quote_plus(password), host)
    return MongoClient(uri)
