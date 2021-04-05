#!/usr/bin/env python
#
import pymongo
from mongoclient import getMongoClient
import pprint

def countByDistrict(housing):
    pipeline = [
        { "$group":
          { "_id": "$district",
            "count": { "$sum": 1 }
          }
        }
    ]
    pprint.pprint(list(housing.aggregate(pipeline)))

def averageRFDByDistrict(housing):
    # pipeline = [
    #     { "$group":
    #       {
    #         "_id": "$district",
    #         "rfd": { "$avg": "$rfd.avg" }
    #       }
    #     }
    # ]
    pipeline = [
        { "$project": { "district": "$district",
                        "neighborhood": "$neighborhood",
                        "rfds": {"$objectToArray": "$rfd"}
                      }
        },
        { "$unwind": "$rfds"},
        { "$group":
          {
            "_id": {"district": "$district",
                    "neighborhood": "$neighborhood"
                   },
            "rfd": { "$avg": "$rfds.v" }
          }
        }
    ]
    pprint.pprint(list(housing.aggregate(pipeline)))

if __name__ == "__main__":
    client = getMongoClient()
    db = client['test']
    housing = db['housing']

    print('Collections names: {}'.format(db.collection_names()))
    for collection_name in db.collection_names():
        print('{}: {}'.format(collection_name, db[collection_name].count_documents({})))
    print('\n=== Count by district ===\n')
    countByDistrict(housing)
    print('\n=== RFD by district/neighborhood ===\n')
    averageRFDByDistrict(housing)
    # pprint.pprint(housing.find({'district': 'Eixample'}).explain())
