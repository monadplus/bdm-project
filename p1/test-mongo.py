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
    pipeline = [
        { "$project": { "district": "$district",
                        "neighborhood": "$neighborhood",
                        "rfds": {"$objectToArray": "$rfd"}
                      }
        },
        { "$unwind": "$rfds"},
        { "$group":
          {
            # "_id": {"district": "$district",
            #         "neighborhood": "$neighborhood"
            #        },
            "_id": "$district",
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
    print('Documents: {}'.format(housing.count_documents({})))
    print('\n=== Count by district ===\n')
    countByDistrict(housing)
    print('\n=== RFD by district ===\n')
    averageRFDByDistrict(housing)
