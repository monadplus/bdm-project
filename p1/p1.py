#!/usr/bin/env python

import json
import os
import re
import csv
import pymongo
from mongoclient import getMongoClient
import happybase

# Datasets
idealistaPath = "../datasets/idealista"
opendatabcnPath = "../datasets/opendatabcn-income"
lookuptablesPath = "../datasets/lookup_tables"

# Idealista Keys
propertyCodeKey = 'propertyCode'
municipalityKey = 'municipality'
districtKey = 'district'
neighborhoodKey = 'neighborhood'
districtIdKey = 'district_id'
neighborhoodIdKey = 'neighborhood_id'

# OpenDataBcn Keys
nomDistricteKey = "Nom_Districte"
nomBarriKey = "Nom_Barri"
anyKey = "Any"
rfdKey = "Índex RFD Barcelona = 100"

def writeJSONToFile(obj, filename):
    with open(filename, 'w') as fp:
        json.dump(obj, fp)
    print(f'{filename} written successfully.')

def getDateFromFile(fileName):
    """Example: 2020_01_02_idealista.json"""
    return re.compile("([\d]{4}_[\d]{2}_[\d]{2})").match(fileName).group(1).replace('_', '-')

def getHousingDict():
    # To avoid duplicates
    housing = dict()
    lookup = getLookupTable('idealista_extended.csv')
    for fileName in os.listdir(idealistaPath):
        filePath = f'{idealistaPath}/{fileName}'
        if os.path.isfile(filePath) and filePath.endswith(".json"):
            with open(filePath, 'r') as fp:
                objs = json.load(fp)
                for obj in objs:
                    id = obj[propertyCodeKey]
                    obj['date'] = getDateFromFile(fileName)
                    # Some housings do not have these very important attributes
                    # We will discard them.
                    if all(key in obj for key in [propertyCodeKey, districtKey, neighborhoodKey]):
                        # Some housing are from other municipies but we do not have rfd value for them.
                        # We will discard them.
                        if obj[municipalityKey] == 'Barcelona':
                            (districtId, neighborhoodId) = lookup[(obj[districtKey], obj[neighborhoodKey])]
                            obj[districtIdKey] = districtId
                            obj[neighborhoodIdKey] = neighborhoodId
                            obj['distance'] = float(obj['distance'])
                            if id in housing:
                                old = housing[id]
                                # REVIEW older objects are removed
                                if old['date'] <= obj['date']:
                                    housing[id] = obj
                            else:
                                housing[id] = obj
    return housing

def getOpenDataBcnDict():
    opendatabcn = dict()
    lookup = getLookupTable('income_opendatabcn_extended.csv')
    for fileName in os.listdir(opendatabcnPath):
        filePath = f'{opendatabcnPath}/{fileName}'
        if os.path.isfile(filePath) and filePath.endswith(".csv"):
            with open(filePath, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    key = lookup[(row[nomDistricteKey], row[nomBarriKey])]
                    value = dict([(row[anyKey], float(row[rfdKey]))])
                    if key in opendatabcn:
                        opendatabcn[key].update(value)
                    else:
                        opendatabcn[key] = value
    # compute average
    from statistics import mean
    for value in opendatabcn.values():
        value['avg'] = mean(value.values())
    return opendatabcn

# Careful, some neighborhood names are repeated among districts.
def getLookupTable(filename):
    lookupTable = dict()
    with open(f'{lookuptablesPath}/{filename}', 'r') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        for row in reader:
            key = (row[districtKey], row[neighborhoodKey])
            lookupTable[key] = (row[districtIdKey], row[neighborhoodIdKey])

    return lookupTable

def reconciliate(housing, opendatabcn):
    for value in housing.values():
        districtId = value[districtIdKey]
        neighborhoodId = value[neighborhoodIdKey]
        key = (districtId, neighborhoodId)
        value['rfd'] = opendatabcn[key]

def storeToMongoDB(housing, opendatabcn):
    houses = list(housing.values())
    client = getMongoClient()
    db = client['test']
    # housing
    collection = db['housing']
    collection.drop()
    collection.insert_many(houses)
    collection.create_index([('district', pymongo.ASCENDING), ('neighborhood', pymongo.ASCENDING)])
    # rfd
    collection = db['rfd']
    collection.drop()
    for (k, v) in opendatabcn.items():
        (district, neighborhood) = k
        data = {'district': district
                , 'neighborhood': neighborhood
                , 'rfd': v['avg']
                }
        collection.insert_one(data)
    collection.create_index([('district', pymongo.ASCENDING), ('neighborhood', pymongo.ASCENDING)])

def storeToHBase(housing, opendatabcn):
    host = os.getenv('THRIFT_HOST') or 'hbase-docker' # localhost
    port = int(os.getenv('THRIFT_PORT') or '9090') # 49167

    def deleteAllTables(conn: happybase.Connection) -> None:
        for table in conn.tables():
            conn.disable_table(table)
            conn.delete_table(table)
            print("Deleted: " + table.decode('utf-8'))

    def printRowCount(table: happybase.Table) -> None:
        print("Number of rows in {}: {}".format(table.name.decode('utf-8'), len(list(table.scan()))))

    conn = happybase.Connection(host, port)

    deleteAllTables(conn)

    conn.create_table('housing', {'cf1': dict(max_versions=1), 'cf2': dict(max_versions=1)})
    table = conn.table('housing')
    with table.batch() as b:
        for k,v in housing.items():
            # TODO store int/floats in a more efficient way
            #      smore fields where omitted
            d = { 'cf1:price': str(v['price'])
                  , 'cf1:rfd_avg': str(v['rfd']['avg'])
                  , 'cf2:district': v['district']
                  , 'cf2:neighborhood': v['neighborhood']
                  , 'cf2:date': v['date']
                  # Missing
                  # , 'cf2:floor': v['floor']
                  , 'cf2:propertyType': v['propertyType']
                  , 'cf2:status': v['status']
                  , 'cf2:size': str(v['size'])
                  , 'cf2:rooms': str(v['rooms'])
                  , 'cf2:bathrooms': str(v['bathrooms'])
                  , 'cf2:latitude': str(v['latitude'])
                  , 'cf2:longitude': str(v['longitude'])
                  , 'cf2:distance': str(v['distance'])
                  , 'cf2:newDevelopment': str(v['newDevelopment'])
                  # Missing
                  # , 'cf2:hasLift': str(v['hasLift'])
                  , 'cf2:priceByArea': str(v['priceByArea'])
                  }
            b.put(k, d)
    printRowCount(table)

    conn.create_table('rfd', {'cf1': dict(max_versions=1)})
    table = conn.table('rfd')
    with table.batch() as b:
        for (district, neighborhood), v in opendatabcn.items():
            d = { 'cf1:rfd_avg': str(v['avg']) }
            # Split by (district, neighborhood) = key.split('-')
            b.put(f'{district}-{neighborhood}', d)
    printRowCount(table)

if __name__ == "__main__":
    housing = getHousingDict()
    opendatabcn = getOpenDataBcnDict()
    reconciliate(housing, opendatabcn)
    # writeJSONToFile(housing, 'housing.json')
    # storeToMongoDB(housing, opendatabcn)
    storeToHBase(housing, opendatabcn)
    print('Finished successfully')
