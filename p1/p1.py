#!/usr/bin/env python

import csv
import json
import os
import re
from typing import Optional, Tuple

# Hbase
import happybase

# MongoDB
import pymongo
from mongoclient import getMongoClient

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
poblacioKey = "Població"
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
    for fileName in os.listdir(opendatabcnPath):
        filePath = f'{opendatabcnPath}/{fileName}'
        if os.path.isfile(filePath) and filePath.endswith(".csv"):
            with open(filePath, 'r') as csvfile:
                reader = csv.DictReader(csvfile, delimiter=',')
                for row in reader:
                    key = (row[nomDistricteKey], row[nomBarriKey], row[anyKey])
                    value = {'population': row[poblacioKey], 'rfd': float(row[rfdKey])}
                    opendatabcn[key] = value
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

class LookupTables():
    def __init__(self, idealistaToOpen, openToIdealista):
        self.idealistaToOpen = idealistaToOpen
        self.openToIdealista = openToIdealista

def getLookupTables() -> LookupTables:
    def getKey(d, value) -> Optional[Tuple[str, str]]:
        for k, v in d.items():
            if v == value:
                return k
        return None

    def link(fromm, to):
        d = dict()
        for k,v in fromm.items():
            v2 = getKey(to, v)
            if v2 is None:
                print('Missing: ', v)
            else:
                d[k] = v2
        return d

    lookupIdealista = getLookupTable('idealista_extended.csv')
    lookupOpen      = getLookupTable('income_opendatabcn_extended.csv')
    idealistaToOpen = link(lookupIdealista, lookupOpen)
    openToIdealista = link(lookupOpen, lookupIdealista)
    return LookupTables(idealistaToOpen, openToIdealista)

# TODO update to the new model
# def storeToMongoDB(housing, opendatabcn):
#     houses = list(housing.values())
#     client = getMongoClient()
#     db = client['test']
#     # housing
#     collection = db['housing']
#     collection.drop()
#     collection.insert_many(houses)
#     collection.create_index([('district', pymongo.ASCENDING), ('neighborhood', pymongo.ASCENDING)])
#     # rfd
#     collection = db['rfd']
#     collection.drop()
#     for (k, v) in opendatabcn.items():
#         (district, neighborhood) = k
#         data = {'district': district
#                 , 'neighborhood': neighborhood
#                 , 'rfd': v['avg']
#                 }
#         collection.insert_one(data)
#     collection.create_index([('district', pymongo.ASCENDING), ('neighborhood', pymongo.ASCENDING)])

def storeToHBase(housing, opendatabcn, lookupTables: LookupTables) -> None:
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

    conn.create_table('housing', {  'cf1': dict(max_versions=1)
                                  , 'cf2': dict(max_versions=1)
                                  , 'cf3': dict(max_versions=1)
                                  , 'cf4': dict(max_versions=1)})
    table = conn.table('housing')
    with table.batch() as b:
        for k,v in housing.items():
            d = {
                  # Optimize query: correlation price/RFD
                    'cf1:district': v['district']
                  , 'cf1:neighborhood': v['neighborhood']
                  , 'cf1:price': str(v['price'])

                  # Optimize query: Average number of new listings per day.
                  , 'cf2:date': v['date']

                  # Geolocation
                  , 'cf3:latitude': str(v['latitude'])
                  , 'cf3:longitude': str(v['longitude'])

                  # the rest
                  , 'cf4:propertyType': v['propertyType']
                  , 'cf4:status': v['status']
                  , 'cf4:size': str(v['size'])
                  , 'cf4:rooms': str(v['rooms'])
                  , 'cf4:bathrooms': str(v['bathrooms'])
                  , 'cf4:distance': str(v['distance'])
                  , 'cf4:newDevelopment': str(v['newDevelopment'])
                  , 'cf4:priceByArea': str(v['priceByArea'])

                  # Missing values
                  # , 'cf4:floor': v['floor']
                  # , 'cf4:hasLift': str(v['hasLift'])
                  }
            b.put(k, d)
    printRowCount(table)

    conn.create_table('opendatabcn', {'cf1': dict(max_versions=1)})
    table = conn.table('opendatabcn')
    with table.batch() as b:
        for (district, neighborhood, year), v in opendatabcn.items():
            d = { 'cf1:rfd':  str(v['rfd'])
                , 'cf1:population':  str(v['population'])
                }
            b.put(f'{district}-{neighborhood}-{year}', d)
    printRowCount(table)

    def createLookupTable(conn, name, d):
        conn.create_table(name, {'cf1': dict(max_versions=1)})
        table = conn.table(name)
        with table.batch() as b:
            for (originDistr, originHood), (destDistr, destHood) in d.items():
                b.put(f'{originDistr}-{originHood}'
                        , { 'cf1:district':  destDistr, 'cf1:neighborhood':  destHood})
        printRowCount(table)

    createLookupTable(conn, 'idealista-to-open', lookupTables.idealistaToOpen)
    createLookupTable(conn, 'open-to-idealista', lookupTables.openToIdealista)

if __name__ == "__main__":
    housing = getHousingDict()
    opendatabcn = getOpenDataBcnDict()
    lookupTables = getLookupTables()
    storeToHBase(housing, opendatabcn, lookupTables)
    # writeJSONToFile(housing, 'housing.json')
    # storeToMongoDB(housing, opendatabcn)
    print('Finished successfully')
