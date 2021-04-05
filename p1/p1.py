#!/usr/bin/env python

import json
import os
import re
import csv
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

def loadHousingMongo(housing, opendatabcn):
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

if __name__ == "__main__":
    housing = getHousingDict()
    opendatabcn = getOpenDataBcnDict()
    reconciliate(housing, opendatabcn)
    # writeJSONToFile(housing, 'housing.json')
    loadHousingMongo(housing, opendatabcn)
    print('Finished successfully')
