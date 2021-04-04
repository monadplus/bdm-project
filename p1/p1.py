#!/usr/bin/env python

import json
import os
import re
import csv

# Datasets
idealistaPath = "../datasets/idealista"
opendatabcnPath = "../datasets/opendatabcn-income"
lookuptablesPath = "../datasets/lookup_tables"

# Idealista Keys
propertyCodeKey = 'propertyCode'
districtKey = 'district'
neighborhoodKey = 'neighborhood'

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
    housing = dict()
    for fileName in os.listdir(idealistaPath):
        filePath = f'{idealistaPath}/{fileName}'
        if os.path.isfile(filePath) and filePath.endswith(".json"):
            with open(filePath, 'r') as fp:
                objs = json.load(fp)
                for obj in objs:
                    id = obj[propertyCodeKey]
                    obj['date'] = getDateFromFile(fileName)
                    if all(key in obj for key in [propertyCodeKey, districtKey, neighborhoodKey]):
                        if id in housing:
                            old = housing[id]
                            # Older objects are removed
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
                    #key = row[nomDistricteKey] + "-" + row[nomBarriKey]
                    key = (row[nomDistricteKey], row[nomBarriKey])
                    value = dict([(row[anyKey], row[rfdKey])])
                    if key in opendatabcn:
                        opendatabcn[key].update(value)
                    else:
                        opendatabcn[key] = value
    return opendatabcn

# TODO
def getLookupTableDict():
    pass

# TODO
def reconciliate(housing, opendatabcn, lookuptable):
    pass

if __name__ == "__main__":
    housing = getHousingDict()
    opendatabcn = getOpenDataBcnDict()
    lookuptable = getLookupTableDict()
    housingWithRfd = reconciliate(housing, opendatabcn, lookuptable)
    writeJSONToFile(housingWithRfd, 'housing.json')
