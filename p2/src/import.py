#!/usr/bin/env python3

import json
import os
import csv

from mongoclient import CustomMongoClient

datasetsPath = '../datasets/'
incomePath = datasetsPath+'income_opendata/income_opendata_neighborhood.json'
ocupacioPath = datasetsPath + 'inhabitants_opendata/inhabitants.csv'
lookupTablesDir = datasetsPath+'lookup_tables/'

if __name__ == "__main__":
    client = CustomMongoClient()
    client.dropDB()

    # Income
    with open(incomePath, 'r') as fp:
        objs = list(map(json.loads, fp.readlines()))
        client.store(objs, 'income')

    # Extra dataset
    with open(ocupacioPath, 'r', encoding='utf-8') as fp:
        reader = csv.DictReader(fp)
        objs = list(reader)
        client.store(objs, 'inhabitants')

    # Lookup Table
    for path in os.listdir(lookupTablesDir):
       (fileName, ext) = os.path.splitext(path)
       if ext == '.json':
           with open(lookupTablesDir+path, 'r') as fp:
                objs = list(map(json.loads, fp.readlines()))
                client.store(objs, fileName)

    # Display what is in mongoDB
    client.statistics()
