#!/usr/bin/env python3

import json
import os

from mongoclient import CustomMongoClient
from os import _PathType

datasetsPath: _PathType = '../datasets/'
incomePath: _PathType = datasetsPath+'income_opendata/income_opendata_neighborhood.json'
lookupTablesDir: _PathType = datasetsPath+'lookup_tables/'

if __name__ == "__main__":
    client = CustomMongoClient()
    client.dropDB()

    with open(incomePath, 'r') as fp:
        objs = list(map(json.loads, fp.readlines()))
        client.store(objs, 'income')

    for path in os.listdir(lookupTablesDir):
       (fileName, ext) = os.path.splitext(path)
       if ext == '.json':
           with open(lookupTablesDir+path, 'r') as fp:
                objs = list(map(json.loads, fp.readlines()))
                client.store(objs, fileName)

    client.statistics()
