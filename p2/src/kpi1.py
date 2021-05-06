#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import os

idealistaDir = '../datasets/idealista/'

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[4]") \
        .appName("BDM") \
        .getOrCreate()

    sc = spark.sparkContext

    for dirPath in os.listdir(idealistaDir):
       if os.path.isdir(idealistaDir+dirPath):
         for path in os.listdir(idealistaDir+dirPath):
           (fileName, ext) = os.path.splitext(path)
           if ext == '.parquet':
                df = spark.read.parquet(idealistaDir+dirPath+"/"+path)
                df.select(df.address).show()
