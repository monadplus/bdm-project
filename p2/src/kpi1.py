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

    dfs = [];
    for dirPath in os.listdir(idealistaDir):
        if os.path.isdir(idealistaDir+dirPath):
            df = spark.read.parquet(idealistaDir+dirPath)
            dfs.append(df)

    rdd = sc.union(list(map(lambda df: df.rdd, dfs)))
    print(rdd.count())
