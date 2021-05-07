#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import os
import datetime
from pyspark.sql.functions import lit
import re
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from operator import add

idealistaDir = '../datasets/idealista/'

def parseDateFromName(name):
    """Example: 2020_01_02_idealista"""
    str = re.compile("([\d]{4}_[\d]{2}_[\d]{2})").match(name).group(1).replace('_', '-')
    return datetime.datetime.strptime(str, '%Y-%m-%d')

def byMaxDate(a, b) -> bool:
    if a.date > b.date:
        return a
    else:
        return b


#### Average number of new listings per day ####
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
            date = parseDateFromName(dirPath)
            df = spark.read.parquet(idealistaDir+dirPath)
            if "neighborhood" in df.columns:
                if "district" in df.columns:
                    df = df \
                        .withColumn('date', lit(date)) \
                        .withColumn('distance', col('distance').cast(DoubleType())) \
                        .filter((col('municipality') == 'Barcelona') & (col('neighborhood').isNotNull()) & (col('district').isNotNull()))
                    dfs.append(df)

    rdd = sc.union(list(map(lambda df: df.rdd, dfs)))

    rdd = rdd \
      .map(lambda x: (x.propertyCode, x)) \
      .reduceByKey(byMaxDate) \
      .map(lambda kv: (kv[1].date, 1)) \
      .reduceByKey(add) \

    # Do not coalesce + write in a cluster since it will send all your data
    # to the drive and store it in its local filesystem.
    rdd \
      .coalesce(1) \
      .sortByKey(ascending=True) \
      .toDF(['date', 'listings_count'])\
      .write \
      .csv('out/kp1.csv', header=True, mode='overwrite')
