#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import os
import re
from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType


class SparkClient():
    """
    All datasets are cached in memory after loaded.
    """

    __isLoaded = False

    def __init__(self, num_processors=4):
        self.sparkSession = SparkSession \
            .builder \
            .master(f"local[{num_processors}]") \
            .appName("myApp") \
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

        self.sc = self.sparkSession.sparkContext

    def load(self):
        """This is computationally costly since loads all datasets in the cluster's memory."""
        if not self.__isLoaded:
            print('Make sure to have mongoDB running in localhost(test:1234)')
            print('Make sure mongoDB has the data loaded, otherwise run import.py')
            self.loadIdealista()
            self.loadIncome()
            self.loadLookup()
            self.__isLoaded = True

    def loadLookup(self):
        self.incomeDistrictRDD = self.__loadMongoRDD('income_lookup_district')
        self.incomeNeighborRDD = self.__loadMongoRDD('income_lookup_neighborhood')
        self.rentDistrictRDD = self.__loadMongoRDD('rent_lookup_district')
        self.rentNeighbordRDD = self.__loadMongoRDD('rent_lookup_neighborhood')

    def loadIncome(self):
        self.incomeRDD = self.__loadMongoRDD('income')

    def loadIdealista(self):
        def maxByDate(a, b) -> bool:
            if a.date > b.date: return a
            else: return b

        if not self.__isLoaded:
            dfs = [];
            idealistaDir = '../datasets/idealista/'
            for dirPath in os.listdir(idealistaDir):
                if os.path.isdir(idealistaDir+dirPath):
                    date = self.__parseDateFromName(dirPath)
                    df = self.sparkSession.read.parquet(idealistaDir+dirPath)
                    if "neighborhood" in df.columns:
                        if "district" in df.columns:
                            df = df \
                                .withColumn('date', lit(date)) \
                                .withColumn('distance', col('distance').cast(DoubleType())) \
                                .filter((col('municipality') == 'Barcelona') & (col('neighborhood').isNotNull()) & (col('district').isNotNull()))
                            dfs.append(df)

            rdd = self.sc.union(list(map(lambda df: df.rdd, dfs)))

            self.idealistaRDD = rdd \
                .map(lambda x: (x.propertyCode, x)) \
                .reduceByKey(maxByDate) \
                .map(lambda kv: kv[1]) \
                .cache()


    def __loadMongoRDD(self, collection):
        return self.sparkSession\
                   .read.format("com.mongodb.spark.sql.DefaultSource") \
                   .option('uri', f"mongodb://test:1234@127.0.0.1/test.{collection}?authSource=admin") \
                   .load() \
                   .rdd \
                   .cache()

    def __parseDateFromName(self,name):
        """Example: 2020_01_02_idealista"""
        str = re.compile("([\d]{4}_[\d]{2}_[\d]{2})").match(name).group(1).replace('_', '-')
        return datetime.datetime.strptime(str, '%Y-%m-%d')

# Number of listings per day
# You must average on the visualization tool
def kpi1(client: SparkClient):
    # Do not coalesce + write in a real cluster
    # since it will send all your data to the driver
    # and store it in its local filesystem.
    client.idealistaRDD \
      .map(lambda x: (x.date, 1)) \
      .reduceByKey(add) \
      .coalesce(1) \
      .sortByKey(ascending=True) \
      .toDF(['date', 'listings_count'])\
      .write \
      .csv('out/kp1.csv', header=True, mode='overwrite')

# Correlation of rent price and family income per neighborhood
# Output: district,neighbor,family,mean(price),mean(rfd)
def kpi2(client: SparkClient):
    return None

# TODO
#  * Load new dataset
#  * New query using the new dataset
def kpi3():
    return None

if __name__ == "__main__":
    client  = SparkClient(num_processors=4)
    client.load()
    kpi1(client)
    # kpi2()
    # kpi3()
