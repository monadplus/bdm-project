#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import datetime
import os
import re
from operator import add

from pyspark.sql import SparkSession
from pyspark import RDD, SparkContext
from pyspark.sql.functions import col, lit
from pyspark.sql.types import DoubleType
from typing import Tuple
from pyspark import StorageLevel

class SparkClient():

    """
    All datasets are cached in memory after loaded in order to reuse them through the queries.
    """

    sparkSession: SparkSession

    sc: SparkContext

    # Idealista
    idealistaRDD: RDD

    # OpenDataBcn
    incomeRDD: RDD

    # Lookup Table
    incomeNeighborRDD: RDD
    rentNeighbordRDD : RDD
    # incomeDistrictRDD: RDD
    # rentDistrictRDD : RDD

    # Load RDDs once.
    __isLoaded: bool = False

    def __init__(self, num_processors: int = 4):
        # For more options: https://spark.apache.org/docs/latest/configuration.html
        self.sparkSession = SparkSession \
            .builder \
            .master(f"local[{num_processors}]") \
            .appName("myApp") \
            .config('spark.driver.memory', '16g')\
            .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
            .getOrCreate()

        self.sc = self.sparkSession.sparkContext

    def load(self) -> None:
        """This is computationally costly since loads all datasets in the cluster's memory."""
        if not self.__isLoaded:
            print('-'*40)
            print('Make sure to have mongoDB running in localhost(test:1234)')
            print('Make sure mongoDB has the data loaded, otherwise run import.py')
            print('-'*40)
            self.loadIdealista()
            self.loadIncome()
            self.loadLookup()
            self.__isLoaded = True
        print('Data loaded.')

    def loadLookup(self) -> None:
        # self.incomeDistrictRDD = \
        #     self.__loadMongoRDD('income_lookup_district').cache()
        #
        # self.rentDistrictRDD = \
        #     self.__loadMongoRDD('rent_lookup_district').cache()

        self.incomeNeighborRDD = \
            self.__loadMongoRDD('income_lookup_neighborhood') \
                .map(lambda x: (x['neighborhood'], x['neighborhood_reconciled'])) \
                .cache()

        self.rentNeighbordRDD = \
            self.__loadMongoRDD('rent_lookup_neighborhood') \
                .map(lambda x: (x['ne'], x['ne_re'])) \
                .cache()

    def loadIncome(self) -> None:
        self.incomeRDD = self.__loadMongoRDD('income').cache()

    def loadIdealista(self) -> None:
        def maxByDate(a, b) -> bool:
            if a.date > b.date: return a
            else: return b

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

    def __loadMongoRDD(self, collection: str) -> SparkSession:
        return self.sparkSession\
                   .read.format("mongo") \
                   .option('uri', f"mongodb://test:1234@127.0.0.1/test.{collection}?authSource=admin") \
                   .option('mongodb.keep_alive_ms', '50000') \
                   .load() \
                   .rdd

    def __parseDateFromName(self, name: str) -> datetime:
        """Example: 2020_01_02_idealista"""
        str = re.compile("([\d]{4}_[\d]{2}_[\d]{2})").match(name).group(1).replace('_', '-')
        return datetime.datetime.strptime(str, '%Y-%m-%d')

# Number of listings per day
# You must average on the visualization tool
def kpi1(client: SparkClient) -> None:
    # Do not coalesce + write in a real cluster
    # since it will send all your data to the driver
    # and store it in its local filesystem.
    rdd = client.idealistaRDD \
      .map(lambda x: (x.date, 1)) \
      .reduceByKey(add) \
      .cache()

    rdd \
      .coalesce(1) \
      .sortByKey(ascending=True) \
      .toDF(['date', 'listings_count'])\
      .write \
      .csv('out/kp1.csv', header=True, mode='overwrite')

    # Average #listings per day
    (sum, n) = rdd.map(lambda kv: (kv[1], 1)).reduce(lambda a,b: (a[0] + b[0], a[1] + b[1]))
    print(f'Average number of listings per day: {sum/n}')

# Correlation of rent price and family income per neighborhood
# Output: district,neighbor,family,mean(price),mean(rfd)
def kpi2(client: SparkClient) -> None:
    def f(x: Tuple[str, Tuple[float, str]]) -> Tuple[str, float]:
        (_, (price, ne_re)) = x
        return (ne_re, price)

    def latestRFD(x):
        x.sort(reverse=True, key=lambda x: x['year'])
        return x[0]['RFD']

    # BOGUS: without this collect, the last stage on rdd1 loops forever.
    #        we have been investigating this problem with no clue.
    client.rentNeighbordRDD.collect()

    # RDD[(neigh: str, mean_price: float)]
    rdd1 = client.idealistaRDD \
            .map(lambda x: (x['neighborhood'], (x['price'], 1))) \
            .reduceByKey(lambda a,b: (a[0] + b[0], a[1] + b[1])) \
            .mapValues(lambda x: x[0]/x[1]) \
            .join(client.rentNeighbordRDD) \
            .map(f)

    # RDD[(neigh: str, RFD: float)]
    rdd2 = client.incomeRDD \
                 .map(lambda x: (x['neigh_name'], latestRFD(x['info']))) \
                 .join(client.incomeNeighborRDD) \
                 .map(f)

    # Merge data and save to local disk (do not do this in production).
    # Note: some strings are quoted (probably .csv does this)
    rdd1.join(rdd2) \
        .coalesce(1) \
        .map(lambda kv: (kv[0], kv[1][0], kv[1][1])) \
        .toDF(['neighborhood', 'mean_price', 'rfd'])\
        .write \
        .csv('out/kp2.csv', header=True, mode='overwrite')

# TODO
#  * Load new dataset
#  * New query using the new dataset
def kpi3(client: SparkClient) -> None:
    return None

if __name__ == "__main__":
    client  = SparkClient(num_processors=8)
    client.load()
    kpi1(client)
    # kpi2(client)
    # kpi3(client)
