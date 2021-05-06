#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[4]") \
    .appName("BDM") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config("spark.mongodb.input.uri", "mongodb://test:1234@127.0.0.1/test.income?authSource=admin") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()
df.show()

collections = ["income_lookup_neighborhood",
               "rent_lookup_district",
               "rent_lookup_neighborhood",
               "income_lookup_district"]
for coll in collections:
    df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
           .option('uri', f"mongodb://test:1234@127.0.0.1/test.{coll}?authSource=admin") \
           .load()
    df.printSchema()
    df.show()
