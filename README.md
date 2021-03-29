# Big Data Management: Project

## P1: Data Design

Propose storage, data model and structure for each dataset. Candidates:
- HDFS + file format(SequenceFile, Avro, Parquet)
- HBase
- MongoDB

Document:
- For each dataset, propose design and data storage.
- Present a high-level diagram of the data transformations.

Code:
- ETL for the proposed design

## P2: Descriptive Analysis

- Data integration and reconciliation
- Technologies: Apache Spark (core/sql) and visualization tool (e.g. Tableau)

## P3: Predictive Analysis

- Distributed machine learning and real-time data prediction
- Technologies: Apache Spark (mllib), Apache Kafka, visualization tool (e.g. Kibana)

## HDFS: docker

Follow instructions from https://github.com/big-data-europe/docker-hadoop

## Libs

- Apache Parquet (HDFS support): https://github.com/mjakubowski84/parquet4s#quick-start
- Spark Core: https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/sql
- Spark MLLib: https://github.com/apache/spark/tree/master/examples/src/main/scala/org/apache/spark/examples/mllib
- Tableau: https://help.tableau.com/current/pro/desktop/en-us/examples_sparksql.htm
- Kafka: https://github.com/cakesolutions/scala-kafka-client
- Kibana can't query Spark directly.
