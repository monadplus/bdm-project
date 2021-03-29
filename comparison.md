# Pros and Cons

When suggesting a storage/processing solution you should take into account several factors e.g. the data shape, the access pattern, the kind of processing, etc.

**Shape/Quantity**: one entity kind (Housing) with ~20 attributes but the most important ones are: district, neighborhood, size, price, rfd.  We will store a small amount of data ~10-20MB.

**Access Pattern**:

- For `P2` we will do some descriptive analysis i.e. describe/summarize the data using Spark and Tableau. This part will probably require projecting our data and do some aggregation operations on top of it.

- For `P3` we will do predictive analysis i.e. regression. We will use Spark MLLib. Regression will probably use the whole dataset (or at least most of the features) i.e. sequential scan. Not sure about what they want us to do with Kafka/Kibana since Kibana is a visualization tool for Elasticsearch and Kafka is an event-source streaming framework.

## HDFS

Storage. Sequential scans. Efficient append. Fault-tolerance. SequenceFile (horizontal, schemaless). Avro (Horizontal, has schema). Parquet (Hybrid, has schema, vertical then horizontal).

**Pros**

- Spark has native support for it (example with Parquet: https://sparkbyexamples.com/spark/spark-read-write-files-from-hdfs-txt-csv-avro-parquet-json/)
- For P2, HDFS is optimized for sequential scans and we will need to read the whole data in order to do summaries/aggregations over it. Spark can read from a single column using parquet (see https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) which is great for projections.
- Same for P3.
- The choice between SequenceFile and Parquet depends on the type of workload. I would suggest using both formats and evaluate its performance.

**Cons:**

- HDFS is not a data processing framework so it requires Spark/MapReduce/etc to do the actual processing.

## HBase

Key-value store. Wide column store. HDFS as storage layer. Indexed by row, column and timestamp key. Value is an uninterpreted array of bytes. Replication and fault-tolerance. Distributed. Key/Range search. Single row transaction. Zookeeper. Region and Family. Memstore. Storefile (format HFile) stored as SSTable.

**Pros**

- Spark has native support for it (https://github.com/nerdammer/spark-hbase-connector)
- Great for filtering by key but I do not see it being beneficial for our problem since most of our processing is based on the whole dataset being loaded in memory.

**Cons:**

- Overhead (manager, indexes, data structures) of HBase when HDFS is enough since most of the time we will be loading the set in memory and doing the filters in memory.

## MongoDB

Document store, collections, BSON, metadatada stored for fast query through MongoDB query language. No structure by default. Schema can be enforced. Replication and fault-tolerance. Real-time aggregations. Rich indexing. Transaction isolation/integrity/consistency on update. Read/Write requires n confirmations. Support for multi-document update (MongoDB 4.0). Horiz. fragmentation by range. Can be distributed.

**Pros**

- Spark has native support for it (https://github.com/raphaelbrugier/spark-mongo-example/blob/master/src/main/scala/com/github/rbrugier/MongoSparkMain.scala).
- It's a (limited) data processing framework by itself.
- Native support for json.

**Cons:**

- MongoDB is a limited data processing framework by itself. So adding Spark over it is kind of redundant and mongoDB is not well known for its storage architecture. I do not see us using mongoDB's queries and then running spark over these results since we can do the same over Spark and much more.

| MongoDB                   | HBase               |
|---------------------------|---------------------|
| Aggregation Pipeline      | MapReduce/Spark     |
| Document                  | Wide Column         |
| Data types                | Uninterpreted Bytes |
| Expressive Query Language | Key-Value           |

HBase is well suited to key-value workloads with high volume random read and write access patterns.
MongDB provides a much richer model similar to a RDBMS (data model, ACID transactions, rich query framework).

## Conclusion

In my opinion HDFS + Spark is the way to go. In a real project, I would use S3/Google Cloud Storage instead of HDFS since building and mainting your own cluster of HDFS is an overkill unless you need a very scalable storage system.

The question is which file format we use: SequenceFile or Parquet.
