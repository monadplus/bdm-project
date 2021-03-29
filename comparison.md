# Pros and Cons

**HDFS**: Storage. Sequential scans. Efficient append. Fault-tolerance. SequenceFile (horizontal, schemaless). Avro (Horizontal, has schema). Parquet (Hybrid, has schema, vertical then horizontal).

**HBase**: key-value store. Wide column store. HDFS as storage layer. Indexed by row, column and timestamp key. Value is an uninterpreted array of bytes. Replication and fault-tolerance. Distributed. Key/Range search. Single row transaction. Zookeeper. Region and Family. Memstore. Storefile (format HFile) stored as SSTable.

**MongoDB**: document store, collections, BSON, metadatada stored for fast query through MongoDB query language. No structure by default. Schema can be enforced. Replication and fault-tolerance. Real-time aggregations. Rich indexing. Transaction isolation/integrity/consistency on update. Read/Write requires n confirmations. Support for multi-document update (MongoDB 4.0). Horiz. fragmentation by range. Can be distributed.

## MongoDB vs HBase

| MongoDB                   | HBase               |
|---------------------------|---------------------|
| Aggregation Pipeline      | MapReduce/Spark     |
| Document                  | Wide Column         |
| Data types                | Uninterpreted Bytes |
| Expressive Query Language | Key-Value           |

HBase is well suited to key-value workloads with high volume random read and write access patterns. MongDB provides a much richer model similar to a RDBMS (data model, ACID transactions, rich query framework).
