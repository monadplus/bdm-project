# P1: Data Design

This part is divided in three scripts:

- `main.py`: the main script, ingest and transforms the data from `datasets` and then stores it in _hbase_. 
- `test-hbase.py`: for testing hbase.

## How-to

We prepared a [Docker Compose](https://docs.docker.com/compose/install/) for hbase and mongodb.

First run hbase docker image:

``` sh
cd hbase-docker
chmod +x start-hbase.sh
# You may need to run this as sudo
./start-hbase.sh
```

This script will run a standalone dockerized version of hbase.
The output should be similar to this one:

``` sh
start-hbase.sh: Starting HBase container
start-hbase.sh: Container has ID 80020f6bfab452c2b033f38cbcea9c748348254e42164a840cc69a7850cef7d4
start-hbase.sh: /etc/hosts already contains hbase-docker hostname and IP
start-hbase.sh: Connect to HBase at localhost on these ports
  REST API        127.0.0.1:49169
  REST UI         http://127.0.0.1:49168/
  Thrift API      127.0.0.1:49167
  Thrift UI       http://127.0.0.1:49166/
  Zookeeper API   127.0.0.1:49170
  Master UI       http://127.0.0.1:49165/

start-hbase.sh: OR Connect to HBase on container hbase-docker
  REST API        hbase-docker:8080
  REST UI         http://hbase-docker:8085/
  Thrift API      hbase-docker:9090
  Thrift UI       http://hbase-docker:9095/
  Zookeeper API   hbase-docker:2181
  Master UI       http://hbase-docker:16010/

start-hbase.sh: For docker status:
start-hbase.sh: $ id=80020f6bfab452c2b033f38cbcea9c748348254e42164a840cc69a7850cef7d4
start-hbase.sh: $ docker inspect $id
```

Now, you can run the _ETL_ script `main.py`:

``` sh
# Replace <host> and <port> by the values given from `start-hbase.sh`
THRIFT_HOST=<host> THRIFT_PORT=<port> ./main.py
```

Finally, you can test hbase data running some queries:

``` sh
# Replace <host> and <port> by the values given from `start-hbase.sh`
THRIFT_HOST=<host> THRIFT_PORT=<port> ./test-hbase.py
```

Don't forget to stop the containers

``` sh
cd hbase-docker
# You may need to run this as sudo
./stop-hbase.sh
```

### MongoDB

> DEPRECATED !

Our first choice was _MongoDB_ and so we prepared the same code that we did for _Hbase_.
In order to store the datasets in mongo you need to:

``` sh
# Start standalone mongodb
cd mongodb
docker-compose up -d

# In main.py you need to uncoment the line
storeToMongoDB(housing, opendatabcn)

# Now you can run the script
./main.py
./test-mongo.py
```
