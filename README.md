## Environment
```
$ python3 --version
Python 3.10.8
```

## Installation
```bash
$ pip3 install -U -r requirements.txt
```

## MQTT client
### Example
Please modify the `mqtt_client.py` file config variables to fit you Thingsboard info.
```bash
$ python3 mqtt_client.py
```

```txt
               _________________           ______________________________
               |               |  publish  |                            |
               |  mqtt client  | ========> |  Thingsboard MQTT server.  |
               |               | <======== |                            |
               |_______________| subscribe |____________________________|
```

#### Reference
* https://thingsboard.io/docs/user-guide/access-token/
* https://thingsboard.io/docs/user-guide/certificates/

## Websocket client

### Example
Please modify the `ws_client.py` file config variables to fit you Thingsboard info.
```bash
$ python3 ws_client.py
```

### Check swagger
`http://HOST:PORT/apidocs`

### Data Flow

```txt
               _________________         ______________________________
data query     |               |  queue  |  history time series data  |
==============>|  http server  |========>|  subsribe asyncio worker   |------------------|
               |_______________|         |____________________________|    ______________v________________
                       |                              ^                    |                             |
               ________v_________                     |                    | Thingsboard Websocket server|
               |   background   |---------------------|                    |_____________________________|
               |  asyncio loop  |        ______________________________                  ^
               |________________|------->|  latest time series data   |                  |
                                         |  subsbribe asyncio worker  |------------------|
                                         |____________________________|
```

#### Reference
* https://thingsboard.io/docs/user-guide/telemetry/#websocket-api
* https://github.com/thingsboard/thingsboard/blob/v3.1.1/application/src/test/java/org/thingsboard/server/controller/BaseWebsocketApiTest.java

#### TODO
* make history query by day into multiple partition limited query to iothub to prevent database performance issue

## Kafka client

### Example
Please modify the `kafka_client.py` file config variables to fit you Thingsboard info.
```bash
$ wget https://raw.githubusercontent.com/thingsboard/thingsboard/v2.4.1/common/transport/transport-api/src/main/proto/transport.proto
$ protoc -I=./ --python_out=./ ./transport.proto
$ python3 kafka_client.py
```

## Cassandra

### Cassandra Example
Please modify the `cassandra_client.py` file config variables to fit you Thingsboard info.
```bash
$ python3 cassandra_client.py
```

### Spark Cassandra Example
```
$ pyspark --version
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.2.0
      /_/
                        
Using Scala version 2.12.15, OpenJDK 64-Bit Server VM, 11.0.16.1
spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.2.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions spark-cassandra.py
```

