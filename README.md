## Installation
```bash
$ pip3 install -U -r requirements.txt
```

## MQTT client
### Environment
Please modify the `mqtt_client.py` file config variables to fit you Thingsboard info.

### Run program

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

### Environment
Please modify the `ws_client.py` file config variables to fit you Thingsboard info.

### Run program

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

### Environment
Please modify the `kafka_client.py` file config variables to fit you Thingsboard info.
```bash
$ wget https://raw.githubusercontent.com/thingsboard/thingsboard/v2.4.1/common/transport/transport-api/src/main/proto/transport.proto
$ protoc -I=./ --python_out=./ ./transport.proto
```

### Run program

```bash
$ python3 kafka_client.py
```

## Cassandra client

### Environment
Please modify the `cassandra_client.py` file config variables to fit you Thingsboard info.
```bash
```

### Run program

```bash
$ python3 cassandra_client.py
```
