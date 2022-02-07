## Installation
```bash
$ pip3 install -U -r requirements.txt
```

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
               |_______________|         |____________________________|    ______________v_____________
                       |                              ^                    |                          |
               ________v_________                     |                    | IoT Hub Websocket server |
               |   background   |---------------------|                    |__________________________|
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
