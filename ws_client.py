import os
import math
from datetime import datetime
import time
import logging
import threading
import asyncio
import queue
import json
import urllib
from typing import List, Dict, Tuple

import websockets
import requests
from flask import Flask, jsonify, g
from flasgger import Swagger

TB_HOST = 's://HOST' # prefix: 'http' / 'ws' + 's' = 'https' / 'wss'
TB_USERNAME = 'USERNAME' # thingsboard username
TB_PWD = 'PASSWORD' # thingsboard password
KEYS = 'voltage,current' # keys ae seperated by ','
ENTITY_IDS = [
    "079ac380-5d51-11ec-bcb2-e13346ea26cf",
    "152abfa0-5d51-11ec-a06d-bfc2e316b768"
]
DAILY_DATA_POINT_LIMIT = 86400 * len(KEYS.split(','))
FLASK_HOST = '0.0.0.0'
FLASK_PORT = 5000

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s %(message)s')

app = Flask(__name__)
swagger = Swagger(app)
req_queue = queue.Queue()


@app.route('/fetch/<start_date>/<end_date>/')
def fetch(start_date, end_date):
    """Start a fetch data source task from specific date,
       then subsribe as telementry from IoT Hub
    ---
    parameters:
      - name: start_date
        in: path
        type: string
        required: true
        default: '2021-12-7'
      - name: end_date
        in: path
        type: string
        required: true
        default: '2021-12-7'
    responses:
      200:
        description: should started a task from start_date 00:00:00 ~ end_date 23:59:59
    """
    try:
        dt0 = datetime.strptime(start_date, '%Y-%m-%d')
        dt0 = dt0.replace(hour=0, minute=0, second=0)
        st0 = dt0.strftime('%Y-%m-%d %H:%M:%S')
        dt1 = datetime.strptime(end_date, '%Y-%m-%d')
        dt1 = dt1.replace(hour=23, minute=59, second=59)
        st1 = dt1.strftime('%Y-%m-%d %H:%M:%S')
        #loop1 = asyncio.new_event_loop()
        #asyncio.set_event_loop(loop1)
        #loop1.run_until_complete(asyncio.gather(fetch1(1)))
        data = (int(dt0.timestamp() * 1000), int(dt1.timestamp() * 1000))
        global req_queue
        req_queue.put(data)

        if dt1 < dt0:
            return {'message': 'please specify a valid time range'}
        return {'message': 'started a {} ~ {} task'.format(st0, st1)}
    except Exception as ex:
        return {'message': str(ex)}


class ThingsboardRestClient():

    def __init__(self, username='', password=''):
        self.username = username
        self.password = password
        self.tb_token = ''

    def get_auth_headers(self) -> Dict:
        return {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'X-Authorization': 'Bearer {}'.format(self.tb_token)
        }

    def login(self, tb_url='http://tb:8080') -> Tuple[str, int]:
        # login to IOT Hub
        data = {
            "username": self.username,
            "password": self.password
        }
        message, status_code = '', 200
        r = requests.post('{}/auth/login'.format(tb_url), json=data)
        status_code = r.status_code
        if status_code == 200:
            self.tb_token = r.json()['token']
        else:
            message = r.json()['message']
        return message, status_code


def get_all_entities_history_cmd_topic(start_ts, end_ts, limit):
    '''
    history topic example
    {
        "tsSubCmds": [],
        "historyCmds": [
            {
                "entityType": "DEVICE",
                "entityId": "152abfa0-5d51-11ec-a06d-bfc2e316b768",
                "keys": "current",
                "startTs": 1638845703803,
                "endTs": 1638932103803,
                "interval": 1000,
                "limit": 200,
                "agg": "NONE",
                "cmdId": 4 # subscriptionId in response
            }
        ],
        "attrSubCmds": []
    }
    '''
    obj = {
        "tsSubCmds": [],
        "historyCmds": [],
        "attrSubCmds": []
    }
    for idx in range(len(ENTITY_IDS)):
        d = {
            "entityType": "DEVICE",
            "entityId": ENTITY_IDS[idx],
            "keys": KEYS,
            "startTs": start_ts,
            "endTs": end_ts,
            "interval": 1000,
            "limit": limit,
            "agg": "NONE",
            "cmdId": idx # subscriptionId in response
        }
        obj['historyCmds'].append(d)
    topic = json.dumps(obj)
    return topic


def get_all_entities_latest_cmd_topic():
    '''
    latest topic example
    {
        "tsSubCmds": [
            {
                "entityType": "DEVICE",
                "entityId": "079ac380-5d51-11ec-bcb2-e13346ea26cf",
                "scope": "LATEST_TELEMETRY",
                "keys": keys,
                "cmdId": 1 # subscriptionId in response
            },
            {
                "entityType": "DEVICE",
                "entityId": "152abfa0-5d51-11ec-a06d-bfc2e316b768",
                "scope": "LATEST_TELEMETRY",
                "keys": keys,
                "cmdId": 2
            }
        ],
        "historyCmds": [],
        "attrSubCmds": []
    }
    '''
    obj = {
        "tsSubCmds": [],
        "historyCmds": [],
        "attrSubCmds": []
    }
    for idx in range(len(ENTITY_IDS)):
        d = {
            "entityType": "DEVICE",
            "entityId": ENTITY_IDS[idx],
            "scope": "LATEST_TELEMETRY",
            "keys": KEYS,
            "cmdId": idx # subscriptionId in response
        }
        obj['tsSubCmds'].append(d)
    topic = json.dumps(obj)
    return topic


async def subscribe_history_worker():
    logging.info('start subscribe history')
    global req_queue
    while True:
        try:
            (start_ts, end_ts) = req_queue.get(timeout=1)
        except Exception as ex:
            pass
        else:
            limit = math.ceil((end_ts - start_ts) / 86400000) * DAILY_DATA_POINT_LIMIT
            client = ThingsboardRestClient(TB_USERNAME, TB_PWD)
            message, status_code = client.login(f'http{TB_HOST}/api')
            if status_code == 200:
                token = client.tb_token
                try:
                    uri = f'ws{TB_HOST}/api/ws/plugins/telemetry?token={token}'
                    async with websockets.connect(uri) as websocket:
                        topic = get_all_entities_history_cmd_topic(start_ts,
                                end_ts, limit)
                        logging.info('subscribe history topic: {}'.format(topic))
                        await websocket.send(topic)
                        msg = await websocket.recv()
                        logging.info(msg)
                        '''
                        {
                          "subscriptionId": 7,
                          "errorCode": 0,
                          "errorMsg": null,
                          "data": {
                            "current": [
                              [
                                1638801910000, // timestamp (ms)
                                "5"
                              ],
                              [
                                1638800110000, // timestamp (ms)
                                "6"
                              ]
                            ]
                          },
                          "latestValues": {
                            "current": 1638773110000
                          }
                        }
                        '''
                except Exception as ex:
                    logging.info(str(ex))
                finally:
                    pass
            else:
                # TODO handle login failure
                pass
        await asyncio.sleep(1)


async def subscribe_latest_worker():
    logging.info('start subscribe latest')
    while True:
        client = ThingsboardRestClient(TB_USERNAME, TB_PWD)
        message, status_code = client.login(f'http{TB_HOST}/api')
        if status_code == 200:
            token = client.tb_token
            try:
                uri = f'ws{TB_HOST}/api/ws/plugins/telemetry?token={token}'
                async with websockets.connect(uri) as websocket:
                    topic = get_all_entities_latest_cmd_topic()
                    logging.info('subscribe latest topic: {}'.format(topic))
                    await websocket.send(topic)
                    while True:
                        msg = await websocket.recv()
                        logging.info(msg)
                        '''
                        {
                            "subscriptionId": 2,
                            "errorCode": 0,
                            "errorMsg": null,
                            "data": {
                                "current": [
                                    [
                                        1638801910000, // timestamp (ms)
                                        "5"
                                    ]
                                ],
                                "voltage": [
                                    [
                                        1638801910000, // timestamp (ms)
                                        "3305"
                                    ]
                                ]
                            },
                            "latestValues": {
                                "voltage": 1638801910000,
                                "current": 1638801910000
                            }
                        }
                        '''
            except Exception as ex:
                logging.info(str(ex))
            finally:
                pass
        else:
            logging.info(message)
        await asyncio.sleep(1) # time.sleep(5)

def start():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    asyncio.ensure_future(subscribe_latest_worker())
    asyncio.ensure_future(subscribe_history_worker())
    loop.run_forever()


if __name__ == '__main__':
    threading.Thread(target=start, daemon=True).start()
    app.run(host=FLASK_HOST, port=FLASK_PORT)
