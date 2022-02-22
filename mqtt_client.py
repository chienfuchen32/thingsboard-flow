import ssl
import abc
import time
import json
import configparser
import threading
import random
from typing import Tuple

import paho.mqtt.client as mqtt

import logging
logging.basicConfig(level=logging.INFO)

TIMEOUT = 10
CONNECT_KEEP_ALIVE = 60
HOST = 'HOST'


class MqttClient(abc.ABC):
    def __init__(self, host: str, port: int, client_id: str):
        logging.info(f'{host}, {port}, {client_id}')
        self.client = mqtt.Client(client_id)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        self.host = host
        self.port = port
        self.t = threading.Thread(target=self.start_loop, daemon=True)
        self.t.start()

    def on_publish(self, client, userdata, mid):
        # logging.info(f'{time.time()}, {mid}')
        pass

    def on_connect(self, client, userdata, rc, *extra_params):
        '''
        The callback for when the client receives a CONNACK response from the
        server.
        Subscribing in on_connect() means that if we lose the connection and
        reconnect then subscriptions will be renewed.
        '''
        logging.info('connection successful')
        self.client.subscribe('v1/devices/me/rpc/response/+')
        self.client.subscribe('v1/devices/me/rpc/request/+', 1)

    def on_message(self, client, userdata, message):
        if 'v1/devices/me/rpc/response/' in message.topic:
            # client RPC
            logging.info(f'client rpc {message.payload}')
        elif 'v1/devices/me/rpc/request/' in message.topic:
            # serevr RPC
            payload = json.loads(message.payload.decode('utf-8'))
            logging.info(f'server rpc {payload}')
            r_id = message.topic.split('v1/devices/me/rpc/request/')[1]
            obj = {'success': True, 'data': {}}
            self.client.publish(f'v1/devices/me/rpc/response/{r_id}',
                                json.dumps(obj))

    def on_disconnect(self, client, userdata, rc):
        logging.info(f'disconnected {client}, {userdata}, {rc}')
        try:
            self.client.reconnect()
        except Exception as ex:
            logging.info(str(ex))

    def start(self):
        self.client.connect(self.host, self.port,
                            keepalive=CONNECT_KEEP_ALIVE)
        time.sleep(1) # safer operation wait for connection
        #payload = json.dumps(msg)
        #self.client.publish('v1/devices/me/rpc/request/1', payload, 1)
        while True:
            # telemetry
            payload = json.dumps({"key1": random.randint(0, 1000)})
            topic = 'v1/devices/me/telemetry'
            self.client.publish(topic, payload, 1)
            # end device telemetry
            payload = json.dumps({
                  "END-DEIVCE": [
                    {
                      "ts": int(time.time() * 1000),
                      "values": {
                        "key1": random.randint(0, 1000)
                      }
                    }
                  ]
                })
            self.client.publish('v1/gateway/telemetry', payload, 1)
            time.sleep(5)

    def start_loop(self):
        self.client.loop_forever(timeout=TIMEOUT, max_packets=1,
                                 retry_first_connection=True)


class MqttAccessTokenClient(MqttClient):
    def __init__(self, host: str, client_id: str, access_token: str):
        '''
        Args:
            host (str): MQTT broker host
            client_id (str): MQTT client ID
            access_token (str): default device authentication to thingsboard

        Ref: https://thingsboard.io/docs/user-guide/access-token/
        '''
        super().__init__(host, 1883, client_id)

        self.client.username_pw_set(access_token)


class MqttX509Client(MqttClient):
    def __init__(self, host, client_id,
                 ca_crt_file, client_key_file, client_crt_file,
                 tls_insecure_set=False):
        '''
        Args:
            host (str): MQTT broker host
            client_id (str): MQTT client ID
            ca_crt_file (str): ca certificate file with path
            client_key_file (str): client private key file with path
            client_crt_file (str): client certificate file with path
            tls_insecure_set (bool): set False in production
        Ref: https://thingsboard.io/docs/user-guide/certificates/
        '''
        super().__init__(host, 8883, client_id)
        self.client.tls_set(ca_certs=ca_crt_file,
                            keyfile=client_key_file,
                            certfile=client_crt_file,
                            cert_reqs=ssl.CERT_REQUIRED,
                            tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)

        self.client.tls_insecure_set(tls_insecure_set)

def start_mqtt_access_token_client():
    client = MqttAccessTokenClient(HOST, 'GATEWAY', 'GATEWAY')
    return client

def start_mqtt_x509_client():
    client = MqttX509Client(HOST, 'GATEWAY', './ca.crt',
                            './client.key', './client.crt')
    return client

if __name__ == '__main__':
    client = start_mqtt_x509_client()
    client.start()
