#!/usr/bin/env python3

import asyncio
import socket
import random
import time
import json

import paho.mqtt.client as mqtt

HOST = 'HOST'
PORT = 1883
CLIENT_ID_PREFIX = 'DEVICE'
END_DEVICE_ID_PREFIX = 'END_DEVICE'
INTERVAL = 1
TOTAL_TIME = 300
NUM_GATEWAY = 10
GATEWAY_MODE = True
NUM_END_DEVICE = 100
TOTAL_MESSAGE = INTERVAL * TOTAL_TIME
TOPIC_TELEMETRY = 'v1/devices/me/telemetry'
TOPIC_TELEMETRY_GATEWAY = 'v1/gateway/telemetry'


class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):

        def cb():
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())

    def on_socket_close(self, client, userdata, sock):
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):

        def cb():
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                break


class AsyncMqttExample:
    def __init__(self, loop):
        self.loop = loop

    def on_connect(self, client, userdata, flags, rc):
        pass

    def on_message(self, client, userdata, msg):
        if not self.got_message:
            print("Got unexpected message: {}".format(msg.decode()))
        else:
            self.got_message.set_result(msg.payload)

    def on_disconnect(self, client, userdata, rc):
        pass

    async def main(self):
        self.got_message = None

        self.client = []
        for idx in range(NUM_GATEWAY):
            client_id = f'{CLIENT_ID_PREFIX}-{idx}'
            client = mqtt.Client(client_id=client_id)
            client.username_pw_set(client_id)
            client.on_connect = self.on_connect
            client.on_message = self.on_message
            client.on_disconnect = self.on_disconnect
            aioh = AsyncioHelper(self.loop, client)
            client.connect(HOST, PORT, 60)
            client.socket().setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2048)
            self.client.append(client)
            await asyncio.sleep(0.001)

        for c in range(TOTAL_MESSAGE):
            print('send ', c)
            for idx in range(NUM_GATEWAY):
                payload = {
                            "batteryLevel": random.randint(0, 20000),
                            "leakage": bool(random.getrandbits(1)),
                            "pulseCounter": random.randint(0, 100)
                          }
                self.client[idx].publish(TOPIC_TELEMETRY,
                                         json.dumps(payload).encode('utf-8'),
                                         qos=1)
                #msg = await self.got_message
                #print("Got response with {} bytes".format(len(msg)))
                #self.got_message = None

                if GATEWAY_MODE:
                    payload = {}
                    for idx_end_device in range(NUM_END_DEVICE):
                        payload[f'{END_DEVICE_ID_PREFIX}-{idx}-{idx_end_device}'] = [
                            {
                                'ts': int(time.time() * 1000),
                                'values': {
                                    'latitude': round(random.uniform(-90, 90), 2),
                                    'longitude': round(random.uniform(-180, 180), 2),
                                    'speed': round(random.uniform(0, 100), 2),
                                    'fuel': random.randint(0, 100),
                                    'batteryLevel': random.randint(0, 20000),
                                    'voltage': random.randint(0, 100),
                                    'ampere': random.randint(0, 100),
                                    'cpu': random.randint(0, 100),
                                    'ram': random.randint(0, 100),
                                    'disk': random.randint(0, 100),
                                }
                            }
                        ]
                    self.client[idx].publish(TOPIC_TELEMETRY_GATEWAY,
                                             json.dumps(payload).encode('utf-8'),
                                             qos=1)

            await asyncio.sleep(INTERVAL)
        for c in range(TOTAL_MESSAGE):
            self.client[idx].disconnect()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(AsyncMqttExample(loop).main())
    loop.close()
    print("Finished")
