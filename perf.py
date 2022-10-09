#!/usr/bin/env python3

import asyncio
import socket
import random
import json

import paho.mqtt.client as mqtt

HOST = 'HOST'
PORT = 1883
CLIENT_ID_PREFIX = 'DEVICE'
INTERVAL = 1
TOTAL_TIME = 86400
NUM_DEVICE = 5000
TOTAL_MESSAGE = INTERVAL * TOTAL_TIME


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
        for idx in range(NUM_DEVICE):
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
            for idx in range(NUM_DEVICE):
                payload = {
                            "batteryLevel": random.randint(0, 20000),
                            "leakage": bool(random.getrandbits(1)),
                            "pulseCounter": random.randint(0, 100)
                          }
                self.client[idx].publish('v1/devices/me/telemetry',
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
