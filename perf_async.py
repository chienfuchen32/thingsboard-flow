import os
import socket
import ssl
import asyncio
import time
from decimal import Decimal
import random
import json
import logging
import threading
import multiprocessing

import paho.mqtt.client as mqtt

logger_format = '%(asctime)s:%(threadName)s:%(message)s'
logging.basicConfig(format=logger_format, level=logging.INFO,
                    datefmt='%H:%M:%S')

HOST = os.getenv('HOST', 'HOST')
PORT = int(os.getenv('PORT', 8883))
DEVICE_NAME = os.getenv('DEVICE_NAME', 'DEVICE_NAME')
NUM_PROCESS = int(os.getenv('NUM_PROCESS'), 8)
TOTAL_DEVICE_NUM = int(os.getenv('TOTAL_DEVICE_NUM'), 45000)
TOTAL_VM = int(os.getenv('TOTAL_VM', 8))
TOTAL_DEVICE_NUM_PER_VM = TOTAL_DEVICE_NUM // TOTAL_VM
IDX_VM = int(os.getenv('IDX_VM', 0))
IDX_FIRST = IDX_VM * TOTAL_DEVICE_NUM_PER_VM
IDX_LAST = TOTAL_DEVICE_NUM if IDX_VM == TOTAL_VM - 1 else (IDX_VM + 1) * TOTAL_DEVICE_NUM_PER_VM
BIND_END_DEVICE_NUM = int(os.getenv('BIND_END_DEVICE_NUM', 2))
CONNECTION_WAIT = int(os.getenv('CONNECTION_WAIT', 60))
RECONNECTION_WAIT = int(os.getenv('RECONNECTION_WAIT', 5))
IS_PUBLISH = True if os.getenv('IS_PUBLISH', 'True') == 'True' else False
MAX_QUEUED_MESSAGES = int(os.getenv('MAX_QUEUED_MESSAGES', 0))
MAX_INFLIGHT_MESSAGES = int(os.getenv('MAX_INFLIGHT_MESSAGES', 20))
PUBLISH_INTERVAL = int(os.getenv('PUBLISH_INTERVAL', 1))
TOTAL_INTERVAL = int(os.getenv('TOTAL_INTERVAL', 7 * 24 * 60 * 60)) # in second
TOTAL_TIMES = TOTAL_INTERVAL * int(Decimal(1) // Decimal(str(PUBLISH_INTERVAL)))
logging.info('estimate publish times {}, will be finined after {} sec'.format(TOTAL_TIMES, TOTAL_INTERVAL))

assert TOTAL_VM < TOTAL_DEVICE_NUM
assert NUM_PROCESS < TOTAL_DEVICE_NUM_PER_VM

global_connection_array = multiprocessing.Array('i', [0 for _ in range(NUM_PROCESS)])


class AsyncioHelper:
    def __init__(self, loop, client):
        self.loop = loop
        self.client = client
        self.client.on_socket_open = self.on_socket_open
        self.client.on_socket_close = self.on_socket_close
        self.client.on_socket_register_write = self.on_socket_register_write
        self.client.on_socket_unregister_write = self.on_socket_unregister_write

    def on_socket_open(self, client, userdata, sock):
        # print("Socket opened")

        def cb():
            # print("Socket is readable, calling loop_read")
            client.loop_read()

        self.loop.add_reader(sock, cb)
        self.misc = self.loop.create_task(self.misc_loop())
        '''
        asyncio loop reader or task number limit, cause connection limitation?
        Because this is single
        thread in this process. if CPU usage gets high enough, connection
        might get timeout in a period of time. so keep reconnecting would be
        a good choise.
        '''

    def on_socket_close(self, client, userdata, sock):
        # print("Socket closed")
        self.loop.remove_reader(sock)
        self.misc.cancel()

    def on_socket_register_write(self, client, userdata, sock):
        # print("Watching socket for writability.")

        def cb():
            # print("Socket is writable, calling loop_write")
            client.loop_write()

        self.loop.add_writer(sock, cb)

    def on_socket_unregister_write(self, client, userdata, sock):
        # print("Stop watching socket for writability.")
        self.loop.remove_writer(sock)

    async def misc_loop(self):
        while self.client.loop_misc() == mqtt.MQTT_ERR_SUCCESS:
            try:
                await asyncio.sleep(60)
            except asyncio.CancelledError:
                break
        # print("misc_loop finished")


class AsyncMqttClientActor:
    def __init__(self, loop):
        self.loop = loop
        self.time_res = {}
        self.total_time = 0
        self.pid = -1

    def on_connect(self, client, userdata, flags, rc):
        # logging.info('{} connected'.format(self.username))
        global_connection_array[self.pid] += 1

    def try_reconnect(self):
        time.sleep(10)
        self.client.reconnect()

    def on_disconnect(self, client, userdata, rc):
        logging.info('{} disconnected'.format(self.username))
        # self.disconnected.set_result(rc)
        global_connection_array[self.pid] -= 1

        t = threading.Thread(target=self.try_reconnect,
                             args=(),
                             daemon=True)
        t.start()

    def on_publish(self, client, userdata, mid):
        '''
        self.time_res[mid] = time.time()
        self.total_time += self.time_res[mid] - self.time_req[mid]
        '''
        pass

    def on_message(self, client, userdata, msg):
        '''
        if not self.got_message:
            logging.error("Got unexpected message: {}".format(msg.decode()))
        else:
            self.got_message.set_result(msg.payload)
        '''
        pass

    async def main(self, pid, username, total_times):
        self.username = username
        self.pid = pid
        self.time_req = {}  # {"index": ts}
        self.time_res = {}  # {"index": ts}
        self.total_time = 0
        # print('init client {}'.format(username))
        self.disconnected = self.loop.create_future()
        self.got_message = None

        self.client = mqtt.Client(client_id=username)
        self.client.username_pw_set(username)
        self.client.tls_set(cert_reqs=ssl.CERT_NONE,
                            tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
        self.client.reconnect_delay_set(min_delay=1, max_delay=120)
        self.client.tls_insecure_set(False)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        self.client.on_disconnect = self.on_disconnect
        self.client.max_queued_messages_set(MAX_QUEUED_MESSAGES)
        self.client.max_inflight_messages_set(MAX_INFLIGHT_MESSAGES)

        _ = AsyncioHelper(self.loop, self.client)

        self.client.connect(HOST, PORT, 60)
        self.client.socket().setsockopt(socket.SOL_SOCKET,
                                        socket.SO_SNDBUF, 2048)
        # wait all devices connected to MQTT broker
        await asyncio.sleep(CONNECTION_WAIT)

        for c in range(total_times):
            if IS_PUBLISH is True:
                # self.got_message = self.loop.create_future()

                # end device
                message = {}
                for e in range(BIND_END_DEVICE_NUM):
                    message_ed = [{
                        'ts': int(time.time() * 1000),
                        'values': {
                            'pulseCounter': random.randint(0, 100),
                            'leakage': bool(random.getrandbits(1)),
                            'batteryLevel': random.randint(0, 10)
                        }
                    }]
                    message['{}-END-DEVICE-{}'.format(self.username, e)] = message_ed
                # print(message)
                # self.time_req[c+1] = time.time()  # for timestamp summary
                self.client.publish('v1/gateway/telemetry',
                                    payload=json.dumps(message), qos=1)
                # logging.info('{} publish'.format(self.username))
                #  Direct device
                message = json.dumps({
                        "pulseCounter": random.randint(0, 100),
                        "leakage": bool(random.getrandbits(1)),
                        "batteryLevel": random.randint(0, 10)
                    })
                # self.time_req[c+1] = time.time()  # for timestamp summary
                self.client.publish('v1/devices/me/telemetry',
                                    payload=message, qos=1)
                # msg = await self.got_message
                # print("Got response with {} bytes".format(len(msg)))
                # self.got_message = None
            await asyncio.sleep(PUBLISH_INTERVAL)

        self.client.disconnect()
        # print("Disconnected: {}".format(await self.disconnected))
        return self.total_time, self.time_req, self.time_res


def print_connect():
    counter = 999999999999999
    while True:
        if counter < 0:
            break
        sum_gca = 0
        for i in range(len(global_connection_array)):
            sum_gca += global_connection_array[i]
        print('print_connect', sum_gca, global_connection_array[:])
        time.sleep(10)
        counter -= 1


async def main(loop, pid, idx, last_idx):
    tasks = []
    time_start = time.time()
    print(f'process use {idx}-{last_idx}')
    for sn in range(idx, last_idx):
        device_name = '{}-{}'.format(DEVICE_NAME, sn)
        await asyncio.sleep(0.033)  # init client
        task = asyncio.ensure_future(
                    AsyncMqttClientActor(loop).main(pid,
                                                    device_name,
                                                    TOTAL_TIMES))
        tasks.append(task)
    await asyncio.wait(tasks)
    time_end = time.time()
    logging.info('spend {} sec'.format(time_end - time_start))


def _main(pid, idx, last_idx):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop, pid, idx, last_idx))
    loop.close()
    logging.info("Finished")


if __name__ == '__main__':
    process = []
    process.append(
        multiprocessing.Process(target=print_connect,
                                args=(),
                                daemon=True))
    for i in range(NUM_PROCESS):
        idx = IDX_FIRST + i * (TOTAL_DEVICE_NUM_PER_VM // NUM_PROCESS)
        last_idx = -1
        if i == NUM_PROCESS - 1:
            last_idx = IDX_LAST
        else:
            last_idx = IDX_FIRST + \
                       (i + 1) * (TOTAL_DEVICE_NUM_PER_VM // NUM_PROCESS)
        process.append(
            multiprocessing.Process(target=_main,
                                    args=(i, idx, last_idx,),
                                    daemon=True))
    for i in range(len(process)):
        process[i].start()
    for i in range(len(process)):
        process[i].join()
