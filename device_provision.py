import os
import time
import abc
import urllib
from typing import List, Dict, Tuple
import logging
import requests
import sys

logging.basicConfig(level=logging.INFO)

DEVICE_NAME_PREFIX = 'DEVICE'
IS_GATEWAY = False
TOTAL_RETRY = 30
RETRY_INTERVAL = 1
TOTAL_NUM_DEVICE = 100
TOTAL_RETRY = 30
THINGSBOARD_URL = sys.argv[1]
THINGSBOARD_ACCOUNT = sys.argv[2]
THINGSBOARD_PASSWORD = sys.argv[3]


class Device:
    def __init__(self, name):
        self.name = name
        self.create_at = time.time()


class ThingsboardRestClient():

    def __init__(self, username='', password='',
                 tenant_id='', user_id='', authority=''):
        self.username = username
        self.password = password
        self.tb_token = ''
        self.user_id = user_id
        self.tenant_id = tenant_id
        self.customer_id = ''
        self.authority = authority
        self.name = ''

    def to_jwt_info(self):
        info = {
            'tenant_id': self.tenant_id,
            'user_id': self.user_id,
            'authority': self.authority
        }
        return info

    def is_tenant_admin(self):
        return self.authority == 'TENANT_ADMIN'

    def is_sys_admin(self):
        return self.authority == 'SYS_ADMIN'

    def get_auth_headers(self) -> Dict:
        return {
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'X-Authorization': 'Bearer {}'.format(self.tb_token)
        }

    def login(self) -> Tuple[str, int]:
        # login to IOT Hub
        data = {
            "username": self.username,
            "password": self.password
        }
        message, status_code = '', 200
        r = requests.post('{}/auth/login'.format(THINGSBOARD_URL), json=data)
        status_code = r.status_code
        if status_code == 200:
            self.tb_token = r.json()['token']
        else:
            message = r.json()['message']
            return message, status_code
        # get user profile
        r = requests.get('{}/auth/user'.format(THINGSBOARD_URL),
                         headers=self.get_auth_headers())
        status_code = r.status_code
        if status_code == 200:
            data = r.json()
            self.user_id = data['id']['id']
            self.tenant_id = data['tenantId']['id']
            self.customer_id = data['customerId']['id']
            self.name = data['name']
            self.authority = data['authority']
        else:
            message = r.json()['message']
        return message, status_code

    def device_provisioning(self,
                            device=None,
                            credentials_value='',
                            credentials_type='X509_CERTIFICATE',
                            is_gateway=False) -> Tuple[str, str, int]:
        '''
            device (Device): Device metadata
            credentials_value (str): X509 Certificate, or access token
            credentials_type (str): 'X509_CERTIFICATE' or 'ACCESS_TOKEN'
            is_gateway (bool): determine device is gateway
        '''
        device_name = device.name
        # create device
        device_type = 'default'
        data = {
            'name': device_name,
            'additionalInfo': {
                'gateway': is_gateway
            },
            'type': device_type
        }
        device_id, message, status_code = '', '', 200
        session = requests.Session()
        try:
            r = session.post('{}/device'.format(THINGSBOARD_URL),
                             headers=self.get_auth_headers(), json=data)
            status_code = r.status_code
            if status_code == 200:
                ''' example
                {
                    "id": {
                        "entityType": "DEVICE",
                        "id": "9b5bca70-85cf-11ea-b38e-191921c779a2"
                    },
                    "createdTime": 1587692752620,
                    "additionalInfo": {
                        "gateway": true
                    },
                    "tenantId": {
                        "entityType": "TENANT",
                        "id": "af88ff00-6a8f-11ea-b03a-93b53481ffac"
                    },
                    "customerId": {
                        "entityType": "CUSTOMER",
                        "id": "13814000-1dd2-11b2-8080-808080808080"
                    },
                    "name": "tesst",
                    "type": "default",
                    "label": null
                }
                '''
                device_id = r.json()['id']['id']
            else:
                message = r.json()['message']
                if message == 'Device with such name already exists!':  # 400
                    logging.info('tb provisioning message 1: %s', message)
                    device_id, message, status_code = self.get_tenant_device_id(device_name)
                    if status_code == 200:
                        pass
                    else:
                        logging.info('tb provisioning message 2: %s', message)
                        return '', message, status_code
                else:
                    logging.info('tb provisioning message 3: %s', message)
                    return '', message, status_code

            # load device credential to Thingsboard
            data_credential = {}
            retry_count = 0
            success = False
            while retry_count < TOTAL_RETRY:
                r = session.get('{}/device/{}/credentials'.format(
                                THINGSBOARD_URL, device_id),
                                headers=self.get_auth_headers())
                status_code = r.status_code
                if status_code == 200:
                    data_credential = r.json()
                    success = True
                    break
                else:
                    message = r.json()['message']
                time.sleep(RETRY_INTERVAL)
                retry_count += 1
            if success is False:
                logging.info('tb provisioning message 4: %s', message)
                return '', message, status_code
            # modify device credential
            retry_count = 0
            success = False
            while retry_count < TOTAL_RETRY:
                if credentials_type == 'ACCESS_TOKEN':
                    # check ability with tb, if not able to do, return 401
                    # if token expired, ask user login again
                    '''
                    {
                        'id': {
                            'id': 'f0dfae40-6a8f-11ea-b03a-93b53481ffac'
                        },
                        'deviceId': {
                            'entityType': 'DEVICE',
                            'id': 'f0de75c0-6a8f-11ea-b03a-93b53481ffac'
                        },
                        'credentialsType': 'ACCESS_TOKEN',
                        'credentialsId': '',
                        'credentialsValue': None,
                        'createdTime': 1593671683353
                    }
                    '''
                    # Save device credential to IOT Hub
                    data_credential['credentialsId'] = credentials_value
                    data_credential['credentialsType'] = credentials_type
                    r = session.post('{}/device/credentials'.format(THINGSBOARD_URL),
                                     headers=self.get_auth_headers(),
                                     json=data_credential)
                    status_code = r.status_code
                    if status_code == 200:
                        success = True
                        break
                    else:
                        message = r.json()['message']
                        if status_code == 400 and message == 'Device credentials are already assigned to another device!':
                            # or bad request
                            # Specified credentials are already registered!
                            # TODO bearly happen, pring log
                            pass
                        message += device_name
                        time.sleep(RETRY_INTERVAL)
                        retry_count += 1
                elif credentials_type == 'X509_CERTIFICATE':
                    # check ability with tb, if not able to do, return 401
                    # if token expired, ask user login again
                    '''
                    {
                        'id': {
                            'id': 'f0dfae40-6a8f-11ea-b03a-93b53481ffac'
                        },
                        'deviceId': {
                            'entityType': 'DEVICE',
                            'id': 'f0de75c0-6a8f-11ea-b03a-93b53481ffac'
                        },
                        'credentialsType': 'X509_CERTIFICATE',
                        'credentialsId': None,
                        'credentialsValue': '',
                        'createdTime': 1593671683353
                    }
                    '''
                    # Save device credential to IOT Hub
                    data_credential['credentialsValue'] = credentials_value
                    data_credential['credentialsType'] = credentials_type
                    r = session.post('{}/device/credentials'.format(THINGSBOARD_URL),
                                     headers=self.get_auth_headers(),
                                     json=data_credential)
                    status_code = r.status_code
                    if status_code == 200:
                        success = True
                        break
                    else:
                        message = r.json()['message']
                        if status_code == 400 and message == 'Specified credentials are already registered!':
                            # or bad request
                            # Specified credentials are already registered!
                            # TODO bearly happen, pring log
                            pass
                        message += device_name
                        time.sleep(RETRY_INTERVAL)
                        retry_count += 1
            if success is False:
                logging.info('tb provisioning message 5-0: %s', message)
                return '', message, status_code
        except Exception as e:
            message = 'Error {e}'
            status_code = 500
            logging.info('tb provisioning message: %s', message)
            return '', message, status_code
        finally:
            session.close()
        return device_id, message, status_code

    def get_tenant_device_ids(self, device_name='',
                              limit=20) -> Tuple[List[str], str, int]:
        parameters = urllib.parse.urlencode({
            'textSearch': device_name,
            'limit': limit,
        })
        device_ids = []
        message, status_code = '', 200
        r = requests.get('{}/tenant/devices?{}'.format(
                         THINGSBOARD_URL, parameters),
                         headers=self.get_auth_headers())
        status_code = r.status_code
        if status_code == 200:
            '''
            {
                'data': [
                    {
                     'additionalInfo': {'gateway': false},
                     'createdTime': 1590460546989,
                     'customerId': {
                        'entityType': 'CUSTOMER',
                        'id': '13814000-1dd2-11b2-8080-808080808080'
                     },
                     'id': {
                        'entityType': 'DEVICE',
                        'id': '9ad495d0-9ef9-11ea-a230-8b813649c07f'
                     },
                     'label': null,
                     'name': 'DEVICE-0',
                     'tenantId': {
                        'entityType': 'TENANT',
                        'id': '304869b0-9a73-11ea-80b5-bbe2b88b726d'
                     },
                     'type': 'default'
                    }, ...
                ],
                'hasNext': false,
                'nextPageLink': null
            }
            '''
            devices = r.json()['data']  # List[Dict]
            device_ids = [device['id']['id'] for device in devices]
        else:
            message = r.json()['message']

        return device_ids, message, status_code

    def get_tenant_device_id(self, device_name='') -> Tuple[str, str, int]:
        parameters = urllib.parse.urlencode({
            'textSearch': device_name,
            'limit': 500,  # tb 2
            # 'pageSize': 10, # tb 3
            # 'page':0 #tb 3
        })
        device_id = ''
        message, status_code = '', 200
        r = requests.get('{}/tenant/devices?{}'.format(
                         THINGSBOARD_URL, parameters),
                         headers=self.get_auth_headers())
        status_code = r.status_code
        if status_code == 200:
            '''
            {
                'data': [
                    {
                     'additionalInfo': {'gateway': false},
                     'createdTime': 1590460546989,
                     'customerId': {
                        'entityType': 'CUSTOMER',
                        'id': '13814000-1dd2-11b2-8080-808080808080'
                     },
                     'id': {
                        'entityType': 'DEVICE',
                        'id': '9ad495d0-9ef9-11ea-a230-8b813649c07f'
                     },
                     'label': null,
                     'name': 'DEVICE-0',
                     'tenantId': {
                        'entityType': 'TENANT',
                        'id': '304869b0-9a73-11ea-80b5-bbe2b88b726d'
                     },
                     'type': 'default'
                    }, ...
                ],
                'hasNext': false,
                'nextPageLink': null
            }
            '''
            devices = r.json()['data']  # List[Dict]
            for device in devices:
                if device['name'] == device_name:
                    device_id = device['id']['id']
                    break
        else:
            message = r.json()['message']
        return device_id, message, status_code

    def delete_device(self, device_id: str) -> Tuple[str, int]:
        message, status_code = '', 200
        r = requests.delete('{}/device/{}'.format(
                            THINGSBOARD_URL, device_id),
                            headers=self.get_auth_headers())
        status_code = r.status_code
        if status_code == 200:
            pass
        else:
            message = r.json()['message']
        return message, status_code


def init_tb_rest_client_by_jwt(dictionary: Dict) -> ThingsboardRestClient:
    tenant_id = dictionary.get('tenant_id', '')
    user_id = dictionary.get('user_id', '')
    authority = dictionary.get('authority', '')
    return ThingsboardRestClient(tenant_id=tenant_id,
                                 user_id=user_id,
                                 authority=authority)


if __name__ == '__main__':
    tb_client = ThingsboardRestClient(THINGSBOARD_ACCOUNT,
                                      THINGSBOARD_PASSWORD)
    message, status_code = tb_client.login()
    assert message == ''
    assert status_code == 200
    lines = ['userName\n']
    for i in range(TOTAL_NUM_DEVICE):
        device_name = f'{DEVICE_NAME_PREFIX}-{i}'
        lines.append('{}\n'.format(device_name))
        # TODO add retry, delete device if existed
        retry_count = 0
        success = False
        while success is False and retry_count < TOTAL_RETRY:
            device = Device(name, tb_client.user_id)
            device_id, message, status_code = tb_client.device_provisioning(
                    device, device_name, 'ACCESS_TOKEN', IS_GATEWAY)
            if status_code == 200:
                success = True
                break
            device_id, message, status_code = tb_client.get_tenant_device_id(device_name)
            if status_code != 200:
                tb_client.delete_device(device_id)
                print('device provision retry', device_name)
            time.sleep(1)
            retry_count += 1
        if success is False:
            print('device provision failed', device_name)
    current_path = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(current_path, 'mqtt.csv'), 'w') as f:
        f.writelines(lines)
