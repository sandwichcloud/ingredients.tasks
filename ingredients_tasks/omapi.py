from contextlib import contextmanager

from pypureomapi import Omapi, OmapiErrorNotFound
from simple_settings import settings


class OmapiClient(object):
    def __init__(self, dhcp_server, port, key_name, key):
        self.dhcp_server = dhcp_server
        self.port = port
        self.key_name = key_name
        self.key = key
        self.client = None

    def connect(self):
        self.client = Omapi(self.dhcp_server, self.port, self.key_name, self.key)

    def disconnect(self):
        self.client.close()

    def add_host(self, ip, mac):
        # TODO: lookup if ip already exists and delete it
        try:
            old_mac = self.client.lookup_mac(ip)
            self.client.del_host(old_mac)
        except OmapiErrorNotFound:
            pass
        self.client.add_host(ip, mac)

    @classmethod
    @contextmanager
    def client_session(cls):
        omapi_client = OmapiClient(settings.DHCP_SERVER_IP, settings.DHCP_OMAPI_PORT, settings.DHCP_KEY_NAME,
                                   settings.DHCP_B64_KEY)
        omapi_client.connect()

        try:
            yield omapi_client
        finally:
            omapi_client.disconnect()
