import os

from .log_settings import LOGGING

__all__ = ["VERSION", "NEED_CERTIFICATE", "KEYTAB_PATH",
           "PRINCIPAL", "ImpalaConstants", "LOGGING"]

VERSION = "v1"
NEED_CERTIFICATE = True     # if your hadoop cluster need certificate
KEYTAB_PATH = os.path.join(os.path.expanduser("~"), 'dataengineering.keytab')
PRINCIPAL = "dataengineering"
SERVER_PORT = 8889


class ImpalaConstants:
    HOST = 'gs-server-1000'
    PORT = 21050
    USER = 'dataengineering'
    VERSION = "2.9.0-cdh5.12.1"

