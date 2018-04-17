import logging

from impala.dbapi import connect
from .settings import ImpalaConstants, NEED_CERTIFICATE
from .error import ImpalaConnectError, ImpalaQueryError


class ImpalaWrapper:
    def __init__(self, host=ImpalaConstants.HOST, port=ImpalaConstants.PORT,
                 user=ImpalaConstants.USER, database=None, sql=None,
                 auth_required=NEED_CERTIFICATE):
        self.host = host
        self.port = int(port)
        self.user = user
        self.database = database
        self.sql = "explain %s" % sql
        self.auth_required = auth_required

    def cursor(self):
        if self.auth_required:
            auth_mechanism = 'GSSAPI'
        else:
            auth_mechanism = 'NOSASL'
        try:
            return connect(self.host, self.port,
                           auth_mechanism=auth_mechanism).cursor()
        except Exception as err:
            logging.error(err)
            raise ImpalaConnectError(message=str(err))

    def explain(self):
        cursor = self.cursor()

        try:
            cursor.execute("use %s" % self.database)
            cursor.execute("set explain_level=2")
            cursor.execute(self.sql)
        except Exception as err:
            logging.warning(err)
            raise ImpalaQueryError(message=str(err))
        else:
            for line in cursor:
                yield line[0]
        finally:
            cursor.close()


