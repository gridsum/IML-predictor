from tornado import gen
from http import HTTPStatus
from concurrent.futures import ThreadPoolExecutor
from tornado.concurrent import run_on_executor
import logging

from .predict import get_model_name, predict
from .error import ErrorCode, MemError, UnKnownError, ModelFileNotFoundError
from .feature import get_features
from .impala_client import ImpalaWrapper
from .response import PredictSuccessResponse
from .util import json_validate
from .base_handler import BaseHandler
from .settings import ImpalaConstants


MEMORY_PREDICT = {
    'type': 'object',
    'properties': {
        'sql': {
            'type': 'string',
            'minLength': 1
        },
        'db': {
            'type': 'string',
            'minLength': 1
        },
        'pool': {'type': 'string'},
    },
    'required': ['sql', 'db']
}


class MemoryPredictHandler(BaseHandler):
    executor = ThreadPoolExecutor()

    @gen.coroutine
    @json_validate(MEMORY_PREDICT, ErrorCode.PARAMETER_ERROR)
    def post(self):
        sql = self.data.get('sql')
        db = self.data.get('db')
        pool = self.data.get('pool', 'default')
        pool = "root." + pool if not pool.startswith("root") and \
               pool != "default" else pool
        try:
            explain_result = yield self.get_explain_result(db, sql) 
            features = get_features(explain_result, ImpalaConstants.VERSION)
            model_name = get_model_name(pool)
            model = self.application.models.get(model_name)
            if not model:
                logging.error("Model %s not exists" % model_name)
                raise ModelFileNotFoundError("Model %s not exists" % model_name)
            result = predict(model, features)
        except MemError as err:
            self.send_error(status_code=err.status_code,
                            error_message=err.get_error_body())
        except Exception as err:
            unknown_err = UnKnownError(message=str(err))
            self.send_error(status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                            error_message=unknown_err.get_error_body())
        else:
            self.write(PredictSuccessResponse(result).get_response())

    @run_on_executor
    def get_explain_result(self, db, sql):
        return ImpalaWrapper(database=db, sql=sql).explain()
