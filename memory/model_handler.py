from tornado import gen
from datetime import datetime
from tornado.escape import json_decode
from json import JSONDecodeError
from concurrent.futures import ProcessPoolExecutor
import os
import shutil
import logging

from .base_handler import BaseHandler
from .error import ParameterError, ErrorCode
from .response import ModelBuildResponse, ModelStatusResponse
from .constants import ModelStatus, DATE_FORMAT
from .model.task import Task
from .model.settings import MODEL_DIR
from .util import ModelFactory


def validate(date):
    datetime.strptime(str(date), DATE_FORMAT)


def submit_model_task(**kwargs):
    start_day = kwargs.get("start_day")
    end_day = kwargs.get("end_day")
    generate_feature = kwargs.get("generate_feature", True)
    cross_validate = kwargs.get("cross_validate", True)
    Task(start_day, end_day).run(generate_feature, cross_validate)


class ModelBaseHandler(BaseHandler):
    @property
    def model_status(self):
        return self.application.model_status

    @model_status.setter
    def model_status(self, value):
        self.application.model_status = value


class ModelBuildHandler(ModelBaseHandler):
    @gen.coroutine
    def prepare(self):
        try:
            self.data = json_decode(self.request.body)
        except JSONDecodeError:
            raise ParameterError("request.body is not json format")
        try:
            validate(self.data['start_day'])
            validate(self.data['end_day'])
        except (KeyError, ValueError, TypeError) as err:
            raise ParameterError(str(err))

    @gen.coroutine
    def post(self):
        if self.model_status == ModelStatus.RUNNING:
            message = "Models are being built now, try after some time"
            self.write(ModelBuildResponse(error_code=ErrorCode.FAILURE,
                       message=message).get_response())
        else:
            self.model_status = ModelStatus.RUNNING
            self.write(ModelBuildResponse(error_code=ErrorCode.SUCCESS,
                       message="Start building models").get_response())
            self.finish()
            try:
                executor = ProcessPoolExecutor(max_workers=1)
                yield executor.submit(submit_model_task, **self.data)
                for file in os.listdir(MODEL_DIR):
                    if file.endswith(".tmp"):
                        shutil.move(os.path.join(MODEL_DIR, file),
                                    os.path.join(MODEL_DIR, file[:-4]))
            except Exception as err:
                logging.exception(err)
                self.model_status = ModelStatus.FAILED
            else:
                self.application.models = ModelFactory.prepare_model()
                self.model_status = ModelStatus.FINISHED


class ModelStatusHandler(ModelBaseHandler):
    @gen.coroutine
    def get(self):
        self.write(ModelStatusResponse(status_code=self.model_status,
                   status_str=self.model_status.phrase).get_response())