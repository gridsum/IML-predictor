from tornado import gen
from tornado.web import HTTPError, RequestHandler
from tornado.log import app_log, gen_log
from tornado.escape import json_decode
from http import HTTPStatus
from json import JSONDecodeError

from .error import MemError
from .error import ParameterError


class BaseHandler(RequestHandler):
    """Base request handler class"""
    @gen.coroutine
    def prepare(self):
        if self.request.body:
            try:
                self.data = json_decode(self.request.body)
            except JSONDecodeError:
                raise ParameterError("request.body is not json format")

    def write_error(self, status_code, **kwargs):
        if "error_message" in kwargs:
            self.write(kwargs["error_message"])

        elif 'exc_info' in kwargs and isinstance(kwargs['exc_info'][1],
                                                 MemError):
            exception = kwargs['exc_info'][1]
            self.set_status(exception.status_code)
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(exception.get_error_body())

        else:
            self.write(str(kwargs["exc_info"]))

    def log_exception(self, typ, value, tb):
        """
        record uncaught exception in handler
        """
        if isinstance(value, HTTPError):
            if value.log_message:
                message_format = "%d %s: " + value.log_message
                args = ([value.status_code, self._request_summary()] +
                        list(value.args))
                gen_log.warning(message_format, *args)

        else:
            # if not internal error, do not record
            if isinstance(value, MemError) and (
                        value.status_code != HTTPStatus.INTERNAL_SERVER_ERROR):
                return

            params = " ".join(" ".join(
                    self.request.body.decode('utf-8').split("\n")).split())

            app_log.error("Uncaught exception %s\n%r\n%s",
                          self._request_summary(),
                          self.request, params, exc_info=(typ, value, tb))