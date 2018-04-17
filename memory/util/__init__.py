import logging
import subprocess
import jsonschema
from functools import wraps
from http import HTTPStatus


from ..error import ParameterError
from ..settings import KEYTAB_PATH, PRINCIPAL
from .model_util import ModelFactory


__all__ = ["json_validate", "clear_certification", "refresh_certification",
           "safety_certification", "ModelFactory"]


def clear_certification():
    subprocess.check_output(['kdestroy'])


def refresh_certification():
    subprocess.check_output(['kinit', '-R'])
    logging.info("refresh certificate")


def safety_certification():
    subprocess.check_output(['kinit', '-kt', KEYTAB_PATH, PRINCIPAL])
    logging.info("certificate")


def json_validate(schema, error_code):
    """
    校验json数据格式
    :param schema:
    :param error_code: sever自己的对应error_code
    :return:
    """

    def decorator(func):
        @wraps(func)
        def wrapper(handler, *args, **kwargs):
            try:
                jsonschema.validate(handler.data, schema)
            except AttributeError:
                raise ParameterError(
                    'request headers: Content-Type should be application/json',
                    error_code=error_code,
                    status_code=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)

            except jsonschema.exceptions.ValidationError as err:
                raise ParameterError(str(err))

            return func(handler, *args, **kwargs)

        return wrapper

    return decorator