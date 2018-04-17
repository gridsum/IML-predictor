from http import HTTPStatus


class ErrorCode:
    SUCCESS = 0
    PARAMETER_ERROR = 1
    IMPALA_CONNECT_ERROR = 2
    IMPALA_QUERY_ERROR = 3
    NetWorkError = 4
    MODEL_FILE_NOT_FOUND_ERROR = 5
    UNKNOWN_ERROR = 6
    FAILURE = -1


class MemError(Exception):
    def __init__(self, message=None, status_code=None, error_code=None):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code

    def get_error_body(self):
        return {
            "error_code": self.error_code,
            "message": self.message
        }


class UnKnownError(MemError):
    def __init__(self, message=None, status_code=None,
                 error_code=ErrorCode.UNKNOWN_ERROR):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class ParameterError(MemError):
    def __init__(self, message="parameter error",
                 status_code=HTTPStatus.BAD_REQUEST,
                 error_code=ErrorCode.PARAMETER_ERROR):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class ImpalaConnectError(MemError):
    def __init__(self, message="impala connect error",
                 status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                 error_code=ErrorCode.IMPALA_CONNECT_ERROR):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class ImpalaQueryError(MemError):
    def __init__(self, message="impala query error",
                 status_code=HTTPStatus.BAD_REQUEST,
                 error_code=ErrorCode.IMPALA_QUERY_ERROR):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class NetWorkError(MemError):
    def __init__(self, message="network error",
                 status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                 error_code=ErrorCode.NetWorkError):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code


class ModelFileNotFoundError(MemError):
    def __init__(self, message="model file not found",
                 status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
                 error_code=ErrorCode.MODEL_FILE_NOT_FOUND_ERROR):
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
