from .error import ErrorCode


class PredictSuccessResponse:
    def __init__(self, memory_limit=None, error_code=ErrorCode.SUCCESS):
        self.mem = memory_limit
        self.error_code = error_code

    def get_response(self):
        return self.__dict__


class ModelBuildResponse:
    def __init__(self, error_code, message):
        self.error_code = error_code
        self.message = message

    def get_response(self):
        return self.__dict__


class ModelStatusResponse:
    def __init__(self, status_code, status_str):
        self.status_code = status_code
        self.status_str = status_str

    def get_response(self):
        return self.__dict__
