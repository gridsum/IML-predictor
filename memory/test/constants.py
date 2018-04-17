from ..settings import VERSION, SERVER_PORT


class ServerConstants(object):
    # 本地测试
    HOST = "127.0.0.1"
    PORT = SERVER_PORT


class UrlConstants(object):
    URL_PREFIX = "http://{host}:{port}/{version}/impala/memory".format(
        host=ServerConstants.HOST,
        port=ServerConstants.PORT,
        version=VERSION
    )

    MEMORY_PREDICT_TEMPLATE = URL_PREFIX + '/predict'
    MEMORY_MODEL_BUILD_TEMPLATE = URL_PREFIX + '/model_build'
    MEMORY_MODEL_STATUS_TEMPLATE = URL_PREFIX + '/model_status'
