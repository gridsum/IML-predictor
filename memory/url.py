from .settings import VERSION
from .predict_handler import MemoryPredictHandler
from .model_handler import ModelBuildHandler, ModelStatusHandler

url = [
    (r'/{vn}/impala/memory/predict'.format(vn=VERSION),
     MemoryPredictHandler),
    (r'/{vn}/impala/memory/model_build'.format(vn=VERSION),
     ModelBuildHandler),
    (r'/{vn}/impala/memory/model_status'.format(vn=VERSION),
     ModelStatusHandler)
]
