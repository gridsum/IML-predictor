import tornado.web

from .url import url
from .constants import ModelStatus
from .util import ModelFactory


class Application(tornado.web.Application):
    def __init__(self, *args, **kwargs):
        self.models = ModelFactory.prepare_model()
        self.model_status = ModelStatus.FINISHED
        super(Application, self).__init__(*args, **kwargs)

application = Application(
    handlers=url,
    autoreload=True
)
