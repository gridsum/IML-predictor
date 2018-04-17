import logging.config
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.autoreload
from tornado.options import define, options
import os

from memory.application import application
from memory.settings import LOGGING, NEED_CERTIFICATE, SERVER_PORT
from memory.util import refresh_certification, safety_certification
from memory.model.settings import MODEL_DIR


define("port", default=SERVER_PORT, help="run on the given port", type=int)


def main():
    logging.config.dictConfig(LOGGING)
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(options.port)

    print("Development server is running at http://127.0.0.1:%s" % options.port)
    print("Quit the server with Control-C")

    if NEED_CERTIFICATE:
        safety_certification()
        # refresh every hour
        tornado.ioloop.PeriodicCallback(refresh_certification, 1000 * 3600).start()
        # refresh every five days
        tornado.ioloop.PeriodicCallback(safety_certification, 1000 * 3600 * 24 * 5).start()

    tornado.ioloop.IOLoop.current().start()


if __name__ == "__main__":
    main()
