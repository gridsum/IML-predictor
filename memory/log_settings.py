LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'debug': {
            'format': '%(asctime)s %(name)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
        'simple': {
            'format': '%(asctime)s  %(levelname)s %(message)s',
            'datefmt': '%Y-%m-%d %H:%M:%S',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'filters': [],
            'class': 'logging.StreamHandler',
            'formatter': 'debug',
        },
        'info_handler': {
            'level': 'INFO',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'backupCount': 7,
            'filename': '/var/log/gdp/memory/info/server_monitor_access.log',
            'formatter': 'simple',
        },
        'exception_handler': {
            'level': 'WARNING',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'when': 'midnight',
            'backupCount': 7,
            'filename': '/var/log/gdp/memory/error/server_monitor_exception.log',
            'formatter': 'simple',
        },

    },
    'loggers': {
        '': {
            'handlers': ['console', ],
            'level': 'DEBUG',
            'propagate': False,
        },
        'tornado.general': {
            'handlers': ['info_handler', 'exception_handler'],
            'level': 'INFO',
            'propagate': True,
        },
    }
}