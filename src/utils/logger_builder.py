import logging
import logging.config


class LoggerBuilder(object):
    """Logger builder class which stores logger configuration"""

    def __init__(self):
        self.dict_log_config = {
            'version': 1,
            'handlers': {
                'consoleHandler': {
                    'level': 'DEBUG',
                    'class': 'logging.StreamHandler',
                    'formatter': 'consoleFormatter',
                    'stream': 'ext://sys.stdout'
                }
            },
            'loggers': {
                '': {
                    'handlers': ['consoleHandler'],
                    'level': 'DEBUG',
                }
            },
            'formatters': {
                'consoleFormatter': {
                    'format': '%(asctime)s [%(levelname)s] %(message)s <%(name)s>'
                }
            }
        }

    def build(self):
        logging.config.dictConfig(self.dict_log_config)
        return logging.getLogger()
