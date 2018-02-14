import os
import sys

sys.path.insert(0, '..')
from settings import *


class Config(object):
    CELERY_TIMEZONE = CELERY_TIMEZONE
    BROKER_URL = BROKER_URL
    CELERY_RESULT_BACKEND = CELERY_RESULT_BACKEND
    CELERY_SEND_TASK_SENT_EVENT = True
    ELASTICSEARCH_HOST = ELASTICSEARCH_HOST


class DevelopmentConfig(Config):
    pass


config = {
    'development': DevelopmentConfig,
}
