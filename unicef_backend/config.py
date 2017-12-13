import os

BASEDIR = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    DEBUG = False
    SECRET_KEY = 'S3CRET K3Y'

    CELERY_TIMEZONE = 'America/Mexico_City'
    BROKER_URL = os.getenv('BROKER_URL')
    CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND')
    CELERY_SEND_TASK_SENT_EVENT = True


class DevelopmentConfig(Config):
    DEBUG = True


class ProductionConfig(Config):
    pass


config = {'development': DevelopmentConfig, 'production': ProductionConfig}
