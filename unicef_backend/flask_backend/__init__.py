import os
from flask import Flask
from celery import Celery
from elasticsearch_dsl.connections import connections
from flask_backend.views import api
from flask_backend.config import config

from flask_backend.views import *
from flasgger import Swagger
from flask_cors import CORS


celery = Celery()
class CustomProxyFix(object):

    def __init__(self, app):
        self.app = app

    def __call__(self, environ, start_response):
        host = environ.get('HTTP_X_FHOST', '')
        if host:
            environ['HTTP_HOST'] = host
        return self.app(environ, start_response)


def create_app(config_name=None):
    if config_name is None:
        config_name = os.environ.get('CONFIG', 'development')
    app = Flask(__name__)
    CORS(app)
    app.config.from_object(config[config_name])
    celery.config_from_object(app.config)

    connections.create_connection(
        hosts=[app.config['ELASTICSEARCH_HOST']], timeout=20)

    app.register_blueprint(api)
    app.wsgi_app = CustomProxyFix(app.wsgi_app)
    swagger = Swagger(app)

    return app
