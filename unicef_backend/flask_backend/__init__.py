import os
from flask import Flask
from celery import Celery
from elasticsearch_dsl.connections import connections
from flask_backend.views import api
from flask_backend.config import config

celery = Celery()


def create_app(config_name=None):
    if config_name is None:
        config_name = os.environ.get('CONFIG', 'development')
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    celery.config_from_object(app.config)

    connections.create_connection(
        hosts=[app.config['ELASTICSEARCH_HOST']], timeout=20)

    app.register_blueprint(api)

    return app
