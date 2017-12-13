import os

from flask import Flask
from celery import Celery

from config import config

celery = Celery()


def create_app(config_name=None):
    if config_name is None:
        config_name = os.environ.get('CONFIG', 'development')
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    celery.config_from_object(app.config)

    from unicef_backend.views import api
    app.register_blueprint(api)

    return app
