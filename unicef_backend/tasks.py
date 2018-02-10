import os
from datetime import datetime, timedelta

from temba_client.v2 import TembaClient

from flask_backend import celery
from manage import *

mx_client = TembaClient('rapidpro.datos.gob.mx', os.getenv('TOKEN_MX'))


@celery.task()
def download_task():
    #Download runs
    after = datetime.utcnow() - timedelta(minutes=30)
    after = after.isoformat()
    update_runs(after)
    #Download constacts
    download_contacts()
