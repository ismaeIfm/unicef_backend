import os
from datetime import datetime, timedelta

from temba_client.v2 import TembaClient

from flask_backend import celery


mx_client = TembaClient('rapidpro.datos.gob.mx', os.getenv('TOKEN_MX'))


@celery.task()
def download_task():
    print('*' * 10)
    #after = datetime.utcnow() - timedelta(minutes=30)
    #after = after.isoformat()
    #last_contacts = mx_client.get_contacts(after=after).all(
    #    retry_on_rate_exceed=True)
    #print(last_contacts)
    #last_messages = mx_client.get_messages(
    #after=after, folder="inbox").all(retry_on_rate_exceed=True)
    #print(last_messages)
