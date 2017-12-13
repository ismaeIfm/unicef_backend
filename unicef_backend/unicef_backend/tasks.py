import os
from datetime import datetime, timedelta

from temba_client.v2 import TembaClient
#from celery.signals import task_postrun
#from celery.utils.log import get_task_logger
from unicef_backend import celery

#logger = get_task_logger(__name__)

#@celery.task
#def log(message):
#    """Print some log messages"""
#    logger.debug(message)
#    logger.info(message)
#    logger.warning(message)
#    logger.error(message)
#    logger.critical(message)
mx_client = TembaClient('rapidpro.datos.gob.mx', os.getenv('TOKEN_MX'))


@celery.task()
def download_task():
    print('*' * 10)
    after = datetime.utcnow() - timedelta(minutes=30)
    after = after.isoformat()
    last_contacts = mx_client.get_contacts(after=after).all(
        retry_on_rate_exceed=True)
    print(last_contacts)
    last_messages = mx_client.get_messages(
        after=after, folder="inbox").all(retry_on_rate_exceed=True)
    print(last_messages)
