from bson.errors import InvalidDocument
from pymongo import MongoClient
from tqdm import tqdm

from temba_client.v2 import TembaClient

TOKEN_MX="0032fe79dbddceae3f4a86e86a726e16b88ec88e"

mx_client = TembaClient('rapidpro.datos.gob.mx', TOKEN_MX)
client = MongoClient('localhost', 27017)
db = client['test-db']

for c in tqdm(
        mx_client.get_contacts().all(),
        desc='==> Getting Contacts'):
    db['contacts'].insert_one({
        'blocked': c.blocked,
        'created_on': c.created_on,
        'fields': c.fields,
        'groups': [{'uuid':i.uuid, 'name':i.name} for i in c.groups],
        'language': c.language,
        'modified_on': c.modified_on,
        'name': c.name,
        'stopped': c.stopped,
        'urns': c.urns,
        'uuid': c.uuid
    })

