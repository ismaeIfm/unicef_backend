from bson.errors import InvalidDocument
from pymongo import MongoClient
from tqdm import tqdm

from temba_client.v2 import TembaClient

TOKEN_MX = "8bfb6527b94aa97d3dc693daa5b98757ad25ffd6"

mx_client = TembaClient('rapidpro.datos.gob.mx', TOKEN_MX)
client = MongoClient('localhost', 27017)
db = client['test-db']

for c in tqdm(
        mx_client.get_contacts().all(),
        desc='==> Getting Contacts + Messages'):
    db['test-contacts'].insert_one({
        'blocked': c.blocked,
        'created_on': c.created_on,
        'fields': c.fields,
        'groups': [i.uuid for i in c.groups],
        'language': c.language,
        'modified_on': c.modified_on,
        'name': c.name,
        'stopped': c.stopped,
        'urns': c.urns,
        'uuid': c.uuid
    })
    for m in mx_client.get_messages(contact=c).all():
        db['test-messages'].insert_one({
            'broadcast': m.broadcast,
            'channel': m.channel.uuid,
            'contact': m.contact.uuid,
            'created_on': m.created_on,
            'direction': m.direction,
            'id': m.id,
            'labels': [i.uuid for i in m.labels],
            'modified_on': m.modified_on,
            'sent_on': m.sent_on,
            'status': m.status,
            'text': m.text,
            'type': m.type,
            'urn': m.urn,
            'visibility': m.visibility
        })
