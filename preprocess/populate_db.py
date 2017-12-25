from bson.errors import InvalidDocument
from pymongo import MongoClient
from tqdm import tqdm

from dateutil.parser import parse
from temba_client.v2 import TembaClient

TOKEN_MX="0032fe79dbddceae3f4a86e86a726e16b88ec88e"

mx_client = TembaClient('rapidpro.datos.gob.mx', TOKEN_MX)
client = MongoClient('localhost', 27017)
db = client['test-db']



########################### Format to date ###############################


"""
Parse string date depends on format
"""
def format_date(fields_dic, key):
    item = fields_dic[key]
    if item:
        try:
            return parse(item).isoformat()
        except ValueError:
            pass
    return item


########################### Download data ################################
for c in tqdm(
        mx_client.get_contacts().all(),
        desc='==> Getting Contacts'):
    #Only save misalud contacts
    if not "MIGRACION_PD"  in [i.name for i in c.groups]:
        #Normalize date
        c.fields["rp_duedate"] = format_date(c.fields, "rp_duedate")
        c.fields["rp_deliverydate"] = format_date(c.fields,"rp_deliverydate")
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

