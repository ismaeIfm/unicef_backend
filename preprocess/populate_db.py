from bson.errors import InvalidDocument
from pymongo import MongoClient
from tqdm import tqdm
from datetime import datetime, timedelta
from dateutil.parser import parse
from temba_client.v2 import TembaClient

import load_flows 
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
def download_contacts():
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
    
##################### To add to tasks ###########################


def create_base_node(run,contact):
        return {  'flow_uuid': run.flow.uuid,
                  'flow_name': run.flow.name,
                  'contact_uuid': run.contact.uuid,
                  'rp_deliverydate': contact["fields"]["rp_deliverydate"],
                  'rp_duedate': contact["fields"]["rp_duedate"],
                  'rp_state_number': contact["fields"]["rp_state_number"],
                  'rp_mun': contact["fields"]["rp_mun"],
                  'urns': contact["urns"],
                  'rp_atenmed': contact["fields"]["rp_atenmed"],
                  'rp_ispregnant': contact["fields"]["rp_ispregnant"],
                  'groups': [{'uuid':i["uuid"], 'name':i["name"]} for i in contact["groups"]]}

def search_contact(uuid):
    contact = db["contacts"].find_one({"uuid": uuid})
    if not contact: #Need to update datebase 
       contacts = mx_client.get_contacts(uuid=uuid)
       if contacts:
           c = contacts[0]
           contact = c.serialize()
           insert_one_contact(c) 
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
    return contact


def insert_value_run(run):
    for value in run.values:
        contact = search_contact(run.contact.uuid)
        value_dic = create_base_node(run,contact)
        value_dic["node"] = value,
        value_dic["category"] = run.values[value].category
        value_dic["time"] = run.values[value].time
        value_dic["response"] = run.values[value].value
        db['values'].insert_one(value_dic)


def update_runs(after=None, last_runs=None):
    if not last_runs:
        last_runs = mx_client.get_runs(after = after).all(retry_on_rate_exceed=True)
    for run in last_runs:
        if run.flow.uuid == "07d56699-9cfb-4dc6-805f-775989ff5b3f": #MiAlerta
            insert_value_run(run)
        elif run.flow.uuid == "dbd5738f-8700-4ece-8b8c-d68b3f4529f7": #Cancela
            insert_value_run(run)
        for path_item in run.path:
          #Search action
          action = db["actions"].find_one({"action_id":path_item.node})
          if not action:
              #We ignore the path item if has a split or a group action
              continue
          contact = search_contact(run.contact.uuid)
          action_dict = create_base_node(run,contact)
          action_dict['time'] = path_item.time
          action_dict['action_uuid'] = action["action_id"]
          action_dict['msg'] = action["msg"] 
          db['runs'].insert_one(action_dict)
    
#################### Download  temporal data  ###########################
# Secuencia para tener datos verdaderos en la base de datos 
# Sin descargar toda la base
download_contacts()
after = datetime.utcnow() - timedelta(days=1)
after = after.isoformat()
update_runs(after)

print ("Descargando alerta")
#Temporal download mialerta runs
runs = mx_client.get_runs(flow="07d56699-9cfb-4dc6-805f-775989ff5b3f").all()
update_runs(last_runs=runs)

print("Descargando cancela")    
#Temporal download cancela runs    
runs = mx_client.get_runs(flow="dbd5738f-8700-4ece-8b8c-d68b3f4529f7").all()
update_runs(last_runs=runs)

