import json
import os
import re
from datetime import datetime, timedelta

from dateutil.parser import parse
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Index
from flask_script import Manager
from temba_client.v2 import TembaClient
from tqdm import tqdm

from unicef_backend import create_app
from unicef_backend.indexes import Action, Contact, Run, Value
from unicef_backend.utils_els import _format_date, _format_str

app = create_app('development')
mx_client = TembaClient('rapidpro.datos.gob.mx', os.getenv('TOKEN_MX'))
manager = Manager(app)

CONTACT_FIELDS = {
    'rp_deliverydate': _format_date,
    'rp_state_number': _format_str,
    'rp_ispregnant': _format_str,
    'rp_mun': _format_str,
    'rp_atenmed': _format_str,
    'rp_Mamafechanac': _format_date,
    'rp_duedate': _format_date
}
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
    return None


@manager.command
def delete_index(force=False):
    index = Index('dashboard')
    index.delete(ignore=404)


@manager.command
def load_flows(force=False):
    Action.init()
    data = json.load(open('actions.json'))
    for i in tqdm(data, desc='==> Getting Actions'):
        for x, y in i.items():
            message = y["base"] if "base" in y else y["spa"]
            message = message["base"] if "base" in message and type(
                message) == dict else message
            Action(**{'action_id': x, 'msg': message, '_id': x}).save()


def insert_one_contact(c):
    fields = {k: v(c.fields.get(k, '')) for k, v in CONTACT_FIELDS.items()}
    contact = {
        '_id': c.uuid,
        'urns': c.urns,
        'created_on': c.created_on,
        'groups': [{
            'uuid': i.uuid,
            'name': i.name
        } for i in c.groups],
        'modified_on': c.modified_on,
        'uuid': c.uuid,
        'name': c.name,
        'language': c.language,
        'fields': fields,
        'stopped': c.stopped,
        'blocked': c.blocked
    }
    cs = Contact(**contact)
    cs.save()
    return cs


@manager.command
def download_contacts(force=False):
    Contact.init()
    #Group.init()
    Run.init()
    Value.init()
    for c in tqdm(mx_client.get_contacts().all(), desc='==> Getting Contacts'):
        #Only save misalud contacts
        if not "MIGRACION_PD" in [i.name for i in c.groups]:
            #Normalize date
            insert_one_contact(c)


def search_contact(uuid):
    try:
        contact = Contact.get(id=uuid)
    except NotFoundError:  #Need to update datebase
        contacts = mx_client.get_contacts(uuid=uuid)
        if contacts:
            c = contacts[0]
            contact = c.serialize()
            contact = insert_one_contact(c)
    return contact


def create_base_node(run, contact):
    if not contact.get('fields'):
        print(contact)
    return {
        'flow_uuid': run.flow.uuid,
        'flow_name': run.flow.name,
        'contact_uuid': run.contact.uuid,
        'rp_deliverydate': contact.get("fields", {}).get("rp_deliverydate"),
        'rp_duedate': contact.get("fields", {}).get("rp_duedate"),
        'rp_state_number': contact.get("fields", {}).get("rp_state_number"),
        'rp_mun': contact.get("fields", {}).get("rp_mun"),
        'urns': contact["urns"],
        'rp_atenmed': contact.get("fields", {}).get("rp_atenmed"),
        'rp_ispregnant': contact.get("fields", {}).get("rp_ispregnant"),
        'groups': [i for i in contact.get("groups", [])]
    }


def insert_value_run(run):
    for value in run.values:

        contact = search_contact(run.contact.uuid)
        value_dic = create_base_node(run, contact.to_dict())
        value_dic["node"] = value,
        value_dic["category"] = run.values[value].category
        value_dic["time"] = run.values[value].time
        value_dic["response"] = run.values[value].value
        Value(**value_dic).save()


def update_runs(after=None, last_runs=None):
    if not last_runs:
        last_runs = mx_client.get_runs(after=after).all(
            retry_on_rate_exceed=True)
    for run in last_runs:
        if run.flow.uuid == "07d56699-9cfb-4dc6-805f-775989ff5b3f":  #MiAlerta
            insert_value_run(run)
        elif run.flow.uuid == "dbd5738f-8700-4ece-8b8c-d68b3f4529f7":  #Cancela
            insert_value_run(run)
        for path_item in run.path:
            #Search action
            try:
                action = Action.get(id=path_item.node)
            except NotFoundError:
                #We ignore the path item if has a split or a group action
                continue
            contact = search_contact(run.contact.uuid)
            action_dict = create_base_node(run, contact.to_dict())
            action_dict['time'] = path_item.time
            action_dict['action_uuid'] = action["action_id"]
            action_dict['msg'] = action["msg"]
            Run(**action_dict).save()


@manager.command
def preprocess_runs(force=False):
    after = datetime.utcnow() - timedelta(days=1)
    after = after.isoformat()
    update_runs(after)
    print("Descargando alerta")
    #Temporal download mialerta runs
    runs = mx_client.get_runs(
        flow="07d56699-9cfb-4dc6-805f-775989ff5b3f").all()
    update_runs(last_runs=runs)

    print("Descargando cancela")
    #Temporal download cancela runs
    runs = mx_client.get_runs(
        flow="dbd5738f-8700-4ece-8b8c-d68b3f4529f7").all()
    update_runs(last_runs=runs)


if __name__ == '__main__':
    manager.run()
