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

from unicef_backend import create_app, settings
from unicef_backend.indexes import Action, Contact, Run, Value
from unicef_backend.utils import _format_date, _format_str

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


def insert_one_contact(c):
    fields = {k: v(c.fields.get(k, '')) for k, v in CONTACT_FIELDS.items()}
    groups = [{'uuid': i.uuid, 'name': i.name} for i in c.groups]
    contact = {
        '_id': c.uuid,
        'urns': c.urns,
        'created_on': c.created_on,
        'groups': groups,
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


def search_contact(uuid):
    try:
        contact = Contact.get(id=uuid)
    except NotFoundError:  #Need to update datebase
        contacts = mx_client.get_contacts(uuid=uuid).all()
        if contacts:
            contact = insert_one_contact(contacts[0])
        else:
            return ""
    return contact


def insert_value_run(run):
    for value in run.values:

        value_dict = {
            'flow_uuid': run.flow.uuid,
            'flow_name': run.flow.name,
            'contact_uuid': run.contact.uuid,
            'node': value,
            'category': run.values[value].category,
            'time': run.values[value].time,
            'response': run.values[value].value
        }

        v = Value(**value_dict)
        v.meta.parent = run.contact.uuid
        v.save()


def get_type_flow(flow_name):
    aux_flow = lambda fs: any([i in flow_name for i in fs])
    if aux_flow(settings.CONSEJOS_FLOWS):
        return 'consejos'
    elif aux_flow(settings.RETOS_FLOWS):
        return 'retos'
    elif aux_flow(settings.RECORDATORIOS_FLOWS):
        return 'recordatorios'
    elif aux_flow(settings.PLANIFICACION_FLOWS):
        return 'planificacion'
    elif aux_flow(settings.INCENTIVOS_FLOWS):
        return 'incentivos'
    elif aux_flow(settings.PREOCUPACIONES_FLOWS):
        return 'preocupaciones'
    else:
        return 'otros'


def insert_run(run, path_item, action):

    run_dict = {
        'flow_uuid': run.flow.uuid,
        'flow_name': run.flow.name,
        'contact_uuid': run.contact.uuid,
        'type': get_type_flow(run.flow.name),
        'action_uuid': action['action_id'],
        'time': path_item.time,
        'msg': action['msg'],
    }

    r = Run(**run_dict)
    r.meta.parent = run.contact.uuid
    r.save()


def update_runs(after=None, last_runs=None):
    if not last_runs:
        last_runs = mx_client.get_runs(after=after).all(
            retry_on_rate_exceed=True)
    for run in last_runs:
        search_contact(run.contact.uuid)
        if run.flow.uuid == settings.MIALERTA_FLOW:  #MiAlerta
            insert_value_run(run)
        elif run.flow.uuid == settings.CANCEL_FLOW:  #Cancela
            insert_value_run(run)
        for path_item in run.path:
            try:
                action = Action.get(id=path_item.node)  # Search action
            except NotFoundError:
                #We ignore the path item if has a split or a group action
                continue
            insert_run(run, path_item, action)


def load_runs_from_csv(force = False):
    import csv
    import ast
    path = None
    with open('runs.csv') as csvfile:
        reader = csv.DictReader(csvfile, delimiter='|')
        for row in  reader:
            flow_uuid = row["flow_uuid"].strip()
            flow_name = row["flow_name"].strip() if row["flow_name"] else ""
            contact_uuid = row["contact_uuid"].strip() if row["contact_uuid"] else ""
            if not row["path"]:
                continue
            path = row["path"].strip().replace("null",'"null"')
            search_contact(contact_uuid)
            for path_item in ast.literal_eval(path):
                try:
                    action = Action.get(id=path_item["node_uuid"])  # Search action
                except NotFoundError:
                    #We ignore the path item if has a split or a group action
                    continue
                run_dict = {
                    'flow_uuid': flow_uuid,
                    'flow_name': flow_name,
                    'contact_uuid': contact_uuid,
                    'type': get_type_flow(flow_name),
                    'action_uuid': action['action_id'],
                    'time': path_item["arrived_on"],
                    'msg': action['msg'],
                }
                r = Run(**run_dict)
                r.meta.parent = run.contact.uuid
                r.save()

@manager.command
def load_flows(force=False):
    Action.init()
    data = json.load(open('actions.json'))
    for i in tqdm(data, desc='==> Getting Actions'):
        for _id, m in i.items():
            message = m["base"] if "base" in m else m["spa"]
            message = message["base"] if "base" in message and type(
                message) == dict else message
            Action(**{'action_id': _id, 'msg': message, '_id': _id}).save()


@manager.command
def download_contacts(force=False):
    date = (datetime.utcnow() - timedelta(days=30)).isoformat()
    contacts = mx_client.get_contacts(after=date).all()
    for c in tqdm(contacts, desc='==> Getting Contacts'):
        #Only save misalud contacts
        if not "MIGRACION_PD" in [i.name for i in c.groups]:
            #Normalize date
            insert_one_contact(c)


@manager.command
def delete_index(force=False):
    index = Index(settings.INDEX)
    index.delete(ignore=404)


@manager.command
def create_index():
    index = Index(settings.INDEX)
    index.delete(ignore=404)
    for t in [Action, Contact, Run, Value]:
        index.doc_type(t)
    index.create()


@manager.command
def download_test_runs(force=False):
    after = datetime.utcnow() - timedelta(days=1)
    after = after.isoformat()
    update_runs(after)
    print("Descargando alerta")
    #Temporal download mialerta runs
    runs = mx_client.get_runs(flow=settings.MIALERTA_FLOW).all()
    update_runs(last_runs=runs)

    print("Descargando cancela")
    #Temporal download cancela runs
    runs = mx_client.get_runs(flow=settings.CANCEL_FLOW).all()
    update_runs(last_runs=runs)


if __name__ == '__main__':
    manager.run()
