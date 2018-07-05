import json
import os
import re
from datetime import datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import Index
from flask_script import Manager
from temba_client.v2 import TembaClient
from tqdm import tqdm

import settings
from flask_backend import create_app
from rapidpro_proxy.indexes import Action, Contact, Run
from rapidpro_proxy.utils import (_format_date, _format_str,
                                  _get_difference_dates)

app = create_app('development')
mx_client = TembaClient('rapidpro.datos.gob.mx', os.getenv('TOKEN_MX'))
manager = Manager(app)

CONTACT_FIELDS = {
    'rp_deliverydate': _format_date,
    'rp_state_number': _format_str,
    'rp_ispregnant': _format_str,
    'rp_mun_cve': _format_str,
    'rp_atenmed': _format_str,
    'rp_mamafechanac': _format_date,
    'rp_duedate': _format_date,
    'rp_razonalerta': _format_str,
    'rp_razonbaja': _format_str,
    'calidad_antropometria': _format_str,
    'calidad_crecimuterino': _format_str,
    'calidad_lactancia': _format_str,
    'calidad_presionarterial': _format_str,
    'calidad_signosalarma': _format_str,
    'calidad_vacunas': _format_str
}

def parse_date_from_rp(field):
    if not field:
        return ""
    if isinstance(field, datetime):
        return field
    date_rp = field[:-1] if field[-1]=="." else field
    try:
        match = re.match(r"([0-9]+)",date_rp, re.I)
        if match and int(match.group()) <=31: #Then begin with days
            parse_date = parse(parse(date_rp,dayfirst=True).strftime("%d-%m-%Y"))
        else:
            parse_date = parse(parse(date_rp).strftime("%d-%m-%Y"))
    except ValueError:
        parse_date = ""
    return parse_date

def insert_one_contact(c):
    fields = {k: v(c.fields.get(k, '')) for k, v in CONTACT_FIELDS.items()}
    ###### Check if is baby to add pregnant_week
    week_birth = None
    ####################        BABY CONTACT        ############################
    if c.fields["rp_deliverydate"] and c.fields["rp_deliverydate"] != 'NULL':
        if not c.fields["rp_duedate"]:  # Then we dont have idea of pregnant week
            week_birth = 0
        else:
            try:
                duedate  = parse_date_from_rp(c.fields["rp_duedate"])
                delivery = parse_date_from_rp(c.fields["rp_deliverydate"])
                week_birth = _get_difference_dates(
                    duedate,
                    delivery, 'w')
            except ValueError:
                pass
    ##################       ALWAYS PARSE DATES ################################
    c.fields["rp_mamafechanac"] = parse_date_from_rp(c.fields["rp_mamafechanac"])
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
    if week_birth:
        week_birth = 0 if week_birth <= 0 else week_birth
        contact["pregnant_week"] = 40 - week_birth
    if  any(["PREGNANT" in g.name for g in c.groups ]) or \
        any(["PUERPERIUM" in g.name for g in c.groups ]) or \
        any(["PERSONAL_SALUD" in g.name for g in c.groups ]):
        cs = Contact(**contact)
        cs.save()
        return cs
    else:
        return None


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


def insert_run(run, path_item, action, c):
    contact_age = _get_difference_dates(start_date=c.fields.rp_mamafechanac, end_date=path_item.time,element='y')
    baby_age = _get_difference_dates( start_date= c.fields.rp_deliverydate,
                                      end_date = path_item.time,
                                     element='m')
    pregnant_difference = _get_difference_dates(start_date=c.fields.rp_duedate,
                                                end_date=path_item.time,
                                                element='w')
    week_pregnant, trimester_baby_age = None, None
    if pregnant_difference and pregnant_difference <= 40:
        week_pregnant = 40 - pregnant_difference if pregnant_difference <= 40 else 41
    if baby_age and baby_age >= 0 and baby_age <= 24:
        trimester_baby_age = (baby_age+2) // 3
        c.update_baby_age(trimester_baby_age)
    run_dict = {
        'urns': c.urns,
        'flow_uuid': run.flow.uuid,
        'flow_name': run.flow.name,
        'contact_uuid': run.contact.uuid,
        'type': get_type_flow(run.flow.name),
        'action_uuid': action['action_id'],
        'time': path_item.time,
        'msg': action['msg'],
        'responded': run.responded,
        'exit_type': run.exit_type,
        'is_one_way': False if run.values and run.responded else True,
        'fields': {
            'rp_ispregnant': _format_str(c.fields.rp_ispregnant),
            'rp_state_number': _format_str(c.fields.rp_state_number),
            'rp_mun_cve': _format_str(c.fields.rp_mun_cve),
            'rp_atenmed': _format_str(c.fields.rp_atenmed),
            'rp_razonalerta': _format_str(c.fields.rp_razonalerta),
            'rp_razonbaja': _format_str(c.fields.rp_razonbaja),
            'contact_age': contact_age,
        },
        'baby_age': trimester_baby_age,
        'pregnant_week': week_pregnant
    }
    r = Run(**run_dict)
    r.meta.parent = run.contact.uuid
    r.save()


def update_runs(after=None, last_runs=None):
    if not last_runs:
        last_runs = mx_client.get_runs(after=after).all(
            retry_on_rate_exceed=True)
    for run in last_runs:
        c = search_contact(run.contact.uuid)
        if not c:
            continue
        if get_type_flow(run.flow.name) == "otros" and not run.flow.uuid in [settings.CANCEL_FLOW, settings.MIALERTA_FLOW]:
            continue
        try:
            if run.flow.uuid in [
                    settings.MIALERTA_FLOW, settings.CANCEL_FLOW
            ]:
                contacts = mx_client.get_contacts(uuid=run.contact.uuid).all()
                contact = contacts[0]
                c.fields.rp_razonalerta = contact.fields.get('rp_razonalerta')
                c.fields.rp_razonbaja = contact.fields.get('rp_razonbaja')
                c.save()

            elif run.flow.uuid in settings.UPDATE_CONTACT_UUIDS:
                contacts = mx_client.get_contacts(uuid=run.contact.uuid).all()
                contact = contacts[0]
                if c.fields.rp_state_number != contact.fields.get(
                        'rp_mun_cve'
                ) or c.fields.rp_ispregnant != contact.fields.get(
                        'rp_ispregnant'
                ) or c.fields.rp_state_number != contact.fields.get(
                        'rp_state_number'
                ) or c.fields.rp_deliverydate != contact.fields.get(
                        'rp_deliverydate'):
                    aux_c = c.to_dict()
                    del aux_c['uuid']
                    c.fields["rp_duedate"]      = parse_date_from_rp(contact.fields.get('rp_duedate'))
                    c.fields["rp_deliverydate"] = parse_date_from_rp(contact.fields.get('rp_deliverydate'))
                    Contact(**aux_c).save()
                    c.fields.rp_state_number = contact.fields.get('rp_mun_cve')
                    c.fields.rp_ispregnant = contact.fields.get(
                        'rp_ispregnant')
                    c.fields.rp_state_number = contact.fields.get(
                        'rp_state_number')
                    new_date =  contact.fields.get('rp_deliverydate')
                    if new_date and  any(c.isdigit() for c in new_date):
                        c.fields.rp_deliverydate = new_date
                    c.save()
        except IndexError:
            continue

        for path_item in run.path:
            try:
                action = Action.get(id=path_item.node)  # Search action
            except NotFoundError:
                #We ignore the path item if has a split or a group action
                continue
            insert_run(run, path_item, action, c)
            break #Only run first msg

def load_flows():
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
    contacts = mx_client.get_contacts(group="ALL").all()
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
    for t in [Action, Contact, Run]:
        index.doc_type(t)
    index.create()
    load_flows()


@manager.command
def download_test_runs(force=False):
    after = datetime.utcnow() - timedelta(days=2)
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
