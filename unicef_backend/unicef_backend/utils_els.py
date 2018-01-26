from datetime import date, datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from elasticsearch_dsl import A, Q

from unicef_backend.indexes import Contact, Run, Value

FIELDS_STATE = "fields.rp_state_number"
FIELDS_MUN = "fields.rp_mun"
FIELDS_DELIVERY = "fields.rp_deliverydate"

MIALERTA_FLOW = "07d56699-9cfb-4dc6-805f-775989ff5b3f"
MIALERTA_NODE = "response_1"

CANCEL_FLOW = "dbd5738f-8700-4ece-8b8c-d68b3f4529f7"
CANCEL_NODE = "response_3"


def _format_date(item):
    if item:
        try:
            return parse(item).isoformat()
        except ValueError:
            pass
    return None


def _format_str(item):
    if item:
        try:
            return str(item)
        except ValueError:
            pass
    return None


def decorator(argument):
    def date_decorator(function):
        """ Decorator to change start_date and end_date parameters to
            query dictionary
        """

        def wrapper(*args, **kwargs):
            start_date = kwargs["start_date"] if "start_date" in kwargs else ""
            end_date = kwargs["end_date"] if "end_date" in kwargs else ""
            filter_date = {}

            if start_date:
                filter_date["gte"] = start_date
            if end_date:
                filter_date["lte"] = end_date
            if filter_date:
                if argument == "rp_deliverydate":
                    filter_date = Q(
                        'range', fields__rp_deliverydate=filter_date)
                else:
                    filter_date = Q('range', created_on=filter_date)

                kwargs["filter"] = [filter_date]
            kwargs.pop('start_date', None)
            kwargs.pop('end_date', None)

            return function(*args, **kwargs)

        return wrapper

    return date_decorator


@decorator('rp_deliverydate')
def get_mom_age(filter=[], query=[]):
    query.extend(filter)
    s = Contact.search()
    q = s.query('bool', must=query)
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)
    print(q.count())


########################PENDIENTE
@decorator('rp_deliverydate')
def get_mom_age_by_state(filter=[], query=[]):
    query.extend(filter)
    s = Contact.search()
    q = s.query('bool', must=query)
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)
    print(q.count())

    #q.aggs['per_state'].bucket(
    #    'mom_age_by_state',
    #    'range',
    #    field='fields.rp_mamaedad',
    #    ranges=[{
    #        "to": 18.0
    #    }, {
    #        "from": 18.0,
    #        "to": 35.0
    #    }, {
    #        "from": 35.0
    #    }])

    #response = q.execute()
    #return {
    #    i['key']:
    #    {j['key']: j['doc_count']
    #     for j in i.mom_age_by_state.buckets}
    #    for i in response.aggregations.per_state.buckets
    #}


def get_mom_age_by_mun(state, query=[]):
    s = Contact.search()
    query.append(Q('term', fields__rp_state_number=state))
    q = s.query('bool', must=query)

    a = A('terms', field=FIELDS_MUN, size=2147483647)
    q.aggs.bucket('per_mun', a)
    q.aggs['per_mun'].bucket(
        'mom_age_by_mun',
        'range',
        field='fields.rp_mamaedad',
        ranges=[{
            "to": 18.0
        }, {
            "from": 18.0,
            "to": 35.0
        }, {
            "from": 35.0
        }])
    response = q.execute()

    return {
        i['key']: {j['key']: j['doc_count']
                   for j in i.mom_age_by_mun.buckets}
        for i in response.aggregations.per_mun.buckets
    }


##########################################################################
#                             Contacts part                              #
##########################################################################


#@date_decorator
def get_contacts_by_group(filter_date={}):
    pass


@decorator('created_on')
def get_contacts_by_channel(filter=[], query=[]):
    s = Contact.search()
    # Get facebook contacts
    q = s.query('bool', must=filter + query + [Q('match', urns='facebook')])
    facebook_contacts = q.count()
    # Get sms contacts
    q = s.query('bool', must=filter + query + [Q('match', urns='tel')])
    sms_contacts = q.count()
    return {"fb": facebook_contacts, "sms": sms_contacts}


@decorator('created_on')
def get_contacts_by_state(filter=[], query=[]):
    s = Contact.search()
    q = s.query('bool', must=query + filter)
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)

    response = q.execute()
    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_state.buckets
    }


@decorator('created_on')
def get_contacts_by_mun(state_number, filter=[], query=[]):
    s = Contact.search()
    q = s.query(
        'bool',
        must=query + filter +
        [Q('term', fields__rp_state_number=state_number)])
    a = A('terms', field=FIELDS_MUN, size=2147483647)
    q.aggs.bucket('per_mun', a)
    response = q.execute()

    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_mun.buckets
    }


@decorator('created_on')
def get_contacts_by_hospital(filter=[], query=[]):
    s = Contact.search()
    q = s.query('bool', must=query + filter)
    a = A('terms', field='fields.rp_atenmed', size=2147483647)
    q.aggs.bucket('per_hospital', a)

    response = q.execute()
    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_hospital.buckets
    }


#@date_decorator()
def get_contacts_by_baby_age(query=[]):
    s = Contact.search()
    q = s.query('bool', must=query)
    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    trimesters = [{
        "from": start_date_pointer - relativedelta(months=idx * 3),
        "to": end_date_pointer - relativedelta(months=(idx - 1) * 3)
    } for idx in range(1, 9)]

    a = A('range', field=FIELDS_DELIVERY, ranges=trimesters)
    q.aggs.bucket('contacts_by_baby_age', a)

    return response.aggregations.contacts_by_baby_age.buckets


##########################################################################
#                             Babies part                                #
##########################################################################


@decorator('rp_deliverydate')
def get_babies_by_state(query=[], filter=[]):
    return get_contacts_by_state(
        query=filter + query + [Q('term', fields__rp_ispregnant='0')])


@decorator('rp_deliverydate')
def get_babies_by_municipio(state_number, filter=[], query=[]):
    return get_contacts_by_mun(
        state_number,
        query=filter + query + [Q('term', fields__rp_ispregnant='0')])


@decorator('rp_deliverydate')
def get_babies_by_hospital(filter=[], query=[]):
    return get_contacts_by_hospital(
        query=filter + query + [Q('term', fields__rp_ispregnant='0')])


#@decorator('rp_deliverydate')
def get_babies_by_week():
    s = Contact.search()
    q = s.query('bool', must=query + [Q('term', fields__rp_ispregnant='1')])
    weeks = []
    weeks = [{
        "from": datetime.now() + timedelta(days=7 * i),
        "to": datetime.now() + timedelta(days=(i + 1) * 7)
    } for i in range(1, 40)]
    weeks.append({"from": datetime.now() + timedelta(days=40 * 7)})
    weeks.append({"to": datetime.now() + timedelta(days=7)})
    a = A('range', field='fields.rp_duedate', ranges=weeks)
    q.aggs.bucket('babies_per_week', a)

    response = q.execute()

    return response.aggregations.babies_per_week.buckets


##########################################################################
#                              States part                               #
##########################################################################
@decorator('rp_duedate')
def get_pregnant_by_state(filter=[]):
    return get_contacts_by_state(filter +
                                 [Q('term', fields__rp_ispregnant='1')])


@decorator('created_on')
def get_personal_by_state(filter={}):
    return get_contacts_by_state(filter +
                                 [Q('term', groups__name='PERSONAL_SALUD')])


@decorator('created_on')
def get_hostpital_by_state(filter=[]):
    #query.extend(filter)
    s = Contact.search()
    q = s.query('bool', must=filter)
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    s.aggs.bucket('per_state', a)
    q.aggs['per_state'].bucket(
        'hospital_by_state',
        'terms',
        field='fields.rp_atenmed',
        size=2147483647)
    response = q.execute()

    return {
        i['key']:
        {j['key']: j['doc_count']
         for j in i.hospital_by_state.buckets}
        for i in response.aggregations.per_state.buckets
    }


@decorator('created_on')
def get_channel_by_state(filter=[]):
    result = {}
    s = Contact.search()
    # Get facebook contacts
    q = s.query('bool', must=filter + query + [Q('match', urns='facebook')])
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)
    response = q.execute()
    result['facebook'] = {
        i['key']: i['doc_count']
        for i in response.aggregations.per_state.buckets
    }

    q = s.query('bool', must=filter + query + [Q('match', urns='tel')])
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)
    response = q.execute()
    result['sms'] = {
        i['key']: i['doc_count']
        for i in response.aggregations.per_state.buckets
    }
    return result


#@decorator('created_on')
def get_baby_age_by_state(filter=[]):
    s = Contact.search()

    a = A('terms', field=FIELDS_STATE, size=2147483647)
    s.aggs.bucket('per_state', a)

    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    trimesters = [{
        "from": start_date_pointer - relativedelta(months=idx * 3),
        "to": end_date_pointer - relativedelta(months=(idx - 1) * 3)
    } for idx in range(1, 9)]

    s.aggs['per_state'].bucket(
        'contacts_by_baby_age',
        'range',
        field=FIELDS_DELIVERY,
        ranges=trimesters)

    response = s.execute()

    return response.aggregations.per_state.buckets


##########################################################################
#                         Municipios part                               #
##########################################################################
#@decorator
def get_pregnant_by_municipio(state, filter=[]):
    return get_contacts_by_mun(
        state, query=filter + [Q('term', fields__rp_ispregnant='1')])


@decorator('created_on')
def get_personal_by_municipio(state, filter=[]):
    return get_contacts_by_mun(
        state, query=filter + [Q('term', groups__name='PERSONAL_SALUD')])


@decorator('created_on')
def get_hostpital_by_municipio(state, filter=[]):
    s = Contact.search()
    q = s.query(
        'bool', must=[Q('term', fields__rp_state_number=state)] + filter)
    a = A('terms', field=FIELDS_MUN, size=2147483647)
    q.aggs.bucket('per_mun', a)

    q.aggs['per_mun'].bucket(
        'hospital_by_mun', 'terms', field="fields.rp_atenmed", size=2147483647)

    return {
        i['key']:
        {j['key']: j['doc_count']
         for j in i.hospital_by_mun.buckets}
        for i in response.aggregations.per_mun.buckets
    }


@decorator('created_on')
def get_channel_by_municipio(state, filter=[]):
    result = {}
    s = Contact.search()
    # Get facebook contacts
    q = s.query(
        'bool',
        must=filter + query + [
            Q('term', fields__rp_state_number=state),
            Q('match', urns='facebook')
        ])
    a = A('terms', field=FIELDS_MUN, size=2147483647)
    q.aggs.bucket('per_mun', a)
    response = q.execute()
    result['facebook'] = {
        i['key']: i['doc_count']
        for i in response.aggregations.per_mun.buckets
    }

    q = s.query('bool', must=filter + query + [Q('match', urns='tel')])
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_mun', a)
    response = q.execute()
    result['sms'] = {
        i['key']: i['doc_count']
        for i in response.aggregations.per_mun.buckets
    }
    return result


#@date_decorator
def get_baby_age_by_municipio(state, filter=[]):
    s = Contact.search()
    q = s.query(
        'bool', must=[Q('term', fields__rp_state_number=state)] + filter)
    a = A('terms', field=FIELDS_MUN, size=2147483647)
    q.aggs.bucket('per_mun', a)

    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    trimesters = [{
        "from": start_date_pointer - relativedelta(months=idx * 3),
        "to": end_date_pointer - relativedelta(months=(idx - 1) * 3)
    } for idx in range(1, 9)]

    q.aggs['per_mun'].bucket(
        'contacts_by_baby_age',
        'range',
        field=FIELDS_DELIVERY,
        ranges=trimesters)

    response = q.execute()

    return response.aggregations.per_mun.buckets


##########################################################################
#                             Msgs part                                  #
##########################################################################
@decorator('time')
def get_sent_msgs_by_state(filter=[]):
    s = Run.search()
    q = s.query('bool', must=filter)
    a = A('terms', field='rp_state_number', size=2147483647)
    q.aggs.bucket('per_state', a)

    q.aggs['per_state'].bucket(
        'msgs_by_category', 'terms', field='msg', size=10)

    response = q.execute()

    return response.aggregations.per_state.buckets


@decorator('time')
def get_sent_msgs_by_mun(state, filter=[]):
    s = Run.search()
    q = s.query('bool', must=filter + [Q('term', rp_state_number=state)])
    a = A('terms', field='rp_mun', size=2147483647)
    q.aggs.bucket('per_mun', a)

    q.aggs['per_mun'].bucket('msgs_by_category', 'terms', field='msg', size=10)

    respose = q.execute()

    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_mun.buckets
    }


@decorator('time')
def get_sent_msgs_by_flow(filter=[]):
    s = Run.search()
    q = s.query('bool', must=filter)
    a = A('terms', field='type', size=10)
    q.aggs.bucket('per_flow', a)
    response = q.execute()
    return response.aggregations.per_flow.buckets


@decorator('rp_deliverydate')
def get_sent_msgs_by_flow(filter=[]):
    s = Run.search()
    q = s.query('bool', must=filter)
    a = A('terms', field='type', size=10)
    q.aggs.bucket('per_flow', a)
    response = q.execute()
    return response.aggregations.per_flow.buckets


##########################################################################
#                         Mi alerta       (use flow auxiliar methods)    #
##########################################################################
@decorator('time')
def get_mialerta_by_group(filter=[]):
    result = {}
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=MIALERTA_FLOW)] + filter)

    result['baby'] = q.query('term', rp_ispregnant='0').count()
    result['pregnant'] = q.query('term', rp_ispregnant='1').count()
    result['personal'] = q.query('term', groups__name='PERSONAL_SALUD').count()

    return result


@decorator('time')
def get_mialerta_by_state(filter=[]):
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=MIALERTA_FLOW)] + filter)

    a = A('terms', field='rp_state_number', size=2147483647)
    q.aggs.bucket('per_state', a)
    response = q.execute()

    return response.aggregations.per_state.buckets


@decorator('time')
def get_mialerta_by_mun(state, filter=[]):
    s = Run.search()
    q = s.query(
        'bool',
        must=[
            Q('term', flow_uuid=MIALERTA_FLOW),
            Q('term', rp_state_number=state)
        ] + filter)

    a = A('terms', field='rp_mun', size=2147483647)
    q.aggs.bucket('per_mun', a)
    response = q.execute()

    return response.aggregations.per_mun.buckets


#NO encontre
@decorator('time')
def get_mialerta_by_hospital(filter=[]):
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=MIALERTA_FLOW)] + filter)

    a = A('terms', field='rp_atenmed', size=2147483647)
    q.aggs.bucket('per_hospital', a)
    response = q.execute()

    return


@decorator('time')
def get_mialerta_by_channel(filter=[]):
    result = {}
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=MIALERTA_FLOW)] + filter)

    result["fb"] = q.query('match', urns='facebook').count()
    result["sms"] = q.query('match', urns='tel').count()

    return result


#NAin
#@decorator("time")
def get_mialerta_by_baby_age(filter=[]):
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=MIALERTA_FLOW)] + filter)

    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    trimesters = [{
        "from": start_date_pointer - relativedelta(months=idx * 3),
        "to": end_date_pointer - relativedelta(months=(idx - 1) * 3)
    } for idx in range(1, 9)]

    a = A('range', field="rp_deliverydate", ranges=trimesters)
    q.aggs.bucket('contacts_by_baby_age', a)
    response = q.execute()

    return response.aggregations.contacts_by_baby_age.buckets


@decorator("time")
def get_mialerta_msgs_top(filter=[]):
    s = Value.search()
    q = s.query(
        'bool',
        must=[
            Q('term', flow_uuid=MIALERTA_FLOW),
            Q('term', node=MIALERTA_NODE)
        ] + filter)

    a = A('terms', field='category', size=2147483647)
    q.aggs.bucket('per_category', a)
    response = q.execute()

    return response.aggregations.per_category.buckets


##########################################################################
#                        Cancel part   (Use flow auxiliar methods)       #
##########################################################################
@decorator("time")
def get_cancel_by_group(filter=[]):
    result = {}
    s = Value.search()
    q = s.query(
        'bool',
        must=[Q('term', flow_uuid=CANCEL_FLOW),
              Q('term', node=CANCEL_NODE)] + filter)

    result['baby'] = q.query('term', rp_ispregnant='1').count()
    result['pregnant'] = q.query('term', rp_ispregnant='0').count()
    result['personal'] = q.query('term', groups__name='PERSONAL_SALUD').count()
    return result


@decorator("time")
def get_cancel_by_state(filter=[]):
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=CANCEL_FLOW)] + filter)

    a = A('terms', field='rp_state_number', size=2147483647)
    q.aggs.bucket('per_state', a)
    response = q.execute()

    return response.aggregations.per_state.buckets


@decorator('time')
def get_cancel_by_mun(state, filter=[]):
    s = Run.search()
    q = s.query(
        'bool',
        must=[
            Q('term', flow_uuid=CANCEL_FLOW),
            Q('term', rp_state_number=state)
        ] + filter)

    a = A('terms', field='rp_mun', size=2147483647)
    q.aggs.bucket('per_mun', a)
    response = q.execute()

    return response.aggregations.per_mun.buckets


@decorator('time')
def get_cancel_by_hospital(filter=[]):
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=CANCEL_FLOW)] + filter)

    a = A('terms', field='rp_atenmed', size=2147483647)
    q.aggs.bucket('per_hospital', a)
    response = q.execute()

    return response.aggregations.per_hospital.buckets


@decorator('time')
def get_cancel_by_channel(filter=[]):
    result = {}
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=CANCEL_FLOW)] + filter)

    result["fb"] = q.query('match', urns='facebook').count()
    result["sms"] = q.query('match', urns='tel').count()

    return result


#NAin
#@decorator("time")
def get_cancel_by_baby_age(filter=[]):
    s = Run.search()
    q = s.query('bool', must=[Q('term', flow_uuid=CANCEL_FLOW)] + filter)

    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    trimesters = [{
        "from": start_date_pointer - relativedelta(months=idx * 3),
        "to": end_date_pointer - relativedelta(months=(idx - 1) * 3)
    } for idx in range(1, 9)]

    a = A('range', field="rp_deliverydate", ranges=trimesters)
    q.aggs.bucket('contacts_by_baby_age', a)
    response = q.execute()

    return response.aggregations.contacts_by_baby_age.buckets
