from datetime import date, datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from elasticsearch_dsl import A, Q

from unicef_backend.indexes import Contact

FIELDS_STATE = "fields.rp_state_number"
FIELDS_MUN = "fields.rp_mun"


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


# def get_contacts_by_baby_age(query=[]):

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


def get_babies_by_week(query=[]):
    s = Contact.search()
    q = s.query('bool', must=query + [Q('term', fields__rp_ispregnant='1')])
    weeks = []
    #weeks = [{
    #    "from": datetime.now() + timedelta(days=7 * i),
    #    "to": datetime.now() + timedelta(days=(i + 1) * 7)
    #} for i in range(1, 40)]
    weeks.append({"from": datetime.now() + timedelta(days=40 * 7)})
    weeks.append({"to": datetime.now() + timedelta(days=7)})
    a = A('range', field='fields.rp_duedate', ranges=weeks)
    q.aggs.bucket('babies_per_week', a)
