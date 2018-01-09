from elasticsearch_dsl import A, Q

from indexes import Contact

FIELDS_STATE = "fields.rp_state_number"
FIELDS_MUN = "fields.rp_mun"


def get_mom_age_by_state(query=[]):
    s = Contact.search()
    q = s.query('bool', must=query)
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)
    q.aggs['per_state'].bucket(
        'mom_age_by_state',
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
        i['key']:
        {j['key']: j['doc_count']
         for j in i.mom_age_by_state.buckets}
        for i in response.aggregations.per_state.buckets
    }


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


def get_contacts_by_channel():
    s = Contact.search()
    # Get facebook contacts
    q = s.query('match', urns='facebook')
    facebook_contacts = q.count()
    # Get sms contacts
    q = s.query('match', urns='tel')
    sms_contacts = q.count()
    return {"fb": facebook_contacts, "sms": sms_contacts}


def get_contacts_by_state(query=[]):
    s = Contact.search()
    q = s.query('bool', must=query)
    a = A('terms', field=FIELDS_STATE, size=2147483647)
    q.aggs.bucket('per_state', a)

    response = q.execute()
    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_state.buckets
    }


def get_contacts_by_mun(state_number, query=[]):
    s = Contact.search()
    query.append(Q('term', fields__rp_state_number=state))
    q = s.query('bool', must=query)
    a = A('terms', field=FIELDS_MUN, size=2147483647)
    q.aggs.bucket('per_mun', a)

    response = q.execute()

    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_mun.buckets
    }


def get_contacts_by_hospital(query=[]):
    s = Contact.search()
    q = s.query('bool', must=query)
    a = A('terms', field='fields.rp_atenmed', size=2147483647)
    q.aggs.bucket('per_hospital', a)

    response = q.execute()
    return {
        i['key']: i['doc_count']
        for i in response.aggregations.per_hospital.buckets
    }
