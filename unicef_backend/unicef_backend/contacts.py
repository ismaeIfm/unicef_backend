from elasticsearch_dsl import Q

import unicef_backend.settings as settings
from unicef_backend.utils import *


def number_contacts_by_group(filter=[]):

    q = search_contact(filter + [Q('match', group__name='PERSONAL_SALUD')])
    personal_contacts = q.count()

    q = search_contact(filter + [Q('match', fields__rp_ispregnant='1')])
    pregnant_contacts = q.count()

    q = search_contact(filter + [Q('match', fields__rp_ispregnant='0')])
    baby_contacts = q.count()

    groups = {
        'baby': baby_contacts,
        'pregnant': pregnant_contacts,
        'personal': personal_contacts
    }

    return format_result(groups, key='group')


@decorator('created_on')
def number_contacts_by_state(filter=[], query=[]):

    q = search_contact(filter + query)
    q = aggregate_by_state(q)

    response = q.execute()
    return format_aggs_result(
        response.aggregations[BYSTATE_STR].buckets, key='key')


@decorator('created_on')
def number_contacts_by_mun(state, filter=[], query=[]):
    q = search_contact(query + filter +
                       [Q('term', fields__rp_state_number=state)])
    q = aggregate_by_mun(q)
    response = q.execute()

    return format_aggs_result(
        response.aggregations[BYMUN_STR].buckets, key='key')


def number_contacts_by_mom_age():
    pass


#@date_decorator()
def number_contacts_by_baby_age(query=[]):
    q = search_contact(query)
    q = aggregate_by_baby_age(q)
    response = q.execute()

    return response.aggregations[BYBABYAGE_STR].buckets


@decorator('created_on')
def number_contacts_by_hospital(filter=[], query=[]):
    q = search_contact(query + filter)
    q = aggregate_by_hospital(q)
    response = q.execute()

    return format_aggs_result(
        response.aggregations[BYHOSPITAL_STR].buckets, key='key')


@decorator('created_on')
def number_contacts_by_channel(filter=[], query=[]):
    q = search_contact(filter + query + [Q('match', urns='facebook')])
    facebook_contacts = q.count()

    q = search_contact(filter + query + [Q('match', urns='tel')])
    sms_contacts = q.count()

    channels = {'facebook': facebook_contacts, 'sms': sms_contacts}

    return format_result(channels, key='key')


##########################################################################
#                             Babies part                                #
##########################################################################


@decorator('rp_deliverydate')
def number_babies_by_state(query=[], filter=[]):
    return number_contacts_by_state(
        query=filter + query + [Q('term', fields__rp_ispregnant='0')])


@decorator('rp_deliverydate')
def number_babies_by_mun(state, filter=[], query=[]):
    return number_contacts_by_mun(
        state, query=filter + query + [Q('term', fields__rp_ispregnant='0')])


def number_babies_by_mom_age():
    pass


@decorator('rp_deliverydate')
def number_babies_by_hospital(filter=[], query=[]):
    return number_contacts_by_hospital(
        query=filter + query + [Q('term', fields__rp_ispregnant='0')])


#@decorator('rp_deliverydate')
def number_babies_by_week():
    q = search_contact([Q('term', fields__rp_ispregnant='1')])
    q = aggregate_per_week_pregnant(q)
    response = q.execute()
    return response.aggregations[BYWEEKPREGNAT_STR].buckets


##########################################################################
#                              States part                               #
##########################################################################
@decorator('rp_duedate')
def number_pregnant_by_state(filter=[]):
    return number_contacts_by_state(filter +
                                    [Q('term', fields__rp_ispregnant='1')])


@decorator('rp_duedate')
def number_moms_by_state(filter=[]):
    return number_contacts_by_state(filter +
                                    [Q('term', fields__rp_ispregnant='0')])


@decorator('created_on')
def number_personal_by_state(filter=[]):
    return number_contacts_by_state(filter +
                                    [Q('term', groups__name='PERSONAL_SALUD')])


def number_moms_by_state_age():
    pass


#@decorator('created_on')
def number_baby_age_by_state():

    q = search_contact()
    q = aggregate_by_state(q)
    q = aggregate_by_baby_age(q, bucket=BYSTATE_STR)
    response = q.execute()

    return response.aggregations[BYSTATE_STR].buckets


@decorator('created_on')
def number_hostpital_by_state(filter=[]):
    q = search_contact(filter)
    q = aggregate_by_state(q)
    q = aggregate_by_hospital(q, bucket=BYSTATE_STR)
    response = q.execute()

    return format_aggs_aggs_result(
        response,
        key_1='state',
        bucket_1=BYSTATE_STR,
        key_2='hospital',
        bucket_2=BYHOSPITAL_STR)


@decorator('created_on')
def number_channel_by_state(filter=[]):
    result = {}

    q = search_contact(filter + [Q('match', urns='facebook')])
    q = aggregate_by_state(q)
    response = q.execute()
    result['facebook'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYSTATE_STR].buckets
    }

    q = search_contact(filter + [Q('match', urns='tel')])
    q = aggregate_by_state(q)
    response = q.execute()
    result['sms'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYSTATE_STR].buckets
    }
    return result


##########################################################################
#                         Municipios part                               #
##########################################################################
@decorator('rp_duedate')
def number_pregnant_by_mun(state, filter=[]):
    return number_contacts_by_mun(
        state, query=filter + [Q('term', fields__rp_ispregnant='1')])


@decorator('rp_duedate')
def number_moms_by_mun(state, filter=[]):
    return number_contacts_by_mun(
        state, query=filter + [Q('term', fields__rp_ispregnant='0')])


@decorator('created_on')
def number_personal_by_mun(state, filter=[]):
    return number_contacts_by_mun(
        state, query=filter + [Q('term', groups__name='PERSONAL_SALUD')])


def number_moms_by_mun_age():
    pass


#@date_decorator
def number_baby_age_by_mun(state, filter=[]):
    q = search_contact([Q('term', fields__rp_state_number=state)] + filter)
    q = aggregate_by_mun(q)
    q = aggregate_by_baby_age(q, bucket=BYMUN_STR)

    response = q.execute()

    return response.aggregations[BYMUN_STR].buckets


@decorator('created_on')
def number_hostpital_by_mun(state, filter=[]):
    q = search_contact([Q('term', fields__rp_state_number=state)] + filter)
    q = aggregate_by_mun(q)
    q = aggregate_by_hospital(q, bucket=BYMUN_STR)

    response = q.execute()

    return format_aggs_aggs_result(
        response.aggregations[BYMUN_STR].buckets,
        key_1='municipio',
        bucket_1=BYMUN_STR,
        key_2='hospital',
        bucket_2=BYHOSPITAL_STR)


@decorator('created_on')
def number_channel_by_mun(state, filter=[]):
    result = {}
    q = search_contact(filter + [
        Q('term', fields__rp_state_number=state),
        Q('match', urns='facebook')
    ])
    q = aggregate_by_mun(q)
    response = q.execute()
    result['facebook'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYMUN_STR].buckets
    }

    q = search_contact(filter + [
        Q('term', fields__rp_state_number=state),
        Q('match', urns='tel')
    ])
    q = aggregate_by_mun(q)
    response = q.execute()
    result['sms'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYMUN_STR].buckets
    }
    return result
