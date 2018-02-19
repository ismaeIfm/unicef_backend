import sys

from elasticsearch_dsl import Q

import settings as settings
from rapidpro_proxy.utils import *

sys.path.insert(0, '..')


@date_decorator('created_on')
def number_contacts_by_group(filter_date=[]):

    q = search_contact(filter_date +
                       [Q('match', group__name='PERSONAL_SALUD')])
    personal_contacts = q.count()

    q = search_contact(filter_date + [Q('match', fields__rp_ispregnant='1')])
    pregnant_contacts = q.count()

    q = search_contact(filter_date + [Q('match', fields__rp_ispregnant='0')])
    baby_contacts = q.count()

    groups = {
        'baby': baby_contacts,
        'pregnant': pregnant_contacts,
        'personal': personal_contacts
    }

    return format_result(groups, key='group')


@date_decorator('created_on')
def number_contacts_by_state(filter_date=[], query=[]):

    q = search_contact(filter_date + query)
    aggregate_by_state(q)

    response = q.execute()
    return format_aggs_result(
        response.aggregations[BYSTATE_STR].buckets, key='key')


@date_decorator('created_on')
def number_contacts_by_mun(state, filter_date=[], query=[]):
    q = search_contact(query + filter_date +
                       [Q('term', fields__rp_state_number=state)])
    aggregate_by_mun(q)
    response = q.execute()

    return format_aggs_result(
        response.aggregations[BYMUN_STR].buckets, key='key')


@date_decorator('rp_deliverydate')
def number_contacts_by_mom_age(filter_date=[], query=[]):
    q = search_contact(query + filter_date + [
        Q('exists', field='fields.rp_mamafechanac'),
        Q('exists', field='fields.rp_duedate')
    ])
    aggregate_by_mom_age(q)
    response = q.execute()
    return format_aggs_result(response.aggregations[BYMOMAGE_STR].buckets,
                              'group')


@date_decorator('rp_deliverydate')
def number_contacts_by_baby_age(query=[], filter_date=[]):
    q = search_contact(query)
    aggregate_by_baby_age(q)
    response = q.execute()

    return format_aggs_result(response.aggregations[BYBABYAGE_STR].buckets,
                              'trimester')


@date_decorator('created_on')
def number_contacts_by_hospital(filter_date=[], query=[]):
    q = search_contact(query + filter_date)
    aggregate_by_hospital(q)
    response = q.execute()

    return format_aggs_result(
        response.aggregations[BYHOSPITAL_STR].buckets, key='key')


@date_decorator('created_on')
def number_contacts_by_channel(filter_date=[], query=[]):
    q = search_contact(filter_date + query + [Q('match', urns='facebook')])
    facebook_contacts = q.count()

    q = search_contact(filter_date + query + [Q('match', urns='tel')])
    sms_contacts = q.count()

    channels = {'facebook': facebook_contacts, 'sms': sms_contacts}

    return format_result(channels, key='key')


##########################################################################
#                             Babies part                                #
##########################################################################


@date_decorator('rp_deliverydate')
def number_babies_by_state(query=[], filter_date=[]):
    return number_contacts_by_state(
        query=filter_date + query + [Q('term', fields__rp_ispregnant='0')])


@date_decorator('rp_deliverydate')
def number_babies_by_mun(state, filter_date=[], query=[]):
    return number_contacts_by_mun(
        state,
        query=filter_date + query + [Q('term', fields__rp_ispregnant='0')])


@date_decorator('rp_deliverydate')
def number_babies_by_mom_age(query=[], filter_date=[]):
    return number_contacts_by_mom_age(
        query=filter_date + query + [Q('term', fields__rp_ispregnant='0')])


@date_decorator('rp_deliverydate')
def number_babies_by_hospital(filter_date=[], query=[]):
    return number_contacts_by_hospital(
        query=filter_date + query + [Q('term', fields__rp_ispregnant='0')])


#@date_decorator('rp_deliverydate')
#def number_babies_by_baby_age(query=[], filter_date=[]):
#    return number_contacts_by_baby_age(
#        query=filter_date + query + [Q('term', fields__rp_ispregnant='0')])


##########################################################################
#                              States part                               #
##########################################################################
@date_decorator('rp_duedate')
def number_pregnant_by_state(filter_date=[]):
    return number_contacts_by_state(filter_date +
                                    [Q('term', fields__rp_ispregnant='1')])


@date_decorator('rp_deliverydate')
def number_moms_by_state(filter_date=[]):
    return number_contacts_by_state(filter_date +
                                    [Q('term', fields__rp_ispregnant='0')])


@date_decorator('created_on')
def number_personal_by_state(filter_date=[]):
    return number_contacts_by_state(filter_date +
                                    [Q('term', groups__name='PERSONAL_SALUD')])


@date_decorator('rp_deliverydate')
def number_moms_by_state_age(filter_date=[]):
    q = search_contact(filter_date + [
        Q('exists', field='fields.rp_mamafechanac'),
        Q('exists', field='fields.rp_duedate')
    ])
    aggregate_by_state(q)
    aggregate_by_mom_age(q.aggs[BYSTATE_STR], single=False)

    response = q.execute()
    return format_aggs_aggs_result(response, 'state', BYSTATE_STR, 'group',
                                   BYMOMAGE_STR)


@date_decorator('rp_deliverydate')
def number_baby_age_by_state(filter_date=[]):

    q = search_contact()
    aggregate_by_state(q)
    aggregate_by_baby_age(q.aggs[BYSTATE_STR], single=False)
    response = q.execute()

    return format_aggs_aggs_result(response, 'state', BYSTATE_STR, 'trimester',
                                   BYBABYAGE_STR)


@date_decorator('created_on')
def number_hostpital_by_state(filter_date=[]):
    q = search_contact(filter_date)
    aggregate_by_state(q)
    aggregate_by_hospital(q.aggs[BYSTATE_STR], single=False)
    response = q.execute()

    return format_aggs_aggs_result(
        response,
        key_1='state',
        bucket_1=BYSTATE_STR,
        key_2='hospital',
        bucket_2=BYHOSPITAL_STR)


@date_decorator('created_on')
def number_channel_by_state(filter_date=[]):
    result = {}

    q = search_contact(filter_date + [Q('match', urns='facebook')])
    aggregate_by_state(q)
    response = q.execute()
    result['facebook'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYSTATE_STR].buckets
    }

    q = search_contact(filter_date + [Q('match', urns='tel')])
    aggregate_by_state(q)
    response = q.execute()
    result['sms'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYSTATE_STR].buckets
    }
    return result


##########################################################################
#                         Municipios part                               #
##########################################################################
@date_decorator('rp_duedate')
def number_pregnant_by_mun(state, filter_date=[]):
    return number_contacts_by_mun(
        state, query=filter_date + [Q('term', fields__rp_ispregnant='1')])


@date_decorator('rp_deliverydate')
def number_moms_by_mun(state, filter_date=[]):
    return number_contacts_by_mun(
        state, query=filter_date + [Q('term', fields__rp_ispregnant='0')])


@date_decorator('created_on')
def number_personal_by_mun(state, filter_date=[]):
    return number_contacts_by_mun(
        state, query=filter_date + [Q('term', groups__name='PERSONAL_SALUD')])


@date_decorator('rp_deliverydate')
def number_moms_by_mun_age(state, filter_date=[]):
    q = search_contact(filter_date + [
        Q('exists', field='fields.rp_mamafechanac'),
        Q('exists', field='fields.rp_duedate'),
        Q('term', fields__rp_state_number=state)
    ])
    aggregate_by_mun(q)
    aggregate_by_mom_age(q.aggs[BYMUN_STR], single=False)

    response = q.execute()
    return format_aggs_aggs_result(response, 'municipio', BYMUN_STR, 'group',
                                   BYMOMAGE_STR)


@date_decorator('rp_deliverydate')
def number_baby_age_by_mun(state, filter_date=[]):
    q = search_contact([Q('term', fields__rp_state_number=state)] +
                       filter_date)
    aggregate_by_mun(q)
    aggregate_by_baby_age(q.aggs[BYMUN_STR], single=False)

    response = q.execute()

    return format_aggs_aggs_result(response, 'municipio', BYMUN_STR,
                                   'trimester', BYBABYAGE_STR)


@date_decorator('created_on')
def number_hostpital_by_mun(state, filter_date=[]):
    q = search_contact([Q('term', fields__rp_state_number=state)] +
                       filter_date)
    aggregate_by_mun(q)
    aggregate_by_hospital(q.aggs[BYMUN_STR], single=False)

    response = q.execute()

    return format_aggs_aggs_result(
        response,
        key_1='municipio',
        bucket_1=BYMUN_STR,
        key_2='hospital',
        bucket_2=BYHOSPITAL_STR)


@date_decorator('created_on')
def number_channel_by_mun(state, filter_date=[]):
    result = {}
    q = search_contact(filter_date + [
        Q('term', fields__rp_state_number=state),
        Q('match', urns='facebook')
    ])
    aggregate_by_mun(q)
    response = q.execute()
    result['facebook'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYMUN_STR].buckets
    }

    q = search_contact(filter_date + [
        Q('term', fields__rp_state_number=state),
        Q('match', urns='tel')
    ])
    aggregate_by_mun(q)
    response = q.execute()
    result['sms'] = {
        i['key']: i['doc_count']
        for i in response.aggregations[BYMUN_STR].buckets
    }
    return result


@date_decorator('created_on')
def get_calidad_medica_by_state(calidad_field, filter_date=[]):
    q = search_contact(filter_date)
    aggregate_by_state(q)
    aggregate_by_calidad(q.aggs[BYSTATE_STR], calidad_field)

    response = q.execute()

    return response.aggregations[BYSTATE_STR]


@date_decorator('created_on')
def get_calidad_medica_by_mun(state, calidad_field, filter_date=[]):
    q = search_contact(filter_date +
                       [Q('term', fields__rp_state_number=state)])
    aggregate_by_mun(q)
    aggregate_by_calidad(q.aggs[BYMUN_STR], calidad_field)

    response = q.execute()

    return response.aggregations[BYMUN_STR]


@date_decorator('created_on')
def get_calidad_medica_by_hospital(state, calidad_field, filter_date=[]):
    q = search_contact(filter_date)
    aggregate_by_hospital(q)
    aggregate_by_calidad(q.aggs[BYHOSPITAL_STR], calidad_field)

    response = q.execute()

    return response.aggregations[BYHOSPITAL_STR]


@date_decorator('created_on')
def get_calidad_medica_by_mom_age(calidad_field, filter_date=[]):
    q = search_contact(query + filter_date + [
        Q('exists', field='fields.rp_mamafechanac'),
        Q('exists', field='fields.rp_duedate')
    ])
    aggregate_by_mom_age(q)

    aggregate_by_calidad(q.aggs[BYMOMAGE_STR], calidad_field)

    response = q.execute()

    return response.aggregations[BYSTATE_STR]


@date_decorator('created_on')
def get_calidad_medica_by_baby_age(calidad_field, filter_date=[]):
    q = search_contact(filter_date)
    aggregate_by_baby_age(q)
    aggregate_by_calidad(q.aggs[BYBABYAGE_STR], calidad_field)

    response = q.execute()

    return response.aggregations[BYBABYAGE_STR]


@date_decorator('created_on')
def ge_calidad_medica_by_hospital(state, calidad_field, filter_date=[]):
    q = search_contact(filter_date)
    aggregate_per_week_pregnant(q)
    aggregate_by_calidad(q.aggs[BYWEEKPREGNAT_STR], calidad_field)

    response = q.execute()

    return response.aggregations[BYWEEKPREGNAT_STR]
