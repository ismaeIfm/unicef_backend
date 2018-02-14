import sys

from elasticsearch_dsl import Q

import settings
from rapidpro_proxy.utils import *

sys.path.insert(0, '..')

####################### Auxiliar functions ##################


def _aux_number_by_state(fun, filter=[]):
    q = fun()
    q = aggregate_by_state(q)
    q = aggregate_by_run(q, bucket=BYSTATE_STR)
    q = filter_aggregate_by_date(
        q, BYSTATE_STR, RUNSCOUNT_STR, range_date={"time": {}})
    response = q.execute()

    return format_aggs_runs(response.aggregations[BYSTATE_STR].buckets,
                            'state')


def _aux_number_by_mun(state, fun, filter=[]):

    q = fun(parent_querys=[Q('term', fields__rp_state_number=state)])
    q = aggregate_by_mun(q)
    q = aggregate_by_run(q, bucket=BYMUN_STR)
    q = filter_aggregate_by_date(
        q, BYMUN_STR, RUNSCOUNT_STR, range_date={"time": {}})
    response = q.execute()

    return format_aggs_runs(response.aggregations[BYMUN_STR].buckets,
                            'municipio')


def _aux_number_by_hospital(fun, filter=[]):

    q = fun(child_querys=filter)
    q = aggregate_by_hospital(q)
    q = aggregate_by_run(q, bucket=BYHOSPITAL_STR)
    q = filter_aggregate_by_date(
        q, BYHOSPITAL_STR, RUNSCOUNT_STR, range_date={"time": {}})
    response = q.execute()

    return format_aggs_runs(response.aggregations[BYHOSPITAL_STR].buckets,
                            'hospital')


def _aux_channel(fun, query):
    q = aggregate_by_run(fun(parent_querys=[query]))
    q = filter_aggregate_by_date(q, RUNSCOUNT_STR, range_date={"time": {}})
    response = q.execute()
    return response.aggregations[RUNSCOUNT_STR].filter_date.doc_count


def _aux_number_by_channel(fun, filter=[]):
    return {
        q: _aux_channel(fun, Q('match', urns=q))
        for q in ['facebook', 'tel']
    }


#NAin
#@decorator("time")
def _aux_number_by_baby_age(fun, filter=[]):
    q = fun(child_querys=filter)
    q = aggregate_by_baby_age(q)
    q = aggregate_by_run(q, bucket=BYBABYAGE_STR)
    q = filter_aggregate_by_date(
        q, BYBABYAGE_STR, RUNSCOUNT_STR, range_date={"time": {}})
    response = q.execute()

    return response.aggregations.by_baby_age.buckets


##########################################################################
#                             Msgs part                                  #
##########################################################################


@decorator('time')
def number_sent_msgs_by_state(filter=[]):
    q = search_runs_by_contact_info()
    q = aggregate_by_state(q)
    q = aggregate_by_run(q, BYSTATE_STR)
    q = filter_aggregate_by_date(
        q, BYSTATE_STR, RUNSCOUNT_STR, range_date={"time": {}})
    q = aggregate_by_msg(q, BYSTATE_STR, RUNSCOUNT_STR, FILTERDATE_STR)

    response = q.execute()

    return format_aggs_aggs_result_runs_date(response, 'state', BYSTATE_STR,
                                             'msg', BYMSG_STR)


@decorator('time')
def number_sent_msgs_by_mun(state, filter=[]):
    q = search_runs_by_contact_info(
        parent_querys=[Q('term', fields__rp_state_number=state)])
    q = aggregate_by_mun(q)
    q = aggregate_by_run(q, bucket=BYMUN_STR)
    q = filter_aggregate_by_date(
        q, BYMUN_STR, RUNSCOUNT_STR, range_date={"time": {}})
    q = aggregate_by_msg(q, BYMUN_STR, RUNSCOUNT_STR, FILTERDATE_STR)

    response = q.execute()

    return format_aggs_aggs_result_runs_date(response, 'municipio', BYMUN_STR,
                                             'msg', BYMSG_STR)


@decorator('time')
def number_sent_msgs_by_flow(filter=[]):
    q = search_run(filter)
    q = aggregate_by_flow(q)
    response = q.execute()
    return format_aggs_result(response.aggregations[BYFLOW_STR].buckets,
                              'categoria')


@decorator('time')
def number_sent_msgs_by_baby_age(filter=[]):
    q = search_runs_by_contact_info()
    q = aggregate_by_baby_age(q)
    q = aggregate_by_run(q, bucket=BYBABYAGE_STR)
    q = filter_aggregate_by_date(
        q, BYBABYAGE_STR, RUNSCOUNT_STR, range_date={"time": {}})

    q = aggregate_by_msg(q, BYBABYAGE_STR, RUNSCOUNT_STR, FILTERDATE_STR)

    response = q.execute()

    return response.aggregations.by_baby_age.buckets


##########################################################################
#                         Mi alerta       (use flow auxiliar methods)    #
##########################################################################


### This needs review
def aux_mialerta(parent_querys=[], child_querys=[]):
    return search_runs_by_contact_info(
        parent_querys=parent_querys,
        child_querys=[Q('term', flow_uuid=settings.MIALERTA_FLOW)] +
        child_querys)


@decorator('time')
def number_mialerta_by_state(filter=[]):
    return _aux_number_by_state(aux_mialerta, filter=filter)


@decorator('time')
def number_mialerta_by_mun(state, filter=[]):
    return _aux_number_by_mun(state, aux_mialerta, filter=filter)


#NO encontre
@decorator('time')
def number_mialerta_by_hospital(filter=[]):
    return _aux_number_by_hospital(aux_mialerta, filter=filter)


@decorator('time')
def number_mialerta_by_channel(filter=[]):
    return _aux_number_by_channel(aux_mialerta, filter=filter)


#NAin
#@decorator("time")
def number_mialerta_by_baby_age(filter=[]):
    return _aux_number_by_baby_age(aux_mialerta, filter=filter)


#@decorator("time")
#def number_mialerta_msgs_top(filter=[]):
#
#    s = Value.search()
#    q = s.query(
#        'bool',
#        must=[
#            Q('term', flow_uuid=settings.MIALERTA_FLOW),
#            Q('term', node=settings.MIALERTA_NODE)
#        ] + filter)
#
#    a = A('terms', field='category', size=2147483647)
#    q.aggs.bucket('per_category', a)
#    response = q.execute()
#
#    return format_aggs_result(response.aggregations.per_category.buckets,
#                              'category')

##########################################################################
#                        Cancel part   (Use flow auxiliar methods)       #
##########################################################################


def aux_cancela(parent_querys=[], child_querys=[]):
    return search_runs_by_contact_info(
        parent_querys=parent_querys,
        child_querys=[Q('term', flow_uuid=settings.CANCEL_FLOW)] +
        child_querys)


def _aux_cancel_by_group(query):
    q = search_values_by_contact_info(
        parent_querys=[query],
        child_querys=[
            Q('term', flow_uuid=settings.CANCEL_FLOW),
            Q('term', node=settings.CANCEL_NODE)
        ])
    q = aggregate_by_value(q)
    q = filter_aggregate_by_date(q, VALUESCOUNT_STR, range_date={"time": {}})
    response = q.execute()
    return response.aggregations[VALUESCOUNT_STR].filter_date.doc_count


#cambiar por fields
@decorator("time")
def number_cancel_by_group(filter=[]):
    result = {}
    result['baby'] = _aux_cancel_by_group(Q('term', fields__rp_ispregnant='1'))
    result['pregnant'] = _aux_cancel_by_group(
        Q('term', fields__rp_ispregnant='0'))
    result['personal'] = _aux_cancel_by_group(
        Q('term', groups__name='PERSONAL_SALUD'))
    return result


@decorator("time")
def number_cancel_by_state(filter=[]):
    return _aux_number_by_state(aux_cancela, filter=filter)


@decorator('time')
def number_cancel_by_mun(state, filter=[]):
    return _aux_number_by_mun(state, aux_cancela, filter=filter)


@decorator('time')
def number_cancel_by_hospital(filter=[]):
    return _aux_number_by_hospital(aux_cancela, filter=filter)


@decorator('time')
def number_cancel_by_channel(filter=[]):
    return _aux_number_by_channel(aux_cancela, filter=filter)


#NAin
#@decorator("time")
def number_cancel_by_baby_age(filter=[]):
    return _aux_number_by_baby_age(aux_cancela, filter=filter)


##########################################################################
#                   Rate completed messages part                         #
##########################################################################


def aux_rate_completed_messages(parent_querys=[], child_querys=[]):
    total = search_run(child_querys + [
        Q('has_parent', type='contact', query=Q('bool', must=parent_querys))
    ]).count()

    q = search_runs_by_contact_info(parent_querys=parent_querys)
    q = aggregate_by_run(q)
    q = filter_aggregate_by_date(q, RUNSCOUNT_STR, range_date={"time": {}})
    q = filter_completed(q, RUNSCOUNT_STR, FILTERDATE_STR)
    q = aggregate_by_way(q, RUNSCOUNT_STR, FILTERDATE_STR, FILTERCOMPLETED_STR)

    response = q.execute()
    runs_completed = {
        i['key']: i['doc_count']
        for i in response.aggregations.runs_count.filter_date.filter_completed.
        by_way.buckets
    }

    return (runs_completed.get(0, 0) /
            (total - runs_completed.get(1, 0))) * 100


# Si no hay personal de salud que pedo?


@decorator('time')
def rate_completed_messages_by_group(filter=[]):
    result = {}
    result['baby'] = aux_rate_completed_messages(
        parent_querys=[Q('term', fields__rp_ispregnant='1')])
    result['pregnant'] = aux_rate_completed_messages(
        parent_querys=[Q('term', fields__rp_ispregnant='0')])
    result['personal'] = aux_rate_completed_messages(
        parent_querys=[Q('term', groups__name='PERSONAL_SALUD')])
    return result
