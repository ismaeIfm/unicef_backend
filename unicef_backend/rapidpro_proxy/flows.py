from elasticsearch_dsl import Q
import sys
sys.path.insert(0, '..')
import settings
from rapidpro_proxy.indexes import Value
from rapidpro_proxy.utils import *

####################### Auxiliar functions ##################


def _aux_number_by_state(fun, filter=[]):
    q = fun(child_querys=filter)
    q = aggregate_by_state(q)
    q = aggregate_by_run(q, bucket=BYSTATE_STR)
    response = q.execute()

    return format_aggs_runs(response.aggregations[BYSTATE_STR].buckets,
                            'state')


def _aux_number_by_mun(state, fun, filter=[]):

    q = fun(
        child_querys=filter,
        parent_querys=[Q('term', fields__rp_state_number=state)])
    q = aggregate_by_mun(q)
    q = aggregate_by_run(q, bucket=BYMUN_STR)
    response = q.execute()

    return format_aggs_runs(response.aggregations[BYMUN_STR].buckets,
                            'municipio')


def _aux_number_by_hospital(fun, filter=[]):

    q = fun(child_querys=filter)
    q = aggregate_by_hospital(q)
    q = aggregate_by_run(q, bucket=BYHOSPITAL_STR)
    response = q.execute()

    return format_aggs_runs(response.aggregations[BYHOSPITAL_STR].buckets,
                            'hospital')


def _aux_number_by_channel(fun, filter=[]):
    result = {}
    aux_query = lambda query : aggregate_by_run(fun(parent_querys=[query], child_querys=filter)).execute().aggregations[RUNSCOUNT_STR].doc_count

    result["fb"] = aux_query(Q('match', urns='facebook'))
    result["sms"] = aux_query(Q('match', urns='tel'))

    return format_result(result, 'channel')


#NAin
#@decorator("time")
def _aux_number_by_baby_age(fun, filter=[]):
    q = fun(child_querys=filter)
    q = aggregate_by_baby_age(q)
    q = aggregate_by_run(q, bucket=BYBABYAGE_STR)
    response = q.execute()

    return response.aggregations.by_baby_age.buckets


##########################################################################
#                             Msgs part                                  #
##########################################################################


@decorator('time')
def number_sent_msgs_by_state(filter=[]):
    q = search_runs_by_contact_info(child_querys=filter)
    q = aggregate_by_state(q)
    q = aggregate_by_run(q, BYSTATE_STR)
    q = aggregate_by_msg(q, BYSTATE_STR, RUNSCOUNT_STR)

    response = q.execute()

    return format_aggs_aggs_result_runs(response, 'state', BYSTATE_STR, 'msg',
                                        BYMSG_STR)


@decorator('time')
def number_sent_msgs_by_mun(state, filter=[]):
    q = search_runs_by_contact_info(
        child_querys=filter,
        parent_querys=[Q('term', fields__rp_state_number=state)])
    q = aggregate_by_mun(q)
    q = aggregate_by_run(q, bucket=BYMUN_STR)
    q = aggregate_by_msg(q, bucket1=BYMUN_STR, bucket2=RUNSCOUNT_STR)

    response = q.execute()

    return format_aggs_aggs_result_runs(response, 'municipio', BYMUN_STR,
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
    q = search_runs_by_contact_info(child_querys=filter)
    q = aggregate_by_baby_age(q)
    q = aggregate_by_run(q, bucket=BYBABYAGE_STR)
    q = aggregate_by_msg(q, bucket1=BYBABYAGE_STR, bucket2=RUNSCOUNT_STR)

    response = q.execute()

    return response.aggregations.by_baby_age.buckets


##########################################################################
#                         Mi alerta       (use flow auxiliar methods)    #
##########################################################################


def aux_mialerta(parent_querys=[], child_querys=[]):
    return search_runs_by_contact_info(
        parent_querys=parent_querys,
        child_querys=[Q('term', flow_uuid=settings.MIALERTA_FLOW)] +
        child_querys)


@decorator("time")
def number_mialerta_by_group(filter=[]):
    result = {}
    aux_query = lambda query: aggregate_by_value(search_values_by_contact_info(
        parent_querys=[query],
        child_querys=[
            Q('term', flow_uuid=settings.MIALERTA_FLOW),
            Q('term', node=settings.MIALERTA_NODE)
        ] + filter)).execute().aggregations[VALUESCOUNT_STR].doc_count

    result['baby'] = aux_query(Q('term', fields__rp_ispregnant='1'))
    result['pregnant'] = aux_query(Q('term', fields__rp_ispregnant='0'))
    result['personal'] = aux_query(Q('term', groups__name='PERSONAL_SALUD'))
    return result


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


@decorator("time")
def number_mialerta_msgs_top(filter=[]):

    s = Value.search()
    q = s.query(
        'bool',
        must=[
            Q('term', flow_uuid=settings.MIALERTA_FLOW),
            Q('term', node=settings.MIALERTA_NODE)
        ] + filter)

    a = A('terms', field='category', size=2147483647)
    q.aggs.bucket('per_category', a)
    response = q.execute()

    return format_aggs_result(response.aggregations.per_category.buckets,
                              'category')


##########################################################################
#                        Cancel part   (Use flow auxiliar methods)       #
##########################################################################


def aux_cancela(parent_querys=[], child_querys=[]):
    return search_runs_by_contact_info(
        parent_querys=parent_querys,
        child_querys=[Q('term', flow_uuid=settings.CANCEL_FLOW)] +
        child_querys)


@decorator("time")
def number_cancel_by_group(filter=[]):
    result = {}
    aux_query = lambda query: aggregate_by_value(search_values_by_contact_info(
        parent_querys=[query],
        child_querys=[
            Q('term', flow_uuid=settings.CANCEL_FLOW),
            Q('term', node=settings.CANCEL_NODE)
        ] + filter)).execute().aggregations[VALUESCOUNT_STR].doc_count

    result['baby'] = aux_query(Q('term', fields__rp_ispregnant='1'))
    result['pregnant'] = aux_query(Q('term', fields__rp_ispregnant='0'))
    result['personal'] = aux_query(Q('term', groups__name='PERSONAL_SALUD'))
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
