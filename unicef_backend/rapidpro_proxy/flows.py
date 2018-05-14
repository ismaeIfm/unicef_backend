import sys

from elasticsearch_dsl import Q

import settings
from rapidpro_proxy.utils import *

sys.path.insert(0, '..')

####################### Auxiliar functions ##################


def _aux_number_by_state(flow_uuid, field, filter_date=[]):
    q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid)])
    aggregate_by_state(q)
    aggregate_by_razon(q.aggs[BYSTATE_STR], field, single=False)
    response = q.execute()
    return format_aggs_aggs_result(response, 'state', BYSTATE_STR, 'reason',
                                   BYRAZON)


def _aux_number_by_mun(state, flow_uuid, field, filter_date=[]):
    q = search_run(filter_date + [
        Q('term', flow_uuid=flow_uuid),
        Q('term', fields__rp_state_number=state)
    ])
    aggregate_by_mun(q)
    aggregate_by_razon(q.aggs[BYMUN_STR], field, single=False)

    response = q.execute()

    return format_aggs_aggs_result(response, 'municipio', BYMUN_STR, 'reason',
                                   BYRAZON)


def _aux_number_by_hospital(flow_uuid, field, filter_date=[]):
    q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid)])
    aggregate_by_hospital(q)
    aggregate_by_razon(q.aggs[BYHOSPITAL_STR], field, single=False)
    response = q.execute()

    return format_aggs_aggs_result(response, 'hospital', BYHOSPITAL_STR,
                                   'reason', BYRAZON)


def _aux_channel(flow_uuid, field, filter_date=[]):
    q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid)])
    aggregate_by_razon(q, field=field)
    response = q.execute()
    return format_aggs_result(response.aggregations[BYRAZON], 'reason')


def _aux_number_by_channel(flow_uuid, field, filter_date=[]):
    mapping = {'facebook':'facebook', 'tel':'tel', 'twitter':'twitterid'}
    return {
        q: _aux_channel(flow_uuid, field, [Q('match', urns=mapping[q])] + filter_date)
        for q in mapping.keys()
    }


#NAin
@date_decorator("time")
def _aux_number_by_baby_age(flow_uuid, field, filter_date=[]):
    q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid)])
    aggregate_by_baby_age(q)
    aggregate_by_razon(q.aggs[BYBABYAGE_STR], field, single=False)
    response = q.execute()

    return format_aggs_aggs_result(response, 'trimester', BYBABYAGE_STR,
                                   'reason', BYRAZON)


@date_decorator("time")
def _aux_number_by_mom_age(flow_uuid, field, filter_date=[]):
    q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid)])
    aggregate_by_mom_age_run(q)
    aggregate_by_razon(q.aggs[BYMOMAGE_STR], field, single=False)
    response = q.execute()

    return format_aggs_aggs_result(response, 'group', BYMOMAGE_STR, 'reason',
                                   BYRAZON)


def _aux_by_group(query=None, flow_uuid=None, field=None, filter_date=[]):
    if query:
        q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid), query])
    else:
        q = search_run(filter_date + [Q('term', flow_uuid=flow_uuid)])
    aggregate_by_razon(q, field=field)
    response = q.execute()
    return format_aggs_result(response.aggregations[BYRAZON].buckets, 'reason')


##########################################################################
#                             Msgs part                                  #
##########################################################################


@date_decorator('time')
def number_sent_msgs_by_flow(filter_date=[]):
    bureaucratic_flows = [{'match': {'flow_uuid': i}} for i in [settings.CANCEL_FLOW, settings.MIALERTA_FLOW]]
    q = search_run(filter_date + [Q('bool',must_not=bureaucratic_flows)])
    aggregate_by_flow(q)
    response = q.execute()
    return format_aggs_result(response.aggregations[BYFLOW_STR].buckets,
                              'categoria')

##########################################################################
#                         Mi alerta       (use flow auxiliar methods)    #
##########################################################################


@date_decorator("time")
def number_mialerta_by_group(filter_date=[]):
    result = {}
    result['baby'] = _aux_by_group(
        query=Q('term', fields__rp_ispregnant='1'),
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)
    result['pregnant'] = _aux_by_group(
        query=Q('term', fields__rp_ispregnant='0'),
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)
    result['personal'] = _aux_by_group(
        query=Q('term', groups__name='PERSONAL_SALUD'),
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)
    return result


@date_decorator('time')
def number_mialerta_by_state(filter_date=[]):
    return _aux_number_by_state(
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)


@date_decorator('time')
def number_mialerta_by_mun(state, filter_date=[]):
    return _aux_number_by_mun(
        state,
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)


#NO encontre
@date_decorator('time')
def number_mialerta_by_hospital(filter_date=[]):
    return _aux_number_by_hospital(
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)


@date_decorator('time')
def number_mialerta_by_channel(filter_date=[]):
    return _aux_number_by_channel(
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)


@date_decorator("time")
def number_mialerta_by_baby_age(filter_date=[]):
    return _aux_number_by_baby_age(
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)


@date_decorator("time")
def number_mialerta_by_mom_age(filter_date=[]):
    return _aux_number_by_mom_age(
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)


@date_decorator("time")
def total_number_mialerta(filter_date=[]):
    result = {}
    result["response"] =  _aux_by_group(
        flow_uuid='07d56699-9cfb-4dc6-805f-775989ff5b3f',
        field='fields.rp_razonalerta',
        filter_date=filter_date)
    return result

##########################################################################
#                        Cancel part   (Use flow auxiliar methods)       #
##########################################################################

@date_decorator("time")
def total_number_cancel(filter_date=[]):
    result = {}
    result["response"] =  _aux_by_group(
        flow_uuid=settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)
    return result


#cambiar por fields
@date_decorator("time")
def number_cancel_by_group(filter_date=[]):
    result = {}
    result['baby'] = _aux_by_group(
        query=Q('term', fields__rp_ispregnant='1'),
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)
    result['pregnant'] = _aux_by_group(
        query=Q('term', fields__rp_ispregnant='0'),
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)
    result['personal'] = _aux_by_group(
        query=Q('term', groups__name='PERSONAL_SALUD'),
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)
    return result


@date_decorator("time")
def number_cancel_by_state(filter_date=[]):
    return _aux_number_by_state(
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)


@date_decorator('time')
def number_cancel_by_mun(state, filter_date=[]):
    return _aux_number_by_mun(
        state,
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)


@date_decorator('time')
def number_cancel_by_hospital(filter_date=[]):
    return _aux_number_by_hospital(
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)


@date_decorator('time')
def number_cancel_by_channel(filter_date=[]):
    return _aux_number_by_channel(
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)


@date_decorator("time")
def number_cancel_by_baby_age(filter_date=[]):
    return _aux_number_by_baby_age(
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)


@date_decorator("time")
def number_cancel_by_mom_age(filter_date=[]):
    return _aux_number_by_mom_age(
        flow_uuid= settings.CANCEL_FLOW,
        field='fields.rp_razonbaja',
        filter_date=filter_date)


##########################################################################
#                   Rate completed messages part                         #
##########################################################################


def aux_rate_completed_messages(query, filter_date=[]):
    try:
        total = search_run(filter_date + [query]).count()
        bureaucratic_flows = [{'match': {'flow_uuid': i}} for i in [settings.CANCEL_FLOW, settings.MIALERTA_FLOW]]

        q = search_run(filter_date + [query,Q('bool',must_not=bureaucratic_flows),Q('term', exit_type='completed')])
        aggregate_by_way(q)
        response = q.execute()
        runs_completed = {
            i['key']: i['doc_count']
            for i in response.aggregations[BYWAY_STR].buckets
        }
        return (runs_completed.get(0, 0) /
                (total - runs_completed.get(1, 0))) * 100
    except ZeroDivisionError:
        return 0


# Si no hay personal de salud que pedo?


@date_decorator('time')
def rate_completed_messages_by_group(filter_date=[]):
    result = {}
    result['baby'] = aux_rate_completed_messages(
        Q('term', fields__rp_ispregnant='1'), filter_date=filter_date)
    result['pregnant'] = aux_rate_completed_messages(
        Q('term', fields__rp_ispregnant='0'), filter_date=filter_date)
    result['personal'] = aux_rate_completed_messages(
        Q('term', groups__name='PERSONAL_SALUD'), filter_date=filter_date)
    return result


@date_decorator('time')
def rate_completed_messages_by_channel(filter_date=[]):
    mapping = {'facebook':'facebook', 'tel':'tel', 'twitter':'twitterid'}

    return {
        q: aux_rate_completed_messages(Q('match', urns=mapping[q]), filter_date)
        for q in  mapping.keys()
    }


@date_decorator('time')
def rate_completed_messages_by_state(filter_date=[]):
    q = search_run(filter_date)
    aggregate_by_state(q)
    filter_completed(q.aggs[BYSTATE_STR])
    aggregate_by_way(
        q.aggs[BYSTATE_STR].aggs[FILTERCOMPLETED_STR], single=False)
    response = q.execute()
    return format_rate_completed_messages(response.aggregations[BYSTATE_STR],
                                          'state')


@date_decorator('time')
def rate_completed_messages_by_mun(state, filter_date=[]):
    q = search_run(filter_date + [Q('term', fields__rp_state_number=state)])
    aggregate_by_mun(q)
    filter_completed(q.aggs[BYMUN_STR])
    aggregate_by_way(q.aggs[BYMUN_STR].aggs[FILTERCOMPLETED_STR], single=False)
    response = q.execute()
    return format_rate_completed_messages(response.aggregations[BYMUN_STR],
                                          'mun')


@date_decorator('time')
def rate_completed_messages_by_hospital(filter_date=[]):
    q = search_run(filter_date)
    aggregate_by_hospital(q)
    filter_completed(q.aggs[BYHOSPITAL_STR])
    aggregate_by_way(
        q.aggs[BYHOSPITAL_STR].aggs[FILTERCOMPLETED_STR], single=False)
    response = q.execute()
    return format_rate_completed_messages(
        response.aggregations[BYHOSPITAL_STR], 'hospital')


@date_decorator('time')
def rate_completed_messages_by_message(filter_date=[]):
    bureaucratic_flows = [{'match': {'flow_uuid': i}} for i in [settings.CANCEL_FLOW, settings.MIALERTA_FLOW]]
    q = search_run(filter_date + [Q('bool',must_not=bureaucratic_flows)] + [Q('term', responded=True)])
    aggregate_by_msg(q)
    filter_completed(q.aggs[BYMSG_STR])
    aggregate_by_way(q.aggs[BYMSG_STR].aggs[FILTERCOMPLETED_STR], single=False)
    aggregate_by_flow(q.aggs[BYMSG_STR].aggs[FILTERCOMPLETED_STR].aggs[BYWAY_STR], single=False)
    response = q.execute()
    return format_rate_completed_messages_by_msgs(response.aggregations[BYMSG_STR],
                                          'message')


@date_decorator('time')
def rate_completed_messages_by_mom_age(filter_date=[]):
    q = search_run(filter_date)
    aggregate_by_mom_age_run(q)
    filter_completed(q.aggs[BYMOMAGE_STR])
    aggregate_by_way(
        q.aggs[BYMOMAGE_STR].aggs[FILTERCOMPLETED_STR], single=False)
    response = q.execute()
    return format_rate_completed_messages(response.aggregations[BYMOMAGE_STR],
                                          'age')


#@date_decorator('time')
#def rate_completed_messages_by_message(filter_date=[]):
#    q = search_run(filter_date)
#    q = aggregate_by_msg(q)
#    filter_completed(q.aggs[BYMSG_STR])
#    aggregate_by_way(q.aggs[BYMSG_STR].aggs[FILTERCOMPLETED_STR], single=False)
#    response = q.execute()
#    return response.aggregations[BYMSG_STR]
