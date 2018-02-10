import sys
from datetime import date, datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
from elasticsearch_dsl import A, Q

import settings
from rapidpro_proxy.indexes import Contact, Run

sys.path.insert(0, '..')
################### Constants of aggregation's names #################
BYSTATE_STR = 'by_state'
BYMUN_STR = 'by_mun'
BYHOSPITAL_STR = 'by_hospital'
BYBABYAGE_STR = 'by_baby_age'
BYWEEKPREGNAT_STR = 'by_week_pregnant'
BYMSG_STR = 'by_msg'

RUNSCOUNT_STR = 'runs_count'
VALUESCOUNT_STR = 'values_count'

BYFLOW_STR = 'by_flow'

####################### Auxiliar functions ##################


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
            start_date = kwargs[
                "start_date"] if "start_date" in kwargs and kwargs["start_date"] else ""
            end_date = kwargs[
                "end_date"] if "end_date" in kwargs and kwargs["end_date"] else ""
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


def search_contact(querys=[]):
    s = Contact.search()
    return s.query('bool', must=querys)


def search_run(querys=[]):
    s = Run.search()
    return s.query('bool', must=querys)


def search_runs_by_contact_info(parent_querys=[], child_querys=[]):
    return search_contact(
        parent_querys +
        [Q('has_child', type='run', query=Q('bool', must=child_querys))])


def search_values_by_contact_info(parent_querys=[], child_querys=[]):
    return search_contact(
        parent_querys +
        [Q('has_child', type='value', query=Q('bool', must=child_querys))])


def aggregate_by_state(q):
    a = A('terms', field=settings.FIELDS_STATE, size=2147483647)
    q.aggs.bucket(BYSTATE_STR, a)
    return q


def aggregate_by_mun(q):
    a = A('terms', field=settings.FIELDS_MUN, size=2147483647)
    q.aggs.bucket(BYMUN_STR, a)
    return q


def aggregate_by_hospital(q, bucket=None):
    if bucket:
        q.aggs[bucket].bucket(
            BYHOSPITAL_STR,
            'terms',
            field='fields.rp_atenmed',
            size=2147483647)
    else:
        a = A('terms', field='fields.rp_atenmed', size=2147483647)
        q.aggs.bucket(BYHOSPITAL_STR, a)
    return q


def aggregate_by_baby_age(q, bucket=None):
    start_date_pointer = datetime.utcnow()
    #end_date_pointer = datetime.utcnow()
    trimesters = [{
        "from":
        start_date_pointer - relativedelta(months=idx * 3),
        "to":
        start_date_pointer - relativedelta(months=(idx - 1) * 3),
        "key":
        str(9 - idx)
    } for idx in range(1, 9)]
    if bucket:
        q.aggs[bucket].bucket(
            BYBABYAGE_STR,
            'date_range',
            field=settings.FIELDS_DELIVERY,
            ranges=trimesters,
            keyed=True)
    else:
        a = A(
            'date_range',
            field=settings.FIELDS_DELIVERY,
            ranges=trimesters,
            keyed=True)
        q.aggs.bucket(BYBABYAGE_STR, a)
    return q


def aggregate_per_week_pregnant(q, bucket=None):
    weeks = [{
        "from": datetime.now() + timedelta(days=7 * i),
        "to": datetime.now() + timedelta(days=(i + 1) * 7),
        "key": str(i + 1)
    } for i in range(1, 40)]
    weeks.append({
        "from": datetime.now() + timedelta(days=40 * 7),
        "key": "41"
    })
    weeks.append({"to": datetime.now() + timedelta(days=7), "key": "0"})
    if bucket:
        q.aggs[bucket].bucket(
            BYWEEKPREGNAT_STR,
            'date_range',
            field=settings.FIELDS_DELIVERY,
            ranges=weeks,
            keyed=True)
    else:
        a = A(
            'date_range', field='fields.rp_duedate', ranges=weeks, keyed=True)
        q.aggs.bucket(BYWEEKPREGNAT_STR, a)
    return q


def aggregate_by_run(q, bucket=None):
    if bucket:
        q.aggs[bucket].bucket(RUNSCOUNT_STR, 'children', type='run')
    else:
        a = A('children', type='run')
        q.aggs.bucket(RUNSCOUNT_STR, a)
    return q


def aggregate_by_value(q, bucket=None):
    if bucket:
        q.aggs[bucket].bucket(VALUESCOUNT_STR, 'children', type='value')
    else:
        a = A('children', type='value')
        q.aggs.bucket(VALUESCOUNT_STR, a)
    return q


def aggregate_by_msg(q, bucket1, bucket2):
    q.aggs[bucket1].aggs[bucket2].bucket(
        BYMSG_STR, 'terms', field='msg', size=10)
    return q


def aggregate_by_flow(q):
    a = A('terms', field='type', size=10)
    q.aggs.bucket(BYFLOW_STR, a)
    return q


def format_aggs_result(result, key):
    return [{key: i['key'], 'count': i['doc_count']} for i in result]


def format_result(result, key):
    return [{key: k, 'count': v} for k, v in result.items()]


def format_aggs_aggs_result(result, key_1, bucket_1, key_2, bucket_2):
    return [{
        key_1:
        i['key'],
        'result': [{
            key_2: j['key'],
            'count': j['doc_count']
        } for j in i[bucket_2].buckets]
    } for i in result.aggregations[bucket_1].buckets]


def format_aggs_aggs_result_runs(result, key_1, bucket_1, key_2, bucket_2):
    return [{
        'state':
        i['key'],
        'result': [{
            'msg': j['key'],
            'count': j['doc_count']
        } for j in i[RUNSCOUNT_STR][bucket_2].buckets]
    } for i in result.aggregations[bucket_1].buckets]


def format_aggs_runs(result, key):
    return [{
        key: i['key'],
        'count': i[RUNSCOUNT_STR]['doc_count']
    } for i in result]
