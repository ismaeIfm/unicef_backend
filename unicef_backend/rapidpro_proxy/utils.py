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
BYMOMAGE_STR = 'by_mom_age'
BYWEEKPREGNAT_STR = 'by_week_pregnant'
BYMSG_STR = 'by_msg'
BYWAY_STR = 'by_way'
BYCALIDAD_STR = 'by_calidad'

FILTERDATE_STR = 'filter_date'
FILTERCOMPLETED_STR = 'filter_completed'

RUNSCOUNT_STR = 'runs_count'
VALUESCOUNT_STR = 'values_count'

BYFLOW_STR = 'by_flow'
BYRAZON = 'by_razon'

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


def _get_difference_dates(start_date, end_date, element):
    if not start_date or not end_date:
        return None
    else:
        result = -1
        if element == 'm':
            result = relativedelta(
                end_date.replace(tzinfo=None),
                start_date.replace(tzinfo=None)).months
        elif element == 'y':
            result = relativedelta(
                end_date.replace(tzinfo=None),
                start_date.replace(tzinfo=None)).years
        elif element == 'w':
            result = relativedelta(
                end_date.replace(tzinfo=None),
                start_date.replace(tzinfo=None)).weeks
    return result if result >= 0 else None


def date_decorator(argument):
    def decorator_wrapper(function):
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
                elif argument == "rp_duedate":
                    filter_date = Q('range', fields__rp_duedate=filter_date)
                elif argument == "time":
                    filter_date = Q('range', time=filter_date)
                else:
                    filter_date = Q('range', created_on=filter_date)

                kwargs["filter_date"] = [filter_date]
            kwargs.pop('start_date', None)
            kwargs.pop('end_date', None)
            return function(*args, **kwargs)

        return wrapper

    return decorator_wrapper


def search_contact(querys=[]):
    s = Contact.search()
    return s.query('bool', must=querys)


def search_run(querys=[]):
    s = Run.search()
    return s.query('bool', must=querys)


def aggregate_by_state(q):
    a = A('terms', field=settings.FIELDS_STATE, size=2147483647)
    q.aggs.bucket(BYSTATE_STR, a)


def aggregate_by_mun(q):
    a = A('terms', field=settings.FIELDS_MUN, size=2147483647)
    q.aggs.bucket(BYMUN_STR, a)


def aggregate_by_hospital(q, single=True):
    if single:
        a = A('terms', field='fields.rp_atenmed', size=2147483647)
        q.aggs.bucket(BYHOSPITAL_STR, a)
    else:

        q.bucket(
            BYHOSPITAL_STR,
            'terms',
            field='fields.rp_atenmed',
            size=2147483647)

def auxiliar_contacts_by_mom(q, is_pregnant):
    total_mom = q.count()
    aggregate_by_mom_age(q, is_pregnant=is_pregnant)
    response = q[0:total_mom].execute()
    return format_aggs_result(response.aggregations[BYMOMAGE_STR].buckets,
                              'group')




def aggregate_by_mom_age(q, single=True, is_pregnant=True):
    groups = [{"from": 35}, {"from": 19, "to": 35}, {"from": 0, "to": 19}]

    duedate_str = "doc['fields.rp_duedate'].value" if is_pregnant else "doc['fields.rp_deliverydate'].value"

    mombirth_str = "doc['fields.rp_mamafechanac'].value"
    milliseconds2years_str = "/ 1000 / 60 / 60 / 24 / 365"
    script_str = "({} - {}) {}".format(duedate_str, mombirth_str,
                                       milliseconds2years_str)
    if single:
        a = A(
            'range',
            script={"lang": "painless",
                    "source": script_str},
            ranges=groups)
        q.aggs.bucket(BYMOMAGE_STR, a)
    else:
        q.bucket(
            BYMOMAGE_STR,
            'range',
            script={"lang": "painless",
                    "source": script_str},
            ranges=groups)


def aggregate_by_mom_age_run(q, single=True):
    groups = [{"from": 35}, {"from": 19, "to": 35}, {"from": 0, "to": 19}]

    if single:
        a = A('range', field='fields.contact_age', ranges=groups)
        q.aggs.bucket(BYMOMAGE_STR, a)
    else:
        q.bucket(
            BYMOMAGE_STR, 'range', field='fields.contact_age', ranges=groups)


def aggregate_by_baby_age(q, single=True):
    if single:
        a = A('terms', field='baby_age')
        q.aggs.bucket(BYBABYAGE_STR, a)
    else:
        q.bucket(BYBABYAGE_STR, 'terms', field='baby_age')


def aggregate_per_week_pregnant(q, single=True):
    if single:
        a = A('terms', field='pregnant_week', size=41)
        q.aggs.bucket(BYWEEKPREGNAT_STR, a)
    else:
        q.bucket(BYWEEKPREGNAT_STR, 'terms', field='pregnant_week', size=41)


def aggregate_by_msg(q, single=True):
    if single:
        a = A('terms', field='msg', size=10)
        q.aggs.bucket(BYMSG_STR, a)
    else:
        q.bucket(BYMSG_STR, 'terms', field='msg', size=10)


def aggregate_by_flow(q, single=True):
    if single:
        a = A('terms', field='type', size=10)
        q.aggs.bucket(BYFLOW_STR, a)
    else:
        q.bucket(BYFLOW_STR, 'terms', field='type',size=10)


def aggregate_by_way(q, single=True):
    if single:
        a = A('terms', field='is_one_way')
        q.aggs.bucket(BYWAY_STR, a)
    else:
        q.bucket(BYWAY_STR, 'terms', field='is_one_way')


def aggregate_by_razon(q, field=None, single=True):
    if single:
        a = A('terms', field=field)
        q.aggs.bucket(BYRAZON, a)

    else:
        q.bucket(BYRAZON, 'terms', field=field)


def aggregate_by_calidad(q, calidad=None):
    q.bucket(BYCALIDAD_STR, 'terms', field='{}.{}'.format('fields', calidad))


def filter_completed(q):
    q.bucket(FILTERCOMPLETED_STR, 'filter', term={'exit_type': 'completed'})


def format_aggs_result(result, key):
    return [{key: i['key'], 'count': i['doc_count']} for i in result]


def format_result(result, key):
    return [{key: k, 'count': v} for k, v in result.items()]


def format_aggs_aggs_result_msg(result, key_1, bucket_1, key_2, bucket_2, key_3, bucket_3):
    return [{
        key_1:
        i['key'],
        'result': [{
            key_2: j['key'],
            'count': j['doc_count'],
            'type': j[bucket_3].buckets[0]["key"] if j[bucket_3].buckets else "otros"
        } for j in sorted(i[bucket_2].buckets, key = lambda x: x['doc_count'], reverse=True) ]
    } for i in result.aggregations[bucket_1].buckets if i[bucket_2].buckets]

def format_aggs_aggs_result(result, key_1, bucket_1, key_2, bucket_2):
    return [{
        key_1:
        i['key'],
        'result': [{
            key_2: j['key'],
            'count': j['doc_count']
        } for j in i[bucket_2].buckets]
    } for i in result.aggregations[bucket_1].buckets if i[bucket_2].buckets]

def format_rate_completed_messages_by_msgs(result, key):
    formatted_result = []
    for i in result:
        total = i['doc_count']
        type_msg ="otros"
        if i[FILTERCOMPLETED_STR][BYWAY_STR].buckets:
            if i[FILTERCOMPLETED_STR][BYWAY_STR].buckets[0][BYFLOW_STR]:
                type_msg = i[FILTERCOMPLETED_STR][BYWAY_STR].buckets[0][BYFLOW_STR][0]["key"]
        runs_completed = {
            j['key']: j['doc_count']
            for j in i[FILTERCOMPLETED_STR][BYWAY_STR]
        }
        try:
            rate = (runs_completed.get(0, 0) /
                    (total - runs_completed.get(1, 0))) * 100
        except ZeroDivisionError:
            rate = 0

        formatted_result.append({key: i['key'], 'rate': rate, 'type':type_msg})
    return sorted(formatted_result, key = lambda x:x["rate"], reverse=True)

def format_rate_completed_messages(result, key):
    formatted_result = []
    for i in result:
        total = i['doc_count']
        runs_completed = {
            j['key']: j['doc_count']
            for j in i[FILTERCOMPLETED_STR][BYWAY_STR]
        }
        try:
            rate = (runs_completed.get(0, 0) /
                    (total - runs_completed.get(1, 0))) * 100
        except ZeroDivisionError:
            rate = 0

        formatted_result.append({key: i['key'], 'rate': rate})
    return formatted_result
