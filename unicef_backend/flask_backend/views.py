from datetime import datetime

from dateutil.relativedelta import relativedelta
from flask import Blueprint, jsonify, make_response
from webargs import fields
from webargs.flaskparser import use_kwargs

from rapidpro_proxy import contacts, flows

api = Blueprint('api', __name__)
#############################################################
#                     Restrictions                          #
#############################################################
date_args = {
    'start_date': fields.DateTime(missing=None),
    'end_date': fields.DateTime(missing=None)
}

mun_args = {'state': fields.Integer(required=True)}
mun_args.update(date_args)


def configure_deliverydate(start_date=None, end_date=None):
    if start_date:
        start_date = start_date - relativedelta(years=2)
    return start_date, end_date


def configure_duedate(start_date=None, end_date=None):
    if end_date:
        end_date = end_date + relativedelta(months=9)
    return start_date, end_date


#############################################################
#                      Endpoints                            #
#############################################################


##################      Users part     ######################
@api.route("/users_by_type", methods=['POST', 'GET'])
def view_user_by_type():
    response = contacts.number_contacts_by_group()
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_state(start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_contacts_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_users_by_mun(state, start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_contacts_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mom_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_mom_age(start_date, end_date):
    """ """
    response = contacts.number_contacts_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_baby_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_baby_age(start_date, end_date):
    """ """
    response = {}
    lista = contacts.number_contacts_by_baby_age()
    for dictionary in lista:
        from_key = datetime.strptime(lista[dictionary]["from_as_string"],
                                     "%Y-%m-%dT%H:%M:%S.%fZ")
        trimester = int((datetime.now() - from_key).days / 90)
        response[trimester] = lista[dictionary]["doc_count"]

    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_hospital(start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_contacts_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_channel", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_channel(start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_contacts_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Babies part     ######################
@api.route("/babies_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_babies_by_state(start_date, end_date):
    """
    filter by deliverydate date
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_babies_by_mun(state, start_date, end_date):
    """
    filter by deliverydate date
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_mom_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_babies_by_mom_age(start_date, end_date):
    """ """
    response = contacts.number_babies_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_babies_by_hospital(start_date, end_date):
    """
    filter by deliverydate date
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      States part     ######################
@api.route("/pregnants_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_pregnants_by_state(start_date, end_date):
    """
    filter by duedate date
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_pregnant_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/moms_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_moms_by_state(start_date, end_date):
    """
    filter by duedate date
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_moms_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/personal_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_personal_by_state(start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_personal_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mom_age_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mom_age_by_state(start_date, end_date):
    """ """
    response = contacts.number_moms_by_state_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/baby_age_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_baby_age_by_state(start_date, end_date):
    """ """
    response = {}
    lista = contacts.number_baby_age_by_state()
    for dictionary in lista:
        lista_bucket = dictionary["by_baby_age"]["buckets"]
        response[dictionary["key"]] = {}
        for item in lista_bucket:
            from_key = datetime.strptime(lista_bucket[item]["from_as_string"],
                                         "%Y-%m-%dT%H:%M:%S.%fZ")
            trimester = int((datetime.now() - from_key).days / 90)
            response[dictionary["key"]][trimester] = lista_bucket[item][
                "doc_count"]
    return make_response(jsonify({'response': response}), 200)


@api.route("/hospitals_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_hospitals_by_state(start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_hostpital_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/channel_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_channel_by_state(start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_channel_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Municipios part     ######################
@api.route("/pregnants_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_pregnants_by_mun(state, start_date, end_date):
    """
    filter by duedate date
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_pregnant_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/moms_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_moms_by_mun(state, start_date, end_date):
    """
    filter by duedate date
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_moms_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/personal_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_personal_by_mun(state, start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_personal_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mom_age_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_mom_age_by_mun(state, start_date, end_date):
    """ """
    response = contacts.number_baby_age_by_mun(state)
    return make_response(jsonify({'response': response}), 200)


@api.route("/baby_age_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_baby_age_by_mun(state, start_date, end_date):
    """ """
    response = {}
    lista = contacts.number_baby_age_by_mun(state)
    for dictionary in lista:
        lista_bucket = dictionary["by_baby_age"]["buckets"]
        response[dictionary["key"]] = {}
        for item in lista_bucket:
            from_key = datetime.strptime(lista_bucket[item]["from_as_string"],
                                         "%Y-%m-%dT%H:%M:%S.%fZ")
            trimester = int((datetime.now() - from_key).days / 90)
            response[dictionary["key"]][trimester] = lista_bucket[item][
                "doc_count"]
    return make_response(jsonify({'response': response}), 200)


@api.route("/hospitals_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_hospitals_by_mun(state, start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_hostpital_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/channel_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_channel_by_mun(state, start_date, end_date):
    """
    filter by created_on date
    """
    response = contacts.number_channel_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Mialerta part     ######################
@api.route("/mialerta_by_group", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mialerta_by_group(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_mialerta_by_group(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mialerta_by_state(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_mialerta_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_mialerta_by_mun(state, start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_mialerta_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mialerta_by_hospital(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_mialerta_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_channel", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mialerta_by_channel(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_mialerta_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_msgs", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mialerta_msgs(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_mialerta_msgs_top(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      cancela part     ######################
@api.route("/cancela_by_group", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_cancela_by_group(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_cancel_by_group(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_cancela_by_state(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_cancel_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_cancela_by_mun(state, start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_cancel_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_cancela_by_hospital(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_cancel_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_channel", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_cancela_by_channel(start_date, end_date):
    """
    filter by time date
    """
    response = flows.number_cancel_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


"""
curl -X POST \
  http://localhost:5000/users_by_mun \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 5b2faa43-1c46-5047-e6f7-13a0fa99279d' \
  -d '{"state": "29", "start_date": "2017-8-20T00:00:00"}'
 """
