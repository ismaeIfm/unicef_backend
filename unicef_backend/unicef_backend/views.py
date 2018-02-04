from datetime import datetime

from flask import Blueprint, jsonify, make_response
from webargs import fields
from webargs.flaskparser import use_kwargs

from unicef_backend import contacts

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
    response = contacts.number_contacts_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_users_by_mun(state, start_date, end_date):
    response = contacts.number_contacts_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mom_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_mom_age(start_date, end_date):
    response = contacts.number_contacts_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_baby_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_baby_age(start_date, end_date):
    response = {}
    lista = contacts.number_contacts_by_baby_age(
        start_date=start_date, end_date=end_date)
    for dictionary in lista:
        from_key = datetime.strptime(dictionary["from_as_string"],
                                     "%Y-%m-%dT%H:%M:%S.%fZ")
        trimester = (datetime.now() - from_key).days / 90
        response[trimester] = dictionary["doc_count"]

    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_hospital(start_date, end_date):
    response = contacts.number_contacts_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_channel", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_channel(start_date, end_date):
    response = contacts.number_contacts_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Babies part     ######################
@api.route("/babies_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_babies_by_state(start_date, end_date):
    response = contacts.number_babies_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_babies_by_mun(state, start_date, end_date):
    response = contacts.number_babies_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_mom_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_babies_by_mom_age(start_date, end_date):
    response = contacts.number_babies_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_babies_by_hospital(start_date, end_date):
    response = contacts.number_babies_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_week", methods=['POST', 'GET'])
def view_babies_by_week():
    response = contacts.number_babies_by_week()
    return make_response(jsonify({'response': response}), 200)


##################      States part     ######################
@api.route("/pregnants_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_pregnants_by_state(start_date, end_date):
    response = contacts.number_pregnant_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/moms_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_moms_by_state(start_date, end_date):
    response = contacts.number_moms_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/personal_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_personal_by_state(start_date, end_date):
    response = contacts.number_personal_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mom_age_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_mom_age_by_state(start_date, end_date):
    response = contacts.number_moms_by_state_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/hospitals_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_hospitals_by_state(start_date, end_date):
    response = contacts.number_hostpital_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/baby_age_by_state", methods=['POST', 'GET'])
def view_baby_age_by_state():
    response = contacts.number_baby_age_by_state()
    return make_response(jsonify({'response': response}), 200)


@api.route("/channel_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_channel_by_state(start_date, end_date):
    print(start_date)
    print(end_date)
    response = contacts.number_channel_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Municipios part     ######################
"""
curl -X POST \
  http://localhost:5000/users_by_mun \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 5b2faa43-1c46-5047-e6f7-13a0fa99279d' \
  -d '{"state": "29"}'
 """

# Anadir consideraciones en fechas dependiendo del campo
