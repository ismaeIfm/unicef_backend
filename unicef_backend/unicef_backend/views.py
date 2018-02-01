from datetime import datetime

from flask import Blueprint, jsonify, make_response
from unicef_backend import contacts
from webargs import fields
from webargs.flaskparser import use_kwargs

api = Blueprint('api', __name__)
#############################################################
#                     Restrictions                          #
#############################################################
date_args = {'start_date': fields.DateTime(), 'end_date': fields.DateTime()}

mun_args = {'state': fields.Integer(required=True)}
mun_args.update(date_args)

#############################################################
#                      Endpoints                            #
#############################################################


@api.route("/users_by_type", methods=['POST', 'GET'])
def view_user_by_type():
    response = contacts.number_contacts_by_group()
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_state", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_state(start_date, end_date):
    response = contacts.number_contacts_by_state()
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mun", methods=['POST', 'GET'])
@use_kwargs(mun_args)
def view_users_by_mun(state, start_date, end_date):
    response = contacts.number_contacts_by_mun(state)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mom_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_mom_age(start_date, end_date):
    response = contacts.number_contacts_by_mom_age()
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_baby_age", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_baby_age(start_date, end_date):
    response = {}
    lista = contacts.number_contacts_by_baby_age()
    for dictionary in lista:
        from_key = datetime.strptime(dictionary["from_as_string"],
                                     "%Y-%m-%dT%H:%M:%S.%fZ")
        trimester = (datetime.now() - from_key).days / 90
        response[trimester] = dictionary["doc_count"]

    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_hospital", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_hospital(start_date, end_date):
    response = contacts.number_contacts_by_hospital()
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_channel", methods=['POST', 'GET'])
@use_kwargs(date_args)
def view_users_by_channel(start_date, end_date):
    response = contacts.number_contacts_by_channel()
    return make_response(jsonify({'response': response}), 200)


"""
curl -X POST \
  http://localhost:5000/users_by_mun \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 5b2faa43-1c46-5047-e6f7-13a0fa99279d' \
  -d '{"state": "29"}'
 """

# Anadir consideraciones en fechas dependiendo del campo
