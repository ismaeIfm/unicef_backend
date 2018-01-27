from flask import Blueprint, jsonify, make_response
from webargs import fields
from webargs.flaskparser import use_kwargs

from unicef_backend import contacts

api = Blueprint('api', __name__)

mun_args = {
    'state': fields.Integer(required=True),
    'start_date': fields.DateTime(),
    'end_date': fields.DateTime()
}


@api.route("/")
def hello():
    return "Hello World!"


@api.route("/number_babies_by_mun", methods=['POST'])
@use_kwargs(mun_args, locations=('json', 'form', 'query'))
def ge_number_babies_by_mun(state, start_date, end_date):

    return make_response(
        jsonify({
            'response': contacts.number_babies_by_mun(state)
        }), 200)


"""
curl -X POST \
  http://localhost:5000/number_babies_by_mun \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 5b2faa43-1c46-5047-e6f7-13a0fa99279d' \
  -d '{"state": "29"}'
 """

# Anadir consideraciones en fechas dependiendo del campo
