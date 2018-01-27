from flask import Blueprint
from webargs import fields
from webargs.flaskparser import use_kwargs

api = Blueprint('api', __name__)

mun_args = {
    'state': fields.Integer(required=True),
    'start_date': fields.DateTime(),
    'end_date': fields.DateTime()
}


@api.route("/")
def hello():
    return "Hello World!"


@api.route("/pregnant_by_state")
@use_kwargs(mun_args, locations=('json', 'form', 'query'))
def get_pregnant_by_state(state, start_date, end_date):
    print(state, start_date, end_date)


# Anadir consideraciones en fechas dependiendo del campo
