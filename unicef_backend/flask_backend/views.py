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

mun_args = {'state': fields.Str(required=True)}
mun_args.update(date_args)

calidad_args = {'category': fields.Str(required=True)}
calidad_args.update(date_args)

calidad_mun_args = {'category': fields.Str(required=True)}
calidad_mun_args.update(mun_args)

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
@api.route("/users_by_type", methods=['GET'])
def view_user_by_type():
    """Usuarios agrupados por tipo.
       El endpoint sin filtro de temporalidad.
    ---
    tags:
      - Numero de participantes
    responses:
      200:
        description: Usuarios agrupados por tipo de usuario
    """
    response = contacts.number_contacts_by_group()
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_users_by_state(start_date, end_date):
    """Usuarios agrupados por municipio.
       El endpoint utilizala fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Numero de participantes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de inicio
        default: "2014-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_contacts_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_users_by_mun(state, start_date, end_date):
    """Usuarios agrupados por estado basado en la clave INEGI
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales, el estado es obligatorio.
    ---
    tags:
      - Numero de participantes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2014-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 09
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_contacts_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_mom_age", methods=['GET'])
@use_kwargs(date_args)
def view_users_by_mom_age(start_date, end_date):
    """Usuarios agrupados por edad de la madre cuando nacio su hijo
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.


       **Las fechas son opcionales
    ---
    tags:
      - Numero de participantes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2014-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_contacts_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_baby_age", methods=['GET'])
@use_kwargs(date_args)
def view_users_by_baby_age(start_date, end_date):
    """Usuarios agrupados por edad del bebe
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Numero de participantes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2014-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_contacts_by_baby_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_hospital", methods=['GET'])
@use_kwargs(date_args)
def view_users_by_hospital(start_date, end_date):
    """Usuarios agrupados por tipo de atencion medica
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Numero de participantes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2014-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_contacts_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/users_by_channel", methods=['GET'])
@use_kwargs(date_args)
def view_users_by_channel(start_date, end_date):
    """Usuarios agrupados por canal de comunicacion
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Numero de participantes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2014-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_contacts_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Babies part     ######################
@api.route("/babies_by_channel", methods=['GET'])
@use_kwargs(date_args)
def view_babies_by_channel(start_date, end_date):
    """Bebes agrupados por estado
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.
       **Las fechas son opcionales
    ---
    tags:
      - Numero de bebes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_babies_by_state(start_date, end_date):
    """Bebes agrupados por estado
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Numero de bebes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_babies_by_mun(state, start_date, end_date):
    """Bebes agrupados por municipio dado un estado basado en la clave INEGI
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Numero de bebes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_week", methods=['GET'])
@use_kwargs(date_args)
def view_babies_by_week(start_date, end_date):
    """Bebes agrupados por la semana de gestacion al nacer.
       El endpoint utiliza la fecha rp_duedate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Numero de bebes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_babies_by_week(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_mom_age", methods=['GET'])
@use_kwargs(date_args)
def view_babies_by_mom_age(start_date, end_date):
    """Bebes agrupados por la edad de la mama cuando tuvieron al hijo.
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Numero de bebes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/babies_by_hospital", methods=['GET'])
@use_kwargs(date_args)
def view_babies_by_hospital(start_date, end_date):
    """Bebes agrupados por el lugar de atencion medica
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Numero de bebes
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_babies_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      States part     ######################
@api.route("/pregnants_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_pregnants_by_state(start_date, end_date):
    """Mujeres embarazadas por estado
       El endpoint utiliza la fecha rp_duedate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_pregnant_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/moms_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_moms_by_state(start_date, end_date):
    """Madres por estado
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_moms_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/personal_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_personal_by_state(start_date, end_date):
    """Personal por estado
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_personal_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mom_age_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_mom_age_by_state(start_date, end_date):
    """Madres agrupadas por estado y por edad
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_moms_by_state_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/baby_age_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_baby_age_by_state(start_date, end_date):
    """Bebes agrupados por estado y por edad
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_baby_age_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/hospitals_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_hospitals_by_state(start_date, end_date):
    """Usuarios agrupados por estado y lugar de atencion medica
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_hostpital_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/channel_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_channel_by_state(start_date, end_date):
    """Usuarios agrupados por estado y canal de comunicacion
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales.
    ---
    tags:
      - Estados (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_channel_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Municipios (INEGI) part     ######################
@api.route("/pregnants_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_pregnants_by_mun(state, start_date, end_date):
    """Mujeres embarazadas agrupados por municipio dado un estado basado en la clave INEGI
       El endpoint utiliza la fecha rp_duedate de los contactos para filtrar por temporalidad.

       **Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-10T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_pregnant_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/moms_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_moms_by_mun(state, start_date, end_date):
    """Madres agrupados por municipio dado un estado basado en la clave INEGI
       El endpoint utiliza la fecha rp_duedate de los contactos para filtrar por temporalidad.

      **Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.number_moms_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/personal_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_personal_by_mun(state, start_date, end_date):
    """Personal agrupados por municipio dado un estado basado en la clave INEGI
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 09
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_personal_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mom_age_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_mom_age_by_mun(state, start_date, end_date):
    """Madres agrupadas por municipio dado un estado y agrupadas por edad
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.
       Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_moms_by_mun_age(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/baby_age_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_baby_age_by_mun(state, start_date, end_date):
    """Bebes agrupados por municipio dado un estado y agrupados por edad
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.
       Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_baby_age_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/hospitals_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_hospitals_by_mun(state, start_date, end_date):
    """Usuarios por municipio dado un estado y por atencion medica
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 09
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.number_hostpital_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/channel_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_channel_by_mun(state, start_date, end_date):
    """Usuarios  agrupados por canal de comunicacion y  agrupados por municipio dado un estado
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

       **Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Municipios (INEGI)
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 09
        required: True
    responses:
      200:
        description: Los usuarios pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.number_channel_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      Mialerta part     ######################
@api.route("/mialerta_by_group", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_group(start_date, end_date):
    """Detonaciones de mialerta agrupados por grupo
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_group(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_state(start_date, end_date):
    """Detonaciones de mialerta agrupados por estado
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_mom_age", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_mom_age(start_date, end_date):
    """Detonaciones de mialerta agrupados la edad de la mama
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_baby_age", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_baby_age(start_date, end_date):
    """Detonaciones de mialerta agrupados la edad del bebe
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_baby_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_mialerta_by_mun(state, start_date, end_date):
    """Detonaciones de mialerta agrupados por municipio dado un estado  basado en la clave INEGI
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       ** Los parametros de fechas son opcionales. El estado es obligatorio, con dos digitios (ejemplo: 09)
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 09
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_hospital", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_hospital(start_date, end_date):
    """Detonaciones de mialerta agrupados por atencion medica
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_channel", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_channel(start_date, end_date):
    """Detonaciones de mialerta agrupados por canal de comunicacion
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_mialerta_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/mialerta_by_type", methods=['GET'])
@use_kwargs(date_args)
def view_mialerta_by_type(start_date, end_date):
    """Top de razones de mialerta
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Mialerta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.total_number_mialerta(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      cancela part     ######################
@api.route("/cancela_by_type", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_type(start_date, end_date):
    """Top de razones de cancela
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.total_number_cancel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)

@api.route("/cancela_by_group", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_group(start_date, end_date):
    """Detonaciones de cancela agrupados tipo de contacto
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_group(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_state(start_date, end_date):
    """Detonaciones de cancela agrupados por estado  basado en la clave INEGI
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_cancela_by_mun(state, start_date, end_date):
    """Detonaciones de cancela agrupados por municipio
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 09
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_mun(
        state, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_hospital", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_hospital(start_date, end_date):
    """Detonaciones de cancela agrupados por atencion medica
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_mom_age", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_mom_age(start_date, end_date):
    """Detonaciones de cancela agrupados por la edad de la mama
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_baby_age", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_baby_age(start_date, end_date):
    """Detonaciones de cancela agrupados por la edad del bebe
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_baby_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/cancela_by_channel", methods=['GET'])
@use_kwargs(date_args)
def view_cancela_by_channel(start_date, end_date):
    """Detonaciones de cancela agrupados por canal de comunicacion
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Cancela
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_cancel_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################      msgs part     ######################
@api.route("/msgs_by_topic", methods=['GET'])
@use_kwargs(date_args)
def view_msgs_by_topic(start_date, end_date):
    """Mensajes enviados agrupados por tema del mensaje
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.

       **Las fechas son opcionales
    ---
    tags:
      - Mensajes enviados
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.number_sent_msgs_by_flow(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


################## Tasa de respuesta #########################
@api.route("/rate_by_group", methods=['GET'])
@use_kwargs(date_args)
def view_rate_by_group(start_date, end_date):
    """Tasa de respuesta promedio por tipo de usuaria
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_group(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/rate_by_channel", methods=['GET'])
@use_kwargs(date_args)
def view_rate_by_channel(start_date, end_date):
    """Tasa de respuesta promedio por canal de comunicacion
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_channel(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/rate_by_hospital", methods=['GET'])
@use_kwargs(date_args)
def view_rate_by_hospital(start_date, end_date):
    """Tasa de respuesta promedio por atencion medica
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_hospital(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/rate_by_message", methods=['GET'])
@use_kwargs(date_args)
def view_rate_by_message(start_date, end_date):
    """Tasa de respuesta promedio por mensajes
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-05-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_message(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/rate_by_mom_age", methods=['GET'])
@use_kwargs(date_args)
def view_rate_by_mom_age(start_date, end_date):
    """Tasa de respuesta promedio por edad de la mama
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_mom_age(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/rate_by_state", methods=['GET'])
@use_kwargs(date_args)
def view_rate_by_state(start_date, end_date):
    """Tasa de respuesta promedio por estado
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_state(
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/rate_by_mun", methods=['GET'])
@use_kwargs(mun_args)
def view_rate_by_mun(state,start_date, end_date):
    """Tasa de respuesta promedio por municipio
       El endpoint utiliza la fecha time de los runs para filtrar por temporalidad.
       Las fechas son opcionales
    ---
    tags:
      - Tasa de respuesta
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = flows.rate_completed_messages_by_mun(state,
        start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


##################### Calidad medica ##########################
@api.route("/calidad_medica_by_channel", methods=['GET'])
@use_kwargs(calidad_args)
def view_calidad_medica_by_channel(start_date, end_date,category):
    """Calidad medica por estado
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales. La categoria es obligatoria:
       *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_presionarterial
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.get_calidad_medica_by_channel(category, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)

@api.route("/calidad_medica_by_state", methods=['GET'])
@use_kwargs(calidad_args)
def view_calidad_medica_by_state(start_date, end_date,category):
    """Calidad medica por estado
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales. La categoria es obligatoria:
       *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_crecimuterino
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.get_calidad_medica_by_state(category, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/calidad_medica_by_mun", methods=['GET'])
@use_kwargs(calidad_mun_args)
def view_calidad_medica_by_mun(state, start_date, end_date,category):
    """Calidad medica por municipio dado un estado
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales. La categoria es obligatoria:
       *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: state
        in : query
        description: Estado con el numero INEGI
        type: string
        default: 29
        required: True
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_crecimuterino
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.get_calidad_medica_by_mun(
        state,
        category,
        start_date=start_date,
        end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/calidad_medica_by_hospital", methods=['GET'])
@use_kwargs(calidad_args)
def view_calidad_medica_by_hospital(start_date, end_date, category):
    """Calidad medica por atencion medica
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

        **Las fechas son opcionales. La categoria es obligatoria:
        *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_crecimuterino
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.get_calidad_medica_by_hospital(
        category, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/calidad_medica_by_mom_age", methods=['GET'])
@use_kwargs(calidad_args)
def view_calidad_medica_by_mom_age(start_date, end_date, category):
    """Calidad medica por edad de la mama
       El endpoint utiliza la fecha rp_duedate de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales. La categoria es obligatoria:
       *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_crecimuterino
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_duedate(start_date, end_date)
    response = contacts.get_calidad_medica_by_mom_age(
        category, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)


@api.route("/calidad_medica_by_baby_age", methods=['GET'])
@use_kwargs(calidad_args)
def view_calidad_medica_by_baby_age(start_date, end_date, category):
    """Calidad medica por edad del bebe
       El endpoint utiliza la fecha rp_deliverydate de los contactos para filtrar por temporalidad.

      **Las fechas son opcionales. La categoria es obligatoria:
      *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_crecimuterino
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    start_date, end_date = configure_deliverydate(start_date, end_date)
    response = contacts.get_calidad_medica_by_baby_age(
        category, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)



@api.route("/calidad_medica_by_pregnant_week", methods=['GET'])
@use_kwargs(calidad_args)
def view_calidad_medica_by_pregnant_week(start_date, end_date,category):
    """Calidad medica por semana al nacer del bebe
       El endpoint utiliza la fecha created_on de los contactos para filtrar por temporalidad.

       **Las fechas son opcionales. La categoria es obligatoria:
       *** Las categorias validas son: calidad_antropometria, calidad_crecimuterino, calidad_lactancia, calidad_presionarterial, calidad_signosalarma, calidad_vacunas
    ---
    tags:
      - Calidad medica
    parameters:
      - name: start_date
        in: query
        type: string
        description: Fecha de incio
        default: "2016-8-20T00:00:00"
      - name: end_date
        in: query
        type: string
        description: Fecha final
        default: "2018-5-20T00:00:00"
      - name: category
        in : query
        description: Categoria de calidad
        type: string
        default: calidad_presionarterial
        required: True
    responses:
      200:
        description: Las detonaciones pueden ser filtrados por fecha de inicio y fecha final
    """
    response = contacts.get_calidad_medica_by_pregnant_weeks(category, start_date=start_date, end_date=end_date)
    return make_response(jsonify({'response': response}), 200)
"""
curl -X POST \
  http://localhost:5000/users_by_mun \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -H 'Postman-Token: 5b2faa43-1c46-5047-e6f7-13a0fa99279d' \
  -d '{"state": "29", "start_date": "2017-8-20T00:00:00"}'
 """
