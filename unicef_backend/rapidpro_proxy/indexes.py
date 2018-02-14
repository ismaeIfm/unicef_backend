from elasticsearch_dsl import (Boolean, Date, DocType, Integer, Keyword,
                               MetaField, Object, Text)


class Action(DocType):
    msg = Keyword()
    action_id = Keyword()

    class Meta:
        doc_type = 'action'
        index = 'dashboard'


class Run(DocType):
    flow_uuid = Keyword()
    flow_name = Keyword()
    contact_uuid = Keyword()
    type = Keyword()
    action_uuid = Keyword()
    time = Date()
    msg = Keyword()
    is_one_way = Boolean()
    responded = Boolean()
    exit_type = Keyword()

    class Meta:
        doc_type = 'run'
        index = 'dashboard'
        parent = MetaField(type='contact')


class Value(DocType):
    flow_uuid = Keyword()
    node = Keyword(multi=True)
    contact_uuid = Keyword()
    flow_name = Keyword()
    response = Keyword()
    category = Keyword()
    time = Date()

    class Meta:
        doc_type = 'value'
        index = 'dashboard'
        parent = MetaField(type='contact')


class Contact(DocType):
    urns = Text(multi=True, fields={'raw': Keyword()})
    created_on = Date()
    groups = Object(
        multi=True, properties={"name": Keyword(),
                                "uuid": Keyword()})
    modified_on = Date()
    uuid = Keyword()
    name = Keyword()
    language = Keyword()
    fields = Object(properties={
        'rp_deliverydate': Date(),
        'rp_state_number': Keyword(),
        'rp_ispregnant': Keyword(),
        'rp_mun': Keyword(),
        'rp_atenmed': Keyword(),
        'rp_Mamafechanac': Date(),
        'rp_duedate': Date(),
        'rp_razonalerta': Keyword(),
        'rp_razonbaja': Keyword(),
        'calidad_antropometria': Keyword(),
        'calidad_crecimuterino': Keyword(),
        'calidad_lactancia': Keyword(),
        'calidad_presionarterial': Keyword(),
        'calidad_signosalarma': Keyword(),
        'calidad_vacunas': Keyword()
    })
    stopped = Boolean()
    blocked = Boolean()

    class Meta:
        doc_type = 'contact'
        index = 'dashboard'
