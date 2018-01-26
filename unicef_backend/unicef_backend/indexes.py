from elasticsearch_dsl import (Boolean, Date, DocType, Integer, Keyword,
                               Object, Text)


class Action(DocType):
    msg = Keyword()
    action_id = Keyword()

    class Meta:
        doc_type = 'action'
        index = 'dashboard'


#class Group(DocType):
#    name = Keyword()
#    uuid = Keyword()
#
#    class Meta:
#        doc_type = 'group'
#        index = 'dashboard'


class Run(DocType):
    flow_uuid = Keyword()
    contact_uuid = Keyword()
    rp_duedate = Date()
    rp_mun = Keyword()
    urns = Text(multi=True, fields={'raw': Keyword()})
    rp_ispregnant = Keyword()
    groups = Object(
        multi=True, properties={"name": Keyword(),
                                "uuid": Keyword()})
    flow_name = Keyword()
    type = Keyword()
    rp_deliverydate = Date()
    rp_state_number = Keyword()
    rp_Mamafechanac = Date()
    action_uuid = Keyword()
    rp_atenmed = Keyword()
    time = Date()
    msg = Keyword()

    class Meta:
        doc_type = 'run'
        index = 'dashboard'


class Value(DocType):
    flow_uuid = Keyword()
    node = Keyword(multi=True)
    contact_uuid = Keyword()
    rp_duedate = Date()
    rp_mun = Keyword()
    urns = Text(multi=True, fields={'raw': Keyword()})
    rp_ispregnant = Keyword()
    groups = Object(
        multi=True, properties={"name": Keyword(),
                                "uuid": Keyword()})
    flow_name = Keyword()
    response = Keyword()
    rp_deliverydate = Date()
    rp_state_number = Keyword()
    category = Keyword()
    rp_atenmed = Keyword()
    time = Date()

    class Meta:
        doc_type = 'value'
        index = 'dashboard'


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
        'rp_duedate': Date()
    })
    stopped = Boolean()
    blocked = Boolean()

    class Meta:
        doc_type = 'contact'
        index = 'dashboard'
