from elasticsearch_dsl import (Boolean, Date, DocType, Integer, Keyword,
                               MetaField, Object, Text)


class Action(DocType):
    msg = Keyword()
    action_id = Keyword()

    class Meta:
        doc_type = 'action'
        index = 'dashboard'


class Run(DocType):
    urns = Text(multi=True, fields={'raw': Keyword()})
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
    fields = Object(properties={
        'rp_ispregnant': Keyword(),
        'rp_state_number': Keyword(),
        'rp_mun_cve': Keyword(),
        'rp_atenmed': Keyword(),
        'rp_razonalerta': Keyword(),
        'rp_razonbaja': Keyword(),
        'contact_age': Integer()
    })
    baby_age = Integer()
    pregnant_week = Integer()

    class Meta:
        doc_type = 'run'
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
        'rp_mun_cve': Keyword(),
        'rp_atenmed': Keyword(),
        'rp_mamafechanac': Date(),
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
    pregnant_week = Integer(multi=True)
    baby_age = Integer(multi=True)

    stopped = Boolean()
    blocked = Boolean()

    class Meta:
        doc_type = 'contact'
        index = 'dashboard'

    def update_week(self, week):
        weeks = self.pregnant_week
        if week and week not in weeks:
            weeks.append(week)
            self.pregnant_week = weeks
        return self.save()

    def update_baby_age(self, baby_age):
        trims = self.baby_age
        if baby_age and baby_age not in trims:
            trims.append(baby_age)
            self.baby_age = trims
        return self.save()
