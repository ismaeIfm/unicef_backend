from elasticsearch_dsl import Boolean, Date, DocType, Keyword, Object


class Action(DocType):
    msg = Keyword()
    action_id = Keyword()

    class Meta:
        doc_type = 'action'
        index = 'dashboard'


class Group(DocType):
    name = Keyword()
    uuid = Keyword()

    class Meta:
        doc_type = 'group'
        index = 'dashboard'


class Run(DocType):
    flow_uuid = Keyword()
    contact_uuid = Keyword()
    rp_duedate = Date()
    rp_mun = Keyword()
    urns = Keyword(multi=True)
    rp_ispregnant = Keyword()
    groups = Keyword(multi=True)
    flow_name = Keyword()
    rp_deliverydate = Date()
    rp_state_number = Keyword()
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
    urns = Keyword(multi=True)
    rp_ispregnant = Keyword()
    groups = Keyword(multi=True)
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
    urns = Keyword(multi=True)
    created_on = Date()
    groups = Keyword(multi=True)
    modified_on = Date()
    uuid = Keyword()
    name = Keyword()
    language = Keyword()
    fields = Object(properties={
        "ext_loc_nom_loc_cs10": Keyword(),
        "rp_psvideos": Keyword(),
        "rp_psmotiva": Keyword(),
        "rp_height": Keyword(),
        "rp_name": Keyword(),
        "rp_uncaught_counter": Keyword(),
        "rp_uncaught_time": Keyword(),
        "rp_prosperapal": Keyword(),
        "rp_corner": Keyword(),
        "rp_psapoyo": Keyword(),
        "rp_miprueba_mialta": Keyword(),
        "rp_bmi": Keyword(),
        "testlocgroup": Keyword(),
        "rp_isaux": Keyword(),
        "pd1_nextwshpdate": Keyword(),
        "rp_missingheight": Keyword(),
        "rp_from_io": Keyword(),
        "rp_pscarteles": Keyword(),
        "rp_duedate": Date(),
        "rp_remgroup": Keyword(),
        "rp_getdata": Keyword(),
        "rp_psplatica": Keyword(),
        "rp_celular": Keyword(),
        "rp_psoccupation": Keyword(),
        "rp_psname": Keyword(),
        "rp_mialta_deliverydate": Keyword(),
        "rp_psinfomisalud": Keyword(),
        "rp_miprueba_mialtams": Keyword(),
        "rp_cesarea": Keyword(),
        "rp_colonia": Keyword(),
        "rp_mialta_duedate": Keyword(),
        "ext_cl_nombre_clcat": Keyword(),
        "rp_mun": Keyword(),
        "ext_namef": Keyword(),
        "rp_pastpreg": Keyword(),
        "rp_prestadorss": Keyword(),
        "ext_namem": Keyword(),
        "ext_name": Keyword(),
        "rp": Keyword(),
        "rp_missingapgar": Keyword(),
        "rp_remfollow1": Keyword(),
        "rp_remfollow3": Keyword(),
        "rp_remfollow2": Keyword(),
        "rp_remfollow4": Keyword(),
        "medprueba_enrolamiento": Keyword(),
        "medprueba_enrolamientootra": Keyword(),
        "rp_complicacionpast": Keyword(),
        "rp_isvocalaux": Keyword(),
        "rp_mialta_apptdate": Keyword(),
        "rp_deliverytype": Keyword(),
        "rp_long": Keyword(),
        "rp_ispregnant": Keyword(),
        "rp_mialta_deliverydatems": Keyword(),
        "rp_mamafechanac": Keyword(),
        "rp_babyname": Keyword(),
        "ext_loc_nom_mun_cs10": Keyword(),
        "rp_mialta_init2": Keyword(),
        "rp_pspantallas": Keyword(),
        "rp_psinscrip": Keyword(),
        "rp_clphone": Keyword(),
        "rp_alerta_time": Keyword(),
        "pd1_clues": Keyword(),
        "rp_missingdeliverydate": Keyword(),
        "pd1_duedate": Keyword(),
        "rp_missingweightbaby": Keyword(),
        "rp_entidad": Keyword(),
        "rp_psentera": Keyword(),
        "rp_missingbmi": Keyword(),
        "rp_deliverydate_counter": Keyword(),
        "ext_clues": Keyword(),
        "rp_phonenumber": Keyword(),
        "rp_state": Keyword(),
        "rp_remtotalappts": Keyword(),
        "rp_pspago": Keyword(),
        "rp_mialta_ps": Keyword(),
        "rp_psvalora": Keyword(),
        "rp_deliverydate": Date(),
        "rp_mialta_started": Keyword(),
        "rp_apptdate": Keyword(),
        "rp_pssust": Keyword(),
        "rp_treatmentarm": Keyword(),
        "rp_mialta_initms": Keyword(),
        "rp_plantransport": Keyword(),
        "rp_weight": Keyword(),
        "rp_remapptdatefinal": Keyword(),
        "rp_complicacionpresent": Keyword(),
        "rp_altasms_group": Keyword(),
        "rp_psrazon": Keyword(),
        "rp_missingid": Keyword(),
        "rp_tel": Keyword(),
        "rp_state_number": Keyword(),
        "rp_missingapptdate": Keyword(),
        "rp_mialta_enddate": Keyword(),
        "rp_apgar": Keyword(),
        "rp_duedate_counter": Keyword(),
        "rp_dayssincemens": Keyword(),
        "rp_lat": Keyword(),
        "rp_missingweight": Keyword(),
        "rp_mialta_counter": Keyword(),
        "rp_psdesconoce": Keyword(),
        "id": Keyword(),
        "pd1_treatmentarm": Keyword(),
        "rp_isps": Keyword(),
        "rp_isvocal": Keyword(),
        "rp_psgrupo": Keyword(),
        "rp_mialerta_time": Keyword(),
        "rp_atenmed": Keyword(),
        "pd1_nextapptdate": Keyword(),
        "rp_mialta_duedatems": Keyword(),
        "rp_mamaedad": Keyword(),
        "rp_babyweight": Keyword(),
        "rp_psjuris": Keyword(),
        "rp_mialta_init": Keyword(),
        "rp_hasbaby": Keyword(),
        "rp_wshpdate": Keyword(),
        "rp_missingduedate": Keyword(),
        "ext_loc_nom_ent_cs10": Keyword(),
        "rp_altasms_time": Keyword(),
        "rp_remapptdate5": Keyword(),
        "rp_remapptdate4": Keyword(),
        "rp_remapptdate3": Keyword(),
        "rp_remapptdate2": Keyword(),
        "rp_remapptdate1": Keyword(),
        "ext_birthday": Keyword(),
        "rp_planhospital": Keyword(),
        "pd1_appts": Keyword(),
    })
    stopped = Boolean()
    blocked = Boolean()

    class Meta:
        doc_type = 'contact'
        index = 'dashboard'
