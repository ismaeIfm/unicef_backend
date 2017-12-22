import re
##################  Connection to mongo  ##################
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['test-db']

####################### Constants #######################
MOM_AGE_C1 = 18
MOM_AGE_C2 = 35
FIELDS_STATE = "fields.rp_state_number"



""" Return number of contacts by category type: personal, pregnant, or with baby """
""" Output: {'baby': 2349, 'personal': 11, 'pregnant': 293} """
def get_contacts_by_group():
    # Get personal type (Filter by group)
    personal_contacts = db["contacts"].find({'groups.name':'PERSONAL_SALUD'}).count()
    # Get pregnant type (Filter by group)
    pregnant_contacts = db["contacts"].find({'groups.name':'PREGNANT_MS'}).count()
    # Get baby type (Filter by variable)
    baby_contacts = db["contacts"].find({'fields.rp_ispregnant':'0'}).count()
    return {"baby":baby_contacts,
            "pregnant": pregnant_contacts,
            "personal":personal_contacts}


""" Return number of contacts by channel type: facebook, sms """
""" Output: {'fb': 118, 'sms': 3824} """
def get_contacts_by_urns(query = {}):
    fb_regx = re.compile("^facebook", re.IGNORECASE)
    sms_regx = re.compile("^tel", re.IGNORECASE)
    # Get facebook contacts
    query["urns"]=fb_regx
    facebook_contacts = db["contacts"].find(query).count()
    query["urns"]=sms_regx
    sms_contacts = db["contacts"].find(query).count()
    return {"fb":facebook_contacts, "sms": sms_contacts}


####################### Aggregation methods ##############################

#                             Contacts part                              #
# Auxiliar map reduce, if we need to count, we use format key {count:1}
def auxiliar_map_reduce(mapper,reducer = None,query = {}):
    if not reducer:
        reducer = '''function(key, values) {
                var result = { count:0};
                values.forEach(function(value) {
                result.count += value.count; })
                return result; }'''
    results = db["contacts"].map_reduce(mapper, reducer, "states",query=query)
    return  [i for i in  results.find()]


""" Return number of contacts by state inegi number """
""" Output: [{u'_id': None, u'value': {u'count': 3933.0}},
             {u'_id': u'0', u'value': {u'count': 5.0}},
             {u'_id': u'09', u'value': {u'count': 3.0}},
             {u'_id': u'15', u'value': {u'count': 1.0}},
             {u'_id': u'29', u'value': {u'count': 18.0}}]"""
def get_contacts_by_state(query={}):
    mapper = 'function(){ emit(this.fields.rp_state_number, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


""" Return number of contacts by municipio depends on state inegi number """
""" Output [{u'_id': u'Benito Juarez', u'value': {u'count': 1.0}},
            {u'_id': u'Cuauhtemoc', u'value': {u'count': 1.0}},
            {u'_id': u'Tlalpan', u'value': {u'count': 1.0}}] """
def get_contacts_by_mun(state_number,query={}):
    mapper = 'function() { emit(this.fields.rp_mun, { count: 1});}'
    query[FIELDS_STATE]=str(state_number)
    return auxiliar_map_reduce(mapper,query=query)


""" Return number of contacts by medical affiliation """
""" Output [{u'_id': None, u'value': {u'count': 3723.0}}, {u'_id': u'Farmacias', u'value': {u'count': 3.0}},
            {u'_id': u'IMSS', u'value': {u'count': 13.0}}, {u'_id': u'ISSSTE', u'value': {u'count': 35.0}},
            {u'_id': u'Inst Nac', u'value': {u'count': 11.0}}, {u'_id': u'Other', u'value': {u'count': 3.0}},
            {u'_id': u'Otro', u'value': {u'count': 10.0}}, {u'_id': u'Privado', u'value': {u'count': 39.0}},
            {u'_id': u'SP', u'value': {u'count': 123.0}}]"""
def get_contacts_by_atenmed(query={}):
    mapper = 'function() { emit(this.fields.rp_atenmed, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


""" Return number of contacts by age category """
""" Output {'0-18': 17.0, '18-35': 175.0, '35': 58.0} """
def get_mom_age(query={}):
    mapper = 'function() { emit(this.fields.rp_mamaedad, { count: 1});}'
    age_dict = auxiliar_map_reduce(mapper,query=query)
    category = {"0-18":0, "18-35":0, "35":0}
    for item in age_dict:
        if not item["_id"]: #None values
            continue
        age = [int(s) for s in item["_id"].split() if s.isdigit()]
        if not age:
            continue
        age = age[0]
        #Category
        if age < MOM_AGE_C1:
            category["0-18"] += item["value"]["count"]
        elif MOM_AGE_C1 <= age < MOM_AGE_C2:
            category["18-35"] += item["value"]["count"]
        elif MOM_AGE_C2 <= age:
            category["35"] += item["value"]["count"]
    return category



#                             Babies part                               #
""" Return number of babies by state """
""" Output [{u'_id': None, u'value': {u'count': 2346.0}},
            {u'_id': u'09', u'value': {u'count': 1.0}},
            {u'_id': u'15', u'value': {u'count': 1.0}},
            {u'_id': u'29', u'value': {u'count': 1.0}}]"""
def get_babies_by_state():
    return get_contacts_by_state(query={'fields.rp_ispregnant':'0'})


""" Return number of babies by municipio """
""" Output [{u'_id': u'Cuauhtemoc', u'value': {u'count': 1.0}}]"""
def get_babies_by_municipio(state_number):
    return get_contacts_by_mun(state_number, query={'fields.rp_ispregnant':'0'})


""" Return number of moms by medical affiliation """
""" Output [{u'_id': None, u'value': {u'count': 2244.0}}, {u'_id': u'Farmacias', u'value': {u'count': 3.0}},
            {u'_id': u'IMSS', u'value': {u'count': 5.0}}, {u'_id': u'ISSSTE', u'value': {u'count': 18.0}},
            {u'_id': u'Inst Nac', u'value': {u'count': 4.0}}, {u'_id': u'Other', u'value': {u'count': 1.0}},
            {u'_id': u'Otro', u'value': {u'count': 5.0}}, {u'_id': u'Privado', u'value': {u'count': 18.0}},
            {u'_id': u'SP', u'value': {u'count': 51.0}}]"""
def get_babies_by_atenmed():
    return get_contacts_by_atenmed(query={'fields.rp_ispregnant':'0'})



#                             States part                              #
"""Output [{u'_id': None, u'value': {u'count': 275.0}}, {u'_id': u'0', u'value': {u'count': 4.0}},
           {u'_id': u'09', u'value': {u'count': 1.0}}, {u'_id': u'29', u'value': {u'count': 13.0}}]"""
def get_pregnant_by_state():
    return get_contacts_by_state(query={'groups.name':'PREGNANT_MS'})


""" Output [{u'_id': None, u'value': {u'count': 11.0}}]"""
def get_personal_by_state():
    return get_contacts_by_state(query={'groups.name':'PERSONAL_SALUD'})


""" Output {u'0': {'0-18': 1.0, '18-35': 4.0, '35': 0},
           u'09': {'0-18': 0, '18-35': 3.0, '35': 0},
           u'15': {'0-18': 0, '18-35': 1.0, '35': 0},
           u'29': {'0-18': 1.0, '18-35': 15.0, '35': 0}}"""
def get_mom_age_by_state():
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_mom_age(query={FIELDS_STATE:state})
    return states_age


""" Output: {u'0': [{u'_id': u'IMSS', u'value': {u'count': 1.0}}, {u'_id': u'Inst Nac', u'value': {u'count': 1.0}},
                    {u'_id': u'Other', u'value': {u'count': 1.0}}, {u'_id': u'SP', u'value': {u'count': 2.0}}],
            u'09': [{u'_id': u'Inst Nac', u'value': {u'count': 1.0}}, {u'_id': u'Privado', u'value': {u'count': 2.0}}],
                     u'15': [{u'_id': u'Privado', u'value': {u'count': 1.0}}],
            u'29': [{u'_id': u'Inst Nac', u'value': {u'count': 1.0}}, {u'_id': u'Privado', u'value': {u'count': 1.0}},
                    {u'_id': u'SP', u'value': {u'count': 16.0}}]}"""
def get_atenmed_by_state():
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_contacts_by_atenmed(query={FIELDS_STATE:state})
    return states_age


""" Output: {u'0': {'fb': 1, 'sms': 4}, u'09': {'fb': 2, 'sms': 1},
            u'15': {'fb': 1, 'sms': 0}, u'29': {'fb': 3, 'sms': 15}}"""
def get_urns_by_state():
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_contacts_by_urns(query={FIELDS_STATE:state})
    return states_age


#                         Municipios part                               #
""" Output:  [{u'_id': u'Tlalpan', u'value': {u'count': 1.0}}] """
def get_pregnant_by_mun(state):
    return get_contacts_by_mun(state,query={'groups.name':'PREGNANT_MS'})


""" Output [] """
def get_personal_by_mun(state):
    return get_contacts_by_mun(state,query={'groups.name':'PERSONAL_SALUD'})


""" Output: {u'Benito Juarez': {'0-18': 0, '18-35': 1.0, '35': 0},
             u'Cuauhtemoc': {'0-18': 0, '18-35': 1.0, '35': 0},
             u'Tlalpan': {'0-18': 0, '18-35': 1.0, '35': 0}}"""
def get_mom_age_by_mun(state):
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_mom_age(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age


""" Output: {u'Benito Juarez': [{u'_id': u'Privado', u'value': {u'count': 1.0}}],
                u'Cuauhtemoc': [{u'_id': u'Privado', u'value': {u'count': 1.0}}],
                   u'Tlalpan': [{u'_id': u'Inst Nac', u'value': {u'count': 1.0}}]}"""
def get_atenmed_by_mun(state):
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_contacts_by_atenmed(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age


""" Output: {u'Benito Juarez': {'fb': 1, 'sms': 0},
                u'Cuauhtemoc': {'fb': 1, 'sms': 0},
                   u'Tlalpan': {'fb': 0, 'sms': 1}}"""
def get_urns_by_mun(state):
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_contacts_by_urns(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age

