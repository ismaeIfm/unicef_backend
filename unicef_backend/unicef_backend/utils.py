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
def get_contacts_by_state(query={}):
    mapper = 'function(){ emit(this.fields.rp_state_number, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


""" Return number of contacts by municipio depends on state inegi number """
def get_contacts_by_mun(state_number,query={}):
    mapper = 'function() { emit(this.fields.rp_mun, { count: 1});}'
    query[FIELDS_STATE]=str(state_number)
    return auxiliar_map_reduce(mapper,query=query)


""" Return number of contacts by medical affiliation """
def get_contacts_by_atenmed(query={}):
    mapper = 'function() { emit(this.fields.rp_atenmed, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


""" Return number of contacts by age category """
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
def get_babies_by_state():
    return get_contacts_by_state(query={'fields.rp_ispregnant':'0'})


""" Return number of babies by municipio """
def get_babies_by_municipio(state_number):
    return get_contacts_by_mun(state_number, query={'fields.rp_ispregnant':'0'})


""" Return number of moms by medical affiliation """
def get_babies_by_atenmed():
    return get_contacts_by_atenmed(query={'fields.rp_ispregnant':'0'})



#                             States part                              #
def get_pregnant_by_state():
    return get_contacts_by_state(query={'groups.name':'PREGNANT_MS'})


def get_personal_by_state():
    return get_contacts_by_state(query={'groups.name':'PERSONAL_SALUD'})


def get_mom_age_by_state():
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_mom_age(query={FIELDS_STATE:state})
    return states_age


def get_atenmed_by_state():
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_contacts_by_atenmed(query={FIELDS_STATE:state})
    return states_age


def get_urns_by_state():
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_contacts_by_urns(query={FIELDS_STATE:state})
    return states_age


#                         Municipios part                               #
def get_pregnant_by_mun(state):
    return get_contacts_by_mun(state,query={'groups.name':'PREGNANT_MS'})


def get_personal_by_mun(state):
    return get_contacts_by_mun(state,query={'groups.name':'PERSONAL_SALUD'})

def get_mom_age_by_mun(state):
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_mom_age(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age

def get_atenmed_by_mun(state):
    mun = db["contacts"].find({}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_contacts_by_atenmed(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age


def get_urns_by_mun(state):
    mun = db["contacts"].find({}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_contacts_by_urns(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age

