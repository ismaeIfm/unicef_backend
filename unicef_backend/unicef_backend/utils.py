import re
from datetime import timedelta

##################  Connection to mongo  ##################
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['test-db']


####################### Constants #######################
MOM_AGE_C1 = 18
MOM_AGE_C2 = 35
FIELDS_STATE = "fields.rp_state_number"


####################### Auxiliar functions ##################
def date_decorator(function):
    """ Decorator to change start_date and end_date parameters to
        query dictionary
    """
    def wrapper(*args, **kwargs):
        start_date = kwargs["start_date"] if "start_date" in kwargs else ""
        end_date = kwargs["end_date"] if "end_date" in kwargs else ""
        filter_date = {}
        if start_date:
            filter_date["$gte"] = start_date
        if end_date:
            filter_date["$lte"] = end_date
        if filter_date:
            kwargs["filter_date"] = filter_date
        kwargs.pop('start_date', None)
        kwargs.pop('end_date', None)
        return function(*args, **kwargs)
    return wrapper


def auxiliar_map_reduce(mapper,reducer = None,query = {}):
    """ Auxiliar map reduce, if we need to count, we use format key {count:1}
        Keyword arguments:
        mapper  -- string of map function
        reducer -- string with reduce function (optional)
        query   -- dictionary to filter mongo db (optional)
    """
    if not reducer:
        reducer = '''function(key, values) {
                            var result = { count:0};
                            values.forEach(function(value) {
                               result.count += value.count; })
                            return result; }'''
    results = db["contacts"].map_reduce(mapper, reducer, "states",query=query)
    return  [i for i in  results.find()]


#################### End Auxiliar functions ##################

########## Pendientes
#De ser historica, implica un conteo doble Pendiente
def get_contacts_by_group():
    """ Function to obtain number of contacts by category type:
        personal, pregnant, or with baby """
    # Get personal type (Filter by group)
    personal_contacts = db["contacts"].find({'groups.name':'PERSONAL_SALUD'}).count()
    # Get pregnant type (Filter by group)
    pregnant_contacts = db["contacts"].find({'groups.name':'PREGNANT_MS'}).count()
    # Get baby type (Filter by variable)
    baby_contacts = db["contacts"].find({'fields.rp_ispregnant':'0'}).count()
    return {"baby":baby_contacts,
            "pregnant": pregnant_contacts,
            "personal":personal_contacts}

@date_decorator
def get_mother_age(query={}):
    """ Return number of contacts by age category
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
        query      -- dictionary to filter mongo db
    """
    if filter_date:
        query["created_on"] = filter_date
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

def get_mom_age_by_state():
    """Keyword arguments:
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
        query      -- dictionary to filter mongo db
    """

    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    states_age = {}
    for state in states:
        if state:
            states_age[state] = get_mom_age(query={FIELDS_STATE:state})
    return states_age

@date_decorator
def get_mom_age_by_mun(state, filter_date = {}):
    """ Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_age = {}
    for m in mun:
        if m:
            mun_age[m] = get_mom_age(query={FIELDS_STATE:str(state),'fields.rp_mun':m})
    return mun_age

####### End pendientes
##########################################################################
#                             Contacts part                              #
##########################################################################
@date_decorator
def get_contacts_by_channel(filter_date = {},query = {}):
    """ Function to obtain number of contacts by channel type: facebook, sms
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
        query      -- dictionary to filter mongo db (optional)
    """
    if filter_date:
        query["created_on"] = filter_date
    fb_regx = re.compile("^facebook", re.IGNORECASE)
    sms_regx = re.compile("^tel", re.IGNORECASE)
    # Get facebook contacts
    query["urns"] = fb_regx
    facebook_contacts = db["contacts"].find(query).count()
    # Get sms contacts
    query["urns"] = sms_regx
    sms_contacts = db["contacts"].find(query).count()
    return {"fb":facebook_contacts, "sms": sms_contacts}


@date_decorator
def get_contacts_by_state(filter_date ={},query={}):
    """ Return number of contacts by state inegi number
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter  (optional)
        query      -- dictionary to filter mongo db (optional)
    """
    if filter_date:
        query["created_on"] = filter_date
    mapper = 'function(){ emit(this.fields.rp_state_number, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


@date_decorator
def get_contacts_by_mun(state_number,filter_date={},query={}):
    """ Return number of contacts by municipio depends on state inegi number
        Keyword arguments:
        state_number -- state number inegi
        start_date   -- datetime start date filter (optional)
        end_date     -- datetime end date filter (optional)
        query        -- dictionary to filter mongo db (optional)
    """
    if filter_date:
        query["created_on"] = filter_date
    query[FIELDS_STATE]=str(state_number)
    mapper = 'function() { emit(this.fields.rp_mun, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


@date_decorator
def get_contacts_by_hospital(filter_date={},query={}):
    """ Function to obtain the contacts by medical affiliation
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
        query      -- dictionary to filter mongo db (optional)
    """
    if filter_date:
        query["created_on"] = filter_date
    mapper = 'function() { emit(this.fields.rp_atenmed, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query)


##########################################################################
#                             Babies part                                #
##########################################################################
@date_decorator
def get_babies_by_state(filter_date={}):
    """ Return number of babies by state
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    if filter_date:
        if "$gte" in filter_date:
            filter_date["$gte"] = filter_date["$gte"]-timedelta(years=2)
        query = {"fields.rp_deliverydate": filter_date}
    else:
        query = {'fields.rp_ispregnant':'0'}
    return get_contacts_by_state(query = query)


@date_decorator
def get_babies_by_municipio(state_number, filter_date={}):
    """ Return number of babies by municipio
        Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    if filter_date:
        if "$gte" in filter_date:
            filter_date["$gte"] = filter_date["$gte"]-timedelta(years=2)
        query = {"fields.rp_deliverydate": filter_date}

    else:
        query = {'fields.rp_ispregnant':'0'}
    return get_contacts_by_mun(state_number, query={'fields.rp_ispregnant':'0'})


@date_decorator
def get_babies_by_hospital(filter_date = {}):
    """ Return number of moms by medical affiliation
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    if filter_date:
        if "$gte" in filter_date:
            filter_date["$gte"] = filter_date["$gte"]-timedelta(years=2)
        query = {"fields.rp_deliverydate": filter_date}

    else:
        query = {'fields.rp_ispregnant':'0'}
    return get_contacts_by_atenmed(query = query)



##########################################################################
#                              States part                               #
##########################################################################
@date_decorator
def get_pregnant_by_state(filter_date = {}):
    """Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    if filter_date:
        if "$lte" in filter_date:
            filter_date["$lte"] = filter_date["$lte"]+timedelta(months=9)
        query = {"fields.rp_duedate": filter_date}
    else:
        query = {'fields.rp_ispregnant':'1'}
    return get_contacts_by_state(query=query)


@date_decorator
def get_personal_by_state(filter_date = {}):
    """Keyword arguments:
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
    """
    query = {'groups.name':'PERSONAL_SALUD'}
    if filter_date:
        query["created_on"] = filter_date
    return get_contacts_by_state(query = query)


@date_decorator
def get_hostpital_by_state(filter_date = {}):
    """Keyword arguments:
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
    """
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    state_hospitals = {}
    query = {}
    if filter_date:
        query["created_on"] = filter_date
    for state in states:
        if state:
            query[FIELDS_STATE] = state
            state_hospitals[state] = get_contacts_by_atenmed(query = query)
    return state_hospitals


@date_decorator
def get_channel_by_state(filter_date ={}):
    """Keyword arguments:
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
        query      -- dictionary to filter mongo db
    """
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    state_channels = {}
    query = {}
    if filter_date:
        query["created_on"] = filter_date
    for state in states:
        if state:
            query[FIELDS_STATE] = state
            state_channels[state] = get_contacts_by_channel(query = query)
    return state_channels


##########################################################################
#                         Municipios part                               #
##########################################################################
@date_decorator
def get_pregnant_by_municipio(state, filter_date = {}):
    """Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    if filter_date:
         if "$lte" in filter_date:
             filter_date["$lte"] = filter_date["$lte"]+timedelta(months=9)
         query = {"fields.rp_duedate": filter_date}
    else:
         query = {'fields.rp_ispregnant':'1'}

    return get_contacts_by_mun(state,query = query)


@date_decorator
def get_personal_by_municipio(state, filter_date = {}):
    """Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    query = {'groups.name':'PERSONAL_SALUD'}
    if filter_date:
        query["created_on"] = filter_date
    return get_contacts_by_mun(state,query = query)


@date_decorator
def get_hostpital_by_municipio(state, filter_date = {}):
    """Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_hospitals = {}
    query ={}
    if filter_date:
        query["created_on"] = filter_date
    for m in mun:
        if m:
            query[FIELDS_STATE] = str(state)
            query["fields.rp_mun"] = m
            mun_hospitals[m] = get_contacts_by_hospital(query=query)
    return mun_hospitals


@date_decorator
def get_channel_by_municipio(state, filter_date = {}):
    """Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
    """
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_channels = {}
    query = {}
    if filter_date:
        query["created_on"] = filter_date
    for m in mun:
        if m:
            query[FIELDS_STATE] = str(state)
            query["fields.rp_mun"] = m
            mun_channels[m] = get_contacts_by_channel(query=query)
    return mun_channels

