import re
from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime
from dateutil.parser import parse


##################  Connection to mongo  ##################
from pymongo import MongoClient

client = MongoClient('localhost', 27017)
db = client['test-db']


####################### Constants #######################
MOM_AGE_C1 = 18
MOM_AGE_C2 = 35
FIELDS_STATE = "fields.rp_state_number"
FIELDS_DELIVERY = "fields.rp_deliverydate"
MIALERTA_FLOW = "07d56699-9cfb-4dc6-805f-775989ff5b3f"
MIALERTA_NODE = "response_1" 
CANCEL_FLOW = "dbd5738f-8700-4ece-8b8c-d68b3f4529f7"
CANCEL_NODE = "response_3"
BABY_WEEKS = 40

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
            filter_date["$gte"] = start_date.isoformat()
        if end_date:
            filter_date["$lt"] = end_date.isoformat()
        if filter_date:
            kwargs["filter_date"] = filter_date
        kwargs.pop('start_date', None)
        kwargs.pop('end_date', None)
        return function(*args, **kwargs)
    return wrapper


def auxiliar_map_reduce(mapper,reducer = None,query = {}, database="contacts"):
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
    results = db[database].map_reduce(mapper, reducer, "states",query=query)
    return  [i for i in  results.find()]

#                          Auxiliar flows                                
def auxiliar_mialerta(filter_date, query):
    if filter_date:
        query["time"] = filter_date 
    query["flow_uuid"] = MIALERTA_FLOW
    mapper = 'function() { emit(this.flow_name, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="runs")


def auxiliar_cancel_reasons(filter_date, query):
    """ Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    query["node"]= CANCEL_NODE
    query["flow_uuid"]=CANCEL_FLOW
    if filter_date:
        query["time"] = filter_date 
    mapper = 'function() { emit(this.category, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="values")


def get_flow_by_group(method,filter_date={}):
    result = {}
    #Mother
    query = {'rp_ispregnant':'1'}
    result["baby"] = method(filter_date,query)
    #Pregnant
    query = {'rp_ispregnant':'0'} 
    result["pregnant"] = method(filter_date,query)
    #Personal
    query = {'groups.name':'PERSONAL_SALUD'}
    result["personal"] = method(filter_date,query)
    return result


def get_flow_by_state(flow_uuid, filter_date = {}):
    """Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    query= {"flow_uuid" : flow_uuid}
    if filter_date:
        query["time"] = filter_date
    mapper = 'function(){ emit(this.rp_state_number, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="runs")


def get_flow_by_mun(state_number,flow_uuid,filter_date = {}):
    """Keyword arguments:
        state_number -- state number inegi 
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    query= {"flow_uuid" : flow_uuid}
    if filter_date:
        query["time"] = filter_date
    query["rp_state_number"] = str(state_number)
    mapper = 'function() { emit(this.rp_mun, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="runs")


def get_flow_by_hospital(flow_uuid, filter_date = {}):
    """Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter   (optional)
    """
    query= {"flow_uuid" : flow_uuid}
    if filter_date:
        query["time"] = filter_date
    mapper = 'function() { emit(this.rp_atenmed, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="runs")


def get_flow_by_channel(flow_uuid, filter_date = {}):
    """ Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    query= {"flow_uuid" : flow_uuid}
    if filter_date:
        query["time"] = filter_date 
    fb_regx = re.compile("^facebook", re.IGNORECASE)
    sms_regx = re.compile("^tel", re.IGNORECASE)
    # Get facebook contacts
    query["urns"] = fb_regx
    facebook_contacts = db["runs"].find(query).count()
    # Get sms contacts
    query["urns"] = sms_regx
    sms_contacts = db["runs"].find(query).count()
    return {"fb":facebook_contacts, "sms": sms_contacts}


def get_flow_by_baby_age(flow_uuid ,filter_date = {}):
    """ Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """ 
    response = {}
    query= {"flow_uuid" : flow_uuid}
    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    if "$gte" in filter_date:
        start_date_pointer =  parse(filter_date["$gte"]) 
    if "$lt" in filter_date:
        end_date_pointer = parse(filter_date["$lt"]) 
    for idx in range(1,9):
       start_date = start_date_pointer - relativedelta(months = idx*3)
       end_date = end_date_pointer - relativedelta(months = (idx-1)*3)
       query["rp_deliverydate"] = { "$gte": start_date.isoformat(), 
                                   "$lt": end_date.isoformat() }
       key = str((idx-1)*3)+"-"+str(idx*3)
       response[key] = db["runs"].find(query).count()
    return response

#################### End Auxiliar functions ##################

########## Pendientes

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
def get_contacts_by_group(filter_date={}):
    """ Function to obtain number of contacts by category type:
        personal, pregnant, or with baby """
    # Get personal type (Filter by group) 
    query={"group.name":"PERSONAL_SALUD"}
    if filter_date:
        query["created_on"] = filter_date 
    personal_contacts = db["contacts"].find(query).count() 
    # Get pregnant type (Filter by group)
    if filter_date:
        if "$lt" in filter_date:
            filter_date["$lt"] = parse(filter_date["$lt"])+relativedelta(months=9)
            filter_date["$lt"] = filter_date["$lt"].isoformat()
        query = {"fields.rp_duedate": filter_date}
    else:
        query = {'fields.rp_ispregnant':'1'} 
    pregnant_contacts = db["contacts"].find(query).count()
    # Get baby type (Filter by variable)
    if filter_date:
        if "$gte" in filter_date:
            filter_date["$gte"] = parse(filter_date["$gte"])-relativedelta(years=2)
            filter_date["$gte"] = filter_date["$gte"].isoformat()
        query = {FIELDS_DELIVERY: filter_date}
    else:
        query = {'fields.rp_ispregnant':'0'} 
    baby_contacts = db["contacts"].find(query).count()
    return {"baby":baby_contacts,
            "pregnant": pregnant_contacts,
            "personal":personal_contacts}

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



@date_decorator 
def get_contacts_by_baby_age(filter_date = {},query={}):
    """ Function to obtain the contacts by baby age
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """ 
    response = {}
    start_date_pointer = datetime.utcnow()
    end_date_pointer = datetime.utcnow()
    if "$gte" in filter_date:
        start_date_pointer =  parse(filter_date["$gte"]) 
    if "$lt" in filter_date:
        end_date_pointer = parse(filter_date["$lt"]) 
    for idx in range(1,9):
       start_date = start_date_pointer - relativedelta(months = idx*3)
       end_date = end_date_pointer - relativedelta(months = (idx-1)*3)
       query [FIELDS_DELIVERY] = { "$gte": start_date.isoformat(), 
                                   "$lt": end_date.isoformat() }
       key = str((idx-1)*3)+"-"+str(idx*3)
       response[key] = db["contacts"].find(query).count()
    return response
    
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
            filter_date["$gte"] = parse(filter_date["$gte"])-relativedelta(years=2)
            filter_date["$gte"] = filter_date["$gte"].isoformat()
        query = {FIELDS_DELIVERY: filter_date}
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
            filter_date["$gte"] = filter_date["$gte"]-relativedelta(years=2)
            filter_date["$gte"] = filter_date["$gte"].isoformat()
        query = {FIELDS_DELIVERY: filter_date}

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
            filter_date["$gte"] = filter_date["$gte"]-relativedelta(years=2)
            filter_date["$gte"] = filter_date["$gte"].isoformat()
        query = {FIELDS_DELIVERY: filter_date}

    else:
        query = {'fields.rp_ispregnant':'0'}
    return get_contacts_by_atenmed(query = query)


def get_babies_by_week(filter_date = {}):
    """ Function to obtain the contacts by baby age
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    query = {'fields.rp_ispregnant':'1', "fields.rp_duedate":{"$ne":None}}
    result = {}
    for c in db["contacts"].find(query):
       key = (datetime.now() - parse(c["fields"]["rp_duedate"])).days/7
       if key > 0 or BABY_WEEKS + key < 0: #Only work with negative weeks
           continue
       key = BABY_WEEKS + key
       if key in result:
           result[key] += 1
       else:
           result[key] = 1
    return result 

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
        if "$lt" in filter_date:
            filter_date["$lt"] = parse(filter_date["$lt"])+relativedelta(months=9)
            filter_date["$lt"] = filter_date["$lt"].isoformat()
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


@date_decorator
def get_baby_age_by_state(filter_date ={}):
    """Keyword arguments:
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
    """
    states = db["contacts"].find({}).distinct(FIELDS_STATE)
    state_babies= {}
    query = {}
    for state in states:
        if state:
            query[FIELDS_STATE] = state
            state_babies[state] = get_contacts_by_baby_age(filter_date,query = query)
    return state_babies

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
         if "$lt" in filter_date:
             filter_date["$lt"] = parse(filter_date["$lt"])+relativedelta(months=9)
             filter_date["$lt"] = filter_date["$lt"].isoformat()
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

@date_decorator
def get_baby_age_by_municipio(state, filter_date = {}):
    """Keyword arguments:
        state_number -- state number inegi
        start_date -- datetime start date filter
        end_date   -- datetime end date filter
    """
    mun = db["contacts"].find({FIELDS_STATE:str(state)}).distinct('fields.rp_mun')
    mun_babies = {}
    query = {}
    for m in mun:
        if m:
            query[FIELDS_STATE] = str(state)
            query["fields.rp_mun"] = m
            mun_babies[m] = get_contacts_by_baby_age(filter_date, query)
    return mun_babies

##########################################################################
#                             Msgs part                                  #
##########################################################################
@date_decorator
def get_sent_msgs_by_state(filter_date = {}):
    """ Return number of msgs sent by state inegi number
        Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter  (optional)
    """
    query = {}
    if filter_date:
        query["time"] = filter_date
    mapper = 'function() { emit(this.rp_state_number,{ count: 1});}'
    return  auxiliar_map_reduce(mapper,query=query, database="runs")


@date_decorator
def get_sent_msgs_by_mun(state_number, filter_date = {}):
    """ Return number of msgs sent by municipio depends on state inegi number 
        Keyword arguments:
        state_number -- state number inegi 
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter  (optional)
    """
    query = {}
    if filter_date:
        query["time"] = filter_date
    query["rp_state_number"] = str(state_number)
    mapper = 'function() { emit(this.rp_mun, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="runs")


@date_decorator
def get_sent_msgs_by_flow(filter_date =  {}):
    """ Return number of msgs sent by flow 
        Keyword arguments:
        state_number -- state number inegi 
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter  (optional)
    """
    query = {}
    if filter_date:
        query["time"] = filter_date 
    mapper = 'function() { emit(this.flow_name, { count: 1});}'
    all_flows =  auxiliar_map_reduce(mapper,query=query, database="runs")
    all_flows = sorted(all_flows, key=lambda k: k["value"]["count"],reverse = True) 
    return  [{f["_id"]:f["value"]["count"]} for f in all_flows][:10]


##########################################################################
#                         Mi alerta       (use flow auxiliar methods)    #
##########################################################################
@date_decorator
def get_mialerta_by_group(filter_date = {}):
    return get_flow_by_group(auxiliar_mialerta,filter_date)
    
@date_decorator
def get_mialerta_by_state(filter_date = {}):
    return get_flow_by_state(MIALERTA_FLOW,filter_date)


@date_decorator
def get_mialerta_by_mun(state_number,filter_date = {}):
    return get_flow_by_mun(state_number,MIALERTA_FLOW,filter_date) 


@date_decorator
def get_mialerta_by_hospital(filter_date = {}):
    return get_flow_by_hospital(MIALERTA_FLOW, filter_date)


@date_decorator
def get_mialerta_by_channel(filter_date = {}):
    return get_flow_by_channel(MIALERTA_FLOW, filter_date)

@date_decorator
def get_mialerta_by_baby_age(filter_date = {}):
    return get_flow_by_baby_age(MIALERTA_FLOW, filter_date)

@date_decorator
def get_mialerta_msgs_top(filter_date = {}):
    """ Keyword arguments:
        start_date -- datetime start date filter (optional)
        end_date   -- datetime end date filter (optional)
    """
    query = {"node": MIALERTA_NODE, "flow_uuid":MIALERTA_FLOW}
    if filter_date:
        query["time"] = filter_date 
    mapper = 'function() { emit(this.category, { count: 1});}'
    return auxiliar_map_reduce(mapper,query=query, database="values")


##########################################################################
#                        Cancel part   (Use flow auxiliar methods)       #
##########################################################################
@date_decorator
def get_cancel_by_group(filter_date = {}):
    return get_flow_by_group(auxiliar_cancel_reasons,filter_date) 

@date_decorator
def get_cancel_by_state(filter_date = {}):
    return get_flow_by_state(CANCEL_FLOW,filter_date)

@date_decorator
def get_cancel_by_mun(state_number,filter_date = {}):
    return get_flow_by_mun(state_number,CANCEL_FLOW,filter_date) 

@date_decorator
def get_cancel_by_hospital(filter_date = {}):
    return get_flow_by_hospital(CANCEL_FLOW, filter_date)

@date_decorator
def get_cancel_by_channel(filter_date = {}):
    return get_flow_by_channel(CANCEL_FLOW, filter_date)

@date_decorator
def get_cancel_by_baby_age(filter_date = {}):
    return get_flow_by_baby_age(CANCEL_FLOW, filter_date)

