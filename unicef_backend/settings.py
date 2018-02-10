import os

################### Redis host configuration ##########################
CELERY_TIMEZONE = 'America/Mexico_City'
BROKER_URL = 'redis://localhost:6379/0'
CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

################## Elasticsearch configuration ########################
INDEX = 'dashboard'
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')

##################### Flows rapidpro constants #########################
RETOS_FLOWS = ['reto']
INCENTIVOS_FLOWS = ['incentives']
RECORDATORIOS_FLOWS = ['freePD', 'getBirth', 'miAlerta']
PLANIFICACION_FLOWS = ['planning', 'miAlerta_followUp', 'miAlta']
PREOCUPACIONES_FLOWS = ['prevent', 'concerns', 'miscarriage', 'prematuro']
CONSEJOS_FLOWS = [
    'consejo', 'mosquitos', 'development', 'lineaMaterna', 'milk', 'nutrition',
    'extra', 'labor'
]
MIALERTA_FLOW = "07d56699-9cfb-4dc6-805f-775989ff5b3f"
MIALERTA_NODE = "response_1"
CANCEL_FLOW = "dbd5738f-8700-4ece-8b8c-d68b3f4529f7"
CANCEL_NODE = "response_3"

##################### Contact fields constants #######################
FIELDS_STATE = "fields.rp_state_number"
FIELDS_MUN = "fields.rp_mun"
FIELDS_DELIVERY = "fields.rp_deliverydate"
