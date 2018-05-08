import os

################### Redis host configuration ##########################
CELERY_TIMEZONE = 'America/Mexico_City'
BROKER_URL = os.getenv('BROKER_URL')
CELERY_RESULT_BACKEND = os.getenv('CELERY_RESULT_BACKEND')

################## Elasticsearch configuration ########################
INDEX = 'dashboard'
ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST')

##################### Flows rapidpro constants #########################
RETOS_FLOWS = ['reto']
INCENTIVOS_FLOWS = ['incentives']
RECORDATORIOS_FLOWS = ['freePD', 'getBirth']
PLANIFICACION_FLOWS = ['planning', 'miAlerta_followUp']
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
FIELDS_MUN = "fields.rp_mun_cve"
FIELDS_DELIVERY = "fields.rp_deliverydate"

UPDATE_CONTACT_UUIDS = [
    '8b6ae8ad-84d6-4c6c-80bb-b10a846b23ec',
    'c57676f9-b20a-46e0-b842-8bf44eb780f0',
    '25a57e10-d964-40d2-9802-212eee16f149',
    '8e688d4a-227e-4781-8d86-b97866f29eda',
    'bae111bf-dd0a-4788-8e79-eb780b857513',
    '6178e212-f2d8-4abb-aeaf-613299941ff4',
    '989664a7-4ee0-407b-a1d6-cdb90845b996'
]
