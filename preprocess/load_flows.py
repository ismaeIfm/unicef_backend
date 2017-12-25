from pymongo import MongoClient
from tqdm import tqdm
import json

client = MongoClient('localhost', 27017)
db = client['test-db']

data = json.load(open('actions.json'))

for i in tqdm(data):
    for x,y in i.iteritems():
        message = y["base"]if "base" in y else y["spa"]
        message = message["base"] if "base" in message and type(message) == dict else message
        db['actions'].insert_one({
            'action_id':x,
            'msg': message})

