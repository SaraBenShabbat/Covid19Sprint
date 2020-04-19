from __future__ import print_function
import base64
import redis
import ast
import os
import uuid
from datetime import datetime
import time
import json

def get_patient_id(sensor_id: str) -> str:
    elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
    r = redis.StrictRedis(host=elasticache_endpoint, port=6379, db=3, charset="utf-8", decode_responses=True)
    record = r.hget('mapping_table', sensor_id)
    return str(ast.literal_eval(record.decode('ascii')))
    
def lambda_handler(event, context):
    output = []
    # Redis connection
    elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
    r = redis.StrictRedis(host=elasticache_endpoint, port=6379, db=0,charset="utf-8", decode_responses=True)

    for record in event['records']:
        # Decoding, as we get the date encoded to base64.
        payload = base64.b64decode(record['data'])
        dt = json.loads(payload)
        
        # Here, I have tomodify the code. Retieving the patient_id by sensor_id usinh a redis mapping table.
        patient_id = dt['patientId']
        
        # Retrieving the record belongs to the current patient id, from redis.
        current_known = r.hget('LastKnown', patient_id)
        current_update = r.hget('last_update', patient_id)
        
        if(current_known == None):
            r.hset('LastKnown', patient_id, 
                str({'patientId': patient_id, 'age': dt['age'], 'primery_priority': {},'secondery_priority': {}}))
            current_known = r.hget('LastKnown', patient_id)
        if(current_update == None):
            r.hset('last_update', patient_id , str({'patientId': patient_id, 'updates': {}}))
            current_update = r.hget('last_update', patient_id)  
           
        current_known = ast.literal_eval(current_known)
        current_update = ast.literal_eval(current_update)
        
        uniqe_id = str(uuid.UUID(bytes=os.urandom(16), version=4))
        current_known['Id'] = uniqe_id
        current_update['Id'] = uniqe_id

        primery_priority = dt['primery_priority']
        secondery_priority = dt['secondery_priority']
        
        # Update exis measures - both values and both timestamp.
        ns_epoch = int(time.time_ns() // 1000000)
        current_known['timeTag'] = ns_epoch
        if (dt['age'] != current_known['age']):
            current_known['age'] = dt['age']
        for key, val in primery_priority.items():
            current_known['primery_priority'][key] = val
            current_update['updates'][key] = ns_epoch
        for key, val in secondery_priority.items():
            current_known['secondery_priority'][key] = val
            current_update['updates'][key] = ns_epoch
            
        r.hset('LastKnown', patient_id, str(current_known))
        r.hset('last_update', patient_id, str(current_update))

        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload)
        }
        output.append(output_record)  
        return {'records': output}
