from __future__ import print_function
import base64
import redis
import os
import uuid
import json
import time


elasticache_endpoint = "cv19redis-001.d9jy7a.0001.euw1.cache.amazonaws.com"
redis_mapping = redis.StrictRedis(host=elasticache_endpoint, db=3, charset="utf-8", decode_responses=True)

def get_patient_id(sensor_id: str) -> str:
    return redis_mapping.get(sensor_id)


def recreate_mapping():
    print('---------')


def is_db_failed(event) -> bool:
    # This sample sensor unit id has to appear in the redis, if no - it means it failed.
    sample_unitId = event['records']
    sample_unitId  = json.loads(base64.b64decode(sample_unitId[0]['data']))['unitId']

    if(redis_mapping.exists(sample_unitId) != 1):
        return True
    return False
    

def is_redis_available() -> bool:
    r = redis.StrictRedis(host=elasticache_endpoint, port=6379)
    try:
        r.ping()
    except (redis.exceptions.ConnectionError, redis.exceptions.BusyLoadingError):
        return False
    return True


def lambda_handler(event, context):
    output = []

    for record in event['records']:
        # Decoding, because we get the date encoded to base64.
        payload = base64.b64decode(record['data'])
        dt = json.loads(payload)
        
        if(is_redis_available() == True):
            # Redis connection
            r = redis.StrictRedis(host=elasticache_endpoint, port=6379, db=0,charset="utf-8", decode_responses=True)

            # If the db failed, recreate mapping table.
            if(is_db_failed(event) == True):
                recreate_mapping()
            
            patient_id = get_patient_id(dt['unitId'])
            # Retrieving the record belongs to the current patient id, from redis.
            current_known = r.hget('LastKnown', patient_id)
            current_update = r.hget('last_update', patient_id)
            if(current_known == None):
                r.hset('LastKnown', patient_id, json.dumps({'patientId': patient_id, 'age': dt['age'], 'primery_priority': {},'secondery_priority': {}}))
                current_known = r.hget('LastKnown', patient_id)
            if(current_update == None):
                r.hset('last_update', patient_id , json.dumps({'patientId': patient_id, 'updates': {}}))
                current_update = r.hget('last_update', patient_id)  
           
            current_known = json.loads(current_known)
            current_update = json.loads(current_update)
            
            uniqe_id = str(uuid.UUID(bytes=os.urandom(16), version=4))
            current_known['Id'] = uniqe_id
            current_update['Id'] = uniqe_id

            primery_priority = dt['primery_priority']
            secondery_priority = dt['secondery_priority']

            ns_epoch = int(time.time_ns() // 1000000)
            
            # Update exist measures - both values and timestamp.
            current_known['timeTag'] = ns_epoch
            if (dt['age'] != current_known['age']):
                current_known['age'] = dt['age']
            for key, val in primery_priority.items():
                current_known['primery_priority'][key] = val
                current_update['updates'][key] = ns_epoch
            for key, val in secondery_priority.items():
                current_known['secondery_priority'][key] = val
                current_update['updates'][key] = ns_epoch
            
            r.hset('LastKnown', patient_id, json.dumps(current_known))
            r.hset('last_update', patient_id, json.dumps(current_update))

        # Anyway, transfer the data back to kinesis in order it to be written to ES.
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64.b64encode(payload)        
        }
        output.append(output_record)  
        return {'records': output}
