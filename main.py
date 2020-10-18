import json,os,flask
from google.cloud import pubsub_v1

project = os.getenv('PROJECT_ID')

def set_topic_by_env(ele):
    
    if 'prod' in ele:
        return f"projects/{project}/topics/my-prod-gateway-topic"
    if 'non-prod' in ele:
        return f"projects/{project}/topics/my-non-prod-gateway-topic"
    if 'staging' in ele:
        return f"projects/{project}/topics/my-staging-gateway-topic"
        
def set_compute_topic_by_env(ele):
    
    if 'prod' in ele:
        return f"projects/{project}/topics/my-prod-compute-engine-topic"
    if 'non-prod' in ele:
        return f"projects/{project}/topics/my-nonprod-compute-engine-topic"
    if 'staging' in ele:
        return f"projects/{project}/topics/my-staging-compute-engine-topic"

def cleanup_func(payload:dict):

    if 'name' not in payload:
        return {'message': 'name is required'}
    if 'project' not in payload:
        return {'message': 'project name is required'}
    if 'zone' not in payload:
        return {'message': 'zone is required'}
    if 'env' not in payload:
        return {'message': 'env is required'}
    if 'machine_type' not in payload:
        return {'message': 'machine_type is required'}
    if payload['name']    == '':
        return {'message': 'name value is empty'}
    if payload['project'] == '':
        return {'message': 'project value is empty'}
    if payload['zone']    == '':
        return {'message': 'zone value is empty'}
    if payload['env']     == '':
        return {'message': 'env value is empty'}
    if payload['machine_type'] == '':
        return {'message': 'machine_type value is empty'}
    payload['environment'] = payload['env']
    del payload['env']
    return payload


def my_gateway_func(request:flask.Request):
    try:
        data               =  cleanup_func(request.get_json(force=True))
        if 'message' in data:
            return data
        topic              = set_topic_by_env(data['environment'])
        data['topic_name'] = set_compute_topic_by_env(data['environment'])
        publisher          = pubsub_v1.PublisherClient()
        try:
            response = publisher.publish(topic=topic,data=json.dumps(data).encode('utf-8'))
            id       = response.running()
        except:
            raise
        else:
            return {'status': f'topic successfully added: {id}'}
    except json.decoder.JSONDecodeError:
        return {'message': 'json formatting is incorrect'}
    # topic = set_topic_by_env(p['env'])
    # print(topic)