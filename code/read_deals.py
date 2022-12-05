import os, time
import hubspot
from pprint import pprint
from hubspot.crm.deals import BatchReadInputSimplePublicObjectId, ApiException
from dotenv import load_dotenv

load_dotenv()

default_view = ['dealname', 'id', 'reason', 'priority', 'dealstage', 'associated_startup', 'pipeline']


def list_deals(property_list, after):

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))

    try:
        api_response = client.crm.deals.basic_api.get_page(
            limit=100, 
            properties = property_list,
            after = after,
            archived=False,
            )
        return api_response

    except ApiException as e:
        print("Exception when calling basic_api->get_page: %s\n" % e)


def list_deal_associations(deal_id, to_obect_type):

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))
    try:
        api_response = client.crm.deals.associations_api.get_all(
            deal_id=deal_id, 
            to_object_type=to_obect_type, 
            limit=100
            )
        return api_response
    except ApiException as e:
        print("Exception when calling associations_api->get_all: %s\n" % e)



def get_deals_by_pipeline(property_list, pipeline):
    
    deal_list = []
    after = '1'
    property_list += default_view
    
    while after:

        response = list_deals(property_list, after)
        deals = response.results
        try:
            after = str(response.paging.next.after)
        except AttributeError:
            after = None
        
        for deal in deals:
            if deal.properties['pipeline'] == pipeline:
                deal_list.append(deal.properties)
        time.sleep(0.5)
        
    return deal_list



def batch_read_deal(properties, prop_history, id_property,inputs):
    
    client = hubspot.Client.create(access_token=os.getenv("pm_token"))
    batch_read_input_simple_public_object_id = BatchReadInputSimplePublicObjectId(
        properties=properties, 
        properties_with_history=prop_history, 
        id_property=id_property, 
        inputs=inputs
        )
    try:
        api_response = client.crm.deals.batch_api.read(
            batch_read_input_simple_public_object_id=batch_read_input_simple_public_object_id, 
            archived=False
            )
        return api_response

    except ApiException as e:
        print("Exception when calling batch_api->read: %s\n" % e)
        

def read_deal(deal_id):
    
    client = hubspot.Client.create(access_token=os.getenv("pm_token"))

    try:
        api_response = client.crm.deals.basic_api.get_by_id(
            deal_id=deal_id, 
            archived=False)
        return api_response

    except ApiException as e:
        print("Exception when calling basic_api->get_by_id: %s\n" % e)


