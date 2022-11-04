import hubspot, os
from pprint import pprint
from hubspot.crm.contacts import BatchInputSimplePublicObjectBatchInput, ApiException





def batch_update_contact(dict_list):

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))

    batch_input_simple_public_object_batch_input = BatchInputSimplePublicObjectBatchInput(inputs=dict_list)
    try:
        api_response = client.crm.contacts.batch_api.update(batch_input_simple_public_object_batch_input=batch_input_simple_public_object_batch_input)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling batch_api->update: %s\n" % e)