import hubspot, os
from pprint import pprint
from hubspot.crm.deals import BatchInputSimplePublicObjectBatchInput, ApiException
from dotenv import load_dotenv

load_dotenv()

def batch_update_deals(inputs):

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))

    batch_input_simple_public_object_batch_input = BatchInputSimplePublicObjectBatchInput(
        inputs=inputs)
    try:
        api_response = client.crm.deals.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input_simple_public_object_batch_input
            )
        pprint(api_response)

    except ApiException as e:
        print("Exception when calling batch_api->update: %s\n" % e)





