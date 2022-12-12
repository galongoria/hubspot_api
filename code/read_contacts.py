import os
import hubspot
from pprint import pprint
from hubspot.crm.contacts import BatchReadInputSimplePublicObjectId, ApiException
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("pm_token")


def batch_read_contacts(properties, prop_history, id_property, inputs):

    client = hubspot.Client.create(access_token=token)

    batch_read_input_simple_public_object_id = BatchReadInputSimplePublicObjectId(
        properties=properties,
        properties_with_history=prop_history,
        id_property=id_property,
        inputs=inputs,
    )
    try:
        api_response = client.crm.contacts.batch_api.read(
            batch_read_input_simple_public_object_id=batch_read_input_simple_public_object_id,
            archived=False,
        )
        return api_response

    except ApiException as e:
        print("Exception when calling batch_api->read: %s\n" % e)


def read_conctact(contact_id):
    client = hubspot.Client.create(access_token=token)
    try:
        api_response = client.crm.contacts.basic_api.get_by_id(
            contact_id=contact_id, archived=False
        )
        return api_response

    except ApiException as e:
        print("Exception when calling basic_api->get_by_id: %s\n" % e)
