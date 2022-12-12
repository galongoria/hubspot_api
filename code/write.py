import hubspot, os
import regex as re
from pprint import pprint
from hubspot.crm.companies import (
    ApiException,
    SimplePublicObjectInput,
    BatchInputSimplePublicObjectBatchInput,
)
from hubspot.crm.properties import (
    PropertyCreate,
    ApiException,
    PropertyUpdate,
    ApiException,
)
from hubspot.crm.contacts import BatchInputSimplePublicObjectBatchInput, ApiException
from hubspot.crm.deals import BatchInputSimplePublicObjectBatchInput, ApiException



client = hubspot.Client.create(access_token=os.getenv("pm_token"))



####################################  CONTACT API CALLS ####################################




def batch_update_contact(dict_list):

    batch_input_simple_public_object_batch_input = BatchInputSimplePublicObjectBatchInput(inputs=dict_list)
    try:
        api_response = client.crm.contacts.batch_api.update(batch_input_simple_public_object_batch_input=batch_input_simple_public_object_batch_input)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling batch_api->update: %s\n" % e)




####################################  COMPANY API CALLS ####################################


def write_object_association(
    client, company_id, to_object_type, to_object_id, association_type
):

    try:
        api_response = client.crm.companies.associations_api.create(
            company_id=company_id,
            to_object_type=to_object_type,
            to_object_id=to_object_id,
            association_type=association_type,
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling associations_api->create: %s\n" % e)


def bulk_associations(assoc_dict, to_object_type, association_type):

    """Writes bulk associations for given companies and associatied object id's.

    Object should be a dictionary; key should be company_id and value should be to_object_id.

    """

    for key, value in assoc_dict.items():

        write_object_association(key, to_object_type, value, association_type)


def update_company(company_id, properties):

    simple_public_object_input = SimplePublicObjectInput(properties=properties)
    try:
        api_response = client.crm.companies.basic_api.update(
            company_id=company_id, simple_public_object_input=simple_public_object_input
        )
        pprint(api_response)

    except ApiException as e:
        print("Exception when calling basic_api->update: %s\n" % e)


def batch_update_company(dict_list):

    batch_input_simple_public_object_batch_input = (
        BatchInputSimplePublicObjectBatchInput(inputs=dict_list)
    )

    try:

        api_response = client.crm.companies.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input_simple_public_object_batch_input
        )

        pprint(api_response)

    except ApiException as e:

        print("Exception when calling batch_api->update: %s\n" % e)




####################################  DEAL API CALLS ####################################

def batch_update_deals(inputs):

    batch_input_simple_public_object_batch_input = BatchInputSimplePublicObjectBatchInput(
        inputs=inputs)
    try:
        api_response = client.crm.deals.batch_api.update(
            batch_input_simple_public_object_batch_input=batch_input_simple_public_object_batch_input
            )
        pprint(api_response)

    except ApiException as e:
        print("Exception when calling batch_api->update: %s\n" % e)



####################################  PROPERTY API CALLS ####################################



def create_property(
    name,
    label,
    type,
    field_type,
    group_name,
    options,
    display_order,
    has_unique_value,
    hidden,
    form_field,
    object_type,
    ):
    client = hubspot.Client.create(access_token=os.getenv("pm_token"))
    property_create = PropertyCreate(
        name=name,
        label=label,
        type=type,
        field_type=field_type,
        group_name=group_name,
        options=options,
        display_order=display_order,
        has_unique_value=has_unique_value,
        hidden=hidden,
        form_field=form_field,
    )
    try:
        api_response = client.crm.properties.core_api.create(
            object_type=object_type, property_create=property_create
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling core_api->create: %s\n" % e)


def update_property(
    name,
    label,
    type,
    field_type,
    group_name,
    options,
    display_order,
    hidden,
    form_field,
    object_type,
    ):

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))

    property_update = PropertyUpdate(
        label=label,
        type=type,
        field_type=field_type,
        group_name=group_name,
        options= options,
        display_order=display_order,
        hidden=hidden,
        form_field=form_field,
    )
    try:
        api_response = client.crm.properties.core_api.update(
            object_type=object_type,
            property_name=name,
            property_update=property_update,
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling core_api->update: %s\n" % e)
