import hubspot
from pprint import pprint
from hubspot.crm.companies import ApiException, SimplePublicObjectInput


def write_object_association(
    company_id, to_object_type, to_object_id, association_type
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

    # """Writes bulk associations for given companies and associatied object id's.

    # 	Object should be a dictionary; key should be company_id and value should be to_object_id.
    # """

    for key, value in assoc_dict.items():

        write_object_association(key, to_object_type, value, association_type)


def update_company(company_id, properties):

    simple_public_object_input = SimplePublicObjectInput(properties=properties)
    try:
        api_response = client.crm.companies.basic_api.update(
            company_id=company_id
            simple_public_object_input=simple_public_object_input,
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling basic_api->update: %s\n" % e)


def bulk_update_company(co_dict):

	for key, value in co_dict.items():

		update_company(key, value)
