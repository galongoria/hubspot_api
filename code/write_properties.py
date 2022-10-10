import os
import hubspot
import regex as re
from pprint import pprint
from hubspot.crm.properties import (
    PropertyCreate,
    ApiException,
    PropertyUpdate,
    ApiException,
)


def create_company_property(
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
            object_type="company", property_create=property_create
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling core_api->create: %s\n" % e)


def update_company_property(client):

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
            object_type="company",
            property_name=name,
            property_update=property_update,
        )
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling core_api->update: %s\n" % e)




