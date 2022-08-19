import os
import hubspot
import regex as re
from pprint import pprint
from hubspot.crm.properties import PropertyCreate, ApiException
from dotenv import load_dotenv
from map_industries import make_ind_dict

load_dotenv()


DBDIR = "C:/Users/galon/Sputnik ATX Team Dropbox/Programming Datasets"


def make_industry_options():

    values = [re.sub(r"\s", "_", v.lower()) for v in list(make_ind_dict().keys())]
    labels = list(make_ind_dict().keys())

    return [
        {"label": label, "value": values[n], "displayOrder": n, "hidden": False}
        for n, label in enumerate(labels)
    ]


def create_company_property(
    name,
    label,
    type,
    field_type,
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


if __name__ == "__main__":

    create_company_property(
        "inv_inds",
        "Investment Focus",
        "enumeration",
        "checkbox",
        "companyinformation",
        make_industry_options(),
        2,
        False,
        False,
        True,
    )
