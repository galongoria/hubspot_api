from write_deals import batch_update_deals
from read_deals import get_deals_by_pipeline, list_deal_associations
from read_companies import batch_read_companies
from read_contacts import batch_read_contacts
import pandas as pd
import os

DBDIR = "C:/Users/galon/Sputnik ATX Team Dropbox/Programming Datasets"
CLEAN_DIR = os.path.join(DBDIR, "tables", "clean")
REACHOUT_DIR = os.path.join(CLEAN_DIR, "reachouts")


BATCH1_PATH = os.path.join(REACHOUT_DIR, "saveday_batch1.csv")


### PUT CALLS ###


def batch_move_deal_dict(inpath, stage):

    df = pd.read_csv(inpath)
    ids = list(df["id"].values)

    return [{"id": str(id_), "properties": {"dealstage": stage}} for id_ in ids]


### PULL FILES FROM CRM ###


def get_associations(dict_list):

    for dictionary in dict_list:

        dictionary["associated_company_ids"] = (
            list_deal_associations(dictionary["hs_object_id"], "company").results[0].id
        )
        dictionary["associated_contact_ids"] = [
            result.id
            for result in list_deal_associations(
                dictionary["hs_object_id"], "contact"
            ).results
        ]
        del dictionary["createdate"]

    return (
        pd.DataFrame.from_dict(dict_list)
        .rename(columns={"hs_object_id": "deal_id"})
        .explode("associated_contact_ids")
    )


def merge_associations(df):

    """Uses the columns of the input dataframe as arguments for API calls. Outputs a dataframe with information about the associated entities"""

    company_list = []
    contact_list = []
    contact_inputs = [
        {"id": str(id_)}
        for id_ in list(
            df.loc[
                ~df["associated_contact_ids"].isna(), "associated_contact_ids"
            ].values
        )
    ]
    company_inputs = [
        {"id": str(id_)}
        for id_ in list(
            df.loc[
                ~df["associated_company_ids"].isna(), "associated_company_ids"
            ].values
        )
    ]

    for company in batch_read_companies(
        ["name", "id"], [], None, company_inputs
    ).results:

        company_list.append(
            {
                "associated_company_name": company.properties["name"],
                "associated_company_ids": company.id,
            }
        )

    for contact in batch_read_contacts(
        ["firstname", "lastname", "email", "id"], [], None, contact_inputs
    ).results:

        contact_list.append(
            {
                "associated_first": contact.properties["firstname"],
                "associated_last": contact.properties["lastname"],
                "associated_email": contact.properties["email"],
                "associated_contact_ids": contact.id,
            }
        )

    return df.merge(
        pd.DataFrame.from_dict(company_list), on="associated_company_ids", how="left"
    ).merge(
        pd.DataFrame.from_dict(contact_list), on="associated_contact_ids", how="left"
    )


if __name__ == "__main__":

    associations_dictionary = get_deals_by_pipeline([], "default")

    df = get_associations(associations_dictionary)
    print(merge_associations(df))
