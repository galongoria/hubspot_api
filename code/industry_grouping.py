import os, time, hubspot
import regex as re
import pandas as pd
from dotenv import load_dotenv
from map_industries import make_ind_dict, make_tag_dict
from write_properties import create_company_property
from write_companies import update_company, batch_update_company

load_dotenv()

DBDIR = "C:/Users/galon/Sputnik ATX Team Dropbox/Programming Datasets"
RAW_DIR = os.path.join(DBDIR, "data", "raw")
CLEAN_DIR = os.path.join(DBDIR, "data", "clean")
IND_DIR = os.path.join(CLEAN_DIR, "industry_mapping")
VC_INDUSTRY_COLS = os.path.join(RAW_DIR, "hs", "vc_industry_columns.csv")
STARTPATH = os.path.join(
    CLEAN_DIR, "scraped_data", "crunchbase", "cb_starts_main_scraped.csv"
)
MAPPING_OUTPATH = os.path.join(IND_DIR, "cb_starts_mapped.csv")
START_HERF_PATH = os.path.join(
    CLEAN_DIR, "scraped_data", "crunchbase", "cb_inv_overview_scraped.csv"
)
VC_PATH = os.path.join(RAW_DIR, "hs", "vc_list_export.csv")


"""
Below is a reference for the internal property names in HubSpot:

Investment Focus: pf_inds
Record ID: id
Company name: name
Investor Tags: inv_tags

"""


def make_industry_options(df):

    df = df[~df["pf_inds"].isnull()]
    tags = "".join(list(df["pf_inds"].values))
    labels = list(set(tags.split(";")))
    labels.pop(0)
    values = [re.sub(r" ", "_", label).lower() for label in labels]

    return [
        {"label": label, "value": values[n], "displayOrder": n, "hidden": False}
        for n, label in enumerate(labels)
    ]


def explode_df(csv_inpath, industry_column, split_on):

    """Takes the dataframe and explodes on the industry"""

    df = pd.read_csv(csv_inpath).rename(columns={industry_column: "industries"})
    df["ref"] = df["industries"]
    df = df[~df["industries"].isnull()]
    df["industries"] = df["industries"].apply(lambda x: re.split(split_on, str(x)))
    df = df.explode("industries")
    df["industries"] = df["industries"].apply(lambda x: re.sub(r"&", "and", str(x)))
    df["industries"] = df["industries"].apply(lambda x: x.lower().strip())

    return df.drop("Unnamed: 0", axis=1)


def map_groups(exploded_df, new_col, map_dict):

    """Takes the exploded dataframe and groups the industries more generally"""

    merge_df = exploded_df.copy(deep=True)
    merge_df[new_col] = ""
    exploded_df.reset_index(inplace=True)

    for key, value in map_dict.items():

        map_df = pd.DataFrame({key: value})
        map_df[key] = map_df[key].apply(lambda x: x.lower())
        matches = exploded_df.merge(
            map_df, how="left", left_on="industries", right_on=key
        )
        merge_df.loc[
            matches[~matches[key].isnull()]["index"].values, new_col
        ] += f";{key}"
    return merge_df


def aggregate(df, dictionary):

    return df.groupby(df.index).agg(dictionary)


def mental_health(df):

    df['description'] = df['description'].apply(lambda x: x.lower())
    df.loc[
        df["description"].str.contains("mental health"), "pf_inds"
    ] += ";Mental Health"

    return df


def map_subindustries(ind_list):

    ind_tuples = [
        ("Mental Health", "Health"),
        ("Payment Card Industry (PCI)", "FinTech"),
        ("FinTech", "Financial Services"),
        ("InsurTech", "Insurance"),
        ("HealthTech", "Health"),
        ("Enterprise Software", "Software"),
        ("Streaming Platform", "Arts and Entertainment"),
        ("Transportation", "Automotive"),
        ("Pharmaceuticals", "Health"),
        ("3D Printing", "Manufacturing"),
        ("E-Commerce", "Commerce"),
        ("Multimedia and Graphics Software", "Software"),
        ("AgTech", "Agriculture and Farming"),
    ]

    for i in ind_tuples:
        if i[0] in ind_list and i[1] in ind_list:
            ind_list = [ind for ind in ind_list if ind != i[1]]
    return ";".join(ind_list)


def make_property_dict(df):

    df = df[~df["pf_inds"].isnull()]

    return [
        {
            "id": str(k),
            "properties": {
                # "name": str(df['Company name'].values[n]),
                "pf_inds": re.sub(r' ','_', str(df["pf_inds"].values[n])).lower(),
            },
        }
        for n, k in enumerate(df["Record ID"].values)
    ]


def rate_limit_dict(rate, dict_list):

    end = False

    for i, e in enumerate(dict_list):

        if end == True:
            batch_update_company(dict_list[i::])
            break
        else:
            if i // rate == i / rate:
                if len(dict_list) - (i) < rate:
                    end = True
                    print(f"Arrived at the end! Updating last batch.")
                    continue
                else:
                    print()
                    batch_update_company(dict_list[i : i + rate])
                    print(f"Just updated batches {i}-{i+rate} \n Sleeping shhhh...")
                    time.sleep(10)


def map_tags_to_investors():

    df = map_groups(
        explode_df(STARTPATH, "industries", ","), "pf_inds", make_ind_dict()
    )
    df = aggregate(
        df,
        {
            "pf_inds": lambda x: ("".join(str(s.strip()) for s in set(x))).strip(),
            "href": lambda x: list(set(x))[0],
            "name": lambda x: list(set(x))[0],
            "description": lambda x: list(set(x))[0],
            "ref": lambda x: list(set(x))[0],
        },
    )
    df = mental_health(df)
    df["pf_inds"] = df["pf_inds"].apply(lambda x: x.split(";"))
    df["pf_inds"] = df["pf_inds"].apply(lambda x: map_subindustries(x))
    df = pd.read_csv(START_HERF_PATH).merge(
        df, how="left", left_on="startup_href", right_on="href"
    )

    return df[~df["pf_inds"].isnull()]


def aggregate_industries():

    return (
        map_tags_to_investors()
        .groupby(["id"])
        .agg(
            {
                "pf_inds": lambda x: ";".join(set("".join(set(x)).split(";"))),
            }
        )
    )


if __name__ == "__main__":

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))
    df = pd.read_csv(VC_PATH).merge(
        aggregate_industries(), how="left", left_on="Record ID", right_index=True
    )
    ### Step 1 ###
    # If your property does not already include every value in the current dictionary, create a new one or update the current one. Keep the first object mapped to make_industry_options; will always be true.

    # industry_dict_list = make_industry_options(df)
    # create_company_property('pf_inds',
    #                         'Portfolio Industries',
    #                         'enumeration',
    #                         'checkbox',
    #                         'companyinformation',
    #                         industry_dict_list,
    #                         1,
    #                         False,
    #                         False,
    #                         True)

    ### Step 2 ###
    # Update the comapnies, but USE THE RATE LIMITER. If you don't the update will fail
    # IF YOU MAKE TOO MANY REQUESTS HUBSPOT WILL BE BIG MAD, SO BE CAREFUL.

    prop_dict_list = make_property_dict(df)
    rate_limit_dict(100, prop_dict_list)
