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
VC_INDUSTRY_COLS = os.path.join(RAW_DIR, "hs", "vc_industry_columns.csv")


"""
Below is a reference for the internal property names in HubSpot:


Investment Focus:  inv_inds
Record ID: id
Company name: name
Investor Tags: inv_tags

"""


def make_industry_options(dictionary):

    values = [re.sub(r"\s", "_", v.lower()) for v in list(dictionary.keys())]
    labels = list(dictionary.keys())

    return [
        {"label": label, "value": values[n], "displayOrder": n, "hidden": False}
        for n, label in enumerate(labels)
    ]


def explode_df(path, old_col, split_col, pattern):

    """Takes the dataframe and explodes on the industry"""

    df = pd.read_csv(path).rename(columns={old_col: split_col})

    df = df[~df[split_col].isnull()]
    df[split_col] = df[split_col].apply(lambda x: re.split(pattern, str(x)))
    df = df.explode(split_col)
    df[split_col] = df[split_col].apply(lambda x: re.sub(r"&", "and", str(x)))
    df[split_col] = df[split_col].apply(lambda x: x.lower().strip())

    return df.reset_index(drop=True)


def map_groups(path, old_col, split_col, new_col, pattern, map_dict):

    """Takes the exploded dataframe and groups the industries more generally"""

    exploded_df = explode_df(path, old_col, split_col, pattern).set_index("Record ID")
    all_tags = set(exploded_df[split_col].values)
    merge_df = exploded_df.reset_index()
    exploded_df[new_col] = ""

    for key, value in map_dict.items():

        map_df = pd.DataFrame({key: value})
        map_df[key] = map_df[key].apply(lambda x: x.lower())
        matches = map_df.merge(
            merge_df, how="inner", right_on=split_col, left_on=key
        ).set_index("Record ID")
        exploded_df.loc[matches.index, new_col] += f";{key}"

    grouped = exploded_df.groupby("Record ID").agg(
        {new_col: lambda x: ("".join(str(s.strip()) for s in set(x))).strip()}
    )

    return pd.read_csv(path).merge(
        grouped[grouped[new_col].str.contains(";")],
        how="left",
        left_on="Record ID",
        right_index=True,
    )


def combine_tables(internal, new_col):

    d1, d2 = map_groups(
        VC_INDUSTRY_COLS, "Focus", "vis_ind", new_col, r"/|,", make_tag_dict()
    ).fillna(""), map_groups(
        VC_INDUSTRY_COLS,
        "Industry (Affinity)",
        "aff_ind",
        new_col,
        r"/|;",
        make_tag_dict(),
    ).fillna(
        ""
    )

    d1[new_col] = d1[new_col].astype(str) + d2[new_col].astype(str)
    d1[new_col] = d1[~d1[new_col].isnull()][new_col].apply(
        lambda x: "".join([f"{s.strip()};" for s in set(x.split(";")) if len(x) > 0])
    )

    return [
        {
            "id": str(k),
            "properties": {
                internal: re.sub(r"\s", "_", str(d1[new_col].values[n])).lower(),
                "name": str(d1["Company name"].values[n]),
            },
        }
        for n, k in enumerate(d1["Record ID"].values)
    ]


def rate_limit_dict(client, rate, dict_list):

    end = False

    for i, e in enumerate(dict_list):

        if end == True:
            batch_update_company(client, dict_list[i::])
            break
        else:
            if i // rate == i / rate:
                if len(dict_list) - (i) < rate:
                    end = True
                    print(f"Arrived at the end! Updating last batch.")
                    continue
                else:
                    print()
                    batch_update_company(client, dict_list[i : i + rate])
                    print(f"Just updated batches {i}-{i+rate} \n Sleeping shhhh...")
                    time.sleep(10)


if __name__ == "__main__":

    client = hubspot.Client.create(access_token=os.getenv("pm_token"))