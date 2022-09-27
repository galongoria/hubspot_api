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
STARTPATH = os.path.join(CLEAN_DIR, 'scraped_data', 'crunchbase', 'cb_starts_main_scraped.csv')
MAPPING_OUTPATH = os.path.join(IND_DIR, "cb_starts_mapped.csv")

"""
Below is a reference for the internal property names in HubSpot:


Investment Focus: inv_inds
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


def explode_df(csv_inpath, industry_column, split_on):

    """Takes the dataframe and explodes on the industry"""
    
    df = pd.read_csv(csv_inpath).rename(columns={industry_column: 'industries'})
    df['ref'] = df['industries']
    df = df[~df['industries'].isnull()]
    df['industries'] = df['industries'].apply(lambda x: re.split(split_on, str(x)))
    df = df.explode('industries')
    df['industries'] = df['industries'].apply(lambda x: re.sub(r"&", "and", str(x)))
    df['industries'] = df['industries'].apply(lambda x: x.lower().strip())

    return df.drop('Unnamed: 0', axis=1)


def map_groups(exploded_df, new_col, map_dict):

    """Takes the exploded dataframe and groups the industries more generally"""
    
    merge_df = exploded_df.copy(deep=True)
    merge_df[new_col] = ""
    exploded_df.reset_index(inplace=True)
    
    for key, value in map_dict.items():

        map_df = pd.DataFrame({key: value})
        map_df[key] = map_df[key].apply(lambda x: x.lower())
        matches = exploded_df.merge(map_df, how="left", left_on='industries', right_on=key)
        merge_df.loc[matches[~matches[key].isnull()]['index'].values, new_col] += f";{key}"
    return merge_df


def aggregate(df, dictionary):
    
    
    return df.groupby(df.index).agg(dictionary)


def mental_health(df):
    
    df.loc[df['description'].str.contains('mental health'), 'inv_inds'] += ';Mental Health'

    return df

def map_subindustries(ind_list):
    
    ind_tuples = [
        ("Mental Health", "Health"), 
        ("Payment Card Industry (PCI)", "FinTech"), 
        ("FinTech","Financial Services"),
        ("InsurTech", "Insurance"),
        ("HealthTech", "Health"),
        ("Enterprise Software", "Software"),
        ("Streaming Platform", "Arts and Entertainment"),
        ("Transportation", "Automotive"),
        ("Pharmaceuticals", "Health"),
        ("3D Printing", "Manufacturing"),
        ("E-Commerce", "Commerce"),
        ("Multimedia and Graphics Software","Software"),
        ("AgTech", "Agriculture and Farming"),
        ]
    
    for i in ind_tuples:
        if i[0] in ind_list and i[1] in ind_list:
            ind_list = [ind for ind in ind_list if ind != i[1]]
    return ";".join(ind_list)


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

def main():

    df = map_groups(explode_df(STARTPATH, 'industries', ','), 'inv_inds', make_ind_dict())
    df = aggregate(df, {
            'inv_inds': lambda x: ("".join(str(s.strip()) for s in set(x))).strip(), 
            'href': lambda x: ''.join([s for s in set(x)]),
            'name': lambda x: ''.join([s for s in set(x)]),
            'description': lambda x: ''.join([s for s in set(x)]),
            'ref': lambda x: ''.join([s for s in set(x)]),
    })
    df = mental_health(df)
    df['inv_inds'] = df['inv_inds'].apply(lambda x: x.split(';'))
    df['inv_inds'] = df['inv_inds'].apply(lambda x: map_subindustries(x))
    return df




if __name__ == "__main__":

    main().to_csv(MAPPING_OUTPATH)

    # client = hubspot.Client.create(access_token=os.getenv("pm_token"))