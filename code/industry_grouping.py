import os, time, hubspot
import regex as re
import pandas as pd
from dotenv import load_dotenv
from map_industries import make_ind_dict, make_tag_dict
from write_properties import create_property
from write_companies import update_company, batch_update_company

load_dotenv()


# DIRCCTORIES
DBDIR = "C:/Users/galon/Sputnik ATX Team Dropbox/Programming Datasets"
RAW_DIR = os.path.join(DBDIR, "tables", "raw")
CLEAN_DIR = os.path.join(DBDIR, "tables", "clean")
IND_DIR = os.path.join(CLEAN_DIR, "industry_mapping")


# INPATHS
START_HERF_PATH = os.path.join(
    CLEAN_DIR, "scraped_data", "crunchbase", "cb_inv_overview_scraped.csv"
)
VC_PATH = os.path.join(RAW_DIR, "hs", "vc_list_export.csv")
VC_INDUSTRY_COLS = os.path.join(RAW_DIR, "hs", "vc_industry_columns.csv")
STARTPATH = os.path.join(
    CLEAN_DIR, "scraped_data", "crunchbase", "cb_starts_main_scraped.csv"
)
TX_ANGEL_HS_INPATH = os.path.join(RAW_DIR, 'hs', 'tx_angel_export.csv')
TX_ANGEL_RAW_INPATH = os.path.join(RAW_DIR, 'misc_source', 'tx_angel_contacts.csv')


# OUTPATHS
MAPPING_OUTPATH = os.path.join(IND_DIR, "cb_starts_mapped.csv")



"""
#### Below is a reference for the internal property names in HubSpot ####

Portfolio Industries: pf_inds
Portfolio Focus: top5_inds
Portfolio Main Interest: top1_inds
Investing Tags: pf_tags
Top 5 Tags: top5_tags
Top Tag: top1_tags
Record ID: id
Company name: name
Investor Tags: inv_tags

###########################################################################
"""


def counter(item_list):
    
    count_dict = {}
    for item in item_list:
        if item in count_dict:
            count_dict[item] += 1
        else:
            count_dict[item] = 1
    return count_dict

def rank_top5(count_dict, column):
    
    data = {column: list(count_dict.keys()), 'count': list(count_dict.values())}
    df = pd.DataFrame.from_dict(data)
    df.sort_values(['count'], ascending=False,inplace=True)
    top5 = df[column].values[0:5]

    
    return ''.join([f';{t}' for t in top5])

def rank_top1(count_dict, column):

    data = {column: list(count_dict.keys()), 'count': list(count_dict.values())}
    df = pd.DataFrame.from_dict(data)
    df.sort_values(['count'], ascending=False,inplace=True)
    if len(df) > 0:

        return df[column].values[0]


def explode_df(csv_inpath, industry_column, split_on):

    """Takes the dataframe and explodes on the industry"""

    df = pd.read_csv(csv_inpath).rename(columns={industry_column: "industries"})
    df["ref"] = df["industries"]
    df = df[~df["industries"].isnull()]
    df["industries"] = df["industries"].apply(lambda x: re.split(split_on, str(x)))
    df = df.explode("industries")
    df["industries"] = df["industries"].apply(lambda x: re.sub(r"&", "and", str(x)))
    df["industries"] = df["industries"].apply(lambda x: x.lower().strip())

    return df


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


def make_property_dict(df, column):

    df = df[~df[column].isnull()]
    print(df)

    return [
        {
            "id": str(k),
            "properties": {
                column: re.sub(r' ','_', str(df[column].values[n])).lower(),
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


def startup_ind_main():

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


def startup_tag_main():

    df = map_groups(
        explode_df(STARTPATH, "industries", ","), "pf_tags", make_tag_dict()
    )
    df = aggregate(
        df,
        {
            "pf_tags": lambda x: ("".join(str(s.strip()) for s in set(x))).strip(),
            "href": lambda x: list(set(x))[0],
            "name": lambda x: list(set(x))[0],
            "description": lambda x: list(set(x))[0],
            "ref": lambda x: list(set(x))[0],
        },
    )

    df["pf_tags"] = df["pf_tags"].apply(lambda x: x.split(";"))
    df = pd.read_csv(START_HERF_PATH).merge(
        df, how="left", left_on="startup_href", right_on="href"
    )

    return df[~df["pf_tags"].isnull()]

def tx_angel_ind_main():

    df = map_groups(explode_df(TX_ANGEL_RAW_INPATH, "Preferred Industry", ","), "tx_angel_inds", make_ind_dict())
    df = aggregate(
        df,
        {
            "tx_angel_inds": lambda x: ("".join(str(s.strip()) for s in set(x))).strip(),
            "Description": lambda x: list(set(x))[0],
            "Investor Name": lambda x: list(set(x))[0],
        },
    )
    df["tx_angel_inds"] = df["tx_angel_inds"].apply(lambda x: x.split(";"))
    df["tx_angel_inds"] = df["tx_angel_inds"].apply(lambda x: map_subindustries(x))
    hs_df = pd.read_csv(TX_ANGEL_HS_INPATH)
    hs_df['name'] = hs_df['First Name'] + ' ' + hs_df['Last Name']
    df = df.merge(hs_df, how='right', left_on=['Investor Name', 'Description'],right_on=['name', 'About'])
    df['tx_angel_inds'] = df['tx_angel_inds'].astype('str')

    return df[df['tx_angel_inds'].str.contains(';')]


def tx_angel_tag_main():

    df = map_groups(explode_df(TX_ANGEL_RAW_INPATH, "Preferred Industry", ","), "tx_angel_tags", make_tag_dict())
    df = aggregate(
        df,
        {
            "tx_angel_tags": lambda x: ("".join(str(s.strip()) for s in set(x))).strip(),
            "Description": lambda x: list(set(x))[0],
            "Investor Name": lambda x: list(set(x))[0],
        },
    )
    df["tx_angel_tags"] = df["tx_angel_tags"].apply(lambda x: x.split(";"))
    df["tx_angel_tags"] = df["tx_angel_tags"].apply(lambda x: map_subindustries(x))
    hs_df = pd.read_csv(TX_ANGEL_HS_INPATH)
    hs_df['name'] = hs_df['First Name'] + ' ' + hs_df['Last Name']
    df = df.merge(hs_df, how='right', left_on=['Investor Name', 'Description'],right_on=['name', 'About'])
    df['tx_angel_tags'] = df['tx_angel_tags'].astype('str')

    return df[df['tx_angel_tags'].str.contains(';')]


def get_top1_industries(df, internal_label):

    df['counts'] = df[internal_label].apply(lambda x: counter(x))
    df['top1_inds'] = df['counts'].apply(lambda x: rank_top1(x, 'industry'))
    df['Record ID'] = df.index

    return df


def get_top5_industries(df, internal_label):
    
    df['counts'] = df[internal_label].apply(lambda x: counter(x))
    df['top5_inds'] = df['counts'].apply(lambda x: rank_top5(x, 'industry'))
    df['Record ID'] = df.index
    
    return df

def get_top1_tags(df, internal_label):

    df['counts'] = df[internal_label].apply(lambda x: counter(x))
    df['top1_tags'] = df['counts'].apply(lambda x: rank_top1(x, 'tag'))
    df['Record ID'] = df.index

    return df

def get_top5_tags(df, internal_label):
    
    df['counts'] = df[internal_label].apply(lambda x: counter(x))
    df['top5_tags'] = df['counts'].apply(lambda x: rank_top5(x, 'tag'))
    df['Record ID'] = df.index
    
    return df


def make_options_dataframe(mapping_function, name, groupby):

    """ mapping_function: this should be a function with 'main' in the method's name

    name: this is a column of the mapped industry, which will also be the internal hubspot name

    groupby: this is the record id column, which was different across projects and why it's included as an arugment

    """

    df = (
        mapping_function
        .groupby([groupby])
        .agg(
            {
                name: lambda x: ";".join(set("".join(set(x)).split(";"))),
            }
        )
    )
    df['Record ID'] = df.index
    return df

def make_options_dict(df, column):

    df = df[~df[column].isnull()]
    tags = "".join(list(df[column].values))
    labels = list(set(tags.split(";")))
    labels.pop(0)
    values = [re.sub(r" ", "_", label).lower() for label in labels]

    return [
        {"label": label, "value": values[n], "displayOrder": n, "hidden": False}
        for n, label in enumerate(labels)
    ]


if __name__ == "__main__":

    ### STEP 1 ###
    # If your property does not already include every value in the current dictionary, create a new one or update the current one. Keep the first object mapped to make_industry_options; will always be true.
    """

                            ################## Example ######################


                            df = make_options_dataframe(startup_ind_main(), 'pf_inds', 'id')
                            options_dict_list = make_options_dict(df, 'pf_inds')
                            create_property('pf_inds',
                                            'Portfolio Industries',
                                            'enumeration',
                                            'checkbox',
                                            'companyinformation',
                                            options_dict_list,
                                            1,
                                            False,
                                            False,
                                            True,
                                            'company')

                            #################################################
    """


    # df = make_options_dataframe(tx_angel_tag_main(), 'tx_angel_tags', 'Record ID - Contact')
    # options_dict_list = make_industry_options(df, 'tx_angel_tags')
    # create_property(
    #             'tx_angel_inds',
    #             'Preferred Industries (TX Angels)',
    #             'enumeration',
    #             'checkbox',
    #             'contactinformation',
    #             industry_dict_list,
    #             1,
    #             False,
    #             False,
    #             True,
    #             'contact'

    #     )

    ### Step 2 ###
    # Update the comapnies, but USE THE RATE LIMITER. If you don't the update will fail
    # IF YOU MAKE TOO MANY REQUESTS HUBSPOT WILL BE BIG MAD, SO BE CAREFUL.
    """

                            #################### Example ####################

                                df = startup_ind_main().groupby(['id']).agg({'pf_inds': lambda x: [s for s in (''.join(x).split(';')) if len(s) > 0]})

                                df_list = [industry_options_dataframe(startup_ind_main(), 'pf_inds', 'id'), get_top5_industries(df, 'pf_inds'), get_top1_industries(df, 'pf_inds')]
                                col_list = ['pf_inds', 'top5_inds', 'top1_inds']

                                for i, df in enumerate(df_list):

                                    prop_dict_list = make_property_dict(df, col_list[i])

                                    print(prop_dict_list)
                                    rate_limit_dict(100, prop_dict_list)

                            #################################################
    """

    

