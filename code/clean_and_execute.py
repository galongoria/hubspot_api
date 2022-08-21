import os, hubspot
import regex as re
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

if __name__ == "__main__":

    client = hubspot.Client.create(access_token="YOUR_ACCESS_TOKEN")

