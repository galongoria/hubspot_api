import os
import hubspot
import regex as re
import pandas as pd
from dotenv import load_dotenv
from map_industries import make_ind_dict
from write_properties import create_company_property
from write_companies import update_company

load_dotenv()

DBDIR = "C:/Users/galon/Sputnik ATX Team Dropbox/Programming Datasets"
RAW_DIR = os.path.join(DBDIR, "data", "raw")
CLEAN_DIR = os.path.join(DBDIR, "data", "clean")
VC_INDUSTRY_COLS = os.path.join(RAW_DIR, "hs", "vc_industry_columns.csv")


"""

Below is a reference for the internal property names in HubSpot:


Industry Focus:  inv_inds

Record ID: id

Company name: name


"""


def combine_dict(d1, d2):
	return {
		k: tuple(d[k] for d in (d1, d2) if k in d)
		for k in set(d1.keys()) | set(d2.keys())
	}


def make_industry_options():

	values = [re.sub(r"\s", "_", v.lower()) for v in list(make_ind_dict().keys())]
	labels = list(make_ind_dict().keys())

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


def map_groups(path, old_col, split_col, new_col, pattern):

	"""Takes the exploded dataframe and groups the industries more generally"""

	exploded_df = explode_df(path, old_col, split_col, pattern).set_index("Record ID")
	all_tags = set(exploded_df[split_col].values)
	merge_df = exploded_df.reset_index()
	map_dict = make_ind_dict()
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


def make_aff_visible_dictionary():

	d1, d2 = map_groups(VC_INDUSTRY_COLS, "Focus", "vis_ind", "hs_ind", r"/|,").fillna(
		""
	), map_groups(
		VC_INDUSTRY_COLS, "Industry (Affinity)", "aff_ind", "hs_ind", r"/|;"
	).fillna(
		""
	)

	d1["hs_ind"] = d1["hs_ind"].astype(str) + d2["hs_ind"].astype(str)
	d1["hs_ind"] = d1[~d1["hs_ind"].isnull()]["hs_ind"].apply(
		lambda x: "".join([f"{s.strip()};" for s in set(x.split(";")) if len(x)>0])
	)

	return [
		{
			"id": str(k),
			"properties": {
				"inv_inds": re.sub(r'\s', '_', str(d1["hs_ind"].values[n])).lower(),
				"name": str(d1["Company name"].values[n]),
			},
		}
		for n, k in enumerate(d1["Record ID"].values)
	]




def main():

	client = hubspot.Client.create(access_token=os.getenv("pm_token"))

	dict_list = make_aff_visible_dictionary()
	print(dict_list)

	# for dictionary in dict_list:

	# 	if len(dictionary['properties']['inv_inds']) == 0:
	# 		continue
	# 	else:
	# 		update_company(client, dictionary['id'], dictionary['properties'])




if __name__ == "__main__":


	# create_company_property(
	# 	client,
	# 	"inv_inds",
	# 	"Investment Focus",
	# 	"enumeration",
	# 	"checkbox",
	# 	"companyinformation",
	# 	make_industry_options(),
	# 	2,
	# 	False,
	# 	False,
	# 	True,
	# )

	main()




	

