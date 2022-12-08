from write_deals import batch_update_deals
from read_deals import get_deals_by_pipeline, list_deal_associations
import pandas as pd
import os

DBDIR = "C:/Users/galon/Sputnik ATX Team Dropbox/Programming Datasets"
CLEAN_DIR = os.path.join(DBDIR, 'tables','clean')
REACHOUT_DIR = os.path.join(CLEAN_DIR, 'reachouts')


BATCH1_PATH = os.path.join(REACHOUT_DIR, 'saveday_batch1.csv')



def batch_move_deal_dict(inpath, stage):

    df = pd.read_csv(inpath)
    ids =  list(df['id'].values)

    return [{'id': str(id_), 'properties': {'dealstage': stage}} for id_ in ids]






if __name__ == "__main__":

    dict_list = get_deals_by_pipeline([], 'default')


    print('No excecutions written below main')







