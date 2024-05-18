from sqlalchemy import create_engine,inspect,sql
import pandas as pd
import glob
import os
import re

import warnings
warnings.simplefilter("ignore")

from function.functions_target_reckitt import FunctionTargetReckitt
from google.cloud import secretmanager
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './commercesense-prod-511d8f71bcea1.json'

client = secretmanager.SecretManagerServiceClient()
name = f"projects/674162212838/secrets/db_honeybee_warehouse/versions/latest"
response = client.access_secret_version(name=name)
secret_value = eval(response.payload.data.decode("UTF-8"))

DATABASE_URI = 'postgresql+psycopg2://{user}:{password}@{public_ip}/{db}'.format(**secret_value)
ENGINE = create_engine(DATABASE_URI,connect_args={"application_name":"Local Transformation"})
CONN = ENGINE.connect()


PATH = '/Applications/Folder/Work/seller center/Target Reckitt/target_august_reckitt.xlsx'

list_store_lixus = ['rajasusu','asiasusu','asiabestmart','satusama','satusamababycare','makmuronline','buchikids','jayaabadi','suzuya','chandra','remaja','rajasusu_bandung','rajasusu_bogor','susukuonline','pemulasusu']

# STORE_LIXUS = 'twocare'
YEAR = '2023'

def skip_row(file_name,sheet_name):
    df = pd.read_excel(file_name,header=None,sheet_name=sheet_name)
    idx = df[df.iloc[:,0] == 'category' ].index
    return idx[0]

for store_lixus in list_store_lixus:
    df_store_info = pd.read_excel('./list_store.xlsx')
    df_store_info = df_store_info[(df_store_info['shop_lixus'] == store_lixus)].reset_index(drop=True)
    print("Store lixus: {} ".format(store_lixus))
    df = pd.read_excel(PATH, sheet_name=store_lixus,skiprows=5,dtype=str)

    transform_function = FunctionTargetReckitt(df_store_info)
    df_transformed = transform_function.basicTransform(df)
    


    df_mapping_shop = transform_function.mappingShopDomain(df_transformed)


    ## Get data mapping from db
    db_mapping_platform, db_mapping_shop, db_mapping_shop_category = transform_function.getMappingTableFromDB(CONN,df_mapping_shop)

    ## Lookup table
    df_lookup_shop = transform_function.lookupShop(df_mapping_shop,db_mapping_shop)

    ## Insert snapshot traffic 
    df_target = df_lookup_shop[["date","brand_group","shop_id","gmv"]]
    transform_function.insertDataTargetFullyear(CONN,df_target)

    print("==========================")
    print("")

CONN.close()