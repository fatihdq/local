from sqlalchemy import create_engine,inspect,sql
import pandas as pd
import glob
import os
import re

from function.functions_customer_insight_tokopedia import FunctionCustomerInsightTokopedia

import warnings
warnings.simplefilter("ignore")
from google.cloud import secretmanager

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './commercesense-prod-511d8f71bcea1.json'

client = secretmanager.SecretManagerServiceClient()
name = f"projects/674162212838/secrets/db_honeybee_warehouse/versions/latest"
response = client.access_secret_version(name=name)
secret_value = eval(response.payload.data.decode("UTF-8"))

DATABASE_URI = 'postgresql+psycopg2://{user}:{password}@{public_ip}/{db}'.format(**secret_value)
ENGINE = create_engine(DATABASE_URI,connect_args={"application_name":"Local Transformation"})
CONN = ENGINE.connect()

PLATFORM = 'Tokopedia' ## Constant
PLATFORM_ID = 'TP' ## Constant

STORE_LIXUS = 'sunfresh'
DATA_PATH = 'Customer Tokopedia/customer_insight.xlsx'


df_list_store = pd.read_excel('./list_store.xlsx')
df_list_store = df_list_store[(df_list_store['shop_lixus'] == STORE_LIXUS) & (df_list_store['platform'] == PLATFORM)].reset_index(drop=True)

PATH = '/Applications/Folder/Work/seller center/{}/{}'.format(df_list_store.loc[0,'path'],DATA_PATH)
STORE_DOMAIN = df_list_store.loc[0,'shop_domain']
STORE_CATEGORY = df_list_store.loc[0,'shop_category']

print("Store domain: {}, Store Category: {}".format(STORE_DOMAIN,STORE_CATEGORY))

files = PATH
print(files)

transform_function = FunctionCustomerInsightTokopedia(files, STORE_LIXUS,STORE_DOMAIN, STORE_CATEGORY, PLATFORM, PLATFORM_ID)
df = pd.read_excel(files)
if len(df) != 0:
    df_transformed = transform_function.basicTransform(df)
    
    db_mapping_platform,db_mapping_shop = transform_function.getMappingTableFromDB(CONN,df_transformed)
            
    ## Lookup table
    df_lookup_platform = transform_function.lookupPlatform(df_transformed,db_mapping_platform)
    df_lookup_shop = transform_function.lookupShop(df_lookup_platform,db_mapping_shop)
    df_lookup = df_lookup_shop

    ## Insert snapshot  
    df_snapshot = df_lookup[['date','buyer','buyer_man','buyer_woman','buyer_not_mentioned','new_buyer','reguler_buyer','loyal_buyer','age_17','age_18_23','age_24_34','age_35_44','age_45_','followers','shop_id']]
    transform_function.insertSnapshots(CONN,df_snapshot,)
                
    print("=================================================")
    print("")
CONN.close()