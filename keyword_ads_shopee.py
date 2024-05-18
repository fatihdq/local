from sqlalchemy import create_engine,inspect,sql
import pandas as pd
import glob
import os
import re

from function.functions_keyword_ads_shopee import FunctionKeywordAdsShopee

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

""" 
    PLATFORM ID: 
        SHOPEE = SP
        TOKOPEDIA = TP
        LAZADA = LZ
        BLIBLI = BL 
"""

"""
    PATH MUST BE IN FOLDER ../{category store}/{store name}/{Data type}/{Daily or monthly by year}/{if daily then month name}
"""


"""
    select shop_domain, shop_lixus,platform , shop_category from shop s 
    where shop_lixus notnull 
    and shop_category = 1
    and platform = 1
"""

PLATFORM = 'Shopee' ## Constant
PLATFORM_ID = 'SP' ## Constant

STORE_LIXUS = 'sunfresh'

START_DATE = '2023-02-26'
END_DATE = '2023-02-27'
DATA_PATH = 'Ads Shopee/Daily 2023/2-February'


df_list_store = pd.read_excel('./list_store.xlsx')
df_list_store = df_list_store[(df_list_store['shop_lixus'] == STORE_LIXUS) & (df_list_store['platform'] == PLATFORM)].reset_index(drop=True)

PATH = '/Applications/Folder/Work/seller center/{}/{}'.format(df_list_store.loc[0,'path'],DATA_PATH)
STORE_DOMAIN = df_list_store.loc[0,'shop_domain']
STORE_CATEGORY = df_list_store.loc[0,'shop_category']

print("Store domain: {}, Store Category: {}".format(STORE_DOMAIN,STORE_CATEGORY))
ALL_FILES = sorted(glob.glob(os.path.join(PATH, "*.csv")))

for files in ALL_FILES:
    date_file0 = re.findall(r'\d+',files.split(os.sep)[-1])
    date_file = date_file0[5]+'-'+date_file0[4]+'-'+date_file0[3]

    if START_DATE <= date_file and END_DATE >= date_file:
        print(files)

        transform_function = FunctionKeywordAdsShopee(files, STORE_LIXUS,STORE_DOMAIN, STORE_CATEGORY, PLATFORM, PLATFORM_ID)
        df = pd.read_csv(files,dtype=str,skiprows=6)
        df_transformed = transform_function.basicTransform(df)
        
        db_mapping_platform, db_mapping_shop, db_mapping_shop_category = transform_function.getMappingTableFromDB(CONN,df_transformed)
        
        ## Lookup table
        df_lookup_platform = transform_function.lookupPlatform(df_transformed,db_mapping_platform)
        df_lookup_shop = transform_function.lookupShop(df_lookup_platform,db_mapping_shop)
        df_lookup = df_lookup_shop.copy()
        df_lookup.to_excel('cek.xlsx')

        ## Insert snapshot keyword ads
        df_ads = df_lookup[['date','status','ads_type','start_date','end_date','keyword','view','click','click_percentation','convertion','direct_convertion','percent_convertion','percent_direct_convertion','spending_per_convertion','spending_per_direct_convertion','effectiveness','direct_effectiveness','sold','direct_sold','gmv','direct_gmv','spending','cir','direct_cir','product_name','shop_id']]
        transform_function.insertSnapshotsAds(CONN,df_ads,date_file)
            
        print("=================================================")
CONN.close()