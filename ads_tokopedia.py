from black import transform_line
from sqlalchemy import create_engine,inspect,sql
import pandas as pd
import glob
import os
import re
import datetime

from function.functions_ads_tokopedia import FunctionAdsTokopedia

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
    and platform = 2 
"""
PLATFORM = 'Tokopedia' ## Constant
PLATFORM_ID = 'TP' ## Constant

STORE_LIXUS = 'sunfresh'

START_DATE = '2023-02-01'
END_DATE = '2023-02-07'
DATA_PATH = 'Ads Tokopedia/Daily 2023/2-February'


df_list_store = pd.read_excel('./list_store.xlsx')
df_list_store = df_list_store[(df_list_store['shop_lixus'] == STORE_LIXUS) & (df_list_store['platform'] == PLATFORM)].reset_index(drop=True)

PATH = '/Applications/Folder/Work/seller center/{}/{}'.format(df_list_store.loc[0,'path'],DATA_PATH)
STORE_DOMAIN = df_list_store.loc[0,'shop_domain']
STORE_CATEGORY = df_list_store.loc[0,'shop_category']

print("Store domain: {}, Store Category: {}".format(STORE_DOMAIN,STORE_CATEGORY))

ALL_FILES = sorted(glob.glob(os.path.join(PATH, "*.xlsx")))

def skip_row(file_name):
    df = pd.read_excel(file_name,header=None)
    idx = df[df.iloc[:,0] == 'Tipe Iklan' ].index
    return idx[0]

for files in ALL_FILES:
    date_file0 = " ".join(files.split(os.sep)[-1].split('-')[0].split('_')[1:])
    date_file = datetime.datetime.strptime(date_file0,"%d %B %Y").strftime('%Y-%m-%d')
    if START_DATE <= date_file and END_DATE >= date_file:
        print(files)

        transform_function = FunctionAdsTokopedia(files, STORE_LIXUS,STORE_DOMAIN, STORE_CATEGORY, PLATFORM, PLATFORM_ID)
        df = pd.read_excel(files,sheet_name='Data',skiprows = skip_row(files))
        df_transformed = transform_function.basicTransform(df)
        df.to_excle('cek.xlsx')

        # db_mapping_brand, db_mapping_brand_category, db_mapping_platform, db_mapping_shop, db_mapping_shop_category, db_mapping_product = transform_function.getMappingTableFromDB(CONN,df_transformed)
        
        # ## Mapping product
        # df_mapping_product = transform_function.mappingProduct(df_transformed,db_mapping_brand, db_mapping_product)
        # df_removed_duplicates = transform_function.removeDuplicatesData(df_mapping_product,['id_product_lixus'])
        # transform_function.insertDataObject(CONN,df_removed_duplicates,"mapping_product","id_product_lixus",[],do_nothing=False)


        # ## Lookup table
        # df_lookup_platform = transform_function.lookupPlatform(df_transformed,db_mapping_platform)
        # df_lookup_shop = transform_function.lookupShop(df_lookup_platform,db_mapping_shop)
        # df_lookup_shop_category = transform_function.lookupShopCategory(df_lookup_shop,db_mapping_shop_category)
        # df_lookup_product = transform_function.lookupProduct(df_lookup_shop_category,df_removed_duplicates)
        # df_lookup_brand = transform_function.lookupBrand(df_lookup_product,db_mapping_brand)
        # df_lookup_brand_category = transform_function.lookupBrandCategory(df_lookup_brand,db_mapping_brand_category)
        # df_lookup = df_lookup_brand_category.copy()


        # ## Insert product_merchant_platform
        # df_product = df_lookup[['product_id','product_name','sku','predicted_brand','brand_category','shop_id']]
        # df_product_removed_duplicates = transform_function.removeDuplicatesData(df_product,['product_id'])
        # transform_function.insertDataObject(CONN,df_product_removed_duplicates,"product_merchant_platform","product_id",[],do_nothing=False)

        # ## Insert snapshot ads
        # df_ads = df_lookup[[]]
        # transform_function.insertSnapshotsAds(CONN,df_ads,date_file)
            
        # print("=================================================")
# CONN.close()