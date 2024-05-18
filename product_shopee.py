from sqlalchemy import create_engine,inspect,sql
import pandas as pd
import glob
import os
import re
import datetime
from xlrd import open_workbook  

from function.functions_product_shopee import FunctionProductShopee

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


LIST_STORE = ['rajasusu','makmuronline','suzuya','rajasusu_bandung','kapalperang','twocare','asiasusu','asiabestmart','satusama','satusamababycare','jayaabadi','bebimart','primasusu','buchikids','istanasusu','bajipamai']


START_DATE = '2023-08-16'
END_DATE = '2023-08-16'

DATA_PATH = 'ATP Shopee/Daily 2023/8-August'

for STORE_LIXUS in LIST_STORE:

    df_list_store = pd.read_excel('./list_store.xlsx')
    df_list_store = df_list_store[(df_list_store['shop_lixus'] == STORE_LIXUS) & (df_list_store['platform'] == PLATFORM)].reset_index(drop=True)

    PATH = '/Applications/Folder/Work/seller center/{}/{}'.format(df_list_store.loc[0,'path'],DATA_PATH)
    STORE_DOMAIN = df_list_store.loc[0,'shop_domain']
    STORE_CATEGORY = df_list_store.loc[0,'shop_category']

    print("Store domain: {}, Store Category: {}".format(STORE_DOMAIN,STORE_CATEGORY))
    ALL_FILES = sorted(glob.glob(os.path.join(PATH, "*.xlsx")))


    def getDateFromFilename(file):
        date_str = file.split('_')[-1][0:8]
        date = datetime.datetime.strptime(date_str,'%Y%m%d').strftime('%Y-%m-%d')
        return date

    def skip_row(file_name):
        df = pd.read_excel(file_name,header=None)
        idx = df[df.iloc[:,0] == 'Kode Produk' ].index
        return idx[0]

    # path = "/Applications/Folder/Work/seller center/MOM & BABY/1. Raja Susu/ATP Shopee/Daily 2023/6-June/atp_rajasusu_20230605.xlsx"
    for files in ALL_FILES:
        filename = files.split(os.sep)[-1]
        date_file = getDateFromFilename(filename)
        if START_DATE <= date_file and END_DATE >= date_file:
            print()
            print(files)

            transform_function = FunctionProductShopee(files, STORE_LIXUS,STORE_DOMAIN, STORE_CATEGORY, PLATFORM, PLATFORM_ID)
            df = pd.read_excel(files,dtype=str,skiprows=skip_row(files))
            if len(df) != 0:
                df_transformed = transform_function.basicTransform(df)

                db_mapping_brand, db_mapping_brand_category, db_mapping_platform, db_mapping_shop, db_mapping_shop_category, db_mapping_product = transform_function.getMappingTableFromDB(CONN,df_transformed)

                ## Mapping product
                df_mapping_product = transform_function.mappingProduct(df_transformed,db_mapping_brand, db_mapping_product)
                df_removed_duplicates = transform_function.removeDuplicatesData(df_mapping_product,['id_product_lixus'])
                transform_function.insertDataObject(CONN,df_removed_duplicates,"mapping_product","id_product_lixus",[],do_nothing=False)

                ## Lookup table
                df_lookup_platform = transform_function.lookupPlatform(df_transformed,db_mapping_platform)
                df_lookup_shop = transform_function.lookupShop(df_lookup_platform,db_mapping_shop)
                df_lookup_shop_category = transform_function.lookupShopCategory(df_lookup_shop,db_mapping_shop_category)
                df_lookup_product = transform_function.lookupProduct(df_lookup_shop_category,df_removed_duplicates)
                df_lookup_pci= transform_function.lookupPci(df_lookup_product)
                df_lookup_brand = transform_function.lookupBrand(df_lookup_pci,db_mapping_brand)
                df_lookup_brand_category = transform_function.lookupBrandCategory(df_lookup_brand,db_mapping_brand_category)
                df_lookup = df_lookup_brand_category.copy()
            

                ## Insert product_merchant_platform
                df_product = df_lookup[['product_id','product_name','sku','predicted_brand','brand_category','segment','official_name','pci','shop_id']]
                df_product_removed_duplicates = transform_function.removeDuplicatesData(df_product,['product_id'])
                transform_function.insertDataObject(CONN,df_product_removed_duplicates,"product_merchant_platform","product_id",[],do_nothing=False)

                ## Insert product_variation
                df_product_variation = df_lookup[['variation_id','variation_name','product_id']]
                df_product_variation_rd = transform_function.removeDuplicatesData(df_product_variation,'variation_id')
                df_product_variation_rn = transform_function.removeNullValue(df_product_variation_rd,'variation_id')
                if len(df_product_variation_rn) != 0 :
                        transform_function.insertDataObject(CONN,df_product_variation_rn,"product_variation","variation_id",[],do_nothing=False)

                ## Insert snapshot traffic 
                df_price_and_stock = df_lookup[['date','price','stock','product_id','variation_id','shop_id']]
                transform_function.insertSnapshotsPriceAndStock(CONN,df_price_and_stock,date_file)
                    
                print("=================================================")
                print("")

CONN.close()