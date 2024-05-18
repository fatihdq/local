from sqlalchemy import create_engine,inspect,sql
import pandas as pd
import glob
import os
import re

from function.functions_order_tokopedia import FunctionOrderTokopedia

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
PLATFORM = 'Tokopedia' ## Constant
PLATFORM_ID = 'TP' ## Constant

LIST_STORE = ['rajasusu','otoystore/lynton','tokomilkymart','tokoanugerah','bajipamai','makmuronline','suzuya','tokocleacleo','levinmart','kimmiebabyshop','kapalperang','twocare','asiasusu','asiabestmart','jayaabadi','foodieshop','primasusu','istanasusu']

START_DATE = '2024-03-01'
END_DATE = '2024-03-31'

DATA_PATH = 'Sales Order Tokopedia/Monthly 2024'

for STORE_LIXUS in LIST_STORE:

    df_list_store = pd.read_excel('./list_store.xlsx')
    df_list_store = df_list_store[(df_list_store['shop_lixus'] == STORE_LIXUS) & (df_list_store['platform'] == PLATFORM)].reset_index(drop=True)


    PATH = 'D:/dhiqi/{}/{}'.format(df_list_store.loc[0,'path'],DATA_PATH)
    STORE_DOMAIN = df_list_store.loc[0,'shop_domain']
    STORE_CATEGORY = df_list_store.loc[0,'shop_category']

    print("Store domain: {}, Store Category: {}".format(STORE_DOMAIN,STORE_CATEGORY))
    ALL_FILES = sorted(glob.glob(os.path.join(PATH, "*.xlsx")))

    def skip_row(file_name):
        df = pd.read_excel(file_name,header=None)
        idx = df[df.iloc[:,0] == 'Nomor'].index
        if len(idx) == 0:
            idx = df[df.iloc[:,0] == 'Count'].index
        if len(idx) == 0:
            idx = df[df.iloc[:,0] == 'Order ID'].index
        if len(idx) == 0:
            idx = df[df.iloc[:,0] == 'Nomor Invoice'].index
        return idx[0]

    for files in ALL_FILES:
        date_file0 = re.findall(r'\d+',files.split(os.sep)[-1])
        date_file = date_file0[1][0:4]+'-'+date_file0[1][4:6]+'-'+date_file0[1][6:8]
        if START_DATE <= date_file and END_DATE >= date_file:
            print(files)

            transform_function = FunctionOrderTokopedia(files, STORE_LIXUS,STORE_DOMAIN, STORE_CATEGORY, PLATFORM, PLATFORM_ID)
            
            ## Read Files
            df = pd.read_excel(files,skiprows = skip_row(files))

            
            if len(df) != 0:
                ## Basic Transform
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
                df_lookup_brand = transform_function.lookupBrand(df_lookup_product,db_mapping_brand)
                df_lookup_brand_category = transform_function.lookupBrandCategory(df_lookup_brand,db_mapping_brand_category)
                df_lookup = transform_function.getId(df_lookup_brand_category)


                ## Insert product_merchant_platform
                df_product = df_lookup[['product_id','product_name','sku','predicted_brand','brand_category','segment','shop_id']]
                df_product_removed_duplicates = transform_function.removeDuplicatesData(df_product,['product_id'])
                transform_function.insertDataObject(CONN,df_product_removed_duplicates,"product_merchant_platform","product_id",[],do_nothing=False)

                ## Insert Shipping
                df_shipping = df_lookup[['no_reciept','shipping_option','shipping_cost','discount_shipping','shipping_cost_estimation','recipient_name','insurance']]
                df_shipping_removed_duplicates = transform_function.removeDuplicatesData(df_shipping,['no_reciept'])
                transform_function.insertDataObject(CONN,df_shipping_removed_duplicates,"shipping",'no_reciept',[],do_nothing=False)

                ## Insert customers
                df_customer = df_lookup[['customer_id','username','phone','address','city','province','platform']]
                df_customer_removed_duplicates = transform_function.removeDuplicatesData(df_customer,['customer_id'])
                transform_function.insertDataObject(CONN,df_customer_removed_duplicates,"customers",'customer_id',[],do_nothing=False)
                

                ## Insert Order
                df_order = df_lookup[['order_id','created_datetime','delivery_datetime','payment_datetime','complete_datetime','order_status','order_quantity','total_weight','voucher_seller','voucher_platform','cashback_coin','discount_package','discount_package_platform','discount_package_seller','discount_coin','discount_credit_card','total_payment','bundling_package','note','no_reciept','customer_id','shop_id','platform']]
                df_order_removed_duplicates = transform_function.removeDuplicatesData(df_order,['order_id'])
                transform_function.insertDataObject(CONN,df_order_removed_duplicates,"orders","order_id",[],do_nothing=False)


                ## Insert snapshot order product
                df_order_product = df_lookup[['created_datetime','initial_price','price_after_discount','quantity','selling_price','total_price','total_discount','discount_from_seller','discount_from_platform','total_price_after_discount_transaction','bundling_unit_price','weight','order_id','product_id','shop_id']]
                transform_function.insertSnapshotsTraffic(CONN,df_order_product,date_file)

            print("=================================================")
            print("")
CONN.close()