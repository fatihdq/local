from sqlalchemy import create_engine
import pandas as pd
import glob
import os
import datetime

from function.functions_crawl_store_blibli import FunctionsCrawlStoreBlibli
from google.cloud import secretmanager

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './commercesense-prod-511d8f71bcea1.json'

client = secretmanager.SecretManagerServiceClient()
name = f"projects/674162212838/secrets/db_honeybee_warehouse/versions/latest"
response = client.access_secret_version(name=name)
secret_value = eval(response.payload.data.decode("UTF-8"))

DATABASE_URI = 'postgresql+psycopg2://{user}:{password}@{public_ip}/{db}'.format(**secret_value)
ENGINE = create_engine(DATABASE_URI,connect_args={"application_name":"Local Transformation"})
CONN = ENGINE.connect()

START_TIME = datetime.datetime.now()
PLATFORM = 'Blibli'

if PLATFORM == 'Tokopedia':
    PLATFORM_ID = 'TP'
elif PLATFORM == 'Shopee':
    PLATFORM_ID = 'SP'
elif PLATFORM == 'Blibli':
    PLATFORM_ID = 'BL'
elif PLATFORM == 'LAZADA':
    PLATFORM_ID = 'LZ'


DATA_PATH = '/Applications/Folder/Work/migration_database'
PATH = '{}/blibli/2023-05-09'.format(DATA_PATH)


ALL_FILES = sorted(glob.glob(os.path.join(PATH, "*.csv")))

if __name__ == '__main__':
    for files in ALL_FILES:
        print(files)

        transform = FunctionsCrawlStoreBlibli(files,PLATFORM,PLATFORM_ID)

        df = pd.read_csv(files)
        ## Transform
        df_transformed = transform.basicTransform(df)
        
        # ## Mapping Category
        # df_mapping_category = transform.mappingCategory(df_transformed)
        # df_removed_duplicates_category = transform.removeDuplicatesData(df_mapping_category,['id_cat_lixus'])
        # df_removed_null_category = transform.removeNullValue(df_removed_duplicates_category,'id_cat_origin')
        # transform.insertDataObject(CONN,df=df_removed_null_category,table="mapping_category",primary_key="id_cat_lixus",except_col=[],do_nothing=False)

        # Mapping brand
        df_mapping_brand = transform.mappingBrand(df_transformed)
        df_removed_duplicates_brand = transform.removeDuplicatesData(df_mapping_brand,['predict_name'])
        df_removed_null_brand = transform.removeNullValue(df_removed_duplicates_brand,'predict_name')
        df_removed_null_brand = df_removed_null_brand[df_removed_null_brand["brand"].apply(lambda x: len(x) > 3)].reset_index(drop=True)
        if len(df_removed_null_brand) != 0:
            transform.insertDataObject(CONN,df=df_removed_null_brand,table="mapping_brand",primary_key="predict_name",except_col=[],do_nothing=True)

        ## Mapping Shop
        df_mapping_shop = transform.mappingShop(df_transformed)
        df_removed_duplicates_shop = transform.removeDuplicatesData(df_mapping_shop,['id_shop_lixus'])
        transform.insertDataObject(CONN,df=df_removed_duplicates_shop,table="mapping_shop",primary_key="id_shop_lixus",except_col=[],do_nothing=False)

        ## Get where query shop_id_origin, shop_id_lixus, product_name
        query_shop_id_origin, query_shop_id_lixus, query_product_name = transform.getWhereQueryMappingTable(df_transformed)
        ## Get data from mapping table
        db_mapping_brand, db_mapping_platform, db_dict_mapping_shop, db_mapping_shop, db_mapping_shop_category, db_mapping_product = transform.getMappingTableFromDB(CONN, query_shop_id_origin, query_product_name)
        
        ## Mapping Product
        df_mapping_product = transform.mappingProduct(df_transformed, db_mapping_brand, db_dict_mapping_shop, db_mapping_product)
        df_removed_duplicates_product = transform.removeDuplicatesData(df_mapping_product,['id_product_lixus'])
        transform.insertDataObject(CONN,df=df_removed_duplicates_product,table="mapping_product",primary_key="id_product_lixus",except_col=[],do_nothing=False)
        

        ## Lookup Table
        df_lookup_platform = transform.lookupPlatform(df_transformed,db_mapping_platform)
        df_lookup_shop = transform.lookupShop(df_lookup_platform)
        # df_lookup_category = transform.lookupCategory(df_lookup_shop,df_removed_null_category)
        df_lookup_product = transform.lookupProduct(df_lookup_shop,df_removed_duplicates_product)
        df_lookup_brand = transform.lookupBrand(df_lookup_product,db_mapping_brand)
        df_lookup = transform.getId(df_lookup_brand)


        # ## Insert Category
        # df_category = df_lookup[['cat_id','cat_name','version']]
        # df_category_removed_duplicates = transform.removeDuplicatesData(df_category,['cat_id'])
        # df_category_removed_null = transform.removeNullValue(df_category_removed_duplicates,'cat_id')
        # transform.insertDataObject(CONN,df=df_category_removed_null,table="category",primary_key="cat_id",except_col=[],do_nothing=False)

        # ## Insert Sub Category
        # df_sub_category = df_lookup[['subcat_id','subcat_name','cat_id','version']]
        # df_sub_category_removed_duplicates = transform.removeDuplicatesData(df_sub_category,['subcat_id'])
        # df_sub_category_removed_null = transform.removeNullValue(df_sub_category_removed_duplicates,'subcat_id')
        # transform.insertDataObject(CONN,df=df_sub_category_removed_null,table="subcategory",primary_key="subcat_id",except_col=[],do_nothing=False)

        # ## Insert Sub Category
        # df_sub_sub_category = df_lookup[['subsubcat_id','subsubcat_name','subcat_id','version']]
        # df_sub_sub_category_removed_duplicates = transform.removeDuplicatesData(df_sub_sub_category,['subsubcat_id'])
        # df_sub_sub_category_removed_null = transform.removeNullValue(df_sub_sub_category_removed_duplicates,'subsubcat_id')
        # transform.insertDataObject(CONN,df=df_sub_sub_category_removed_null,table="subsubcategory",primary_key="subsubcat_id",except_col=[],do_nothing=False)

        ## Insert shop
        df_shop = df_lookup[['shop_id','shop_name','shop_domain','location','is_official','shop_url','platform']]
        df_shop_removed_duplicates = transform.removeDuplicatesData(df_shop,['shop_id'])
        transform.insertDataObject(CONN,df=df_shop_removed_duplicates,table="shop",primary_key="shop_id",except_col=[],do_nothing=False)
        
        # Insert product_merchant_platform
        df_product = df_lookup[['product_id','product_name','predicted_brand','product_url','shop_id','platform']]
        df_product_removed_duplicates = transform.removeDuplicatesData(df_product,['product_id'])
        transform.insertDataObject(CONN,df=df_product_removed_duplicates,table="product_merchant_platform",primary_key="product_id",except_col=[],do_nothing=False)

         ## Insert Crawl Type
        df_crawl_type = df_lookup[['crawl_id','crawling_type','filter','region','url','crawling_category','platform']]
        df_crawl_type_removed_duplicates = transform.removeDuplicatesData(df_crawl_type,['crawl_id'])
        transform.insertDataObject(CONN,df=df_crawl_type_removed_duplicates,table="crawl_type",primary_key="crawl_id",except_col=[],do_nothing=False)
        
        # df_snapshot = df_lookup[['crawl_id','product_id','date','rank','page','price','price_before_discount','discount','item_sold','stock','view_count','rating_star','rating_count']]
        # transform.insertSnapshots(CONN,df_snapshot)

    CONN.close()
    print("======== Duration: {} ========".format(round((datetime.datetime.now()-START_TIME).total_seconds(),2)))