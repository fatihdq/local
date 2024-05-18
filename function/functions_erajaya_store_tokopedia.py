import pandas as pd
import numpy as np
import re
from sqlalchemy import sql
from datetime import datetime

class FunctionsErajayaStoreTokopedia(object):

    def __init__(self, file_name, platform):
        self.file_name = file_name
        self.platform = platform

    def removeEmoji(self,string):
        emoji_pattern = re.compile("["
                                    u"\U0001F600-\U0001F64F"  # emoticons
                                    u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                    u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                    u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                    u"\U00002702-\U000027B0"
                                    u"\U000024C2-\U0001F251"
                                    "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r'', string)

    
    def basicTransform(self,df):
        dtype_mapping = {'category_crawl':'string','is_official_store':'float','keyword_store':'string','location':'string','page_number':'int','platform':'string',
'productId':'int','productUrl':'string','referer':'string','region':'string','scrapingtype':'string','shopId':'int',
'shopName':'string','shopUrl':'string','shop_domain':'string','timestamp':'string','id_num':'int','rank':'int','product_key':'string','brand':'string',
'cat_id':'int','category':'string','countReview':'float','countTalk':'float','countView':'float','discount':'float','discountedPrice':'float','itemSold':'float',
'originalPrice':'float','price':'float','productName':'string','rate_1':'float','rate_2':'float','rate_3':'float','rate_4':'float','rate_5':'float',
'ratingScore':'float','stock':'float','subcat_id':'int','subcategory':'string','subsubcat_id':'int','subsubcategory':'string','totalRating':'float','txReject':'float','txSuccess':'float'}


        ## Handling null value
        #####################
        df_null_value = df.copy()
        df_null_value['productName'] = df_null_value['productName'].apply(lambda x : re.sub(r"[\"#@;:<>{}`+=~|.!'?]",'',self.removeEmoji(x)))
        df_null_value['productName'] = df_null_value['productName'].apply(lambda x: re.sub('\'','',x) if '\'' in x else x)
        for col in df_null_value.columns:
            if type(df_null_value.loc[:,col]) == object or type(df_null_value.loc[:,col]) == str:
                df_null_value.loc[:,col] = df_null_value[col].apply(lambda x: re.sub('\'','',x) if '\'' in x else x)
        
        
        df_null_value = df_null_value[pd.isna(df_null_value['productId']) == False].reset_index(drop=True)

        df_changed_dtype = df_null_value.copy()
   
        # df_changed_dtype = df_changed_dtype.astype(dtype_mapping)
        return df_changed_dtype

    def setValueType(self,x):
        if x == '' or  x == None or pd.isna(x):
            a = "{}".format('NULL')
        elif type(x) == str:
            a = "\'{}\'".format(x)
        else:
            a = "{}".format(x)
        return a
        
    def setQueryValue(self,df):
        values_0 = list(map(lambda x: ", ".join(list(map(lambda x:self.setValueType(x),x))) ,df.values.tolist()))
        values = ", ".join(list(map(lambda x:"({})".format(x),values_0)))
        return values


    def insertSnapshots(self,CONN,df):
        list_shop_domain =  df['shop_domain'].unique()
        list_date = df['timestamp'].unique()

        #query crawl_id to input where clause
        query_shop_domain = ", ".join(list(map(lambda x:"\'{}\'".format(x), list_shop_domain)))


        # query list of columns to be inserted into the table
        key = list(df.columns)
        query_col = ", ".join(list(map(lambda x:"\"{}\"".format(x),key)))


        # query list of value to be inserted into the table
        query_value = self.setQueryValue(df)
        
        # query to get distinct date
        query_date = sql.text("""SELECT DISTINCT timestamp FROM mp_tokped_atp_fe
                              WHERE shop_domain IN ({});
                          """.format(query_shop_domain))

        result_date = CONN.execute(query_date).fetchall()
        mapping_date = [datetime.strptime(date['timestamp'],'%Y-%m-%d').strftime('%Y-%m-%d') for date in result_date]

        # check if data is already exist or not
        is_exist = False
        for date in list_date:
            if date in mapping_date:
                is_exist = True
        
        if is_exist == False:
            query = sql.text("""INSERT INTO mp_tokped_atp_fe ({})
                              VALUES {};
                              """.format(query_col, query_value))
            CONN.execute(query)
            print("Success Insert Data To Table")
        else:
            print("Data Already Exist")

    