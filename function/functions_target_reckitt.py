from sqlalchemy import sql
import pandas as pd
import numpy as np
import os
import re
import json
import datetime

class FunctionTargetReckitt(object):
    def __init__(self,df_store_info):
        self.df_store_info = df_store_info

            
    def removeDuplicatesData(self,df,column):
        df = df.drop_duplicates(subset=column,keep='first')
        return df.reset_index(drop=True)


    def findWholeWord(self,w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search



    def changeDtype(self,value,mDtype):
        if mDtype == 'int':
            value = int(float(value))
        if mDtype == 'float':
            value = float(value)
        if mDtype == 'str':
            try:
                value= str(int(float(value)))
            except:
                value= str(value)
        return value

    def basicTransform(self,df):
        rename_column = {"Category":"shop_category", 
                            "Platform":"platform",
                            "Brand":"brand_group",
                            "Date":"date",
                            "GMV":"gmv"}

        selected_columns = ["shop_category","platform","brand_group","date","gmv"]
        columns_str = ["shop_category","platform","brand_group"]
        columns_int = ["gmv"]
        dtype_mapping = {"shop_category":"str","platform":"str","brand_group":"str","date":"str","gmv":"int"}

        ## Rename column
        ################
        df_rename = df.rename(columns=rename_column)
        df_selected = df_rename[selected_columns]


        ## Handling null value
        #####################
        df_null_value = df_selected.copy()
        for col in columns_str:
            df_null_value[col] = df_null_value[col].apply(lambda x: '' if x == '-' or  pd.isna(x) else x)
        for col in columns_int:
            df_null_value[col] = df_null_value[col].apply(lambda x: '0' if x == '-' or x == '' or  pd.isna(x) else x)
        
        df_null_value['brand_group'] = df_null_value['brand_group'].apply(lambda x: x.upper())
        
        df_handle_datetime = df_null_value.copy()
        df_handle_datetime['date'] = df_handle_datetime['date'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'))
        
        ## Change Datatype
        #####################
        df_changed_dtype = df_handle_datetime.copy() 
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))
                

        return df_changed_dtype



    ## Fetching data mappingform database
    def getMappingTableFromDB(self,CONN,df):
         # get list distinct shop_name and product_name
        list_shop_domain = df['shop_domain'].unique()
        
        query_shop_domain = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_shop_domain)))

        #query to get platform data 
        text_query = sql.text("""SELECT platform_id,platform_name FROM platform """)
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_platform = [{'platform_id':arr['platform_id'],'platform_name':arr['platform_name']} for arr in result_query]

        #query to get shop data 
        text_query = sql.text("""SELECT * FROM mapping_shop
                                where shop_domain in ({})""".format(query_shop_domain))
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop = [{'id_shop_lixus':arr['id_shop_lixus'],'id_shop_origin':arr['id_shop_origin'],'shop_domain':arr['shop_domain'],'shop_name':arr['shop_name'],'platform':arr['platform'],'category':arr['category']} for arr in result_query]

        #query to get shop category data 
        text_query = sql.text("""SELECT shop_category_id, shop_category_name FROM shop_category""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop_category = [{'shop_category_id':arr['shop_category_id'],'shop_category_name':arr['shop_category_name']} for arr in result_query]

       
        return db_mapping_platform, db_mapping_shop, db_mapping_shop_category


    def getShopDomain(self,data):

        data['shop_domain'] = self.df_store_info[self.df_store_info['platform']==data['platform']]['shop_domain'].values[0]
        data['shop_lixus'] = self.df_store_info.loc[0,'shop_lixus']
        return data
    def mappingShopDomain(self,df):

        df = df.apply(lambda x: self.getShopDomain(x),axis=1)
        return df
    


    ## make sure every data in dataframe reference with mapping_platform table
    ## to use in lookupPlatform function
    def platformMatch(self,platform,mapping_platform):
        for mp in mapping_platform:
            if mp['platform_name'].upper() == platform.upper():
                platform_id = mp['platform_id']
        return platform_id

    def lookupPlatform(self,df,mapping_platform):
        df.loc[:,'platform'] = df['platform'].apply(lambda x: self.platformMatch(x,mapping_platform))
        return df

    ## make sure every data in dataframe reference with mapping_shop table
    ## to use in lookupShop function
    def shopMatch(self,df_match,mapping_shop):
        for mp in mapping_shop:
            if mp['shop_domain'] == df_match['shop_domain'] and mp['platform'] == df_match['platform']:
                shop_id  = mp['id_shop_lixus']
        df_match['shop_id'] = shop_id
        return df_match

    def lookupShop(self,df,mapping_shop):
        df = df.apply(lambda x: self.shopMatch(x,mapping_shop),axis=1)
        return df
    
    ## make sure every data in dataframe reference with mapping_shop_category table
    ## to use in lookupShopCategory function
    def shopCategoryMatch(self,shop_category,mapping_shop_category):
        for mp in mapping_shop_category:
            if mp['shop_category_name'].upper() == shop_category.upper():
                shop_category_id = mp['shop_category_id']
        return shop_category_id

    def lookupShopCategory(self,df,mapping_shop_category):
        df.loc[:,'shop_category'] = df['shop_category'].apply(lambda x: self.shopCategoryMatch(x,mapping_shop_category))
        return df


    def setValueType(self,x):
        if x == '' or  x == None:
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

    def insertDataTargetFullyear(self,CONN,df):
        list_shop_id =  df['shop_id'].unique()
        list_date = df['date'].unique()
        #query crawl_id to input where clause
        query_shop_id = ", ".join(list(map(lambda x:"\'{}\'".format(x), list_shop_id)))
        date = min(list_date)

        # query list of columns to be inserted into the table
        key = list(df.columns)
        query_col = ", ".join(key)

        # query list of value to be inserted into the table
        query_value = self.setQueryValue(df)
        
        # query to get distinct date
        query_date = sql.text("""SELECT DISTINCT date FROM target_fullyear_reckitt AS tfk
                            JOIN shop AS s ON tfk.shop_id = s.shop_id
                            WHERE s.shop_id IN ({});
                        """.format(query_shop_id))

        result_date = CONN.execute(query_date).fetchall()
        mapping_date = [date['date'].strftime('%Y-%m-%d') for date in result_date]


        # check if data is already exist or not
        is_exist = False
        if date in mapping_date:
            is_exist = True
        
        if is_exist == False:
            query = sql.text("""INSERT INTO target_fullyear_reckitt ({})
                            VALUES {};
                            """.format(query_col, query_value))
            CONN.execute(query)
            print("Success Insert {} Data To Table".format(date))
        else:
            print("{} Data Already Exist".format(date))