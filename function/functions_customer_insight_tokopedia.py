from datetime import datetime
from sqlalchemy import sql
import pandas as pd
import numpy as np
import os
import re

class FunctionCustomerInsightTokopedia(object):
    def __init__(self,file_name,store_lixus,store_domain,store_category,platform,platform_id):
        self.file_name = file_name
        self.store_domain = store_domain
        self.store_lixus = store_lixus
        self.store_category = store_category
        self.platform = platform
        self.platform_id = platform_id

    
    def removeDuplicatesData(self,df,column):
        df = df.drop_duplicates(subset=column,keep='first')
        return df.reset_index(drop=True)

    def removeNullValue(self,df,column):
        df = df[(df[column]!= '') & (df[column]!= None) & (df[column]!= np.nan) & (df[column]!='NaN') & (pd.isna(df[column])!=True)]
        return df.reset_index(drop=True)


    def changeDtype(self,value,mDtype):
        if mDtype == 'int':
            value = int(float(value))
        if mDtype == 'str':
            try:
                value= str(int(float(value)))
            except:
                value= str(value)
        return value
    
    def basicTransform(self,df):
        ## Rename Column
        #################
        						

        rename_column = {'Date':'date',
                        'Pembeli':'buyer',
                        'Laki-Laki':'buyer_man',
                        'Perempuan':'buyer_woman',
                        'Tidak disebutkan':'buyer_not_mentioned',
                        'Pembeli baru':'new_buyer',
                        'Pembeli reguler':'reguler_buyer',
                        'Pembeli setia':'loyal_buyer',
                        '< 17 tahun':'age_17',
                        '18 – 23 tahun':'age_18_23',
                        '24 – 34 tahun':'age_24_34',
                        '35 – 44 tahun':'age_35_44',
                        '> 45 tahun':'age_45_',
                        'Followers':'followers'}
        
        selected_columns = ['date','buyer','buyer_man','buyer_woman','buyer_not_mentioned','new_buyer','reguler_buyer','loyal_buyer','age_17','age_18_23','age_24_34','age_35_44','age_45_','followers']
        dtype_mapping = {'date':'str','buyer':'int','buyer_man':'int','buyer_woman':'int','buyer_not_mentioned':'int','new_buyer':'int','reguler_buyer':'int','loyal_buyer':'int','age_17':'int','age_18_23':'int','age_24_34':'int','age_35_44':'int','age_45_':'int','followers':'int'}
        columns_str = ['date']
        columns_int = ['buyer','buyer_man','buyer_woman','buyer_not_mentioned','new_buyer','reguler_buyer','loyal_buyer','age_17','age_18_23','age_24_34','age_35_44','age_45_','followers']

        df_rename = df.rename(columns=rename_column)
        ## Select require columns
        ##################
        df_selected = df_rename[selected_columns]
        df_selected['date'] = df_selected['date'].apply(lambda x: datetime.strptime(x,'%d/%m/%Y').strftime('%Y-%m-%d'))


        ## Change Datatype
        #####################
        df_changed_dtype = df_selected.copy()
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))

        ## Input Basic Value
        #####################
        df_basic_value = df_changed_dtype.copy()
        df_basic_value.loc[:,'shop_name'] = self.store_domain
        df_basic_value.loc[:,'shop_lixus'] = self.store_lixus
        df_basic_value.loc[:,'platform'] = self.platform
        df_basic_value.loc[:,'shop_category'] = self.store_category

        return df_basic_value
    
    ## Fetching data mappingform database
    def getMappingTableFromDB(self,CONN,df):
        #query to get platform data 
        text_query = sql.text("""SELECT platform_id,platform_name FROM platform """)
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_platform = [{'platform_id':arr['platform_id'],'platform_name':arr['platform_name']} for arr in result_query]

        #query to get shop data 
        text_query = sql.text("""SELECT * FROM mapping_shop
                                WHERE platform = '{}' and shop_domain = '{}' and category = '{}' """.format(self.platform,self.store_domain,self.store_category))
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop = [{'id_shop_lixus':arr['id_shop_lixus'],'id_shop_origin':arr['id_shop_origin'],'shop_domain':arr['shop_domain'],'shop_name':arr['shop_name'],'platform':arr['platform'],'category':arr['category']} for arr in result_query]
       
        return db_mapping_platform, db_mapping_shop
    
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
            if mp['shop_domain'] == df_match['shop_name']:
                shop_id  = mp['id_shop_lixus']
        df_match['shop_id'] = shop_id
        return df_match

    def lookupShop(self,df,mapping_shop):
        df = df.apply(lambda x: self.shopMatch(x,mapping_shop),axis=1)
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

    def insertSnapshots(self,CONN,df):
        list_shop_id =  df['shop_id'].unique()
        list_date = df['date'].unique()

        #query crawl_id to input where clause
        query_shop_id = ", ".join(list(map(lambda x:"\'{}\'".format(x), list_shop_id)))

        # query list of columns to be inserted into the table
        key = list(df.columns)
        query_col = ", ".join(key)

        # query list of value to be inserted into the table
        query_value = self.setQueryValue(df)
        
        # query to get distinct date
        query_date = sql.text("""SELECT DISTINCT date FROM customer_insight AS ci
                            JOIN shop AS s ON ci.shop_id = s.shop_id
                            WHERE s.shop_id IN ({});
                        """.format(query_shop_id))

        result_date = CONN.execute(query_date).fetchall()
        mapping_date = [date['date'].strftime('%Y-%m-%d') for date in result_date]

        # check if data is already exist or not
        is_exist = False
        for date in list_date:
            if date in mapping_date:
                is_exist = True
        
        if is_exist == False:
            query = sql.text("""INSERT INTO customer_insight ({})
                            VALUES {};
                            """.format(query_col, query_value))
            CONN.execute(query)
            print("Success Insert {} Data To Table".format(self.store_domain))
        else:
            print("{} Data Already Exist".format(self.store_domain))