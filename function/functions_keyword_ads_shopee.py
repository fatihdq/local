from sqlalchemy import sql
import pandas as pd
import numpy as np
import os
import re
import datetime

class FunctionKeywordAdsShopee(object):
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

    def findWholeWord(self,w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

    def cleansingNumeric(self,data,columns_int,columns_float,columns_percent):
        for col in columns_int:
            data[col] = int(float(data[col]))
        for col in columns_float:
            data[col] = float(data[col])
        for col in columns_percent:
            data[col] = float(data[col].replace('%',''))
        return data

    def statusType(self,status):
        if status == 'Nonaktif':
            status = 'non-active'
        elif status == 'Berjalan':
            status = 'active'
        elif status == 'Selesai':
            status = 'finished'
        return status
    def adsType(self,ads_type):
        if ads_type == 'Iklan Pencarian Toko':
            ads_type = 'Iklan Toko'
        elif ads_type == 'Iklan Pencarian Produk':
            ads_type = 'Iklan Produk'
        elif ads_type == 'Iklan Produk Serupa':
            ads_type = 'Iklan Produk Serupa'
        return ads_type

    def remove_emoji(self,string):
        emoji_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                "]+", flags=re.UNICODE)
        return emoji_pattern.sub(r'', string)

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
        rename_column_id = {'Status':'status',
                            'Jenis Iklan':'ads_type',
                            'Nama Iklan':'product_name',
                            'Tanggal Mulai':'start_date',
                            'Tanggal Selesai':'end_date',
                            'Kata Pencarian/Penempatan':'keyword',
                            'Dilihat':'view',
                            'Jumlah Klik':'click',
                            'Persentase Klik':'click_percentation',
                            'Konversi':'convertion',
                            'Konversi Langsung':'direct_convertion',
                            'Tingkat konversi':'percent_convertion',
                            'Tingkat Konversi Langsung':'percent_direct_convertion',
                            'Biaya per Konversi':'spending_per_convertion',
                            'Biaya per Konversi Langsung':'spending_per_direct_convertion',
                            'Efektifitas Iklan':'effectiveness',
                            'Efektivitas Langsung':'direct_effectiveness',
                            'Produk Terjual':'sold',
                            'Terjual Langsung':'direct_sold',
                            'Omzet Penjualan':'gmv',
                            'Penjualan Langsung (GMV Langsung)':'direct_gmv',
                            'Biaya':'spending',
                            'Persentase Biaya Iklan terhadap Penjualan dari Iklan (ACOS)':'cir',
                            'Persentase Biaya Iklan terhadap Penjualan dari Iklan Langsung (ACOS Langsung)':'direct_cir'}
        
        rename_column_en = {'Status':'status',
                            'Product Name/Ad Name':'product_name',
                            'Ad Type':'ads_type',
                            'Placement/Keyword':'keyword',
                            'Start Date':'start_date',
                            'End Date':'end_date',
                            'Impression':'view',
                            'Clicks':'click',
                            'CTR':'click_percentation',
                            'Conversions':'convertion',
                            'Direct Conversions':'direct_convertion',
                            'Conversion Rate':'percent_convertion',
                            'Direct Conversion Rate':'percent_direct_convertion',
                            'Cost per Conversion':'spending_per_convertion',
                            'Cost per Direct Conversion':'spending_per_direct_convertion',
                            'ROI':'effectiveness',
                            'Direct ROI':'direct_effectiveness',
                            'Items Sold':'sold',
                            'Direct Items Sold':'direct_sold',
                            'GMV':'gmv',
                            'Direct GMV':'direct_gmv',
                            'Expense':'spending',
                            'CIR':'cir',
                            'Direct CIR':'direct_cir'}

        selected_columns = ['status','ads_type','product_name','start_date','end_date','keyword','view','click','click_percentation','convertion','direct_convertion','percent_convertion','percent_direct_convertion','spending_per_convertion','spending_per_direct_convertion','effectiveness','direct_effectiveness','sold','direct_sold','gmv','direct_gmv','spending','cir','direct_cir']
        columns_str = ['status','ads_type','product_name','start_date','end_date','keyword']
        columns_int = ['view','click','click_percentation','convertion','direct_convertion','percent_convertion','percent_direct_convertion','spending_per_convertion','spending_per_direct_convertion','effectiveness','direct_effectiveness','sold','direct_sold','gmv','direct_gmv','spending','cir','direct_cir']
        dtype_mapping = {'status':'str','ads_type':'str','product_name':'str','start_date':'str','end_date':'str','keyword':'str','view':'int','click':'int','click_percentation':'float','convertion':'float','direct_convertion':'float','percent_convertion':'float','percent_direct_convertion':'float','spending_per_convertion':'float','spending_per_direct_convertion':'float','effectiveness':'float','direct_effectiveness':'float','sold':'int','direct_sold':'int','gmv':'int','direct_gmv':'int','spending':'int','cir':'float','direct_cir':'float'}

        ## numeric null value
        columns_int = ['view', 'click','sold', 'direct_sold', 'gmv', 'direct_gmv','spending']
        columns_float = ['convertion','direct_convertion','spending_per_convertion', 'spending_per_direct_convertion','effectiveness','direct_effectiveness']
        columns_percent = ['click_percentation','percent_convertion', 'percent_direct_convertion','cir', 'direct_cir']

        ## Rename column
        ################
        if 'Product Name/Ad Name' in df.columns:
            df_rename = df.rename(columns=rename_column_en)
        else:
            df_rename = df.rename(columns=rename_column_id)
        
        #Select Require columns
        ########################
        df_selected = df_rename[selected_columns]
        df_selected = df_selected[pd.isna(df_selected['product_name']) == False].reset_index(drop=True)
        df_selected = df_selected[df_selected['product_name']!='Tidak dapat memperoleh informasi produk karena penghapusan'].reset_index(drop=True)
        df_selected = df_selected[df_selected['product_name']!='Data ini belum dicek ulang dan mungkin akan berbeda setelah dicek ulang. Data yang telah dicek akan tersedia pada pukul 09.00 WIB setiap harinya.'].reset_index(drop=True)

        ## Handling null value
        #####################
        df_null_value = df_selected.copy()
        df_null_value.loc[:,'product_name'] = df_null_value['product_name'].apply(lambda x : re.sub(r"[\"#@;:<>{}`+=~|.!'?]",'',self.remove_emoji(x)))
        for col in columns_str:
            df_null_value[col] = df_null_value[col].apply(lambda x: '' if x == '-' or  pd.isna(x) else x)
        for col in columns_int:
            df_null_value[col] = df_null_value[col].apply(lambda x: '0' if x == '-' or x == '' or  pd.isna(x) else x)
        
        ## ads types and status
        df_null_value['ads_type'] = df_null_value['ads_type'].apply(lambda x: self.adsType(x))
        df_null_value['status'] = df_null_value['status'].apply(lambda x: self.statusType(x))

        ## Handling Numeric Value:
        #########################
        df_numeric_value = df_null_value.copy()
        df_numeric_value = df_numeric_value.apply(lambda x: self.cleansingNumeric(x,columns_int,columns_float,columns_percent),axis=1)

        ## Change Datatype
        #####################
        df_changed_dtype = df_numeric_value.copy() 
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))
        
        ## Handling Datetime
        df_handling_datetime = df_changed_dtype.copy()
        df_handling_datetime['start_date'] = df_handling_datetime['start_date'].apply(lambda x: datetime.datetime.strptime(x,'%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S'))
        df_handling_datetime['end_date'] = df_handling_datetime['end_date'].apply(lambda x: '' if x == 'Tidak Terbatas' else datetime.datetime.strptime(x,'%d/%m/%Y %H:%M').strftime('%Y-%m-%d %H:%M:%S'))

        ## Date extraction
        ############
        date = re.findall(r'\d+',self.file_name.split(os.sep)[-1])
        date0 = date[5]+'-'+date[4]+'-'+date[3]
        
        ## Input Basic Value
        #####################
        df_basic_value = df_handling_datetime.copy()
        df_basic_value.loc[:,'date'] = date0
        df_basic_value.loc[:,'shop_name'] = self.store_domain
        df_basic_value.loc[:,'shop_lixus'] = self.store_lixus
        df_basic_value.loc[:,'platform'] = self.platform
        df_basic_value.loc[:,'shop_category'] = self.store_category

        return df_basic_value

    ## Fetching data mappingform database
    def getMappingTableFromDB(self,CONN,df):
         # get list distinct shop_name and product_name
        list_shop_name = df['shop_name'].unique()
        
        #query to get platform data 
        text_query = sql.text("""SELECT platform_id,platform_name FROM platform """)
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_platform = [{'platform_id':arr['platform_id'],'platform_name':arr['platform_name']} for arr in result_query]

        #query to get shop data 
        text_query = sql.text("""SELECT * FROM mapping_shop
                                WHERE platform = '{}' and shop_domain = '{}' and category = '{}' """.format(self.platform,self.store_domain,self.store_category))
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop = [{'id_shop_lixus':arr['id_shop_lixus'],'id_shop_origin':arr['id_shop_origin'],'shop_domain':arr['shop_domain'],'shop_name':arr['shop_name'],'platform':arr['platform'],'category':arr['category']} for arr in result_query]

        #query to get shop category data 
        text_query = sql.text("""SELECT shop_category_id, shop_category_name FROM shop_category""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop_category = [{'shop_category_id':arr['shop_category_id'],'shop_category_name':arr['shop_category_name']} for arr in result_query]
      
        return db_mapping_platform, db_mapping_shop, db_mapping_shop_category

    
    ## Get brand by product_name referenced by mapping brand table
    ## to use in Product Match 
    def getBrand(self,mapping_brand,product_name):
        n_word = 10000
        brand = 'UNKNOWN'
        for lb in mapping_brand:
            if self.findWholeWord(lb['predict_name'].upper())(product_name.upper()):
                n_word0 = self.findWholeWord(lb['predict_name'].upper())(product_name.upper()).span()[0]
                if n_word0 < n_word:
                    n_word = n_word0
                    brand = lb['brand']
        n_word = 10000
        return brand


    



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

    def insertDataObject(self,CONN,df,table,primary_key,except_col,do_nothing):

        if len(except_col) != 0:
            df = df.drop(columns=except_col)

        ## get keys
        key = list(df.columns)
        query_col = ", ".join(key)
        if do_nothing == True:
            query_update_set = "NOTHING"
        else:
            query_update_set = "UPDATE SET {}".format(", ".join(list(map(lambda x:"{} = EXCLUDED.{}".format(x,x),key))))

        query_value = self.setQueryValue(df)

        query = sql.text("""INSERT INTO {table} ({query_col})
                            VALUES {query_value}
                            ON CONFLICT ({primary_key}) DO 
                            {query_update_set};
                            """.format(table=table,
                                        query_col=query_col,
                                        query_value=query_value,
                                        primary_key=primary_key,
                                        query_update_set=query_update_set))
        CONN.execute(query) 
    
    def insertSnapshotsAds(self,CONN,df,file_date):
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
        query_date = sql.text("""SELECT DISTINCT date FROM keyword_ads AS ka
                            JOIN shop AS s ON ka.shop_id = s.shop_id
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
            query = sql.text("""INSERT INTO keyword_ads ({})
                            VALUES {};
                            """.format(query_col, query_value))
            CONN.execute(query)
            print("Success Insert {} {} Data To Table".format(self.store_domain,file_date))
        else:
            print("{} {} Data Already Exist".format(self.store_domain,file_date))