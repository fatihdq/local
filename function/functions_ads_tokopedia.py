from sqlalchemy import sql
import pandas as pd
import numpy as np
import os
import re
import datetime

class FunctionAdsTokopedia(object):
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
        if status == 'Tidak Aktif':
            status = 'non-active'
        elif status == 'Tampil':
            status = 'active'
        elif status == 'Tidak Tampil':
            status = 'active (not showing)'
        elif status == 'Selesai':
            status = 'finished'
        return status

    def adsType(self,ads_type):
        if 'IKLAN TOKO' in ads_type.upper():
            ads_type = 'Iklan Toko'
        elif 'IKLAN PRODUK' in ads_type.upper():
            ads_type = 'Iklan Produk'
        elif 'IKLAN OTOMATIS' in ads_type.upper():
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
        rename_columns = {'Nama Grup':'ads_name',
                                    'Status':'status',
                                    'Tipe Iklan':'ads_type',
                                    'Nama':'product_name',
                                    'Tampil':'view',
                                    'Tampil Teratas':'top_view',
                                    'Klik':'click',
                                    'Persentase Klik':'click_percentation',
                                    'Efektivitas Iklan':'effectiveness',
                                    'Total Terjual':'sold',
                                    'Terjual':'direct_sold',
                                    'Pendapatan':'gmv',
                                    'Pengeluaran':'spending'}
        

        selected_columns = ['ads_name','status','ads_type','product_name','view','top_view','click','click_percentation','effectiveness','sold','direct_sold','gmv','spending']
        columns_str = ['status','ads_type','product_name','product_id','start_date','end_date','keyword']
        columns_int = ['view','click','click_percentation','convertion','direct_convertion','percent_convertion','percent_direct_convertion','spending_per_convertion','spending_per_direct_convertion','effectiveness','direct_effectiveness','sold','direct_sold','gmv','direct_gmv','spending','cir','direct_cir']
        dtype_mapping = {'status':'str','ads_type':'str','product_name':'str','product_id':'str','start_date':'str','end_date':'str','keyword':'str','view':'int','click':'int','click_percentation':'float','convertion':'float','direct_convertion':'float','percent_convertion':'float','percent_direct_convertion':'float','spending_per_convertion':'float','spending_per_direct_convertion':'float','effectiveness':'float','direct_effectiveness':'float','sold':'int','direct_sold':'int','gmv':'int','direct_gmv':'int','spending':'int','cir':'float','direct_cir':'float'}

        ## numeric null value
        columns_int = ['view','top_view','click','sold','direct_sold','gmv','spending']
        columns_float = ['effectiveness']
        columns_percent = ['click_percentation']

        ## Rename column
        ################
        df_rename = df.rename(columns=rename_columns)
        
        #Select Require columns
        ########################
        df_selected = df_rename[selected_columns]
        df_selected = df_selected[pd.isna(df_selected['product_name']) == False].reset_index(drop=True)


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

        df_numeric_value['direct_effectiveness'] = df_numeric_value['effectiveness']
        df_numeric_value['direct_gmv'] = df_numeric_value['gmv']
        df_numeric_value['cir'] = df_numeric_value.apply(lambda x:  0 if x['gmv']==0 or x['spending'] == 0 else round((x['spending']/x['gmv'])*100,2),axis=1)
        df_numeric_value['direct_cir'] = df_numeric_value['cir']
        ## Change Datatype
        #####################
        df_changed_dtype = df_numeric_value.copy() 
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))
        
        
        ## Date extraction
        ############
        date = re.findall(r'\d+',self.file_name.split(os.sep)[-1])
        date0 = date[5]+'-'+date[4]+'-'+date[3]
        
        ## Input Basic Value
        #####################
        df_basic_value = df_changed_dtype.copy()
        df_basic_value.loc[:,'date'] = date0
        df_basic_value.loc[:,'shop_name'] = self.store_domain
        df_basic_value.loc[:,'shop_lixus'] = self.store_lixus
        df_basic_value.loc[:,'platform'] = self.platform
        df_basic_value.loc[:,'shop_category'] = self.store_category

        return df_basic_value