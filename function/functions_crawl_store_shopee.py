import pandas as pd
import numpy as np
import re
from sqlalchemy import sql

class FunctionsCrawlStoreShopee(object):
    def __init__(self, file_name, platform, platform_id):
        self.file_name = file_name
        self.platform = platform
        self.platform_id = platform_id

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
    
    def changeDtype(self,value,mDtype):
        if mDtype == 'int':
            try:
                value = int(float(value))
            except:
                value = 0
        if mDtype == 'float':
            try:
                value = float(value)
            except:
                value = 0.0
        if mDtype == 'str':
            try:
                value= str(int(float(value)))
            except:
                value= str(value)
        return value

    def basicTransform(self,df):
        dtype_mapping = {'crawling_category':'string','crawling_type':'string','filter':'string','region':'string','url':'string','platform':'string',
                'date':'string','rank':'int','page':'int','price':'int','price_before_discount':'int','discount':'float','item_sold':'int','stock':'int','rating_star':'float','rating_count':'int','star_1':'int','star_2':'int','star_3':'int','star_4':'int','star_5':'int',
                'product_id':'string','product_name':'string','product_ctime':'string','predicted_brand':'string','product_url':'string',
                'cat_id':'string','cat_name':'string','subcat_id':'string','subcat_name':'string','subsubcat_id':'string','subsubcat_name':'string',
                'shop_id':'string','shop_name':'string','shop_domain':'string','shop_ctime':'string','location':'string','is_official':'int','location':'string','shop_url':'string'}

        selected_columns = ['crawling_type','filter','crawling_category','region','url','platform','date','rank','page',
                'price','price_before_discount','discount','item_sold','stock','rating_star','rating_count','star_1','star_2','star_3','star_4','star_5',
                'product_id','product_name','product_ctime','predicted_brand','product_url','cat_id','cat_name','subcat_id','subcat_name','subsubcat_id','subsubcat_name',
                'shop_id','shop_name','shop_domain','shop_ctime','location','is_official','shop_url']
        renamed_columns = {'scrapingtype':'crawling_type','keyword_store':'filter','category_crawl':'crawling_category','region':'region','referer':'url','platform':'platform','timestamp':'date','rank':'rank','page_number':'page',
                'price':'price','price_before_discount':'price_before_discount','discount':'discount','historical_sold':'item_sold','stock':'stock','rating_star':'rating_star','review_count':'rating_count','star_1':'star_1','star_2':'star_2','star_3':'star_3','star_4':'star_4','star_5':'star_5',
                'item_id':'product_id','item_name':'product_name','item_ctime':'product_ctime','brand':'predicted_brand','item_referer':'product_url','cat_id':'cat_id','category':'cat_name','subcat_id':'subcat_id','subcategory':'subcat_name','subsubcat_id':'subsubcat_id','subsubcategory':'subsubcat_name',
                'shop_id':'shop_id','shop_name':'shop_name','shop_domain':'shop_domain','shop_ctime':'shop_ctime','location':'location','is_official_store':'is_official','shop_referer':'shop_url'}
                           
        ## Rename column
        ################
        df_renamed = df.rename(columns=renamed_columns)
        #Select Require columns
        ########################
        df_selected = df_renamed[selected_columns]
        idx = df_selected[pd.isnull(df_selected['shop_name'])].index
        df_selected = df_selected.drop(index=idx).reset_index(drop=True)

        ## Handling null value
        #####################
        df_null_value = df_selected.copy()
        df_null_value.loc[:,'product_name'] = df_null_value['product_name'].apply(lambda x : re.sub(r"[\"#@;:<>{}`+=~|.!'?]",'',self.removeEmoji(x)))
        df_null_value.loc[:,'predicted_brand'] = df_null_value['predicted_brand'].apply(lambda x:  '' if pd.isna(x) == True or x == 'Tidak Ada Merek' else x.upper())
        df_null_value.loc[:,'predicted_brand'] = df_null_value['predicted_brand'].apply(lambda x: re.sub('\'','',x) if '\'' in x else x)
        df_null_value.loc[:,'shop_name'] = df_null_value['shop_name'].apply(lambda x: re.sub('\'','',x) if '\'' in x else x)

        for col in df_null_value.columns:
            if type(df_null_value.loc[:,col]) == object:
                df_null_value.loc[:,col] = df_null_value[col].apply(lambda x: re.sub('\'','',x) if '\'' in x else x)

        df_null_value.loc[:,'cat_id'] = df_null_value['cat_id'].apply(lambda x: '' if x == '' or x == None or pd.isna(x) == True else x)
        df_null_value.loc[:,'cat_name'] = df_null_value.apply(lambda x: '' if x['cat_id'] == '' or x['cat_id'] == None or pd.isna(x['cat_id']) == True else x['cat_name'],axis=1)
        df_null_value.loc[:,'subcat_id'] = df_null_value.apply(lambda x: x['cat_id'] if x['subcat_id'] == '' or x['subcat_id'] == None or pd.isna(x['subcat_id']) == True else x['subcat_id'], axis=1)
        df_null_value.loc[:,'subcat_name'] = df_null_value.apply(lambda x: x['cat_name'] if x['subcat_id'] == '' or x['subcat_id'] == None or pd.isna(x['subcat_id']) == True else x['subcat_name'], axis=1)
        df_null_value.loc[:,'subsubcat_id'] = df_null_value.apply(lambda x: x['subcat_id'] if x['subsubcat_id'] == '' or x['subsubcat_id'] == None or pd.isna(x['subsubcat_id']) == True else x['subsubcat_id'], axis=1)
        df_null_value.loc[:,'subsubcat_name'] = df_null_value.apply(lambda x: x['subcat_name'] if x['subsubcat_id'] == '' or x['subsubcat_id'] == None or pd.isna(x['subsubcat_id']) == True else x['subsubcat_name'], axis=1)
        
        ## Change Datatype
        #####################
        df_changed_dtype = df_null_value.copy()
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))
        df_changed_dtype['is_official'] = df_changed_dtype['is_official'].apply(lambda x: True if int(x) == 1 else False)

        ## Handling Price
        #####################
        df_handled_price = df_changed_dtype.copy()
        df_handled_price.loc[:,'price_before_discount'] = df_handled_price.apply(lambda x: x['price'] if x['price_before_discount'] == 0 else x['price_before_discount'], axis=1)
        df_handled_price.loc[:,'price_before_discount'] = df_handled_price['price_before_discount'].apply(lambda x: int(int(x)/100000))
        df_handled_price.loc[:,'price'] = df_handled_price['price'].apply(lambda x: int(int(x)/100000))

        ## Input Basic Value
        #####################
        df_basic_value = df_handled_price.copy()
        df_basic_value.loc[:,'platform'] = self.platform.capitalize()
        
        return df_basic_value

    def removeDuplicatesData(self,df,column):
        df = df.drop_duplicates(subset=column,keep='first')
        return df.reset_index(drop=True)

    def removeNullValue(self,df,column):
        df = df[(df[column]!= '') & (df[column]!= None) & (df[column]!= np.nan) & (df[column]!='NaN') & (pd.isna(df[column])!=True)]
        return df.reset_index(drop=True)

    def mappingCategory(self,df):
        df_not_null = df[df['cat_id']!='']
        category = pd.DataFrame()
        category['id_cat_origin'] = df_not_null['cat_id'].apply(lambda x:str(int(float(x))))
        category['category_name'] = df_not_null['cat_name'].apply(lambda x: str(x))
        category['id_cat_lixus'] = category['id_cat_origin'].apply(lambda x:'CT-{}-{}'.format(self.platform_id,x))
        category['type'] = 'category'

        sub_category = pd.DataFrame()
        sub_category['id_cat_origin'] = df_not_null['subcat_id'].apply(lambda x:str(int(float(x))))
        sub_category['category_name'] = df_not_null['subcat_name'].apply(lambda x: str(x))
        sub_category['id_cat_lixus'] = sub_category['id_cat_origin'].apply(lambda x:'ST-{}-{}'.format(self.platform_id,x))
        sub_category['type'] = 'subcategory'

        sub_sub_category = pd.DataFrame()
        sub_sub_category['id_cat_origin'] = df_not_null['subsubcat_id'].apply(lambda x:str(int(float(x))))
        sub_sub_category['category_name'] = df_not_null['subsubcat_name'].apply(lambda x: str(x))
        sub_sub_category['id_cat_lixus'] = sub_sub_category['id_cat_origin'].apply(lambda x:'SS-{}-{}'.format(self.platform_id,x))
        sub_sub_category['type'] = 'subsubcategory'

        mapping_category = pd.concat([category, sub_category, sub_sub_category])
        mapping_category['platform'] = df_not_null['platform']
        return mapping_category


    def mappingBrand(self,df):
        brand = pd.DataFrame()
        brand['predict_name'] = df['predicted_brand']
        brand['brand'] = df['predicted_brand'].apply(lambda x : str(x).upper())
        brand['shop'] = df['shop_name']

        return brand

    def mappingShop(self,df):
        mapping_shop = pd.DataFrame()
        mapping_shop['id_shop_origin'] = df['shop_id']
        mapping_shop['id_shop_lixus'] = df['shop_id'].apply(lambda x: '{}-{}'.format(self.platform_id,str(x)))
        mapping_shop['shop_name'] = df['shop_name']
        mapping_shop['shop_domain'] = df['shop_domain']
        mapping_shop['platform'] = df['platform']

        return mapping_shop
    
    def findWholeWord(self,w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

    def get_brand(self,mapping_brand,product_name):
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
    
    def getWhereQueryMappingTable(self, df):
        list_shop_id_origin = df['shop_id'].unique()
        list_shop_id_lixus = list(map(lambda x:'{}-{}'.format(self.platform_id,x),list_shop_id_origin))
        list_product_name = df['product_name'].unique()


        # string query for shop_name
        # string query for product_name
        query_shop_id_origin = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_shop_id_origin)))
        query_shop_id_lixus = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_shop_id_lixus)))
        query_product_name = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_product_name)))
        
        return query_shop_id_origin, query_shop_id_lixus, query_product_name
    
    def getMappingTableFromDB(self, CONN, query_shop_id, query_product_name):
        #query to get brand data
        ##########
        text_query = sql.text("""SELECT * FROM mapping_brand""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_brand = [{'predict_name':arr['predict_name'],'brand':arr['brand'],'category':arr['category'],'shop':arr['shop']} for arr in result_query]

        #query to get platform data
        ##########
        text_query = sql.text("""SELECT platform_id,platform_name FROM platform """)
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_platform = [{'platform_id':arr['platform_id'],'platform_name':arr['platform_name']} for arr in result_query]


        #query to get shop data based on platform & shop_id_origin
        ##########
        text_query = sql.text("""SELECT * FROM mapping_shop
                                    WHERE platform = '{}' and id_shop_origin in ({}) """.format(self.platform,query_shop_id))
        result_query = CONN.execute(text_query).fetchall()
        db_dict_mapping_shop = {v['id_shop_origin']:v['id_shop_lixus'] for v in result_query}
        db_mapping_shop = [{'id_shop_lixus':arr['id_shop_lixus'],'id_shop_origin':arr['id_shop_origin'],'shop_domain':arr['shop_domain'],'shop_name':arr['shop_name'],'platform':arr['platform'],'category':arr['category']} for arr in result_query]

        #query to get shop_category data
        ##########
        text_query = sql.text("""SELECT shop_category_id, shop_category_name FROM shop_category""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop_category = [{'shop_category_id':arr['shop_category_id'],'shop_category_name':arr['shop_category_name']} for arr in result_query]


        #query to get product data based on product_name, id_shop_origin, and platform
        ##########
        query = sql.text("""SELECT * from mapping_product mp
                                join mapping_shop ms on mp.id_shop_lixus= ms.id_shop_lixus 
                                WHERE mp.product_name in ({})
                                AND ms.id_shop_origin in ({}) AND ms.platform = '{}'; 
                            """.format(query_product_name,query_shop_id,self.platform))  
        result_query = CONN.execute(query).fetchall()
        db_mapping_product = [{'id_product_lixus':v['id_product_lixus'], 'id_product_origin':v['id_product_origin'], 'product_name':v['product_name'], 'sku':v['sku'], 'predicted_brand':v['predicted_brand'], 'id_shop_lixus':v['id_shop_lixus'], 'platform':v['platform'], 'category':v['category']} for v in result_query]

        return db_mapping_brand, db_mapping_platform, db_dict_mapping_shop, db_mapping_shop, db_mapping_shop_category, db_mapping_product

    def mappingProduct(self, df, db_mapping_brand, db_dict_mapping_shop, db_mapping_product):
        product_keys = ['product_id','product_name','shop_id','platform']
        df_product = df[product_keys]
        def productMatch(df,dmp):
            bol = False
            new_product = {}
            for mp in dmp:
                if df['product_name'] == mp['product_name'] and db_dict_mapping_shop[str(df['shop_id'])] == mp['id_shop_lixus']:
                    if mp['id_product_lixus'][0:3] != '{}-'.format(self.platform_id):
                        new_product['id_product_origin'] = df['product_id']
                        new_product['id_product_lixus'] = mp['id_product_lixus']
                        new_product['product_name'] = mp['product_name']
                        new_product['id_shop_lixus'] = mp['id_shop_lixus']
                        if mp['sku'] == None:
                            new_product['sku'] = ''
                        else:
                            new_product['sku'] = mp['sku']
                        new_product['platform'] =  mp['platform']
                        new_product['category'] = mp['category']
                        if mp['predicted_brand'] == '' or mp['predicted_brand'] == 'nan' or mp['predicted_brand'] == 'UNKNOWN' or mp['predicted_brand'] == 'NULL' or mp['predicted_brand'] == 'null' or mp['predicted_brand'] == None or pd.isnull(mp['predicted_brand']):
                            new_product['predicted_brand'] = self.get_brand(db_mapping_brand,new_product['product_name'])
                        else:
                            new_product['predicted_brand'] = mp['predicted_brand']
                        bol = True

            if bol == False:
                new_product['id_product_origin'] = df['product_id']
                new_product['id_product_lixus'] = "{}-{}".format(self.platform_id,str(df['product_id']))
                new_product['product_name'] = df['product_name']
                new_product['id_shop_lixus'] = db_dict_mapping_shop[str(df['shop_id'])]
                new_product['sku'] = ''
                new_product['platform'] =  df['platform']
                new_product['category'] =  ''
                new_product['predicted_brand'] = self.get_brand(db_mapping_brand,new_product['product_name'])
            
            return new_product
 
        mapping_product = pd.DataFrame.from_dict(list(df_product.apply(lambda y: productMatch(y,list(filter(lambda x: x['product_name'] == y['product_name'],db_mapping_product))),axis=1)),orient='columns')
        return mapping_product

    def lookupBrand(self,df,mapping_brand):
        def brandMatch(predicted_brand,product_name):
            brand = predicted_brand
            if str(predicted_brand) == '' or str(predicted_brand) == 'nan' or str(predicted_brand) == 'UNKNOWN' or str(predicted_brand) == 'NULL' or str(predicted_brand) == 'null' or str(predicted_brand) == str(pd.NA):
                n_word = 10000 
                for lb in mapping_brand:
                    if self.findWholeWord(lb['predict_name'].upper())(product_name.upper()):
                        n_word0 = self.findWholeWord(lb['predict_name'].upper())(product_name.upper()).span()[0]
                        if n_word0 < n_word:
                            n_word = n_word0
                            brand = lb['brand']
                n_word = 10000
            return brand
        
        df.loc[:,'predicted_brand'] = df.apply(lambda x: brandMatch(x['predicted_brand'],x['product_name']),axis=1)
        return df

    def lookupPlatform(self,df,mapping_platform):
        def platformMatch(platform):
            for mp in mapping_platform:
                if mp['platform_name'].upper() == platform.upper():
                    platform_id = mp['platform_id']
            return platform_id

        df.loc[:,'platform'] = df['platform'].apply(lambda x: platformMatch(x))
        return df

    def lookupShop(self,df):
        df.loc[:,'shop_id'] = df.loc[:,'shop_id'].apply(lambda x: '{}-{}'.format(self.platform_id,x))
        return df

    def lookupCategory(self,df,mapping_category):  
        def categoryMatch(value,type):
            for mc in mapping_category.to_dict('records'):
                if str(int(float(mc['id_cat_origin']))) == str(int(float(value))) and mc['type'] == type:
                    temp_cat = mc['id_cat_lixus']
            return temp_cat

        df.loc[:,'cat_id'] = df['cat_id'].apply(lambda x: categoryMatch(x,'category') if x != '' else '')
        df.loc[:,'subcat_id'] = df['subcat_id'].apply(lambda x: categoryMatch(x,'subcategory') if x != '' else '')
        df.loc[:,'subsubcat_id'] = df['subsubcat_id'].apply(lambda x: categoryMatch(x,'subsubcategory') if x != '' else '')
        df.loc[:,'version'] = 'lixus'

        return df

    def lookupProduct(self,df,mapping_product):
        def matchProduct(df_match,mps):
            for mp in mps.to_dict('records'):
                if str(int(float(mp['id_product_origin']))) == str(int(float(df_match['product_id']))) and str(mp['id_shop_lixus']) == str(df_match['shop_id']):
                    product_id  = mp['id_product_lixus'] 
                    predicted_brand = mp['predicted_brand']
            df_match['product_id'] = product_id
            df_match['predicted_brand'] = predicted_brand
            return df_match
        df = df.apply(lambda x: matchProduct(x,mapping_product[mapping_product['id_product_origin']==x['product_id']]),axis=1)
        
        return df

    def getId(self,df):
        def get_first_char(line):
            def countvowels(string):
                vowels = ['A','I','U','E','O']
                result = ''
                for vow in vowels:
                    num = 0
                    for char in string.upper():
                        if vow == char:
                            num +=1
                    result += str(num)
                return result

            line = re.sub(r"[-()\"#/@;:<>{}`'+=~|.!?]", "",line)
            num0 = re.findall(r'\d+',line)
            num = "".join(num0)
            words = line.split()
            n_word = len(line)
            letters = [word[0]+word[-1] for word in words]
            return str("".join(letters))+""+str(n_word)+str(num)+str(countvowels(line))
            
        for idx in range(len(df)):
            df.loc[idx,'crawl_id'] = '{}-{}{}{}{}'.format(self.platform_id,get_first_char(df.loc[idx,'crawling_type']),get_first_char(df.loc[idx,'crawling_category']),df.loc[idx,'filter'].replace(' ','_'),str(df.loc[idx,'platform']))
        
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

    def insertSnapshots(self,CONN,df):
        list_crawl_id =  df['crawl_id'].unique()
        list_date = df['date'].unique()

        #query crawl_id to input where clause
        query_crawl_id = ", ".join(list(map(lambda x:"\'{}\'".format(x), list_crawl_id)))


        # query list of columns to be inserted into the table
        key = list(df.columns)
        query_col = ", ".join(key)

        # query list of value to be inserted into the table
        query_value = self.setQueryValue(df)
        
        # query to get distinct date
        query_date = sql.text("""SELECT DISTINCT date FROM crawl_snapshots
                                WHERE crawl_id IN ({query_crawl_id});
                            """.format(query_crawl_id=query_crawl_id))

        result_date = CONN.execute(query_date).fetchall()
        mapping_date = [date['date'].strftime('%Y-%m-%d') for date in result_date]

        # check if data is already exist or not
        is_exist = False
        for date in list_date:
            if date in mapping_date:
                is_exist = True
        
        if is_exist == False:
            query = sql.text("""INSERT INTO crawl_snapshots ({query_col})
                                VALUES {query_value};
                                """.format(query_col=query_col, query_value=query_value))
            CONN.execute(query)
            print("Success Insert Data To Table")
        else:
            print("Data Already Exist")