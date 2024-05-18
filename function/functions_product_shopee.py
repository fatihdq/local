from sqlalchemy import sql
import pandas as pd
import numpy as np
import os
import re
import json
import datetime

class FunctionProductShopee(object):
    def __init__(self,file_name,store_lixus,store_domain,store_category,platform,platform_id):
        self.file_name = file_name
        self.store_domain = store_domain
        self.store_lixus = store_lixus
        self.store_category = store_category
        self.platform = platform
        self.platform_id = platform_id

        with open('./transformation/mapping/existing_brand_mapping.json','r') as f:
            self.existing_brand_mapping = json.loads(f.read())

        with open('./transformation/mapping/segment_mapping.json','r') as f:
            self.segment_mapping = json.loads(f.read())

        with open('./transformation/mapping/kalbe_pci_mapping.json','r') as f:
            self.kalbe_pci_mapping = json.loads(f.read())

        with open('./transformation/mapping/reckitt_pci_mapping.json','r') as f:
            self.reckitt_pci_mapping = json.loads(f.read())

    def removeDuplicatesData(self,df,column):
        df = df.drop_duplicates(subset=column,keep='first')
        return df.reset_index(drop=True)

    def removeNullValue(self,df,column):
        df = df[(df[column]!= '') & (df[column]!= None) & (df[column]!= np.nan) & (df[column]!='NaN') & (pd.isna(df[column])!=True)]
        return df.reset_index(drop=True)

    def findWholeWord(self,w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search
    

    def changeDtype(self,value,mDtype):
        if mDtype == 'int':
            value = int(float(value))
        if mDtype == 'str':
            try:
                value= str(int(float(value)))
            except:
                value= str(value)
        return value
    
    def getDateFromFilename(self,file):
        date_str = file.split('_')[-1][0:8]
        date = datetime.datetime.strptime(date_str,'%Y%m%d').strftime('%Y-%m-%d')
        return date
    
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

    def basicTransform(self,df):
        rename_column1= {"Kode Produk":"product_id", 
                            "Nama Produk":"product_name",
                            "Kode Variasi":"variation_id",
                            "Nama Variasi":"variation_name",
                            "SKU Induk":"sku",
                            "SKU":"sku_variant",
                            "Harga":"price",
                            "Stok":"stock"
                        }
        
        rename_column2= {"Kode Produk":"product_id", 
                            "Nama Produk":"product_name",
                            "Kode Variasi":"variation_id",
                            "Nama Variasi":"variation_name",
                            "SKU Induk":"sku",
                            "SKU":"sku_variant",
                            "Harga":"price",
                            "Stok Penjual":"stock"
                        }

        selected_columns = ["product_id","product_name","variation_id","variation_name","sku","sku_variant","price","stock"]
        columns_str = ["product_id","product_name","variation_id","variation_name","sku","sku_variant"]
        columns_int = ["price","stock"]
        dtype_mapping = {"product_id":"str","product_name":"str","variation_id":"str","variation_name":"str","sku":"str","price":"int","stock":"int"}

        ## Rename column
        ################
        
        if 'Stok Penjual' in df.columns:    
            df_rename = df.rename(columns=rename_column2)
        else:
            df_rename = df.rename(columns=rename_column1)



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
        


        ## Handling Null SKU
        df_null_sku = df_null_value.copy() 
        for idx in range(len(df_null_sku)):
            if df_null_sku.loc[idx,'sku'] == '':
                df_null_sku.loc[idx,'sku'] = df_null_sku.loc[idx,'sku_variant']
            
        df_null_sku = df_null_sku.drop(columns='sku_variant')
        ## Change Datatype
        #####################
        df_changed_dtype = df_null_sku.copy() 
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))
                

        ## Remove ' in product_name
        ######################
        df_removed_str = df_changed_dtype.copy()
        for col in ['product_name','variation_name']:
            df_removed_str[col] = df_removed_str[col].apply(lambda x: x.replace('\'','') if '\'' in x else x)
        df_removed_str = df_removed_str[df_removed_str['product_name'].isin(["[GIFT]"])==False].reset_index(drop=True)
        
        
        ## Input Basic Value
        #####################
        df_basic_value = df_removed_str.copy()
        df_basic_value.loc[:,'date'] = self.getDateFromFilename(self.file_name.split(os.sep)[-1])
        df_basic_value.loc[:,'shop_name'] = self.store_domain
        df_basic_value.loc[:,'shop_lixus'] = self.store_lixus
        df_basic_value.loc[:,'platform'] = self.platform
        df_basic_value.loc[:,'shop_category'] = self.store_category

        return df_basic_value


    ## Fetching data mappingform database
    def getMappingTableFromDB(self,CONN,df):
         # get list distinct shop_name and product_name
        list_shop_name = df['shop_name'].unique()
        list_product_id = df['product_id'].unique()
        
        # string query for shop_name
        query_shop_name = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_shop_name)))
        query_product_id_origin = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_product_id)))
        
        #query to get brand category data 
        text_query = sql.text("""SELECT brand_category_id, brand_category_name, brand FROM mapping_brand_category""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_brand_category = [{'brand_category_id':arr['brand_category_id'],'brand_category_name':arr['brand_category_name'],'brand':arr['brand']} for arr in result_query]

        #query to get platform data 
        text_query = sql.text("""SELECT platform_id,platform_name FROM platform """)
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_platform = [{'platform_id':arr['platform_id'],'platform_name':arr['platform_name']} for arr in result_query]

        #query to get shop data 
        text_query = sql.text("""SELECT id_shop_lixus, id_shop_origin, shop_domain, shop_name, platform, category FROM mapping_shop
                                WHERE platform = '{}' and shop_domain = '{}' and category = '{}' """.format(self.platform,self.store_domain,self.store_category))
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop = [{'id_shop_lixus':arr['id_shop_lixus'],'id_shop_origin':arr['id_shop_origin'],'shop_domain':arr['shop_domain'],'shop_name':arr['shop_name'],'platform':arr['platform'],'category':arr['category']} for arr in result_query]

        #query to get shop category data 
        text_query = sql.text("""SELECT shop_category_id, shop_category_name FROM shop_category""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_shop_category = [{'shop_category_id':arr['shop_category_id'],'shop_category_name':arr['shop_category_name']} for arr in result_query]

        #query to get brand data based on category
        text_query = sql.text("""SELECT predict_name, brand, category, shop FROM mapping_brand
                              WHERE category = '{}' """.format(self.store_category))
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_brand = [{'predict_name':arr['predict_name'],'brand':arr['brand'],'category':arr['category'],'shop':arr['shop']} for arr in result_query]

        ## query to get data from mapping_product table based on product_name, shop_domain, and platform
        query = sql.text("""SELECT mp.id_product_lixus, mp.id_product_origin, mp.product_name, mp.sku, mp.predicted_brand, mp.id_shop_lixus, mp.platform, mp.category from mapping_product mp
                                join mapping_shop ms on mp.id_shop_lixus= ms.id_shop_lixus 
                                WHERE mp.id_product_origin in ({})
                                AND ms.shop_domain in ({}) AND ms.platform = '{}'; 
                            """.format(query_product_id_origin,query_shop_name,self.platform))  
        result_query = CONN.execute(query).fetchall()
        db_mapping_product = [{'id_product_lixus':v['id_product_lixus'], 'id_product_origin':v['id_product_origin'], 'product_name':v['product_name'], 'sku':v['sku'], 'predicted_brand':v['predicted_brand'], 'id_shop_lixus':v['id_shop_lixus'], 'platform':v['platform'], 'category':v['category']} for v in result_query]

        return db_mapping_brand, db_mapping_brand_category, db_mapping_platform, db_mapping_shop, db_mapping_shop_category, db_mapping_product
    
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


    ## make sure every data in dataframe reference with mapping_product table
    ## to use in mappingProduct function
    def productMatch(self,df,dmp,db_mapping_brand):
            bol = False
            new_product = {}
            for mp in dmp:
                if df['product_id'] == mp['id_product_origin']:
                    new_product['id_product_origin'] = mp['id_product_origin']
                    new_product['id_product_lixus'] = mp['id_product_lixus']
                    new_product['product_name'] = df['product_name']
                    new_product['id_shop_lixus'] = mp['id_shop_lixus']
                    new_product['sku'] = df['sku']
                    new_product['platform'] =  mp['platform']
                    new_product['category'] =  self.store_category
                    if mp['predicted_brand'] == '' or mp['predicted_brand'] == 'nan' or mp['predicted_brand'] == 'UNKNOWN' or mp['predicted_brand'] == 'NULL' or mp['predicted_brand'] == 'null' or mp['predicted_brand'] == None or pd.isnull(mp['predicted_brand']):
                        new_product['predicted_brand'] = self.getBrand(db_mapping_brand,new_product['product_name'])
                    else:
                        new_product['predicted_brand'] = mp['predicted_brand']
                    bol = True
            if bol == False:
                new_product['id_product_origin'] = df['product_id']
                new_product['id_product_lixus'] = "{}-{}".format(self.platform_id,str(df['product_id']))
                new_product['product_name'] = df['product_name']
                new_product['id_shop_lixus'] = dmp[0]['id_shop_lixus']
                new_product['sku'] = df['sku']
                new_product['platform'] =  self.platform
                new_product['category'] =  self.store_category
                new_product['predicted_brand'] = self.getBrand(db_mapping_brand,new_product['product_name'])
                
            return new_product

    def mappingProduct(self,df,db_mapping_brand, db_mapping_product):
        # check and update data for mapping_product table
        keys = ['product_id','product_name','sku','shop_name']
        df_product = df[keys]
        
        mapping_product = pd.DataFrame.from_dict(list(df_product.apply(lambda y: self.productMatch(y,db_mapping_product,db_mapping_brand),axis=1)),orient='columns')
        return mapping_product
    

    def checkVariant(self,data,m):
        isMatch = False
        if pd.isna(m['variant1']) == False and pd.isna(m['variant2']) == False and pd.isna(m['variant3']) == False:
            if m['variant1'].upper() in data['product_name'].upper() or m['variant2'].upper() in data['product_name'].upper() or m['variant3'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['variant1']) == False and pd.isna(m['variant2']) == False and pd.isna(m['variant3']) == True:
            if m['variant1'].upper() in data['product_name'].upper() or m['variant2'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['variant1']) == False and pd.isna(m['variant2']) == True and pd.isna(m['variant3']) == True:
            if m['variant1'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['variant1']) == True and pd.isna(m['variant2']) == True:
            isMatch = True
        return isMatch

    def checkGramation(self,data,m):
        isMatch = False
        if pd.isna(m['gramation1']) == False and pd.isna(m['unit1']) == False and pd.isna(m['unit2']) == False:
            if pd.isna(m['gramation2']) == False:
                if str(m['gramation1']).upper() in re.findall(r'\d+',data['product_name']) or str(m['gramation2']).upper() in re.findall(r'\d+',data['product_name']):
                    if str(m['unit1']).upper() in re.findall(r'\d+',data['product_name']) and str(m['unit2']).upper() in re.findall(r'\d+',data['product_name']):
                        isMatch = True
                    elif str(m['total_gramation']).upper() in re.findall(r'\d+',data['product_name']):
                        isMatch = True
            else:
                if str(m['gramation1']).upper() in re.findall(r'\d+',data['product_name']) and str(m['unit1']).upper() in re.findall(r'\d+',data['product_name']) and str(m['unit2']).upper() in re.findall(r'\d+',data['product_name']):
                        isMatch = True
                elif str(m['total_gramation']).upper() in re.findall(r'\d+',data['product_name']):
                    isMatch = True
        
        elif pd.isna(m['gramation1']) == False and pd.isna(m['unit1']) == False and pd.isna(m['unit2']) == True:
            if pd.isna(m['gramation2']) == False:
                if str(m['gramation1']).upper() in re.findall(r'\d+',data['product_name']) or str(m['gramation2']).upper() in re.findall(r'\d+',data['product_name']):
                    if str(m['unit1']).upper() in re.findall(r'\d+',data['product_name']):
                        isMatch = True
                    elif str(m['total_gramation']).upper() in re.findall(r'\d+',data['product_name']):
                        isMatch = True
                
            else:
                if str(m['gramation1']).upper() in re.findall(r'\d+',data['product_name']) and str(m['unit1']).upper() in re.findall(r'\d+',data['product_name']):
                    isMatch = True
                elif str(m['total_gramation']).upper() in re.findall(r'\d+',data['product_name']):
                    isMatch = True

        elif pd.isna(m['gramation1']) == False and pd.isna(m['unit1']) == True and pd.isna(m['unit2']) == True:
            if pd.isna(m['gramation2']) == False:
                if str(m['gramation1']).upper() in re.findall(r'\d+',data['product_name']) or str(m['gramation2']).upper() in re.findall(r'\d+',data['product_name']):
                    isMatch = True
            else:
                if str(m['gramation1']).upper() in re.findall(r'\d+',data['product_name']):
                    isMatch = True
        
        return isMatch
                
    def checkBrand(self,data,m):
        isMatch = False
        if pd.isna(m['brand1']) == False and pd.isna(m['brand2']) == False and pd.isna(m['brand3']) == False:
            if m['brand1'].upper() in data['product_name'].upper() or m['brand2'].upper() in data['product_name'].upper() or m['brand3'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['brand1']) == False and pd.isna(m['brand2']) == False and pd.isna(m['brand3']) == True:
            if m['brand1'].upper() in data['product_name'].upper() or m['brand2'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['brand1']) == False and pd.isna(m['brand2']) == True and pd.isna(m['brand3']) == True:    
            if m['brand1'].upper() in data['product_name'].upper():
                isMatch = True
        return isMatch

    def checkSubBrand(self,data,m):
        isMatch = False
        if pd.isna(m['sub_brand1']) == False and pd.isna(m['sub_brand2']) == False and pd.isna(m['sub_brand3']) == False and pd.isna(m['sub_brand_integer']) == False:
            if m['sub_brand1'].upper() in data['product_name'].upper() or m['sub_brand2'].upper() in data['product_name'].upper() or m['sub_brand3'].upper() in data['product_name'].upper() or self.findWholeWord(str(int(float(m['sub_brand_integer'].upper()))))(data['product_name'].upper()):
                isMatch = True
        elif pd.isna(m['sub_brand1']) == False and pd.isna(m['sub_brand2']) == False and pd.isna(m['sub_brand3']) == False and pd.isna(m['sub_brand_integer']) == True:
            if m['sub_brand1'].upper() in data['product_name'].upper() or m['sub_brand2'].upper() in data['product_name'].upper() or m['sub_brand3'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['sub_brand1']) == False and pd.isna(m['sub_brand2']) == False and pd.isna(m['sub_brand3']) == True and pd.isna(m['sub_brand_integer']) == True:
            if m['sub_brand1'].upper() in data['product_name'].upper() or m['sub_brand2'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['sub_brand1']) == False and pd.isna(m['sub_brand2']) == True and pd.isna(m['sub_brand3']) == True and pd.isna(m['sub_brand_integer']) == True:
            if m['sub_brand1'].upper() in data['product_name'].upper():
                isMatch = True
        elif pd.isna(m['sub_brand1']) == True and pd.isna(m['sub_brand2']) == True:
            isMatch = True
        
        return isMatch




    def PciMatch(self,data,mapping):
        data['official_name'] = ''
        data['pci'] = ''
        data['product_name'] = data['product_name'].replace('+','')
        for m in mapping:
            if self.checkBrand(data,m) == True:
                if self.checkSubBrand(data,m) == True:
                    if self.checkVariant(data,m) == True:
                        if self.checkGramation(data,m) == True:
                            data['official_name'] = m['official_name']
                            data['pci'] = int(float(m['pci']))
        
        return data

    def lookupPci(self,df):
        pci_mapping = [*self.reckitt_pci_mapping, *self.kalbe_pci_mapping]
        df = df.apply(lambda x: self.PciMatch(x,pci_mapping),axis=1)
        return df
  
    ## make sure every data in dataframe reference with mapping_brand table
    ## to use in lookupBrand function
    def brandMatch(self,predicted_brand,product_name,mapping_brand):
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
    
    def predictExistingBrand(self,predicted_brand, product_name):
        def predictedBrandSearch(predicted_brand, mapping):
            if predicted_brand not in mapping['where_clause']['predicted_brand']:
                return True
            else:
                return False
            

        def brandSearch(product_name, mapping):
            is_match = False
            for brand_name in mapping['where_clause']['include_brand']:
                if self.findWholeWord(brand_name.upper())(product_name.upper()):
                    is_match = True
            return is_match

        def subbrandSearch(product_name, mapping):
            is_match = False
            if len(mapping['where_clause']['include_subbrand']) != 0:
                for subbrand_name in mapping['where_clause']['include_subbrand']:
                    if self.findWholeWord(subbrand_name.upper())(product_name.upper()):
                        is_match = True
            else:
                is_match = True

            return is_match

        def unitSearch(product_name, mapping):
            is_match = False
            if len(mapping['where_clause']['include_unit']) != 0:
                product_name_sub = re.sub(r'\d',' ', product_name)
                for unit in mapping['where_clause']['include_unit']:
                    if self.findWholeWord(unit.upper())(product_name_sub .upper()):
                        is_match = True
            else:
                is_match = True
            return is_match

        def excludeNameSearch(product_name, mapping):
            is_match = False
            if len(mapping['where_clause']['exclude_product_name']) != 0:
                for exclude_pm in mapping['where_clause']['exclude_product_name']:
                    if not self.findWholeWord(exclude_pm.upper())(product_name.upper()):
                        is_match = True
            else:
                is_match = True
            return is_match

        brand = predicted_brand
        is_match = False
        for mapping in self.existing_brand_mapping:
            if is_match == False:
                if predictedBrandSearch(predicted_brand, mapping):
                    if brandSearch(product_name, mapping):
                        if subbrandSearch(product_name, mapping):
                            if unitSearch(product_name, mapping):
                                if excludeNameSearch(product_name, mapping):
                                    brand = mapping['brand']
                                    is_match = True
        return brand
    
    def segmentMatch(self,product_name):
        segment = ''
        is_match = False
        for list_mapping in self.segment_mapping:
            if self.findWholeWord(list_mapping['kw1'])(product_name.upper()):
                if pd.isnull(list_mapping['kw2']) == False:
                    if self.findWholeWord(list_mapping['kw2'])(product_name.upper()) or self.findWholeWord(list_mapping['kw3'])(product_name.upper()) or self.findWholeWord(list_mapping['kw4'])(product_name.upper()):
                        segment = list_mapping['segment']
                        is_match == True
                else:   
                    if is_match == False:
                        segment = list_mapping['segment']
                            
        return segment

    def lookupBrand(self,df,mapping_brand):
        df['predicted_brand'] = df.apply(lambda x: self.brandMatch(x['predicted_brand'],x['product_name'],mapping_brand),axis=1)
        df['predicted_brand'] = df.apply(lambda x: self.predictExistingBrand(x['predicted_brand'],x['product_name']),axis=1)
        df['segment'] = df.apply(lambda x: self.segmentMatch(x['product_name']),axis=1)
        return df
    
    


    
    ## make sure every data in dataframe reference with mapping_brand_category table
    ## to use in lookupBrandCategory function
    def brandCategoryMatch(self,df_match,mapping_brand_category):
        brand_category_id = ''
        for mbc in mapping_brand_category:
            if mbc['brand'] == df_match['predicted_brand']:
                brand_category_id = mbc['brand_category_id']
        df_match['brand_category'] = brand_category_id
        return df_match

    def lookupBrandCategory(self,df,mapping_brand_category):
        df = df.apply(lambda y: self.brandCategoryMatch(y,mapping_brand_category),axis=1)
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


    ## make sure every data in dataframe reference with mapping_product table
    ## to use in lookupProduct function
    def productMatch2(self,df_match,mps):
        for mp in mps.to_dict('records'):
            if str(int(float(mp['id_product_origin']))) == str(int(float(df_match['product_id']))):
                product_id  = mp['id_product_lixus'] 
                predicted_brand = mp['predicted_brand']
        df_match['product_id'] = product_id
        df_match['predicted_brand'] = predicted_brand
        return df_match

    def lookupProduct(self,df,mapping_product):
        df = df.apply(lambda x: self.productMatch2(x,mapping_product[mapping_product['id_product_origin']==x['product_id']]),axis=1)
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

    
    def insertSnapshotsPriceAndStock(self,CONN,df,file_date):
        list_shop_id =  df['shop_id'].unique()
        list_date = df['date'].unique()

        #query crawl_id to input where clause
        query_shop_id = ", ".join(list(map(lambda x:"\'{}\'".format(x), list_shop_id)))
        df = df.drop(columns='shop_id')

        # query list of columns to be inserted into the table
        key = list(df.columns)
        query_col = ", ".join(key)

        # query list of value to be inserted into the table
        query_value = self.setQueryValue(df)
        
        # query to get distinct date
        query_date = sql.text("""SELECT DISTINCT date FROM price_and_stock AS t
                            JOIN product_merchant_platform AS p ON t.product_id = p.product_id
                            JOIN shop AS s ON p.shop_id = s.shop_id
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
            query = sql.text("""INSERT INTO price_and_stock ({})
                            VALUES {};
                            """.format(query_col, query_value))
            CONN.execute(query)
            print("Success Insert {} {} Data To Table".format(self.store_domain,file_date))
        else:
            print("{} {} Data Already Exist".format(self.store_domain,file_date))