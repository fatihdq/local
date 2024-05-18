from sqlalchemy import sql
import pandas as pd
import numpy as np
import re
import datetime
import json

class FunctionOrderTokopedia(object):
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

    def removeDuplicatesData(self,df,column):
        df = df.drop_duplicates(subset=column,keep='first')
        return df.reset_index(drop=True)

    def removeNullValue(self,df,column):
        df = df[(df[column]!= '') & (df[column]!= None) & (df[column]!= np.nan) & (df[column]!='NaN') & (pd.isna(df[column])!=True)]
        return df.reset_index(drop=True)

    def findWholeWord(self,w):
        return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search

    def remove_emoji(self,string):
        emoji_pattern = re.compile("["
                                u"\U0001F600-\U0001F64F"  # emoticons
                                u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                                u"\U0001F680-\U0001F6FF"  # transport & map symbols
                                u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                                u"\U00002702-\U000027B0"
                                u"\U000024C2-\U0001F251"
                                "]+", flags=re.UNICODE)
        try:
            result = emoji_pattern.sub(r'', string)
        except :
            result = emoji_pattern.sub(r'', string.decode())
        return result

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
    
    def handlingNumeric(self,value,dtype):
        if dtype == 'rp':
            try:
                value = int(str(value).replace(r'Rp ','').replace(r'.',''))
            except:
                value = 0
        elif dtype == 'gr':
            try:
                value = int(str(value).replace(r' gr',''))
            except:
                value = 0
        return value

    def basicTransform(self,df):
        column_order_null = ['order_id','payment_datetime','order_status','shipping_cost_estimation','insurance','shipping_cost','total_payment','username','phone','recipient_name', 'address', 'city', 'province', 'shipping_option', 'no_reciept']
        column_null_str = ['sku','note','recipient_name','phone','address','city','free_shipping','discount_package','bundling_package']
        column_numeric = ['bundling_unit_price','discount_from_seller','weight','total_weight','voucher_seller','voucher_platform','cashback_coin','discount_package_platform','discount_package_seller','discount_coin','discount_credit_card','discount_shipping']
        column_str = ['product_name','username','note','recipient_name','address']
        column_nontunai = ['voucher_seller','total_discount','discount_from_platform','shipping_cost_estimation','insurance','shipping_cost']
        column_additional_str = ['bundling_package','discount_package','free_shipping']
        column_additional_numeric = ['bundling_unit_price','discount_from_seller','weight','total_weight','voucher_seller','voucher_platform','cashback_coin','discount_package_platform','discount_package_seller','discount_coin','discount_credit_card','discount_shipping']
        dtype_mapping = {'order_id':'str','order_status':'str','product_name':'str','sku':'str','note':'str','username':'str','phone':'str','recipient_name':'str','address':'str','shipping_option':'str','no_reciept':'str','free_shipping':'str',
                        'quantity':'int','initial_price':'int','total_discount':'int','discount_from_platform':'int','shipping_cost_estimation':'int','insurance':'int','shipping_cost':'int','total_payment':'int'}
        # Rename Columns
        #######################        
        if 'Invoice' in df.columns:
            if 'Notes' not in df.columns:
                df['Notes'] = ''
            df_rename = df.rename(columns={'Invoice':'order_id',
                                        'Payment Date':'payment_datetime',
                                        'Order Status':'order_status',
                                        'Product Name':'product_name',
                                        'Quantity':'quantity',
                                        'Stock Keeping Unit (SKU)':'sku',
                                        'Notes':'note',
                                        'Discount Amount (Rp.)':'total_discount',
                                        'Subsidi Amount (Rp.)':'discount_from_platform',
                                        'Customer Name':'username',
                                        'Customer Phone':'phone',
                                        'Recipient':'recipient_name',
                                        'Recipient Address':'address',
                                        'Courier':'shipping_option',
                                        'Shipping Price + fee (Rp.)':'shipping_cost_estimation',
                                        'Insurance (Rp.)':'insurance',
                                        'Total Shipping Fee (Rp.)':'shipping_cost',
                                        'Total Amount (Rp.)':'total_payment',
                                        'AWB':'no_reciept',
                                        'Bebas Ongkir':'free_shipping'})
            
            required_columns = ['order_id','payment_datetime','order_status','product_name','quantity','sku','note','total_discount','discount_from_platform','username','phone','recipient_name','address','shipping_option','shipping_cost_estimation','insurance','shipping_cost','total_payment','no_reciept','free_shipping']
            additional_columns = {'Harga Awal (Rp.)':'initial_price','Price (Rp.)':'initial_price','Harga Jual (Rp.)':'selling_price','Harga Satuan Bundling (Rp.)':'bundling_unit_price','Paket Bundling':'bundling_package'}
        
        elif 'Nomor Invoice' in df.columns:
            if 'Catatan produk pembeli' not in df.columns:
                df['Catatan produk pembeli'] = ''
            
            if 'Nilai Voucher Toko Terpakai (IDR)' in df.columns:
                df_rename = df.rename(columns={'Nomor Invoice':'order_id',
                                        'Tanggal Pembayaran':'payment_datetime',
                                        'Status Terakhir':'order_status',
                                        'Nama Produk':'product_name',
                                        'Nomor SKU':'sku',
                                        'Catatan produk pembeli':'note',
                                        'Jumlah Produk Dibeli':'quantity',
                                        'Harga Awal (IDR)':'initial_price',
                                        'Diskon Produk (IDR)':'total_discount',
                                        'Harga Jual (IDR)':'selling_price',
                                        'Jumlah Subsidi Tokopedia (IDR)':'discount_from_platform',
                                        'Nilai Voucher Toko Terpakai (IDR)':'voucher_seller',
                                        'Biaya Pengiriman Tunai (IDR)':'shipping_cost_estimation',	
                                        'Biaya Asuransi Pengiriman (IDR)':'insurance',
                                        'Total Biaya Pengiriman (IDR)':'shipping_cost',
                                        'Total Penjualan (IDR)':'total_payment',
                                        'Nama Pembeli':'username',	
                                        'No Telp Pembeli':'phone',
                                        'Nama Penerima':'recipient_name',
                                        'Alamat Pengiriman':'address',	
                                        'Kota':'city',
                                        'Provinsi':'province',
                                        'Nama Kurir':'shipping_option',	
                                        'No Resi / Kode Booking':'no_reciept',
                                        'Waktu Pengiriman Barang':'delivery_time'})
            else: 
                df_rename = df.rename(columns={'Nomor Invoice':'order_id',
                                        'Tanggal Pembayaran':'payment_datetime',
                                        'Status Terakhir':'order_status',
                                        'Nama Produk':'product_name',
                                        'Nomor SKU':'sku',
                                        'Catatan produk pembeli':'note',
                                        'Jumlah Produk Dibeli':'quantity',
                                        'Harga Awal (IDR)':'initial_price',
                                        'Diskon Produk (IDR)':'total_discount',
                                        'Harga Jual (IDR)':'selling_price',
                                        'Jumlah Subsidi Tokopedia (IDR)':'discount_from_platform',
                                        'Nilai Kupon Toko Terpakai (IDR)':'voucher_seller',
                                        'Biaya Pengiriman Tunai (IDR)':'shipping_cost_estimation',	
                                        'Biaya Asuransi Pengiriman (IDR)':'insurance',
                                        'Total Biaya Pengiriman (IDR)':'shipping_cost',
                                        'Total Penjualan (IDR)':'total_payment',
                                        'Nama Pembeli':'username',	
                                        'No Telp Pembeli':'phone',
                                        'Nama Penerima':'recipient_name',
                                        'Alamat Pengiriman':'address',	
                                        'Kota':'city',
                                        'Provinsi':'province',
                                        'Nama Kurir':'shipping_option',	
                                        'No Resi / Kode Booking':'no_reciept',
                                        'Waktu Pengiriman Barang':'delivery_time'})
            
            required_columns = ['order_id','payment_datetime','order_status','product_name','sku','note','quantity','initial_price','total_discount','selling_price','discount_from_platform','voucher_seller','shipping_cost_estimation','insurance','shipping_cost','total_payment','username','phone','recipient_name','address','city','province','shipping_option','no_reciept']
            additional_columns = {'Harga Satuan Bundling (Rp.)':'bundling_unit_price','Paket Bundling':'bundling_package'}
        for key,value in additional_columns.items():
            if key in df.columns:
                df_rename = df_rename.rename(columns={key:value})
                required_columns.append(value)
        
        # select require colums
        #######################
        df_selected = df_rename[required_columns]
        
        # filter complete
        #######################
        df_complete = df_selected[(df_selected['order_status'] == 'Transaksi selesai..\nDana akan diteruskan ke penjual.') | (df_selected['order_status'] == 'Pesanan Selesai')].reset_index(drop=True)


        # Handling_datetime
        ######################
        df_datetime = df_complete.copy()
        df_datetime['payment_datetime'] = df_datetime['payment_datetime'].apply(lambda x: datetime.datetime.strptime(x,'%d-%m-%Y %H:%M:%S').strftime('%Y-%m-%d %H:%M:%S'))
        df_datetime['complete_datetime'] = df_datetime['payment_datetime']
        df_datetime['created_datetime'] = df_datetime['payment_datetime']
        df_datetime['delivery_datetime'] = df_datetime['payment_datetime']

        # add additional columns
        #######################
        df_added_columns = df_datetime.copy()
        for col in column_additional_str:
            if col not in df_added_columns.columns:
                df_added_columns.loc[:,col] = ''
        for col in column_additional_numeric:
            if col not in df_added_columns.columns:
                df_added_columns.loc[:,col] = 0

        # if row order null
        #######################
        df_order_null = df_added_columns.copy()
        for idx in range(len(df_order_null)):
            if pd.isnull(df_order_null.loc[idx,'order_id']) or pd.isnull(df_order_null.loc[idx,'username']):
                for col in column_order_null:
                    df_order_null.loc[idx,col] = df_order_null.loc[idx-1,col]
        
        # Handling str
        ##################
        df_handling_str = df_order_null.copy()
        for col in column_str:
            df_handling_str[col] = df_handling_str[col].apply(lambda x: re.sub(r"[\"#@;:<>{}`+=~|.!'?]", "",x))

        df_handling_str['address'] = df_handling_str['address'].apply(lambda x: '' if pd.isna(x) else x)
        df_handling_str['city'] = df_handling_str['city'].apply(lambda x: '' if pd.isna(x) else x)
        df_handling_str['province'] = df_handling_str['province'].apply(lambda x: '' if pd.isna(x) else x)
        # Handling Non Tunai
        ##################
        df_handling_non_tunai = df_handling_str.copy()
        for col in column_nontunai:
            df_handling_non_tunai[col] = df_handling_non_tunai[col].apply(lambda x: '0' if x == 'Non Tunai' or pd.isna(x) or x == '' else x) 


        # Change dtypes
        ###################
        df_changed_dtype = df_handling_non_tunai.copy() 
        for mKey, mDtype in dtype_mapping.items():
            df_changed_dtype[mKey] = df_changed_dtype[mKey].apply(lambda x: self.changeDtype(x,mDtype))

        # Aggregation
        #####################
        df_agg = df_changed_dtype.copy()
        if 'selling_price' not in df_agg.columns:
            df_agg['selling_price'] = df_agg['initial_price']
        
        df_agg['price_after_discount'] = df_agg.loc[:,'initial_price'] - df_agg.loc[:,'total_discount']
        df_agg['total_price'] = df_agg.loc[:,'selling_price'] * df_agg.loc[:,'quantity']
        df_agg['total_price_after_discount_transaction'] = df_agg.loc[:,'total_price']

        # Get total quantity
        ######################
        df_total_quantity = df_agg.copy()
        order_duplicates = df_total_quantity[df_total_quantity.duplicated('order_id',keep=False)]['order_id'].tolist()
        total_quantity = {}
        for od in order_duplicates:
            total_quantity[od] = df_total_quantity[df_total_quantity['order_id']==od][['quantity']].values.sum()
        for idx in range(len(df_total_quantity)):
            bol = False
            for od in order_duplicates:
                if df_total_quantity.loc[idx,'order_id'] == od:
                    df_total_quantity.loc[idx,'order_quantity'] = total_quantity[od]
                    bol = True
            if bol == False:
                df_total_quantity.loc[idx,'order_quantity'] = df_total_quantity.loc[idx,'quantity']
        df_total_quantity['order_quantity'] = df_total_quantity['order_quantity'].astype(int)

        ## Input Basic Value
        #####################
        df_basic_value = df_total_quantity.copy()
        df_basic_value.loc[:,'shop_name'] = self.store_domain
        df_basic_value.loc[:,'shop_lixus'] = self.store_lixus
        df_basic_value.loc[:,'platform'] = self.platform
        df_basic_value.loc[:,'shop_category'] = self.store_category

        return df_basic_value

    
    ## Fetching data mappingform database
    def getMappingTableFromDB(self,CONN,df):
         # get list distinct shop_name and product_name
        list_shop_name = df['shop_name'].unique()
        list_product_name = df['product_name'].unique()
        
        # string query for shop_name
        query_shop_name = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_shop_name)))
        query_product_name = ", ".join(list(map(lambda x:"\'{}\'".format(x),list_product_name)))
        
        #query to get brand category data 
        text_query = sql.text("""SELECT * FROM mapping_brand_category""")
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_brand_category = [{'brand_category_id':arr['brand_category_id'],'brand_category_name':arr['brand_category_id'],'brand':arr['brand']} for arr in result_query]

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

        #query to get brand data based on category
        text_query = sql.text("""SELECT * FROM mapping_brand
                              WHERE category = '{}' """.format(self.store_category))
        result_query = CONN.execute(text_query).fetchall()
        db_mapping_brand = [{'predict_name':arr['predict_name'],'brand':arr['brand'],'category':arr['category'],'shop':arr['shop']} for arr in result_query]

        ## query to get data from mapping_product table based on product_name, shop_domain, and platform
        query = sql.text("""SELECT * from mapping_product mp
                                join mapping_shop ms on mp.id_shop_lixus= ms.id_shop_lixus 
                                WHERE mp.product_name in ({})
                                AND ms.shop_domain in ({}) AND ms.platform = '{}'; 
                            """.format(query_product_name,query_shop_name,self.platform))  
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
    

    def get_first_char(self,line):
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

    ## make sure every data in dataframe reference with mapping_product table
    ## to use in mappingProduct function
    def productMatch(self,df,dmp,db_mapping_brand,shop_id):
            bol = False
            new_product = {}
            for mp in dmp:
                if df['product_name'] == mp['product_name']:
                    new_product['id_product_origin'] = mp['id_product_origin']
                    new_product['id_product_lixus'] = mp['id_product_lixus']
                    new_product['product_name'] = df['product_name']
                    new_product['id_shop_lixus'] = mp['id_shop_lixus']
                    new_product['sku'] = df['sku']
                    new_product['platform'] =  mp['platform']
                    new_product['category'] =  mp['category']
                    if mp['predicted_brand'] == '' or mp['predicted_brand'] == 'nan' or mp['predicted_brand'] == 'UNKNOWN' or mp['predicted_brand'] == 'NULL' or mp['predicted_brand'] == 'null' or mp['predicted_brand'] == None or pd.isnull(mp['predicted_brand']):
                        new_product['predicted_brand'] = self.getBrand(db_mapping_brand,new_product['product_name'])
                    else:
                        new_product['predicted_brand'] = mp['predicted_brand']
                    bol = True
            if bol == False:
                new_product['id_product_origin'] = "{}-{}".format(str(self.get_first_char(df['product_name'])),str(shop_id))
                new_product['id_product_lixus'] = "{}-{}".format(str(self.get_first_char(df['product_name'])),str(shop_id))
                new_product['product_name'] = df['product_name']
                new_product['id_shop_lixus'] = shop_id
                new_product['sku'] = df['sku']
                new_product['platform'] =  self.platform
                new_product['category'] =  self.store_category
                new_product['predicted_brand'] = self.getBrand(db_mapping_brand,new_product['product_name'])
            
            
            return new_product

    def mappingProduct(self,df,db_mapping_brand, db_mapping_product):
        # check and update data for mapping_product table
        keys = ['product_name','sku','shop_name']
        df_product = df[keys]
        shop_id = db_mapping_product[0]['id_shop_lixus']
        
        mapping_product = pd.DataFrame.from_dict(list(df_product.apply(lambda y: self.productMatch(y,db_mapping_product,db_mapping_brand,shop_id),axis=1)),orient='columns')
        return mapping_product

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
            if str(mp['product_name']) == str(df_match['product_name']):
                product_id  = mp['id_product_lixus'] 
                predicted_brand = mp['predicted_brand']
        df_match['product_id'] = product_id
        df_match['predicted_brand'] = predicted_brand
        return df_match

    def lookupProduct(self,df,mapping_product):
        df = df.apply(lambda x: self.productMatch2(x,mapping_product[mapping_product['product_name']==x['product_name']]),axis=1)
        return df
    
    ## Get ID
    ######################
    def getId(self,df):    
        df['order_product_id'] = df.apply(lambda x: '{}-{}-{}-{}{}'.format(self.platform_id,str(x['order_id']),str(x['product_id']),str(datetime.datetime.strptime(x['payment_datetime'],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d')),str(self.get_first_char(x['bundling_package']))) if x['bundling_package'] != '' else '{}-{}-{}-{}'.format(self.platform_id,str(x['order_id']),str(x['product_id']),str(datetime.datetime.strptime(x['payment_datetime'],'%Y-%m-%d %H:%M:%S').strftime('%Y%m%d'))),axis=1)
        df['order_id'] = df['order_id'].apply(lambda x: '{}-{}'.format(self.platform_id,str(x)))
        df['customer_id'] = df.apply(lambda x: '{}-{}{}'.format(self.platform_id,str(x['username']),str(x['phone'][-4:])),axis=1)
        
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
    
    def insertSnapshotsTraffic(self,CONN,df,file_date):
        list_shop_id =  df['shop_id'].unique()
        list_date = df['created_datetime'].unique()

        start_date = min(list_date)
        end_date = max(list_date)

        #query crawl_id to input where clause
        query_shop_id = ", ".join(list(map(lambda x:"\'{}\'".format(x), list_shop_id)))
        df = df.drop(columns='shop_id')

        # query list of columns to be inserted into the table
        key = list(df.columns)
        query_col = ", ".join(key)

        # query list of value to be inserted into the table
        query_value = self.setQueryValue(df)
        
        # query to get distinct date
        
        query_date = sql.text("""SELECT DISTINCT created_datetime FROM order_product AS op
                            JOIN product_merchant_platform AS p ON op.product_id = p.product_id
                            JOIN shop AS s ON p.shop_id = s.shop_id
                            WHERE s.shop_id IN ({})
                            and created_datetime between '{}' and '{}';
                        """.format(query_shop_id,start_date,end_date))

        result_date = CONN.execute(query_date).fetchall()
        mapping_date = [date['created_datetime'].strftime('%Y-%m-%d') for date in result_date]

      
        
        if len(mapping_date) == 0:
            
            query = sql.text("""INSERT INTO order_product ({})
                            VALUES {};
                            """.format(query_col, query_value))
            CONN.execute(query)
            print("Success Insert {} from {} to {} Data To Table".format(self.store_domain,start_date,end_date))
        else:
            print("{} from {} to {} Data Already Exist".format(self.store_domain,start_date,end_date))
            query_delete = sql.text("""delete from order_product op 
                                    where created_datetime between '{}' and '{}'
                                    and product_id in (
                                    select product_id  from product_merchant_platform pmp 
                                    where shop_id = {}
                                    );
                                    """.format(start_date,end_date, query_shop_id))
                                    
            CONN.execute(query_delete)
            
            print("Delete {} from {} to {} ".format(self.store_domain,start_date,end_date))
            query = sql.text("""INSERT INTO order_product ({})
                                    VALUES {};
                                    """.format(query_col, query_value))


            CONN.execute(query)
            print("Success Insert {} from {} to {} Data To Table".format(self.store_domain,start_date,end_date))