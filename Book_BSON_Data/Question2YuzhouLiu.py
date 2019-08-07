"""
July 17, 2019
@author: Yuzhou Liu
"""

"""
Question 2: Convert Unstructured Data (use Python only) 
1. Load data from the following URL ((file size: ~ 30MB) 
https://s3.amazonaws.com/orim-misc-data/assessment/books.bson 
2. Extract the following data points from bson file, 
convert them into a relationship structure and export 
the final result as csv with following header. 

'title', 'primary_isbn13','asin','apple_ean','google_id','publisher',
'bisac_status','pub_date','us_li st_price','series_name','volume',
'legacy_slugs', 'image', 'description', 'retailer', 'product_uri' 

"""
#%%
# --- Package Used ---
import numpy as np
import pandas as pd
import bson
import urllib3
#%%
# Get Data
http = urllib3.PoolManager()
url = 'https://s3.amazonaws.com/orim-misc-data/assessment/books.bson'
response = http.request('GET', url)
data = bson.decode_all(response.data)
df = pd.DataFrame(data)
#%%
# Lock the target features inside the dataset
aim = set(['title', 'primary_isbn13','asin',
            'apple_ean','google_id',
            'publisher','bisac_status',
            'pub_date','us_list_price',
            'series_name','volume','legacy_slugs',
            'image', 'description',
            'retailer', 'product_uri'])
col = set(df.columns)
common = set(col).intersection(aim)

# Find featrues not overlapping with the dataset
left = aim - common
print(left)

#Find several issues 
# correct  'primary_isbn13' to  'primary_isbn' and 'us_list_price' to 'price'
# Need get 'retailer' and 'product_url' from 'retailer_site_links'

get_col = list(common) + ['primary_isbn','price','retailer_site_links']
#%%
# Break various retailers of one book from feature retailer_site_links
aim_df = df[get_col]
aim_df = aim_df.dropna(subset=['retailer_site_links'])
def keykey(d):
    v = d['6']
    return v
aim_df.retailer_site_links = aim_df.retailer_site_links.apply(keykey)
aim_df.reset_index(drop = True, inplace = True)
aim_df.index.name = 'mark'

split = pd.DataFrame(aim_df.retailer_site_links.values.tolist())
split.index.name = "mark"

split = pd.DataFrame(split.stack())

aim_df = aim_df.join(split, how='inner').drop('retailer_site_links',axis= 1)
#%%
# Break retailer names and product url
split2 = pd.DataFrame(aim_df.iloc[:,-1].values.tolist())
split2.index.name = 'mark'
aim_df = aim_df.drop(aim_df.columns[-1],axis= 1)
aim_df = aim_df.join(split2, how='inner')

#%%
# Clear up the DataFrame
aim_df = aim_df.rename(columns = {'name':'retailer','url':'product_url','price':'us_list_price'})
aim = ['title', 'primary_isbn','asin',
            'apple_ean','google_id',
            'publisher','bisac_status',
            'pub_date','us_list_price',
            'series_name','volume','legacy_slugs',
            'image', 'description',
            'retailer', 'product_url']
aim_df = aim_df[aim]
#%%
# Write csv file
aim_df.to_csv('question2output.csv', index = False)