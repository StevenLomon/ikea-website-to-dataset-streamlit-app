import requests, re, time, random, asyncio, aiohttp, json, os, datetime
from rich import print
import pandas as pd
from datetime import datetime
from app import get_total_number_of_results, split_total_into_batches, get_payloads, fetch, fetch_all, generate_csv

ikea_url = "https://www.ikea.com/se/sv/new/new-products/"
result_name = f"ikea_new_products_{datetime.now().strftime('%m_%d_%Y_%H:%M:%S')}.csv"

keyword = re.search(r'-([^-\s/]+)\/?$', ikea_url).group(1)
api_request_url = "https://sik.search.blue.cdtapps.com/se/sv/search?c=listaf"

total_number_of_results = get_total_number_of_results(keyword)
batches = split_total_into_batches(total_number_of_results)

payloads = get_payloads(keyword, batches)
results = asyncio.run(fetch_all(api_request_url, payloads))

products = [product for sublist in results if sublist for product in sublist]

df_ikea = pd.json_normalize(products)
df_ikea['Color name'] = df_ikea['colors'].apply(lambda x: x[0].get('name') if x and 'name' in x[0] else None)
df_ikea['Color hex'] = df_ikea['colors'].apply(lambda x: x[0].get('hex') if x and 'hex' in x[0] else None)

df_ikea = df_ikea.loc[:,['pipUrl', 'id', 'name', 'typeName', 'mainImageUrl', 'ratingValue', 'ratingCount', 'salesPrice.current.wholeNumber', 'Color name', 'Color hex', 'mainImageAlt']]
df_ikea.rename(columns = {'pipUrl':'URL', 'id':'ID', 'name':'Name', 'typeName':'Type', 'mainImageUrl':'Image URL', 'ratingValue':'Rating value',
                        'ratingCount':'Rating count', 'salesPrice.current.wholeNumber':'Price', 'mainImageAlt':'Description'}, inplace=True)
df_ikea.reset_index(drop=True, inplace=True)

df_ikea.to_csv(result_name)