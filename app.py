import requests, re
from rich import print
import pandas as pd

# ikea_url = "https://www.ikea.com/se/sv/cat/lower-price/"
ikea_url = "https://www.ikea.com/se/sv/cat/soffor-fu003/"
keyword = re.search(r'-([^-\s/]+)\/?$', ikea_url).group(1)
print(keyword)

api_request_url = "https://sik.search.blue.cdtapps.com/se/sv/search?c=listaf"

#payload = "{\"searchParameters\":{\"input\":\"fu003\",\"type\":\"CATEGORY\"},\"zip\":\"11152\",\"store\":\"669\",\"optimizely\":{\"listing_1985_mattress_guide\":null,\"listing_fe_null_test_12122023\":null,\"listing_1870_pagination_for_product_grid\":null,\"listing_2527_nlp_anchor_links\":\"a\",\"sik_listing_2411_kreativ_planner_desktop_default\":\"b\",\"sik_listing_2482_remove_backfill_plp_default\":\"b\"},\"isUserLoggedIn\":false,\"components\":[{\"component\":\"PRIMARY_AREA\",\"columns\":4,\"types\":{\"main\":\"PRODUCT\",\"breakouts\":[\"PLANNER\",\"LOGIN_REMINDER\"]},\"filterConfig\":{\"max-num-filters\":4},\"sort\":\"RELEVANCE\",\"window\":{\"offset\":12,\"size\":48}}]}"
first_payload = f"""{{
    'searchParameters': {{
        'input': '{keyword}',
        'type': 'CATEGORY'
    }},
    'zip': '11152',
    'store': '669',
    'optimizely': {{
        'listing_1985_mattress_guide': null,
        'listing_fe_null_test_12122023': null,
        'listing_1870_pagination_for_product_grid': null,
        'listing_2527_nlp_anchor_links': 'a',
        'sik_listing_2411_kreativ_planner_desktop_default': 'b',
        'sik_listing_2482_remove_backfill_plp_default': 'b'
    }},
    'isUserLoggedIn': false,
    'components': [{{
        'component': 'PRIMARY_AREA',
        'columns': 4,
        'types': {{
            'main': 'PRODUCT',
            'breakouts': ['PLANNER', 'LOGIN_REMINDER']
        }},
        'filterConfig': {{
            'max-num-filters': 4
        }},
        'sort': 'RELEVANCE',
        'window': {{
            'offset': 0,
            'size': 12
        }}
    }}]
}}"""

# print(type(payload))
headers = {
  'Content-Type': 'text/plain'
}

response = requests.request("POST", api_request_url, headers=headers, data=first_payload)

total_number_of_products = None

results = response.json().get('results', [])
if results:
    metadata = results[0].get('metadata', {})
    if metadata:
        total_number_of_products = metadata.get('max')

print(total_number_of_products)

new_payload = f"""{{
    'searchParameters': {{
        'input': '{keyword}',
        'type': 'CATEGORY'
    }},
    'zip': '11152',
    'store': '669',
    'optimizely': {{
        'listing_1985_mattress_guide': null,
        'listing_fe_null_test_12122023': null,
        'listing_1870_pagination_for_product_grid': null,
        'listing_2527_nlp_anchor_links': 'a',
        'sik_listing_2411_kreativ_planner_desktop_default': 'b',
        'sik_listing_2482_remove_backfill_plp_default': 'b'
    }},
    'isUserLoggedIn': false,
    'components': [{{
        'component': 'PRIMARY_AREA',
        'columns': 4,
        'types': {{
            'main': 'PRODUCT',
            'breakouts': ['PLANNER', 'LOGIN_REMINDER']
        }},
        'filterConfig': {{
            'max-num-filters': 4
        }},
        'sort': 'RELEVANCE',
        'window': {{
            'offset': 0,
            'size': 100
        }}
    }}]
}}"""

response = requests.request("POST", api_request_url, headers=headers, data=new_payload)

first_100_items = None

results = response.json().get('results', [])
if results:
    first_100_items = results[0].get('items', [])

products = [item["product"] for item in first_100_items]

df = pd.json_normalize(products)

df['Color name'] = df['colors'].apply(lambda x: x[0]['name'] if x else None)
df['Color hex'] = df['colors'].apply(lambda x: x[0]['hex'] if x else None)
df['Firmness'] = df['quickFacts'].apply(lambda x: x[0]['name'] if x else None)

df_final = df.loc[:,['pipUrl', 'id', 'name', 'typeName', 'mainImageUrl', 'ratingValue', 'ratingCount', 'salesPrice.current.wholeNumber', 'Color name', 'Color hex', 'Firmness', 'mainImageAlt']]
df_final.rename(columns = {'pipUrl':'URL', 'id':'ID', 'name':'Name', 'typeName':'Type', 'mainImageUrl':'Image URL', 'ratingValue':'Rating value',
                           'ratingCount':'Rating count', 'salesPrice.current.wholeNumber':'Price', 'mainImageAlt':'Description'}, inplace=True)

print(df_final)