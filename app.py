import requests, re, time, random, asyncio, aiohttp, json, os
from rich import print
import pandas as pd
import streamlit as st
from io import BytesIO
from dotenv import find_dotenv, load_dotenv
# import snowflake.connector
# from snowflake.connector.pandas_tools import write_pandas
# from sqlalchemy import create_engine
# from snowflake.sqlalchemy import URL

# ikea_url = "https://www.ikea.com/se/sv/cat/lower-price/"
#ikea_url = "https://www.ikea.com/se/sv/cat/soffor-fu003/"

def get_total_number_of_results(keyword, max_retries=3, delay=1):
    api_request_url = "https://sik.search.blue.cdtapps.com/se/sv/search?c=listaf"
    payload = f"""{{
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

    headers = {
    'Content-Type': 'text/plain'
    }

    for attempt in range(max_retries):
        try:
            response = requests.request("POST", api_request_url, headers=headers, data=payload)
            if response.status_code == 200:
                total_number_of_products = None

                results = response.json().get('results', [])
                if results:
                    metadata = results[0].get('metadata', {})
                    if metadata:
                        total_number_of_products = metadata.get('max')
            else:
                print(f"Received status code {response.status_code}") 
            return total_number_of_products
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

        time.sleep(delay)

def split_total_into_batches(total, batch_size=1000):
    """
    Splits the total number of results into batches of specified size.

    Parameters:
    - total_number_of_results: The total number of results to split.
    - batch_size: The size of each batch.

    Returns:
    - A list of tuples, each representing a batch as (offset, size).
    """
    batches = []
    
    # Calculate the number of full batches
    full_batches = total // batch_size
    
    # Iterate to create each full batch
    for i in range(full_batches):
        offset = i * batch_size
        batches.append((offset, batch_size))
    
    # Check for any remaining items to create the last batch
    remaining_items = total % batch_size
    if remaining_items > 0:
        offset = full_batches * batch_size
        batches.append((offset, remaining_items))
    
    return batches

def get_payloads(batches):
    payloads = []
    for i, (offset, size) in enumerate(batches):
        payload_dict = {
            "searchParameters": {
                "input": keyword,
                "type": "CATEGORY"
            },
            "zip": "11152",
            "store": "669",
            "optimizely": {
                "listing_1985_mattress_guide": None,
                "listing_fe_null_test_12122023": None,
                "listing_1870_pagination_for_product_grid": None,
                "listing_2527_nlp_anchor_links": "a",
                "sik_listing_2411_kreativ_planner_desktop_default": "b",
                "sik_listing_2482_remove_backfill_plp_default": "b"
            },
            "isUserLoggedIn": False,
            "components": [{
                "component": "PRIMARY_AREA",
                "columns": 4,
                "types": {
                    "main": "PRODUCT",
                    "breakouts": ["PLANNER", "LOGIN_REMINDER"]
                },
                "filterConfig": {
                    "max-num-filters": 4
                },
                "sort": "RELEVANCE",
                "window": {
                    "offset": offset,
                    "size": size
                }
            }]
        }
        payload_json = json.dumps(payload_dict)
        payloads.append(payload_json)   
    return payloads 

# The FUNDAMENTAL difference here is that our api request url is the same for our four requests. What is different is the PAYLOAD

# NOW that we have our four different payloads, we use async requests to significantly speed up the process of fetching
# all of the dicts with the payloads

async def fetch(sem, session, url, headers, payload_json, max_retries=3, delay=1):
    async with sem:
        for attempt in range(max_retries):
            try:
                # Convert the payload_json string back to a dictionary for the json parameter.
                payload_dict = json.loads(payload_json)
                async with session.post(url, headers=headers, json=payload_dict) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Extract product data
                        product_data = []
                        results = data.get('results', [])
                        if results:
                            items = results[0].get('items', [])
                            for item in items:
                                product = item.get('product')
                                if product:
                                    product_data.append(product)
                        if product_data:
                            return product_data
                        else:
                            print(f"Product data not found in response for URL {url}.")
                            return None
                    elif response.status == 500:
                        print(f"Attempt {attempt + 1}: Server Error for URL {url}. Retrying in {delay} seconds...")
                        await asyncio.sleep(delay)
                    else:
                        print(f"Attempt {attempt + 1}: Status {response.status} for URL {url}. Retrying in {delay} seconds...")
                        await asyncio.sleep(delay)
            except aiohttp.ClientError as e:
                print(f"Request failed: {e}")
                await asyncio.sleep(delay)
        print(f"Failed to fetch {url} after {max_retries} attempts.")
        return None

async def fetch_all(url, payload_jsons, semaphore_value=10):
    sem = asyncio.Semaphore(semaphore_value)  # Control concurrency
    headers = {'Content-Type': 'application/json'}
    async with aiohttp.ClientSession() as session:
        tasks = [asyncio.create_task(fetch(sem, session, url, headers, payload_json)) for payload_json in payload_jsons]
        product_results = await asyncio.gather(*tasks)
        return product_results

def turn_list_of_dicts_into_dfs_and_clean(all_products_list):
    """
    Returns:
    - A tuple with the raw data DataFrame and the cleaned data DataFrame as (df_raw, df_clean)
    """
    df_raw = pd.json_normalize(all_products_list)

    df_raw_copy = df_raw.copy()

    df_raw_copy['Color name'] = df_raw_copy['colors'].apply(lambda x: x[0].get('name') if x and 'name' in x[0] else None)
    df_raw_copy['Color hex'] = df_raw_copy['colors'].apply(lambda x: x[0].get('hex') if x and 'hex' in x[0] else None)
    # The below one mostly applies to sofas which is what I did all testing on :))
    # df_raw_copy['Firmness'] = df_raw_copy['quickFacts'].apply(lambda x: x[0].get('name') if x and 'name' in x[0] else None)

    df_clean = df_raw_copy.loc[:,['pipUrl', 'id', 'name', 'typeName', 'mainImageUrl', 'ratingValue', 'ratingCount', 'salesPrice.current.wholeNumber', 'Color name', 'Color hex', 'mainImageAlt']]
    df_clean.rename(columns = {'pipUrl':'URL', 'id':'ID', 'name':'Name', 'typeName':'Type', 'mainImageUrl':'Image URL', 'ratingValue':'Rating value',
                            'ratingCount':'Rating count', 'salesPrice.current.wholeNumber':'Price', 'mainImageAlt':'Description'}, inplace=True)
    df_clean.reset_index(drop=True, inplace=True)

    return (df_raw, df_clean)

def generate_csv(dataframe, result_name):
    if result_name.endswith('.csv'):
        result_name = result_name
    else:
        result_name = result_name + '.csv'
    dataframe.to_csv(result_name, index=False)
    return result_name

def generate_excel(dataframe, result_name):
    if result_name.endswith('.xlsx'):
        result_name = result_name
    else:
        result_name = result_name + '.xlsx'
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        dataframe.to_excel(writer, index=False, sheet_name='Sheet1')
    output.seek(0)
    return output

### STREAMLIT CODE
st.title('IKEA to CSV Generator')
st.markdown('The URL should be a page listing all products within a product group, for example every available sofa from IKEA. It will generate a file with raw data and a file with clean data. \n\nSample URL: https://www.ikea.com/se/sv/cat/soffor-fu003/')

# User input for IKEA URL
# ikea_url = "https://www.ikea.com/se/sv/cat/soffor-fu003/"
ikea_url = st.text_input('Enter URL from the IKEA website with the products:', '')
result_name = st.text_input('Enter a name for the resulting csv/Excel file:', '')
max_products_to_scrape = st.text_input('Enter maximum amounts of products to scrape (leave blank to scrape all available jobs for the query):', '')

# Radio button to choose the file format
file_format = st.radio("Choose the file format for download:", ('csv', 'xlsx'))

# Button to the result file
if st.button('Generate File'):
    with st.spinner('Generating files, hold on'):
        if ikea_url:
            keyword = re.search(r'-([^-\s/]+)\/?$', ikea_url).group(1)
            api_request_url = "https://sik.search.blue.cdtapps.com/se/sv/search?c=listaf"

            start_time = time.time()
            total_number_of_results = get_total_number_of_results(keyword)
            print(f"Attempting to scrape {total_number_of_results} products!")

            batches = split_total_into_batches(total_number_of_results)
            print(f"Splitting {total_number_of_results} in batches: {batches}")

            payloads = get_payloads(batches)
            results = asyncio.run(fetch_all(api_request_url, payloads))
            # Flatten the list of lists into a single list of products
            products = [product for sublist in results if sublist for product in sublist]

            df_raw, df_clean = turn_list_of_dicts_into_dfs_and_clean(products)
            end_time = time.time()
            print("Done!")
            st.text(f"Done! Scraped {total_number_of_results} products in {end_time - start_time} seconds")

            if file_format == 'csv':
                result_name_raw = f"{result_name}_raw.csv"
                csv_file_raw = generate_csv(df_raw, result_name_raw)
                csv_file_clean = generate_csv(df_clean, result_name)
                with open(csv_file_raw, "rb") as file:
                    st.download_button(label="Download raw data CSV", data=file, file_name=csv_file_raw, mime='text/csv')
                with open(csv_file_clean, "rb") as file:
                    st.download_button(label="Download clean data CSV", data=file, file_name=csv_file_clean, mime='text/csv')
                st.success(f'CSV files generated: {csv_file_raw}, {csv_file_clean}')
            elif file_format == 'xlsx':
                result_name_raw = f"{result_name}_raw"
                excel_file_raw = generate_excel(df_raw, result_name_raw)
                excel_file_clean = generate_excel(df_clean, result_name)
                st.download_button(label="Download raw data Excel", data=excel_file_raw, file_name=f"{result_name}.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                st.download_button(label="Download clean data Excel", data=excel_file_clean, file_name=f"{result_name_raw}.xlsx", mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                st.success(f'Excel file generated: {result_name}.xlsx, {result_name_raw}.xlsx')

        else:
            st.error("Please enter a valid IKEA URL")


load_dotenv(find_dotenv())
snowflake_password = os.getenv("PASSWORD")

# engine = create_engine(URL(
#     account = 'steven@semurai.se',
#     user = 'STEVENLOMONSEMURAIAWS',
#     password = snowflake_password,
#     database = 'IKEA',
#     schema = 'PUBLIC',
#     warehouse = 'MY_FIRST_WAREHOUSE',
#     role = 'ACCOUNTADMIN'
# ))

# print(type(URL))

# with engine.connect() as conn:
#     df_clean.to_sql('IKEA_SOFAS', con=conn.connection, index=False, if_exists='replace')

# Code graveyard :)
# def scrape_products(keyword, offset, size, counter, total_results, max_retries=3, delay=1):
#     api_request_url = "https://sik.search.blue.cdtapps.com/se/sv/search?c=listaf"
#     payload = f"""{{
#         'searchParameters': {{
#             'input': '{keyword}',
#             'type': 'CATEGORY'
#         }},
#         'zip': '11152',
#         'store': '669',
#         'optimizely': {{
#             'listing_1985_mattress_guide': null,
#             'listing_fe_null_test_12122023': null,
#             'listing_1870_pagination_for_product_grid': null,
#             'listing_2527_nlp_anchor_links': 'a',
#             'sik_listing_2411_kreativ_planner_desktop_default': 'b',
#             'sik_listing_2482_remove_backfill_plp_default': 'b'
#         }},
#         'isUserLoggedIn': false,
#         'components': [{{
#             'component': 'PRIMARY_AREA',
#             'columns': 4,
#             'types': {{
#                 'main': 'PRODUCT',
#                 'breakouts': ['PLANNER', 'LOGIN_REMINDER']
#             }},
#             'filterConfig': {{
#                 'max-num-filters': 4
#             }},
#             'sort': 'RELEVANCE',
#             'window': {{
#                 'offset': {offset},
#                 'size': {size}
#             }}
#         }}]
#     }}"""
#     headers = {
#     'Content-Type': 'text/plain'
#     }

#     for attempt in range(max_retries):
#         try:
#             response = requests.request("POST", api_request_url, headers=headers, data=payload)
#             if response.status_code == 200:
#                 json_results = None  
#                 products = []

#                 results = response.json().get('results', [])
#                 if results:
#                     json_results = results[0].get('items', [])

#                 for item in json_results:
#                     products.append(item["product"])

#                 products = [item["product"] for item in json_results]

#                 return products
#             else:
#                print(f"Received status code {response.status_code}") 
#         except requests.exceptions.RequestException as e:
#             print(f"Request failed: {e}")

#         time.sleep(delay)
    
#     return None