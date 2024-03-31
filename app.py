import requests, re, time, random
from rich import print
import pandas as pd
import streamlit as st
from io import BytesIO
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

def scrape_products(keyword, offset, size, counter, total_results, progress_bar, text_placeholder, max_retries=3, delay=1):
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
                'offset': {offset},
                'size': {size}
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
                json_results = None  
                products = []

                results = response.json().get('results', [])
                if results:
                    json_results = results[0].get('items', [])

                for item in json_results:
                    products.append(item["product"])

                    # Update the progress bar and text after each job posting is processed
                    progress = counter / total_results
                    progress = min(max(progress, 0.0), 1.0)  # Clamp the progress value
                    progress_bar.progress(progress)
                    text_placeholder.text(f"Processing {counter} / {total_results}")
                    counter += 1

                products = [item["product"] for item in json_results]

                return products
            else:
               print(f"Received status code {response.status_code}") 
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")

        time.sleep(delay)
    
    return None

def turn_list_of_dicts_into_dfs_and_clean(all_products_list):
    """
    Returns:
    - A tuple with the raw data DataFrame and the cleaned data DataFrame as (df_raw, df_clean)
    """
    df_raw = pd.json_normalize(all_products_list)

    df_raw_copy = df_raw.copy()

    df_raw_copy['Color name'] = df_raw_copy['colors'].apply(lambda x: x[0].get('name') if x and 'name' in x[0] else None)
    df_raw_copy['Color hex'] = df_raw_copy['colors'].apply(lambda x: x[0].get('hex') if x and 'hex' in x[0] else None)
    df_raw_copy['Firmness'] = df_raw_copy['quickFacts'].apply(lambda x: x[0].get('name') if x and 'name' in x[0] else None)

    df_clean = df_raw_copy.loc[:,['pipUrl', 'id', 'name', 'typeName', 'mainImageUrl', 'ratingValue', 'ratingCount', 'salesPrice.current.wholeNumber', 'Color name', 'Color hex', 'Firmness', 'mainImageAlt']]
    df_clean.rename(columns = {'pipUrl':'URL', 'id':'ID', 'name':'Name', 'typeName':'Type', 'mainImageUrl':'Image URL', 'ratingValue':'Rating value',
                            'ratingCount':'Rating count', 'salesPrice.current.wholeNumber':'Price', 'mainImageAlt':'Description'}, inplace=True)
    df_clean.reset_index(drop=True, inplace=True)

    return (df_raw, df_clean)

def scrape_all_products_and_show_progress(keyword, batches, total_results, progress_bar, text_placeholder):
    all_products = []
    counter = 0

    for i, (offset, size) in enumerate(batches):
        print(f"Request #{i+1}")
        products = scrape_products(keyword, offset, size, counter, total_results, progress_bar, text_placeholder)
        time.sleep(random.randint(3,5))
        all_products.extend(products)
    
    print("Done!")

    df_raw, df_clean = turn_list_of_dicts_into_dfs_and_clean(all_products)

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
st.title('IKEA  to CSV Generator')

# User input for LinkedIn URL
ikea_url = st.text_input('Enter URL from the IKEA website with the products:', '')
result_name = st.text_input('Enter a name for the resulting csv/Excel file:', '')
max_products_to_scrape = st.text_input('Enter maximum amounts of products to scrape (leave blank to scrape all available jobs for the query):', '')

# Radio button to choose the file format
file_format = st.radio("Choose the file format for download:", ('csv', 'xlsx'))

# Button to the result file
if st.button('Generate File'):
    if ikea_url:
        # Loading bar
        progress_bar = st.progress(0)
        text_placeholder = st.empty()
        
        keyword = re.search(r'-([^-\s/]+)\/?$', ikea_url).group(1)
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
        'csrf-token': 'ajax:5371233139676576627',
        'Cookie': 'bcookie="v=2&21324318-35a4-4b89-8ccd-66085ea456e6"; li_mc=MTsyMTsxNzExMjc2MTc0OzI7MDIxe9WcWZ2d6Bt7L96zCLaBjXpfuxnqB2ora17i0MVkktc=; lidc="b=VB74:s=V:r=V:a=V:p=V:g=4154:u=247:x=1:i=1711257936:t=1711297019:v=2:sig=AQEI3UFEfjQrzprvxRtR2ODZ2EXxFVpB"; sdsc=22%3A1%2C1711273501254%7EJAPP%2C08tO5%2Fcka%2F8fklcFLQeSLJeOemic%3D; JSESSIONID="ajax:5371233139676576627"; bscookie="v=1&202403141230369a2ffb3d-11be-445e-8196-32de3e951a31AQFV3WHayzR8g95w6TJ6LrZlOyXvi0m3"; g_state={"i_l":0}; li_alerts=e30=; li_at=AQEDASvMh7YFmyS7AAABjmrnuugAAAGOjvQ-6E0AY1fC-ANVhrSwjiNiqIhKYZ1Xib5nml6YE96LyvaMY3LATaVjueFFrqG8UXQNJz_kxu4qPIr20m8fm4URdNFCas5wngLRy2k8BJPw8UGUqCaqXKD7; li_g_recent_logout=v=1&true; li_rm=AQHjnJLrN-yKBQAAAY5q4y9R8BRBllyhPbBn5d_YYX2L59W6HxE_DqKNA8I0kMJ65IWgm2p2lw6Nr-GtGaWvKLjdLWcGo7lk7TxomWVYVRCBBwCg0vdKIUKRO5r3HtOd-9SY1a3tgovir_swKutrRj18DIt1HyV6JLLjK7r_2_Q3Y17vc2CH16R-MR9JvdZ43vTF0Y3FC9phhH2YQIfsbFlThT369bNJPiiDf9KdkGjeERmZH7RAG2iu0b7jY6iAidzkyplMV_nmlyqO_-v-2dRjfqjTYSjZwx0D046PpPzLEu1Vy7RK5SBlfPOm2djsHD8H4sQ32JlCErdlwYI; li_theme=light; li_theme_set=app; timezone=Europe/Stockholm'
        }

        start_time = time.time()
        total_number_of_results = get_total_number_of_results(keyword)
        print(f"Attempting to scrape {total_number_of_results} products!")

        batches = split_total_into_batches(total_number_of_results)
        print(f"Splitting {total_number_of_results} in batches: {batches}")

        df_raw, df_clean = scrape_all_products_and_show_progress(keyword, batches, total_number_of_results, progress_bar, text_placeholder)
        end_time = time.time()
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




        


# engine = create_engine(URL(
#     account = 'steven@semurai.se',
#     user = 'STEVENLOMONSEMURAIAWS',
#     password = 'CbYBF$o7r8$t3?Jt',
#     database = 'IKEA',
#     schema = 'PUBLIC',
#     warehouse = 'MY_FIRST_WAREHOUSE',
#     role = 'ACCOUNTADMIN'
# ))

# print(type(URL))

# with engine.connect() as conn:
#     df_clean.to_sql('IKEA_SOFAS', con=conn.connection, index=False, if_exists='replace')

