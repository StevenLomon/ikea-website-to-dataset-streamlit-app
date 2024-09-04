# IKEA to CSV Generator

This README is created retroactively 6 months after having coded everything haha :')  

A web scraping app that is able to scrape an entire section of the IKEA catalog of 3270 products in 1.95 seconds:  
!["An image showing 3270 products from the IKEA catalog being scraped in 1.95 seconds"](/IKEA.png)  

The web app is up via Streamlit: https://ikea2dataset.streamlit.app/  

This is done by sending the same Post request that the front-end of the website sends to the back-end when displaying the products on the website by setting up a similar payload and using the same api request URL. This has all been fetched by inspecting the Network tab in Google Chrome and playing around with the Fetch/XHR responses in Postman. Since it's a post request, all the variables are in the payload rather than the URL string.  

An initial request is sent to the API, not to get product data but to get the total number of products for the given keyword. The total number of results received from the API response is then split into batches of 1000. Each batch will have its own payload once we actually get the product data.  

This is the only comment past Steven left in the code to share his trains of thought haha:  
"The FUNDAMENTAL difference here is that our api request url is the same for our four requests. What is different is the PAYLOAD.  
NOW that we have our four different payloads, we use async requests to significantly speed up the process of fetching
all of the dicts with the payloads"  

With our batches, the two libraries asyncio and aiohttp are then used to asynchronously fetch and extract product data for every batch. All batch tasks are gathered using fetch_all and asyncio.gather(*tasks).  
The semaphore value controls the concurrency. For instance, if a semaphore is set to 10, only 10 HTTP requests can be active at the same time. If an 11th request is made, it will be paused until one of the first 10 requests completes and releases a permit. The value should balance the need for speed (more concurrent requests) and the need for stability (fewer concurrent requests). It's adjusted based on the server's response and rate limits and after some trial and error, 10 actually ended up being a good value.  

pandas is used to clean and organize the data into both a DataFrame with the raw data and one with the cleaned data. These are then turned into a csv or xlsx depending on what is chosen by the user in the Streamlit UI.  
