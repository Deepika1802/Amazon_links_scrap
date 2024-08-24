import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random

# Function to scrape product links from a category page
def get_amazon_product_links(category_url, max_items=100):
    user_agents = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.5735.110 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.5672.64 Safari/537.36"
    ]
    
    product_links = []
    page = 1
    retries = 5  # Number of retries allowed for 503 errors

    while len(product_links) < max_items:
        headers = {
            "User-Agent": random.choice(user_agents),  # Randomly choose a Chrome user agent
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "DNT": "1"
        }

        url = f"{category_url}&page={page}"
        
        try:
            response = requests.get(url, headers=headers, timeout=10)  # Timeout after 10 seconds
            
            if response.status_code == 503:
                if retries > 0:
                    print(f"503 error. Retrying... ({retries} attempts left)")
                    retries -= 1
                    time.sleep(random.randint(5, 15))  
                    continue
                else:
                    print(f"Failed after multiple retries for URL: {url}")
                    break
            
            if response.status_code != 200:
                print(f"Failed to retrieve page {page} for URL: {url} (Status Code: {response.status_code})")
                break
            
            soup = BeautifulSoup(response.content, "html.parser")
            
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                if '/dp/' in href and href not in product_links:
                    product_links.append("https://www.amazon.in" + href)
                    if len(product_links) >= max_items:
                        break
            
            page += 1
            retries = 5  
            time.sleep(random.randint(5, 15))  
        
        except requests.exceptions.Timeout:
            print("Request timed out. Moving to the next page.")
            continue
        except requests.exceptions.RequestException as e:
            print(f"Error occurred: {e}")
            break
    
    return product_links

# Function to scrape product links from multiple categories
def scrape_amazon_categories(categories):
    all_data = []

    for category_name, category_url in categories.items():
        print(f"Scraping category: {category_name}")
        product_links = get_amazon_product_links(category_url)
        if product_links:  
            category_data = {
                "Category": category_name,
                "Product_Link": product_links
            }
            df_category = pd.DataFrame(category_data)
            all_data.append(df_category)
        else:
            print(f"No links retrieved for category: {category_name}")

    df_all_categories = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    return df_all_categories

# Updated categories with new ones
categories = {
   "Pet Health": "https://www.amazon.in/s?k=Pet+Health",
   "Pet Cleaning": "https://www.amazon.in/s?k=Pet+Cleaning",
   "Drums": "https://www.amazon.in/s?k=Drums",
   "Diaries": "https://www.amazon.in/s?k=Diaries",
   "Home Improvement": "https://www.amazon.in/s?k=Home+Improvement",
   "Hand Tools": "https://www.amazon.in/s?k=Hand+Tools",
   "Sports Footwear": "https://www.amazon.in/s?k=Sports+Footwear",
   "Pet Clothing": "https://www.amazon.in/s?k=Pet+Clothing",
   "Wearable Devices": "https://www.amazon.in/s?k=Wearable+Devices",
   "Car Audio": "https://www.amazon.in/s?k=Car+Audio",
   "E-books": "https://www.amazon.in/s?k=E-books",
   "Camera Accessories": "https://www.amazon.in/s?k=Camera+Accessories",
   "Sunglasses": "https://www.amazon.in/s?k=Sunglasses",
   "Beauty & Personal Care": "https://www.amazon.in/s?k=Beauty+and+Personal+Care",
   "Gaming Accessories": "https://www.amazon.in/s?k=Gaming+Accessories"
}

# Run scraping process
df_all_categories = scrape_amazon_categories(categories)

# Save to CSV if data is available
if not df_all_categories.empty:
    df_all_categories.to_csv("amazon_product_links.csv", index=False)
    print("Scraping complete. Data saved to amazon_product_links.csv")
else:
    print("No data scraped, CSV file not created.")
