import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

def extract_product_data(card, timestamp):
    title = card.find('h3', class_='product-title')
    title = title.get_text(strip=True) if title else "Unknown Product"

    price = card.find('span', class_='price') or card.find('p', class_='price')
    price = price.get_text(strip=True) if price else "Price Unavailable"

    details = card.find_all('p', style=lambda v: v and 'font-size: 14px' in v)
    rating = colors = size = gender = None
    for p in details:
        text = p.get_text(strip=True)
        if text.startswith('Rating:'):
            rating = text.replace('Rating:', '').replace('⭐','').strip()
        elif 'Colors' in text:
            colors = text.strip()
        elif text.startswith('Size:'):
            size = text.strip()
        elif text.startswith('Gender:'):
            gender = text.strip()

    return {
        'Title': title,
        'Price': price,
        'Rating': rating or "Invalid Rating",
        'Colors': colors or "0 Colors",
        'Size': size or "Size: Unknown",
        'Gender': gender or "Gender: Unknown",
        'Timestamp': timestamp
    }

def scrape_page(session, page_num):
    if page_num == 1:
        url = 'https://fashion-studio.dicoding.dev/'
    else:
        url = f'https://fashion-studio.dicoding.dev/?page={page_num}'
    resp = session.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, 'html.parser')
    cards = soup.find_all('div', class_='collection-card')
    timestamp = datetime.now().isoformat()
    return [
        extract_product_data(card, timestamp)
        for card in cards
    ]

def scrape_all_pages(start=1, end=50):
    session = requests.Session()
    session.headers.update({'User-Agent':'Mozilla/5.0'})
    all_products = []
    for i in range(start, end+1):
        try:
            products = scrape_page(session, i)
            all_products.extend(products)
        except:
            pass
        time.sleep(0.3)
    return all_products
