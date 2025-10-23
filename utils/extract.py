import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

def extract_product_data(card, timestamp):
    try:
        title_el = card.find('h3', class_='product-title')
        title = title_el.get_text(strip=True) if title_el else "Unknown Product"
        price_el = card.find('span', class_='price') or card.find('p', class_='price')
        price = price_el.get_text(strip=True) if price_el else "Price Unavailable"
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
    except Exception as e:
        # fallback minimal agar pipeline tetap berjalan
        return {
            'Title': "Unknown Product",
            'Price': "Price Unavailable",
            'Rating': "Invalid Rating",
            'Colors': "0 Colors",
            'Size': "Size: Unknown",
            'Gender': "Gender: Unknown",
            'Timestamp': timestamp
        }

def scrape_page(session, page_num):
    try:
        url = 'https://fashion-studio.dicoding.dev/' if page_num == 1 else f'https://fashion-studio.dicoding.dev/?page={page_num}'
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        cards = soup.find_all('div', class_='collection-card')
        ts = datetime.now().isoformat()
        return [extract_product_data(card, ts) for card in cards]
    except requests.exceptions.RequestException as e:
        # kegagalan jaringan
        return []
    except Exception as e:
        # parsing/struktur HTML tidak sesuai
        return []

def scrape_all_pages(start=1, end=50):
    try:
        session = requests.Session()
        session.headers.update({'User-Agent':'Mozilla/5.0'})
        all_products = []
        for i in range(start, end+1):
            try:
                products = scrape_page(session, i)
                all_products.extend(products)
            except requests.exceptions.RequestException:
                # bila scrape_page mengangkat ulang error jaringan (jika diubah),
                # kita tangani per halaman.
                continue
            except Exception:
                # error lain per halaman
                continue
            time.sleep(0.3)
        return all_products
    except Exception:
        # error di tingkat loop/global
        return []
