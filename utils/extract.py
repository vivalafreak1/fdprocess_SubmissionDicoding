import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

def extract_product_data(card, timestamp):
    try:
        t = card.find("h3", class_="product-title")
        title = t.get_text(strip=True) if t else "Unknown Product"
        p = card.find("span", class_="price") or card.find("p", class_="price")
        price = p.get_text(strip=True) if p else "Price Unavailable"
        details = card.find_all("p", style=lambda v: v and "font-size: 14px" in v)
        rating = colors = size = gender = None
        for x in details:
            text = x.get_text(strip=True)
            if text.startswith("Rating:"):
                rating = text.replace("Rating:", "").replace("⭐","").strip()
            elif "Colors" in text:
                colors = text.strip()
            elif text.startswith("Size:"):
                size = text.strip()
            elif text.startswith("Gender:"):
                gender = text.strip()
        return {
            "Title": title, "Price": price, "Rating": rating or "Invalid Rating",
            "Colors": colors or "0 Colors", "Size": size or "Size: Unknown",
            "Gender": gender or "Gender: Unknown", "Timestamp": timestamp
        }
    except Exception:
        return {
            "Title":"Unknown Product","Price":"Price Unavailable","Rating":"Invalid Rating",
            "Colors":"0 Colors","Size":"Size: Unknown","Gender":"Gender: Unknown","Timestamp": timestamp
        }

def scrape_page(session, page_num):
    try:
        url = "https://fashion-studio.dicoding.dev/" if page_num == 1 else f"https://fashion-studio.dicoding.dev/page{page_num}"
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, "html.parser")
        cards = soup.find_all("div", class_="collection-card")
        ts = datetime.now().isoformat()
        return [extract_product_data(c, ts) for c in cards]
    except requests.exceptions.RequestException:
        return []
    except Exception:
        return []

def scrape_all_pages(start=1, end=50):
    try:
        session = requests.Session()
        session.headers.update({"User-Agent":"Mozilla/5.0"})
        all_products = []
        for i in range(start, end+1):
            try:
                all_products.extend(scrape_page(session, i))
            except requests.exceptions.RequestException:
                continue
            except Exception:
                continue
            time.sleep(0.3)
        return all_products
    except Exception:
        return []
