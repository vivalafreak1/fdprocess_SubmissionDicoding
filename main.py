from utils.extract import scrape_all_pages
from utils.transform import transform_data
from utils.load import save_csv, save_google_sheets

def main():
    raw = scrape_all_pages(1,50)
    clean = transform_data(raw)
    csv_path = save_csv(clean, 'products.csv')
    print(f"CSV saved: {csv_path}")
    gs_url = save_google_sheets(clean, 'Fashion Studio Data', 'google-sheets-api.json')
    print(f"Google Sheets URL: {gs_url}")

if __name__=='__main__':
    main()
