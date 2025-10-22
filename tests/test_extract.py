import pytest
from utils.extract import extract_product_data, scrape_page, scrape_all_pages
from unittest.mock import patch, Mock

def test_extract_complete():
    html = """
    <div class="collection-card">
      <h3 class="product-title">X</h3>
      <span class="price">$1</span>
      <p style="font-size: 14px;">Rating: ⭐ 2.0 / 5</p>
      <p style="font-size: 14px;">4 Colors</p>
      <p style="font-size: 14px;">Size: M</p>
      <p style="font-size: 14px;">Gender: F</p>
    </div>
    """
    from bs4 import BeautifulSoup
    card = BeautifulSoup(html, 'html.parser').div
    data = extract_product_data(card, 'ts')
    assert data['Title']=='X'
    assert data['Price']=='$1'
    assert '2.0' in data['Rating']
    assert data['Colors']=='4 Colors'
    assert data['Size']=='Size: M'
    assert data['Gender']=='Gender: F'

def test_extract_defaults():
    from bs4 import BeautifulSoup
    card = BeautifulSoup('<div></div>','html.parser').div
    data = extract_product_data(card,'ts')
    assert data['Title']=='Unknown Product'
    assert data['Price']=='Price Unavailable'

def test_scrape_page_connection_error():
    mock_session = Mock()
    mock_session.get.side_effect = Exception("Connection failed")
    with pytest.raises(Exception):
        scrape_page(mock_session, 1)

def test_extract_product_data_error_handling():
    # Simulate missing elements
    from bs4 import BeautifulSoup
    html = "<div></div>"
    card = BeautifulSoup(html, 'html.parser').div
    result = extract_product_data(card, 'ts')
    assert result["Title"] == "Unknown Product"

def test_scrape_page_empty_products():
    class DummyResponse:
        def raise_for_status(self): pass
        @property
        def content(self):
            return b'<html><body><div></div></body></html>'
    mock_session = Mock()
    mock_session.get.return_value = DummyResponse()
    products = scrape_page(mock_session, 1)
    # Should return empty list or list of some but no error
    assert isinstance(products, list)

def test_scrape_page_page2_branch():
    # Cover cabang else: ?page= pada URL (baris 40)
    class DummyResponse:
        def raise_for_status(self): pass
        @property
        def content(self):
            return """
            <html><body>
            <div class="collection-card">
              <div class="product-details">
                <h3 class="product-title">Branch Item</h3>
                <span class="price">$10</span>
                <p style="font-size: 14px;">Rating: ⭐ 4.0 / 5</p>
                <p style="font-size: 14px;">3 Colors</p>
                <p style="font-size: 14px;">Size: M</p>
                <p style="font-size: 14px;">Gender: F</p>
              </div>
            </div>
            </body></html>
            """
    mock_session = Mock()
    mock_session.get.return_value = DummyResponse()
    products = scrape_page(mock_session, 2)  # page != 1 -> cabang else
    assert isinstance(products, list)
    assert len(products) == 1
    assert products[0]['Title'] == 'Branch Item'

@patch('utils.extract.time.sleep', lambda x: None)  # hindari delay saat test
@patch('utils.extract.scrape_page')
def test_scrape_all_pages_happy_path(mock_scrape_page):
    # Cover loop scrape_all_pages (baris 52-62)
    mock_scrape_page.side_effect = [
        [
            {
                'Title':'A','Price':'$1','Rating':'3.0 / 5',
                'Colors':'3 Colors','Size':'Size: M','Gender':'Gender: Men',
                'Timestamp':'t1'
            }
        ],
        [
            {
                'Title':'B','Price':'$2','Rating':'4.0 / 5',
                'Colors':'2 Colors','Size':'Size: L','Gender':'Gender: Women',
                'Timestamp':'t2'
            }
        ],
    ]
    data = scrape_all_pages(1, 2)
    assert isinstance(data, list)
    assert len(data) == 2
    titles = [d['Title'] for d in data]
    assert titles == ['A', 'B']

@patch('utils.extract.scrape_page', side_effect=[[{'Title':'A','Price':'$1','Rating':'3/5','Colors':'1 Colors','Size':'Size: S','Gender':'Gender: X','Timestamp':'t'}],
                                                 [{'Title':'B','Price':'$2','Rating':'4/5','Colors':'2 Colors','Size':'Size: M','Gender':'Gender: Y','Timestamp':'t'}]])
def test_scrape_all_pages_sleep_called(mock_scrape_page):
    # Patch time.sleep untuk memastikan baris sleep dieksekusi dan terhitung coverage-nya
    with patch('utils.extract.time.sleep') as mock_sleep:
        data = scrape_all_pages(1, 2)
        assert len(data) == 2
        # Loop dipanggil 2 kali -> sleep seharusnya terpanggil 2 kali
        assert mock_sleep.call_count == 2

def dummy_sleep(_): return None

@patch('utils.extract.time.sleep', side_effect=dummy_sleep)
@patch('utils.extract.scrape_page', return_value=[{'Title':'X','Price':'$1','Rating':'3/5','Colors':'1 Colors','Size':'Size: S','Gender':'Gender: X','Timestamp':'t'}])
def test_scrape_all_pages_sleep_single_iter(mock_scrape_page, mock_sleep):
    # Jalankan 1 iterasi saja namun tetap mengeksekusi sleep (baris 59-60)
    data = scrape_all_pages(1, 1)
    assert len(data) == 1
    assert mock_sleep.call_count == 1

@patch('utils.extract.time.sleep', lambda x: None)  # hindari delay
@patch('utils.extract.scrape_page', side_effect=[
    Exception("boom"),  # iterasi 1 -> masuk except
    [  # iterasi 2 -> normal
        {'Title':'OK','Price':'$1','Rating':'3/5',
         'Colors':'1 Colors','Size':'Size: S','Gender':'Gender: X','Timestamp':'t'}
    ]
])
def test_scrape_all_pages_handles_exception_then_recovers(mock_scrape_page):
    data = scrape_all_pages(1, 2)
    # iterasi 1 gagal -> except terpanggil; iterasi 2 sukses -> data masuk
    assert isinstance(data, list)
    assert len(data) == 1
    assert data[0]['Title'] == 'OK'
    # pastikan dipanggil 2 kali sesuai range(1,2)
    assert mock_scrape_page.call_count == 2