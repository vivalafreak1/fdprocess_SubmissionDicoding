import requests
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
    card = BeautifulSoup(html, "html.parser").div
    data = extract_product_data(card, "ts")
    assert data["Title"] == "X"
    assert data["Price"] == "$1"
    assert "2.0" in data["Rating"]
    assert data["Colors"] == "4 Colors"
    assert data["Size"] == "Size: M"
    assert data["Gender"] == "Gender: F"

def test_extract_defaults():
    from bs4 import BeautifulSoup
    card = BeautifulSoup("<div></div>", "html.parser").div
    data = extract_product_data(card, "ts")
    assert data["Title"] == "Unknown Product"
    assert data["Price"] == "Price Unavailable"

def test_scrape_page_connection_error():
    mock_session = Mock()
    mock_session.get.side_effect = requests.exceptions.RequestException("Connection failed")
    assert scrape_page(mock_session, 1) == []

def test_extract_product_data_error_handling():
    from bs4 import BeautifulSoup
    card = BeautifulSoup("<div></div>", "html.parser").div
    result = extract_product_data(card, "ts")
    assert result["Title"] == "Unknown Product"

def test_scrape_page_empty_products():
    class DummyResponse:
        def raise_for_status(self): ...
        @property
        def content(self):
            return b"<html><body><div></div></body></html>"

    mock_session = Mock()
    mock_session.get.return_value = DummyResponse()
    products = scrape_page(mock_session, 1)
    assert isinstance(products, list)

def test_scrape_page_page2_branch():
    class DummyResponse:
        def raise_for_status(self): ...
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
    products = scrape_page(mock_session, 2)
    assert isinstance(products, list)
    assert len(products) == 1
    assert products[0]["Title"] == "Branch Item"

@patch("utils.extract.time.sleep", lambda x: None)
@patch("utils.extract.scrape_page")
def test_scrape_all_pages_happy_path(mock_scrape_page):
    mock_scrape_page.side_effect = [
        [{"Title":"A","Price":"$1","Rating":"3.0 / 5","Colors":"3 Colors","Size":"Size: M","Gender":"Gender: Men","Timestamp":"t1"}],
        [{"Title":"B","Price":"$2","Rating":"4.0 / 5","Colors":"2 Colors","Size":"Size: L","Gender":"Gender: Women","Timestamp":"t2"}],
    ]
    data = scrape_all_pages(1, 2)
    assert isinstance(data, list)
    assert [d["Title"] for d in data] == ["A","B"]

@patch("utils.extract.scrape_page", side_effect=[
    [{"Title":"A","Price":"$1","Rating":"3/5","Colors":"1 Colors","Size":"Size: S","Gender":"Gender: X","Timestamp":"t"}],
    [{"Title":"B","Price":"$2","Rating":"4/5","Colors":"2 Colors","Size":"Size: M","Gender":"Gender: Y","Timestamp":"t"}]
])
def test_scrape_all_pages_sleep_called(mock_scrape_page):
    with patch("utils.extract.time.sleep") as mock_sleep:
        data = scrape_all_pages(1, 2)
        assert len(data) == 2
        assert mock_sleep.call_count == 2

def dummy_sleep(_): return None

@patch("utils.extract.time.sleep", side_effect=dummy_sleep)
@patch("utils.extract.scrape_page", return_value=[{"Title":"X","Price":"$1","Rating":"3/5","Colors":"1 Colors","Size":"Size: S","Gender":"Gender: X","Timestamp":"t"}])
def test_scrape_all_pages_sleep_single_iter(mock_scrape_page, mock_sleep):
    data = scrape_all_pages(1, 1)
    assert len(data) == 1
    assert mock_sleep.call_count == 1

@patch("utils.extract.time.sleep", lambda x: None)
@patch("utils.extract.scrape_page", side_effect=[Exception("boom"), [{"Title":"OK","Price":"$1","Rating":"3/5","Colors":"1 Colors","Size":"Size: S","Gender":"Gender: X","Timestamp":"t"}]])
def test_scrape_all_pages_handles_exception_then_recovers(mock_scrape_page):
    data = scrape_all_pages(1, 2)
    assert [d["Title"] for d in data] == ["OK"]
    assert mock_scrape_page.call_count == 2

def test_extract_product_data_exception_path():
    bad_card = Mock()
    bad_card.find.side_effect = Exception("parse error")
    res = extract_product_data(bad_card, "ts")
    assert res["Title"] == "Unknown Product"
    assert res["Price"] == "Price Unavailable"

def test_scrape_page_request_exception_path():
    mock_session = Mock()
    mock_session.get.side_effect = requests.exceptions.RequestException("down")
    assert scrape_page(mock_session, 1) == []

@patch("utils.extract.requests.Session")
def test_scrape_all_pages_top_level_exception(mock_sess):
    mock_sess.side_effect = Exception("session init failed")
    assert scrape_all_pages(1, 1) == []

@patch("utils.extract.time.sleep", lambda x: None)
@patch("utils.extract.scrape_page", side_effect=requests.exceptions.RequestException("per-page fail"))
def test_scrape_all_pages_inner_request_exception(mock_sp):
    assert scrape_all_pages(1, 1) == []

def test_scrape_page_parsing_exception_path():
    class BadResponse:
        def raise_for_status(self): ...
        @property
        def content(self):
            return b"<html><body><div class='collection-card'></div></body></html>"

    mock_session = Mock()
    mock_session.get.return_value = BadResponse()
    with patch("utils.extract.extract_product_data", side_effect=Exception("parse boom")):
        assert scrape_page(mock_session, 1) == []
