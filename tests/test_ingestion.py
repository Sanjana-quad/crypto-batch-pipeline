import os
from src.ingestion.coingecko_api import CoinGeckoIngestor

def test_api_fetch():
    ingestor = CoinGeckoIngestor()
    data = ingestor.fetch_data()

    assert isinstance(data, list)
    assert len(data) > 0
    assert "id" in data[0]


def test_file_written():
    ingestor = CoinGeckoIngestor()
    data = ingestor.fetch_data()
    file_path = ingestor.save_to_local(data)

    assert os.path.exists(file_path)