import requests
import time
import json
import os
from datetime import datetime

from src.utils.config_loader import load_config
from src.utils.logger import setup_logger
from src.storage.hdfs_client import upload_to_hdfs

logger = setup_logger()
BASE_PATH = "/opt/airflow/data"

class CoinGeckoIngestor:

    
    def __init__(self):
        self.config = load_config()
        self.base_url = self.config["api"]["base_url"]
        self.endpoint = self.config["api"]["endpoint"]
        self.params = {
            "vs_currency": self.config["api"]["vs_currency"],
            "per_page": self.config["api"]["per_page"],
            "page": self.config["api"]["page"],
        }

        self.output_dir = self.config["ingestion"]["output_dir"]
        self.max_retries = self.config["retry"]["max_retries"]
        self.delay = self.config["retry"]["delay_seconds"]

    def fetch_data(self):
        url = f"{self.base_url}{self.endpoint}"

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(f"Fetching data from CoinGecko (Attempt {attempt})")

                response = requests.get(url, params=self.params, timeout=10)

                if response.status_code == 200:
                    return response.json()
                else:
                    logger.warning(f"Bad response: {response.status_code}")

            except Exception as e:
                logger.error(f"Error during API call: {e}")

            time.sleep(self.delay)

        raise Exception("Max retries exceeded for API call")

    def save_to_local(self, data):
        today = datetime.utcnow().strftime("%Y-%m-%d")
        timestamp = datetime.utcnow().strftime("%H-%M-%S")

        dir_path = os.path.join(BASE_PATH, "raw", f"date={today}")
        os.makedirs(dir_path, exist_ok=True)

        file_path = os.path.join(dir_path, f"crypto_{timestamp}.json")

        with open(file_path, "w") as f:
            for record in data:
                f.write(json.dumps(record) + "\n")

        logger.info(f"Saved data to {file_path}")

        return file_path

    def run(self):
        logger.info("Starting CoinGecko ingestion pipeline")

        data = self.fetch_data()

        if not data:
            raise ValueError("No data received from API")

        file_path = self.save_to_local(data)

        hdfs_path = "/data/raw/"
        upload_to_hdfs(file_path, hdfs_path)

        logger.info("Ingestion completed successfully")

        return file_path


if __name__ == "__main__":
    ingestor = CoinGeckoIngestor()
    ingestor.run()