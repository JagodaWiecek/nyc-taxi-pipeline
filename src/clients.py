import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
def month_range(start: str, end: str) -> list[str]:
    """Return every month from start to end (inclusive) as 'YYYY-MM' strings."""
    year, month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))

    months = []
    while (year, month) <= (end_year, end_month):
        months.append(f"{year}-{month:02d}")
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months
    
class TlcClient:
    """The class to download the data from the TLC website"""

    def __init__(self, config: Config, timeout: int = 120):
        self.config = config
        self.timeout = timeout
        self.session = self._build_session()
        


    @staticmethod
    def _build_session() -> requests.Session:
        """
        The method creates a session with a retry mechanism
        Returns:
            requests.Session: The session
        """
        retry = Retry(total=3, backoff_factor=2, status_forcelist=[500, 502, 503, 504])
        session = requests.Session()
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session
    
    def download_month(self, year_month: str) -> str:
        """Download one month of data into the raw Volume (skip if present)."""
        dest_path = self.config.raw_file_path(year_month)
        if os.path.exists(dest_path):
            print(f"File already exists, skipping: {dest_path}")
            return dest_path

        tmp_path = dest_path + ".tmp"
        url = self.config.source_url(year_month)

        with self.session.get(url, stream=True, timeout=self.timeout) as response:
            response.raise_for_status()
            with open(tmp_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)

        os.replace(tmp_path, dest_path)
        print(f"Downloaded: {dest_path}")
        return dest_path
        
    def download_range(self, start: str, end: str) -> tuple[list[str], list[str]]:
        """Download every month in the range; months not published yet are skipped."""
        downloaded, missing = [], []
        for year_month in month_range(start, end):
            try:
                self.download_month(year_month)
                downloaded.append(year_month)
            except requests.HTTPError as error:
                if error.response is not None and error.response.status_code == 404:
                    print(f"Not published yet, skipping: {year_month}")
                    missing.append(year_month)
                else:
                    raise
        return downloaded, missing