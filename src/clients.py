import logging
import os

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.config import Config
from src.exceptions import DownloadError, MonthNotAvailableError

logger = logging.getLogger(__name__)

def _remove_file(path: str) -> None:
    """Delete a file if it exists.
    If used to remove half-downloaded files."""
    if os.path.exists(path):
        os.remove(path)

def month_range(start: str, end: str) -> list[str]:
    """Return every month from start to end, inclusively.
    Args:
        start (str): The start date in 'YYYY-MM' format.
        end (str): The end date in 'YYYY-MM' format.
    Returns:
        list[str]: The list of months in 'YYYY-MM' format
    """
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
    """The class to download the data from the TLC website
    Args:
        config (Config): The config object
        timeout (int, optional): The timeout for the download. Defaults to 120.
    """

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
        """Download one month into the raw Volume (skip if present).
        Args:
            year_month (str): The month in 'YYYY-MM' format.
        Returns:
            str: The path to the downloaded file
        """
        return self._download_file(
            self.config.source_url(year_month),
            self.config.raw_file_path(year_month),
            year_month,
        )
        
    def download_zone_lookup(self) -> str:
        """Download the taxi zone lookup table into the raw Volume (skip if present).
        Args:
            None
        Returns:
            str: The path to the downloaded file
        """
        return self._download_file(
            self.config.zone_lookup_url,
            self.config.zone_lookup_path,
            "taxi_zone_lookup",
        )
    
    def _download_file(self, url: str, dest_path: str, label: str) -> str:
        """Download url to dest_path (skip if present). Errors are reported with `label`.
        Args:
            url (str): The url to download
            dest_path (str): The path to save the file
            label (str): The label to use in the error message
        Returns:
            str: The path to the downloaded file
        """
        if os.path.exists(dest_path):
            logger.info("File already exists, skipping: %s", dest_path)
            return dest_path

        tmp_path = dest_path + ".tmp"
        logger.info("Downloading %s from %s", label, url)
        try:
            with self.session.get(url, stream=True, timeout=self.timeout) as response:
                response.raise_for_status()
                with open(tmp_path, "wb") as f:
                    f.writelines(response.iter_content(chunk_size=1024 * 1024))
            if os.path.getsize(tmp_path) == 0:
                raise DownloadError(f"{label}: downloaded file is empty")
            os.replace(tmp_path, dest_path)
        except requests.HTTPError as error:
            _remove_file(tmp_path)
            if error.response is not None and error.response.status_code == 404:
                raise MonthNotAvailableError(f"{label}: not published (404)") from error
            raise DownloadError(f"{label}: HTTP error: {error}") from error
        except (requests.RequestException, OSError) as error:
            _remove_file(tmp_path)
            raise DownloadError(f"{label}: download failed: {error}") from error
        except DownloadError:
            _remove_file(tmp_path)
            raise

        logger.info("Downloaded: %s", dest_path)
        return dest_path

    def download_range(self, start: str, end: str) -> tuple[list[str], list[str]]:
        """Download every month in the range; months not published yet are skipped.
        Args:
            start (str): The start date in 'YYYY-MM' format.
            end (str): The end date in 'YYYY-MM' format.
        Returns:
            tuple[list[str], list[str]]: The list of downloaded and missing months
        """
        downloaded, missing = [], []
        for year_month in month_range(start, end):
            try:
                self.download_month(year_month)
                downloaded.append(year_month)
            except MonthNotAvailableError:
                logger.warning("Not published yet, skipping: %s", year_month)
                missing.append(year_month)
        return downloaded, missing
    




