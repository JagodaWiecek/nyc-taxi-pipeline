from src.clients import TlcClient, month_range
from src.config import Config

import pytest
import os
from unittest.mock import MagicMock
import requests

def test_session_retries_on_server_errors():
    """The session retries 3 times on 5xx errors, but not on 404."""
    client = TlcClient(Config())

    retry = client.session.get_adapter("https://example.com").max_retries

    assert retry.total == 3
    assert retry.backoff_factor == 2
    assert 503 in retry.status_forcelist
    assert 404 not in retry.status_forcelist

class FakeConfig:
    """Config that saves files into a temporary folder instead of the Volume."""

    def __init__(self, folder):
        self.folder = folder

    def raw_file_path(self, year_month):
        return str(self.folder / f"yellow_tripdata_{year_month}.parquet")

    def source_url(self, year_month):
        return f"https://example.com/{year_month}.parquet"

def _make_fake_response(chunks):
    """Build a fake HTTP response that can be used in a `with` block.

    `chunks` parameter is the list of byte strings that iter_content() will return,
    simulating a file downloaded in pieces.
    """
    response = MagicMock()
    response.__enter__.return_value = response
    response.iter_content.return_value = chunks
    return response

def test_download_month_saves_file(tmp_path):
    """Downloaded content is written to the destination file.
    The temporary .tmp file must be gone after a successful download.
    """
    client = TlcClient(FakeConfig(tmp_path))
    client.session = MagicMock()
    client.session.get.return_value = _make_fake_response([b"abc", b"def"])

    dest = client.download_month("2024-01")

    with open(dest, "rb") as f:
        assert f.read() == b"abcdef"
    assert not os.path.exists(dest + ".tmp")

def test_download_month_skips_existing_file(tmp_path):
    """If the file already exists, nothing is downloaded or overwritten."""
    client = TlcClient(FakeConfig(tmp_path))
    client.session = MagicMock()
    existing = tmp_path / "yellow_tripdata_2024-01.parquet"
    existing.write_bytes(b"old")

    client.download_month("2024-01")

    client.session.get.assert_not_called()
    assert existing.read_bytes() == b"old"


def test_download_month_leaves_no_file_on_http_error(tmp_path):
    """On an HTTP error the error is raised and no file is left behind.
    This protects against corrupted or half-written files in the raw Volume.
    """
    client = TlcClient(FakeConfig(tmp_path))
    client.session = MagicMock()
    response = _make_fake_response([])
    response.raise_for_status.side_effect = requests.HTTPError("404")
    client.session.get.return_value = response

    with pytest.raises(requests.HTTPError):
        client.download_month("2024-01")

    assert os.listdir(tmp_path) == []


def test_month_range_crosses_year_boundary():
    """Months roll over from December to January correctly."""
    assert month_range("2023-11", "2024-02") == [
        "2023-11", "2023-12", "2024-01", "2024-02"
    ]


