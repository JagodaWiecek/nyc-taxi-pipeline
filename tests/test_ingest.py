from unittest.mock import MagicMock, patch

import pytest

from src.config import Config
from src.exceptions import IngestError
from src.ingest import BronzeIngestor


def _make_ingestor():
    """Build an ingestor with a fake Spark session, which means nothing real is executed."""
    return BronzeIngestor(MagicMock(), Config())

def test_ingest_month_skips_already_loaded_month():
    """A month that is already in bronze is not read or written again."""
    ingestor = _make_ingestor()
    ingestor.is_loaded = MagicMock(return_value=True)
    ingestor.read_month = MagicMock()
    ingestor._write = MagicMock()

    assert ingestor.ingest_month("2024-01") is False

    ingestor.read_month.assert_not_called()
    ingestor._write.assert_not_called()

def test_ingest_month_loads_new_month():
    """A month that is not in bronze yet is read and written once."""
    ingestor = _make_ingestor()
    ingestor.is_loaded = MagicMock(return_value=False)
    ingestor.read_month = MagicMock()
    ingestor._write = MagicMock()

    assert ingestor.ingest_month("2024-01") is True

    ingestor.read_month.assert_called_once_with("2024-01")
    ingestor._write.assert_called_once()

def test_ingest_month_force_reloads_loaded_month():
    """With force=True the month is loaded even if it already exists."""
    ingestor = _make_ingestor()
    ingestor.is_loaded = MagicMock(return_value=True)
    ingestor.read_month = MagicMock()
    ingestor._write = MagicMock()

    assert ingestor.ingest_month("2024-01", force=True) is True

    ingestor.read_month.assert_called_once_with("2024-01")
    ingestor._write.assert_called_once()

def test_ingest_range_returns_only_loaded_months():
    """ingest_range reports only the months that were actually loaded."""
    ingestor = _make_ingestor()
    ingestor.is_loaded = MagicMock(side_effect=lambda ym: ym == "2024-02")
    ingestor.read_month = MagicMock()
    ingestor._write = MagicMock()

    loaded = ingestor.ingest_range("2024-01", "2024-03")

    assert loaded == ["2024-01", "2024-03"]
    assert "2024-02" not in loaded

def test_is_loaded_returns_false_when_table_does_not_exist():
    """On the first run the bronze table does not exist yet."""
    ingestor = _make_ingestor()
    ingestor.spark.catalog.tableExists.return_value = False

    assert ingestor.is_loaded("2024-01") is False

    ingestor.spark.table.assert_not_called()

def test_write_replaces_only_the_given_month():
    """The write uses Delta and replaceWhere, so other months stay untouched."""
    ingestor = _make_ingestor()
    df = MagicMock()

    ingestor._write(df, "2024-01")

    writer = df.write.format.return_value.mode.return_value
    df.write.format.assert_called_once_with("delta")
    df.write.format.return_value.mode.assert_called_once_with("overwrite")
    writer.option.assert_called_once_with("replaceWhere", "_year_month = '2024-01'")
    writer.option.return_value.saveAsTable.assert_called_once_with(
        ingestor.config.bronze_table
    )

class _FakeConfig:
    """Points raw_file_path() at a temporary folder."""

    def __init__(self, folder):
        self.folder = folder

    def raw_file_path(self, year_month):
        return str(self.folder / f"yellow_tripdata_{year_month}.parquet")


def test_read_month_raises_when_file_is_missing(tmp_path):
    """A missing raw file gives a clear IngestError."""
    ingestor = BronzeIngestor(MagicMock(), _FakeConfig(tmp_path))

    with pytest.raises(IngestError):
        ingestor.read_month("2024-01")


def test_read_month_raises_when_columns_are_missing(tmp_path):
    """A source file without the expected columns gives a clear IngestError."""
    (tmp_path / "yellow_tripdata_2024-01.parquet").write_bytes(b"x")
    spark = MagicMock()
    spark.read.parquet.return_value.columns = ["VendorID"]
    ingestor = BronzeIngestor(spark, _FakeConfig(tmp_path))

    with pytest.raises(IngestError):
        ingestor.read_month("2024-01")


def test_ingest_month_wraps_unexpected_errors():
    """Any unexpected failure is reported as IngestError, with the cause kept."""
    ingestor = _make_ingestor()
    ingestor.is_loaded = MagicMock(return_value=False)
    ingestor.read_month = MagicMock(side_effect=RuntimeError("boom"))

    with pytest.raises(IngestError) as excinfo:
        ingestor.ingest_month("2024-01")

    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_ingest_range_continues_after_failure_and_raises_at_the_end():
    """One bad month does not stop the others, but the failure is reported."""
    ingestor = _make_ingestor()
    ingestor.is_loaded = MagicMock(return_value=False)

    def fake_read(year_month):
        if year_month == "2024-02":
            raise IngestError("bad file")
        return MagicMock()

    ingestor.read_month = MagicMock(side_effect=fake_read)
    ingestor._write = MagicMock()

    with pytest.raises(IngestError):
        ingestor.ingest_range("2024-01", "2024-03")

    assert ingestor._write.call_count == 2

def _zone_ingestor(tmp_path, file_exists=True, table_exists=False):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    config = MagicMock()
    config.zone_lookup_table = "workspace.bronze.taxi_zones"
    path = tmp_path / "taxi_zone_lookup.csv"
    if file_exists:
        path.write_text("LocationID,Borough,Zone,service_zone\n1,EWR,Newark Airport,EWR\n")
    config.zone_lookup_path = str(path)
    return BronzeIngestor(spark, config), spark

def test_ingest_zone_lookup_writes_table(tmp_path):
    ingestor, spark = _zone_ingestor(tmp_path)

    with patch("src.ingest.F"):
        assert ingestor.ingest_zone_lookup() is True

    df = spark.read.option.return_value.schema.return_value.csv.return_value
    save = df.withColumn.return_value.withColumn.return_value.write.format.return_value.mode.return_value.saveAsTable
    save.assert_called_once_with("workspace.bronze.taxi_zones")


def test_ingest_zone_lookup_skips_when_table_exists(tmp_path):
    ingestor, spark = _zone_ingestor(tmp_path, table_exists=True)

    assert ingestor.ingest_zone_lookup() is False
    spark.read.option.assert_not_called()
    





