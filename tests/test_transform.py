from unittest.mock import MagicMock, patch

import pytest

from src.config import Config
from src.exceptions import SilverError
from src.transform import SilverTransformer



def _make_transformer():
    """Build a transformer with a fake Spark session, nothing real is executed."""
    return SilverTransformer(MagicMock(), Config())


def _make_ready_transformer():
    """Transformer whose steps are all faked, so only transform_month's logic runs."""
    transformer = _make_transformer()
    transformer.is_loaded = MagicMock(return_value=False)
    bronze = MagicMock()
    bronze.count.return_value = 100
    transformer.read_bronze_month = MagicMock(return_value=bronze)
    transformer.clean = MagicMock()
    transformer._write = MagicMock()
    transformer.spark.table.return_value.filter.return_value.count.return_value = 98
    return transformer


def test_transform_month_skips_loaded_month():
    """A month already in silver is not read, cleaned or written again."""
    transformer = _make_ready_transformer()
    transformer.is_loaded.return_value = True

    assert transformer.transform_month("2024-01") is False

    transformer.read_bronze_month.assert_not_called()
    transformer._write.assert_not_called()


def test_transform_month_processes_new_month():
    """A new month is read from bronze, cleaned and written exactly once."""
    transformer = _make_ready_transformer()

    assert transformer.transform_month("2024-01") is True

    transformer.read_bronze_month.assert_called_once_with("2024-01")
    transformer.clean.assert_called_once_with(transformer.read_bronze_month.return_value)
    transformer._write.assert_called_once()


def test_transform_month_force_reprocesses_loaded_month():
    """With force=True the month is processed even if it is already in silver."""
    transformer = _make_ready_transformer()
    transformer.is_loaded.return_value = True

    assert transformer.transform_month("2024-01", force=True) is True

    transformer._write.assert_called_once()


def test_transform_month_wraps_unexpected_errors():
    """Any unexpected failure becomes a SilverError, with the cause kept."""
    transformer = _make_ready_transformer()
    transformer.read_bronze_month.side_effect = RuntimeError("boom")

    with pytest.raises(SilverError) as excinfo:
        transformer.transform_month("2024-01")

    assert isinstance(excinfo.value.__cause__, RuntimeError)


def test_transform_range_returns_only_processed_months():
    """transform_range reports only the months that were actually processed."""
    transformer = _make_transformer()
    transformer.transform_month = MagicMock(
        side_effect=lambda year_month, force=False: year_month != "2024-02"
    )

    assert transformer.transform_range("2024-01", "2024-03") == ["2024-01", "2024-03"]


def test_transform_range_continues_after_failure_and_raises_at_the_end():
    """One bad month does not stop the others, but the failure is reported."""
    transformer = _make_transformer()

    def fake_transform(year_month, force=False):
        if year_month == "2024-02":
            raise SilverError("bad month")
        return True

    transformer.transform_month = MagicMock(side_effect=fake_transform)

    with pytest.raises(SilverError):
        transformer.transform_range("2024-01", "2024-03")

    assert transformer.transform_month.call_count == 3


def test_read_bronze_month_raises_when_bronze_table_is_missing():
    """A missing bronze table gives a clear SilverError."""
    transformer = _make_transformer()
    transformer.spark.catalog.tableExists.return_value = False

    with pytest.raises(SilverError):
        transformer.read_bronze_month("2024-01")


def test_read_bronze_month_raises_when_month_is_empty():
    """A month with no rows in bronze gives a clear SilverError."""
    transformer = _make_transformer()
    transformer.spark.catalog.tableExists.return_value = True
    filtered = transformer.spark.table.return_value.filter.return_value
    filtered.limit.return_value.count.return_value = 0

    with pytest.raises(SilverError):
        transformer.read_bronze_month("2024-01")


def test_is_loaded_returns_false_when_silver_table_does_not_exist():
    """On the first run the silver table does not exist yet."""
    transformer = _make_transformer()
    transformer.spark.catalog.tableExists.return_value = False

    assert transformer.is_loaded("2024-01") is False

    transformer.spark.table.assert_not_called()

def _zone_transformer(total=265, ids=265, table_exists=False):
    spark = MagicMock()
    spark.catalog.tableExists.return_value = table_exists
    config = MagicMock()
    config.zone_lookup_table = "workspace.bronze.taxi_zones"
    config.silver_zone_table = "workspace.silver.taxi_zones"

    clean = MagicMock()
    clean.count.return_value = total
    clean.filter.return_value.select.return_value.distinct.return_value.count.return_value = ids
    return SilverTransformer(spark, config), spark, clean

def test_transform_zone_lookup_writes_valid_table():
    transformer, spark, clean = _zone_transformer()

    with patch.object(SilverTransformer, "clean_zones", return_value=clean), patch("src.transform.F"):
        assert transformer.transform_zone_lookup() is True

    save = clean.withColumn.return_value.write.format.return_value.mode.return_value.saveAsTable
    save.assert_called_once_with("workspace.silver.taxi_zones")


def test_transform_zone_lookup_rejects_duplicate_ids():
    transformer, _, clean = _zone_transformer(total=265, ids=264)

    with patch.object(SilverTransformer, "clean_zones", return_value=clean), patch("src.transform.F"):
        with pytest.raises(SilverError):
            transformer.transform_zone_lookup()

    clean.withColumn.assert_not_called()
