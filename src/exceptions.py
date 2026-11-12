class DownloadError(Exception):
    """Raised when a TLC file cannot be downloaded."""


class MonthNotAvailableError(DownloadError):
    """Raised when TLC has not published the requested month yet (HTTP 404)."""


class IngestError(Exception):
    """Raised when a raw file cannot be loaded into the bronze table."""