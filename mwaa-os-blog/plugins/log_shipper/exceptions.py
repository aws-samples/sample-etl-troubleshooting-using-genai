"""
Custom exceptions for the MWAA ETL Workflow log shipper.
"""


class LogShipperError(Exception):
    """
    Raised when the LogShipper fails to deliver a LogEvent to OpenSearch
    after exhausting all retry attempts (3 retries, 4 total attempts).

    The original cause is available via the standard exception chaining
    mechanism (``__cause__``).
    """
