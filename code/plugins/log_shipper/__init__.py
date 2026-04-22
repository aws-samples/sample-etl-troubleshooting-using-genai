"""
log_shipper plugin package.

Public API:
    LogEvent          — structured log document written to OpenSearch
    LogShipperError   — raised when delivery fails after all retries
    LogShipper        — delivers LogEvent documents to OpenSearch via SigV4
    SecretNotFoundError — raised when a requested secret does not exist
"""

from plugins.log_shipper.models import LogEvent
from plugins.log_shipper.exceptions import LogShipperError
from plugins.log_shipper.log_shipper import LogShipper
from plugins.secrets import SecretNotFoundError

__all__ = ["LogEvent", "LogShipperError", "LogShipper", "SecretNotFoundError"]
