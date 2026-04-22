"""
LogShipper: delivers LogEvent documents to Amazon OpenSearch Service
using AWS SigV4 (requests-aws4auth) authentication.

No username/password credentials are ever used.
"""

from __future__ import annotations

import json
import sys
import time

import boto3
import requests
from requests_aws4auth import AWS4Auth

from plugins.log_shipper.exceptions import LogShipperError
from plugins.log_shipper.models import LogEvent


class LogShipper:
    """
    Delivers :class:`LogEvent` documents to an Amazon OpenSearch Service index.

    Authentication is performed exclusively via AWS SigV4 signing using
    ``requests-aws4auth``.  No ``Authorization: Basic`` header is ever set.

    Args:
        opensearch_endpoint: Full HTTPS URL of the OpenSearch domain,
            e.g. ``https://search-my-domain.us-east-1.es.amazonaws.com``.
        index_name: Name of the target index, e.g. ``etl-logs-2024-01``.
        region: AWS region where the OpenSearch domain lives, e.g. ``us-east-1``.
    """

    def __init__(self, opensearch_endpoint: str, index_name: str, region: str) -> None:
        self.opensearch_endpoint = opensearch_endpoint
        self.index_name = index_name
        self.region = region

    # ------------------------------------------------------------------
    # Internal delivery (single attempt, no retry logic here)
    # ------------------------------------------------------------------

    def _deliver(self, event: LogEvent) -> None:
        """
        Validate *event*, sign an HTTP POST request with SigV4, and send the
        document to OpenSearch.

        The URL used is::

            {opensearch_endpoint}/{index_name}/_doc

        Raises:
            ValueError: if ``event.validate()`` fails.
            requests.HTTPError: if the HTTP response status code is >= 400.
        """
        # 1. Validate before sending — raises ValueError on invalid data.
        event.validate()

        # 2. Serialize to JSON.
        body = json.dumps(event.to_dict())

        # 3. Obtain AWS credentials from the current boto3 session.
        session = boto3.session.Session()
        credentials = session.get_credentials()
        # Resolve any lazy/refreshable credentials to concrete values.
        resolved = credentials.get_frozen_credentials()

        # 4. Build SigV4 auth object.  The service name for OpenSearch is 'es'.
        aws_auth = AWS4Auth(
            resolved.access_key,
            resolved.secret_key,
            self.region,
            "es",
            session_token=resolved.token,
        )

        # 5. Build the target URL.
        url = f"{self.opensearch_endpoint.rstrip('/')}/{self.index_name}/_doc"

        # 6. POST the document.  requests-aws4auth adds the SigV4
        #    Authorization header automatically; we never set Basic auth.
        response = requests.post(
            url,
            data=body,
            auth=aws_auth,
            headers={"Content-Type": "application/json"},
        )

        # 7. Raise on HTTP errors (4xx / 5xx).
        if response.status_code >= 400:
            response.raise_for_status()

    # ------------------------------------------------------------------
    # Public delivery with retry and exponential backoff
    # ------------------------------------------------------------------

    def ship(self, event: LogEvent) -> None:
        """
        Deliver *event* to OpenSearch with up to 3 retries (4 total attempts).

        Retry schedule:
            Attempt 1: immediate (no delay)
            Attempt 2: wait 1 second
            Attempt 3: wait 2 seconds
            Attempt 4: wait 4 seconds

        If all 4 attempts fail, writes an error message to stderr and raises
        :class:`LogShipperError`, chaining the original exception.

        Args:
            event: The :class:`LogEvent` to deliver.

        Raises:
            LogShipperError: if delivery fails after all 4 attempts.
        """
        # Delays (in seconds) to wait *before* each attempt.
        # Attempt 1 has no delay; attempts 2-4 use exponential backoff.
        delays = [0, 1, 2, 4]
        last_exc: Exception | None = None

        for attempt, delay in enumerate(delays, start=1):
            if delay > 0:
                time.sleep(delay)
            try:
                self._deliver(event)
                return  # success — stop retrying
            except Exception as exc:  # noqa: BLE001
                last_exc = exc

        # All 4 attempts exhausted — emit local error log and raise.
        sys.stderr.write(
            f"LogShipper: failed to deliver event after {len(delays)} attempts "
            f"(run_id={event.run_id!r}, task_name={event.task_name!r}): {last_exc}\n"
        )
        raise LogShipperError(
            f"Failed to deliver LogEvent to OpenSearch after {len(delays)} attempts: {last_exc}"
        ) from last_exc
