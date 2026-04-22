"""
Secrets Manager helper for the MWAA ETL Workflow.

Provides a `get_secret` helper that retrieves and parses secrets from
AWS Secrets Manager at runtime.  No secret values are ever stored in
DAG code, environment variables, or source control.
"""

from __future__ import annotations

import json

import boto3
from botocore.exceptions import ClientError


class SecretNotFoundError(Exception):
    """Raised when the requested secret does not exist in AWS Secrets Manager."""


# Module-level cache: avoids redundant API calls within the same process.
_cache: dict[str, dict] = {}


def get_secret(secret_name: str) -> dict:
    """
    Retrieve a secret from AWS Secrets Manager and return it as a dict.

    The result is cached in-process so repeated calls for the same
    ``secret_name`` do not incur additional API round-trips.

    Args:
        secret_name: The name or ARN of the secret to retrieve.

    Returns:
        A dict containing the secret's key-value pairs.

    Raises:
        SecretNotFoundError: If the secret does not exist
            (``ResourceNotFoundException``).
        botocore.exceptions.ClientError: For any other Secrets Manager
            error (re-raised as-is).
    """
    if secret_name in _cache:
        return _cache[secret_name]

    client = boto3.client("secretsmanager")

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]
        if error_code == "ResourceNotFoundException":
            raise SecretNotFoundError(
                f"Secret '{secret_name}' not found in AWS Secrets Manager."
            ) from exc
        raise

    if "SecretString" in response:
        secret_dict = json.loads(response["SecretString"])
    else:
        # SecretBinary is a bytes object; decode then parse as JSON.
        secret_dict = json.loads(response["SecretBinary"].decode("utf-8"))

    _cache[secret_name] = secret_dict
    return secret_dict
