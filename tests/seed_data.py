#!/usr/bin/env python3
"""
Seed test data for tap-dixa integration tests.

Creates recent conversations and messages in the configured Dixa account so
that integration tests always find records within their time window regardless
of start_date.

What gets created
-----------------
  1 conversation  (email channel, subject "tap-dixa integration test <timestamp>")
  1 message       (inbound, attached to the conversation above)

The data is created via the Dixa REST API (INTEGRATIONS base URL).

Usage
-----
# Minimal – reads config.json in the tap root
python tests/seed_data.py

# Specify an explicit config path
python tests/seed_data.py --config /path/to/config.json

# Dry run (print payloads without calling the API)
python tests/seed_data.py --dry-run

Config file keys (see config.json.example)
------------------------------------------
api_token   – Dixa API token
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

# ──────────────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────────────

BASE_URL = "https://dev.dixa.io"
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.json"


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────

def _load_config(path: str) -> dict:
    config_path = Path(path)
    if not config_path.exists():
        sys.exit(f"ERROR: Config file not found: {config_path}")
    with open(config_path) as f:
        return json.load(f)


def _headers(api_token: str) -> dict:
    return {
        "Authorization": api_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }


def _request(method: str, endpoint: str, headers: dict, payload: dict, dry_run: bool):
    url = f"{BASE_URL}{endpoint}"
    print(f"\n{'[DRY-RUN] ' if dry_run else ''}{method} {url}")
    print(f"  payload: {json.dumps(payload, indent=2)}")

    if dry_run:
        return {"id": "dry-run-id", "dry_run": True}

    response = requests.request(method, url, headers=headers, json=payload, timeout=30)
    if not response.ok:
        hint = ""
        if response.status_code == 400 and "_type" in response.text:
            hint = (
                "\nHINT: '_type' discriminator rejected. "
                "Valid values for POST /v1/conversations: Email, Chat, ContactForm, Callback, Sms.\n"
                "Valid values for CreateMessageInput: Inbound, Outbound."
            )
        sys.exit(
            f"ERROR: {method} {url} returned {response.status_code}\n{response.text}{hint}"
        )
    return response.json()


# ──────────────────────────────────────────────────────────────────────────────
# Seeding
# ──────────────────────────────────────────────────────────────────────────────

def create_end_user(headers: dict, dry_run: bool) -> str:
    """
    Creates a temporary end user and returns their UUID.
    Uses POST /v1/endusers on the Dixa INTEGRATIONS API.
    """
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "displayName": f"tap-dixa test user {now_iso}",
        "email": f"test-seed-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}@example.com",
    }
    result = _request("POST", "/v1/endusers", headers, payload, dry_run)
    if dry_run:
        return "dry-run-user-id"
    user_id = result.get("data", {}).get("id", "dry-run-user-id")
    print(f"  ✓ Created end user id={user_id}")
    return user_id


def create_conversation(headers: dict, requester_id: str, email_integration_id: str, dry_run: bool) -> int:
    """
    Creates a new email conversation and returns its numeric ID.
    Uses POST /v1/conversations on the Dixa INTEGRATIONS API.

    Schema: CreateConversationInput oneOf [Callback, Chat, ContactForm, Email, Sms]
    For Email: _type="Email", requesterId, emailIntegrationId, subject, message required.
    """
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    payload = {
        "_type": "Email",
        "requesterId": requester_id,
        "emailIntegrationId": email_integration_id,
        "subject": f"tap-dixa integration test {now_iso}",
        "message": {
            "_type": "Inbound",
            "content": {
                "_type": "Text",
                "value": "This is a seeded test conversation for tap-dixa integration tests.",
            },
            "attachments": [],
        },
        "language": "en",
    }
    result = _request("POST", "/v1/conversations", headers, payload, dry_run)
    if dry_run:
        return 0
    conversation_id = result.get("data", {}).get("id", 0)
    print(f"  ✓ Created conversation id={conversation_id}")
    return conversation_id


def create_message(headers: dict, conversation_id: int, agent_id: str | None, dry_run: bool):
    """
    Creates an outbound message on the given conversation.
    Uses POST /v1/conversations/{id}/messages on the Dixa INTEGRATIONS API.

    Schema: CreateMessageInput oneOf [Inbound, Outbound]
    Outbound requires agentId. Inbound does not.
    """
    payload = {
        "_type": "Inbound",
        "content": {
            "_type": "Text",
            "value": "Hello, this is a seeded test message for tap-dixa integration tests.",
        },
        "attachments": [],
    }
    result = _request(
        "POST", f"/v1/conversations/{conversation_id}/messages", headers, payload, dry_run
    )
    if dry_run:
        return
    message_id = result.get("data", {}).get("messageId", "unknown")
    print(f"  ✓ Created message id={message_id} on conversation id={conversation_id}")


# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Seed test data for tap-dixa")
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help=f"Path to config.json (default: {DEFAULT_CONFIG_PATH})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print API payloads without making any requests",
    )
    parser.add_argument(
        "--email-integration-id",
        default=None,
        help="Dixa email integration address (e.g. support@email.dixa.io). "
             "Can also be set via config key 'email_integration_id' or env var TAP_DIXA_EMAIL_INTEGRATION_ID.",
    )
    args = parser.parse_args()

    config = _load_config(args.config)
    api_token = config.get("api_token") or os.getenv("TAP_DIXA_API_TOKEN")
    if not api_token:
        sys.exit("ERROR: 'api_token' not found in config or TAP_DIXA_API_TOKEN env var")

    # email_integration_id: the Dixa email address for the integration, e.g. 'support@email.dixa.io'
    email_integration_id = (
        args.email_integration_id
        or config.get("email_integration_id")
        or os.getenv("TAP_DIXA_EMAIL_INTEGRATION_ID")
    )
    if not email_integration_id:
        sys.exit(
            "ERROR: email_integration_id required.\n"
            "Provide via --email-integration-id, config key 'email_integration_id', "
            "or TAP_DIXA_EMAIL_INTEGRATION_ID env var.\n"
            "Tip: run `curl -H 'Authorization: <token>' https://dev.dixa.io/v1/contact-endpoints` "
            "to list available email integration addresses."
        )

    headers = _headers(api_token)
    print(f"Seeding tap-dixa test data {'(DRY RUN)' if args.dry_run else ''}...")

    requester_id = create_end_user(headers, args.dry_run)
    conversation_id = create_conversation(headers, requester_id, email_integration_id, args.dry_run)
    create_message(headers, conversation_id, None, args.dry_run)

    print("\nDone. Re-run integration tests — all streams should now have recent data.")


if __name__ == "__main__":
    main()
