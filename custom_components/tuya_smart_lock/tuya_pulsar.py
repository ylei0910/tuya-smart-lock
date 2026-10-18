"""Tuya Pulsar (cloud push message) client for Tuya Smart Lock.

Vendors a trimmed version of the WebSocket-based message-queue client
from Tuya's own tuya-connector-python SDK
(https://github.com/tuya/tuya-connector-python/blob/main/tuya_connector/openpulsar.py),
rather than depending on the full package or the native pulsar-client
library (which requires a compiled libpulsar binary - a poor fit for a
Home Assistant custom component). This is a plain websocket client
(websocket-client, pure Python) plus AES-ECB decryption via
`cryptography` (already a core Home Assistant dependency).

Runs on its own OS thread (not asyncio), blocked on a real socket read.
That's deliberate: delivery doesn't depend on Home Assistant's event
loop reliably firing scheduled timers during idle periods, which this
integration's own polling has shown to be unreliable on at least one
instance - a real socket read is woken by the kernel the instant a
packet arrives, independent of that.

PHASE 1 (current): log-and-observe only. on_message is called with the
decrypted event dict; nothing is written to Home Assistant entity state
yet, so on_message can safely run directly on this background thread.
A later phase that updates entities will need to hand off to the event
loop via hass.add_job() instead of calling HA state-writing APIs here.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import ssl
import threading
from typing import Callable

import websocket
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from .const import API_REGIONS

_LOGGER = logging.getLogger(__name__)

WS_QUERY_PARAMS = "?ackTimeoutMillis=3000&subscriptionType=Failover"
TOPIC_PROD = "event"
CONNECT_TIMEOUT_SECONDS = 5
PING_INTERVAL_SECONDS = 30
PING_TIMEOUT_SECONDS = 5
RECONNECT_DELAY_SECONDS = 5


def pulsar_ws_host(region: str) -> str:
    """Derive the Pulsar websocket host from the REST API host for this region.

    Matches Tuya's own mapping (openapi.tuya{region}.com -> mqe.tuya{region}.com:8285),
    confirmed against tuya-connector-python's (commented-out, but accurate)
    TuyaCloudPulsarWSEndpoint enum.
    """
    api_host = API_REGIONS[region]  # e.g. "openapi.tuyaeu.com"
    mq_host = api_host.replace("openapi", "mqe", 1)
    return f"wss://{mq_host}:8285/"


class TuyaPulsarClient(threading.Thread):
    """Background-thread websocket client for Tuya's cloud push messages."""

    def __init__(
        self,
        access_id: str,
        access_secret: str,
        region: str,
        on_message: Callable[[dict], None],
    ) -> None:
        super().__init__(daemon=True, name="tuya_smart_lock_pulsar")
        self._access_id = access_id
        self._access_secret = access_secret
        self._ws_endpoint = pulsar_ws_host(region)
        self._on_message = on_message
        self._stop_event = threading.Event()
        self._ws_app = None

    def _topic_url(self) -> str:
        return (
            self._ws_endpoint
            + "ws/v2/consumer/persistent/"
            + self._access_id
            + "/out/"
            + TOPIC_PROD
            + "/"
            + self._access_id
            + "-sub"
            + WS_QUERY_PARAMS
        )

    def _gen_password(self) -> str:
        """Reproduce Tuya's auth password derivation exactly.

        password = md5(access_id + md5(access_secret))[8:24]
        """
        secret_md5 = hashlib.md5(self._access_secret.encode("utf-8")).hexdigest()
        mix = self._access_id + secret_md5
        return hashlib.md5(mix.encode("utf-8")).hexdigest()[8:24]

    def _decrypt_gcm(self, raw_b64: str) -> str:
        """Decrypt a message's 'data' field with AES-GCM.

        Key is the same access_secret[8:24] as ECB. Framing (confirmed
        against Tuya's own, currently-maintained tuya-pulsar-sdk-go -
        pkg/tyutils/aes.go's GcmDecrypt): first 12 bytes of the decoded
        blob are the nonce, the rest is ciphertext with the auth tag
        appended - the standard AEAD convention, which is also exactly
        what cryptography's AESGCM.decrypt expects, so no manual tag
        splitting is needed. No associated data.
        """
        key = self._access_secret[8:24].encode("utf-8")
        raw = base64.b64decode(raw_b64)
        nonce, ciphertext = raw[:12], raw[12:]
        plaintext = AESGCM(key).decrypt(nonce, ciphertext, None)
        return plaintext.decode("utf-8")

    def _decrypt_ecb(self, raw_b64: str) -> str:
        """Decrypt a message's 'data' field: AES-ECB with key=access_secret[8:24], PKCS7-unpadded."""
        key = self._access_secret[8:24].encode("utf-8")
        raw = base64.b64decode(raw_b64)
        decryptor = Cipher(algorithms.AES(key), modes.ECB()).decryptor()
        decrypted = decryptor.update(raw) + decryptor.finalize()
        pad_len = decrypted[-1]
        return decrypted[:-pad_len].decode("utf-8")

    def _decrypt(self, raw_b64: str) -> str:
        """Decrypt a message's 'data' field, trying GCM (the configured/recommended
        mode) first and falling back to ECB.

        This client uses Tuya's older websocket-wrapper transport rather
        than the native Pulsar protocol, and it's unconfirmed whether
        that transport exposes the per-message encryption-mode property
        ("em") the native-protocol SDKs use to pick a decrypt path
        up front. Trying GCM then falling back to ECB avoids depending
        on an unconfirmed field - a successful decrypt always produces
        valid UTF-8 JSON, which is the actual thing that matters.
        """
        try:
            return self._decrypt_gcm(raw_b64)
        except Exception:
            return self._decrypt_ecb(raw_b64)

    def _handle_ws_message(self, ws_app, message: str) -> None:
        message_id = None
        try:
            envelope = json.loads(message)
            message_id = envelope.get("messageId")
            payload = base64.b64decode(envelope["payload"]).decode("ascii")
            data_map = json.loads(payload)
            decrypted = self._decrypt(data_map["data"])
            event = json.loads(decrypted)
        except Exception:
            _LOGGER.exception("Failed to decode/decrypt Pulsar message")
        else:
            try:
                self._on_message(event)
            except Exception:
                _LOGGER.exception("Error in Pulsar message handler")
        finally:
            if message_id is not None:
                self._ack(message_id)

    def _ack(self, message_id) -> None:
        try:
            self._ws_app.send(json.dumps({"messageId": message_id}))
        except Exception:
            _LOGGER.exception("Failed to ack Pulsar message %s", message_id)

    def _handle_ws_open(self, ws_app) -> None:
        # WARNING, not debug: this is the only positive "it's actually
        # working" signal this client ever produces on its own, and a
        # prior version's total silence while quietly dead (see
        # _run_once's docstring) made a real failure indistinguishable
        # from a healthy connection for hours. Rare enough (once per
        # connect, not per-message) that this isn't log spam.
        _LOGGER.warning("Pulsar websocket connected")

    def _handle_ws_error(self, ws_app, error) -> None:
        _LOGGER.warning("Pulsar websocket error: %s", error)

    def _handle_ws_close(self, ws_app, close_status_code, close_msg) -> None:
        _LOGGER.warning(
            "Pulsar websocket closed (code=%s, msg=%s)", close_status_code, close_msg
        )

    def run(self) -> None:
        # Outer try/except as a last-resort safety net: run() is a Thread's
        # entry point, so any exception that escapes it kills this thread
        # silently (Python's default unhandled-thread-exception handling
        # goes to stderr, not Home Assistant's logger) - no more reconnect
        # attempts, ever, with nothing in the HA log explaining why. A
        # previous version had this gap: run_forever() was wrapped, but
        # WebSocketApp(...) construction on each retry iteration was not,
        # and losing visibility into a dead thread for hours is exactly
        # what happened here.
        try:
            self._run_loop()
        except Exception:
            _LOGGER.exception("Pulsar client thread crashed and will not retry")

    def _run_loop(self) -> None:
        header = {
            "Connection": "Upgrade",
            "username": self._access_id,
            "password": self._gen_password(),
        }
        websocket.setdefaulttimeout(CONNECT_TIMEOUT_SECONDS)
        while not self._stop_event.is_set():
            self._run_once(header)
            if not self._stop_event.is_set():
                self._stop_event.wait(RECONNECT_DELAY_SECONDS)

    def _run_once(self, header: dict) -> None:
        """One connect-and-run_forever attempt. Never lets an exception escape.

        Everything that can fail - building the WebSocketApp as much as
        running it - is inside this one try/except, so a bad attempt can
        only end this attempt, never the retry loop itself.
        """
        try:
            self._ws_app = websocket.WebSocketApp(
                self._topic_url(),
                header=header,
                on_open=self._handle_ws_open,
                on_message=self._handle_ws_message,
                on_error=self._handle_ws_error,
                on_close=self._handle_ws_close,
            )
            self._ws_app.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=PING_INTERVAL_SECONDS,
                ping_timeout=PING_TIMEOUT_SECONDS,
            )
        except Exception:
            _LOGGER.exception("Pulsar websocket connection attempt failed")

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws_app is not None:
            try:
                self._ws_app.close()
            except Exception:
                pass
