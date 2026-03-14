"""MQTT subscriber that bridges paho-mqtt to asyncio queue."""

import asyncio
import json
import logging
from typing import Any

import paho.mqtt.client as mqtt

from .config import MQTTConfig

logger = logging.getLogger(__name__)


class MQTTSubscriber:
    """MQTT subscriber using paho-mqtt with asyncio queue bridge."""

    def __init__(
        self,
        config: MQTTConfig,
        message_queue: asyncio.Queue[dict[str, Any]],
        loop: asyncio.AbstractEventLoop,
    ) -> None:
        """Initialize MQTT subscriber.

        Args:
            config: MQTT configuration.
            message_queue: asyncio queue to put received messages on.
            loop: asyncio event loop for thread-safe queue operations.
        """
        self._config = config
        self._queue = message_queue
        self._loop = loop
        self._connected = False
        self._client: mqtt.Client | None = None

    @property
    def connected(self) -> bool:
        """Return whether MQTT client is currently connected."""
        return self._connected

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        rc: int,
        properties: Any = None,
    ) -> None:
        """Callback when connected to MQTT broker."""
        if rc == 0:
            logger.info("Connected to MQTT broker at %s:%d", self._config.host, self._config.port)
            self._connected = True

            # Subscribe to configured topics
            for topic in self._config.topics:
                client.subscribe(topic, qos=self._config.qos)
                logger.info("Subscribed to topic: %s (QoS %d)", topic, self._config.qos)
        else:
            logger.error("MQTT connection failed with code %d", rc)
            self._connected = False

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        rc: int,
        properties: Any = None,
    ) -> None:
        """Callback when disconnected from MQTT broker."""
        self._connected = False
        if rc == 0:
            logger.info("Disconnected from MQTT broker")
        else:
            logger.warning("Unexpected disconnect from MQTT broker, rc=%d", rc)

    def _on_message(
        self,
        client: mqtt.Client,
        userdata: Any,
        message: mqtt.MQTTMessage,
    ) -> None:
        """Callback when a message is received.

        Uses loop.call_soon_threadsafe to bridge from paho thread to asyncio queue.
        """
        try:
            topic = message.topic
            payload_bytes = message.payload

            # Try to decode as JSON, fall back to raw string
            payload: str | dict[str, Any]
            try:
                decoded = payload_bytes.decode("utf-8")
                # Attempt JSON parse
                try:
                    payload = json.loads(decoded)
                except json.JSONDecodeError:
                    # Not JSON, use raw string
                    payload = decoded
            except UnicodeDecodeError:
                # Binary data - encode as base64 for safe transport
                import base64

                payload = base64.b64encode(payload_bytes).decode("ascii")

            msg_dict = {
                "topic": topic,
                "payload": payload,
                "qos": message.qos,
                "retain": message.retain,
            }

            # Bridge to asyncio queue using call_soon_threadsafe
            self._loop.call_soon_threadsafe(self._put_message, msg_dict)

        except Exception:
            logger.exception("Error processing MQTT message on topic %s", message.topic)

    def _put_message(self, msg: dict[str, Any]) -> None:
        """Put message on queue (called from asyncio loop thread)."""
        try:
            self._queue.put_nowait(msg)
        except asyncio.QueueFull:
            logger.warning("Message queue full, dropping message for topic: %s", msg.get("topic"))

    async def start(self) -> None:
        """Start the MQTT subscriber."""
        # Create client with clean_session setting
        self._client = mqtt.Client(
            client_id=self._config.client_id,
            clean_session=self._config.clean_session,
            protocol=mqtt.MQTTv311,
        )

        # Set credentials if provided
        if self._config.username:
            self._client.username_pw_set(self._config.username, self._config.password or "")

        # Set callbacks
        self._client.on_connect = self._on_connect
        self._client.on_disconnect = self._on_disconnect
        self._client.on_message = self._on_message

        # Configure keepalive
        self._client.enable_logger(logger)

        try:
            # Connect to broker
            self._client.connect(
                self._config.host,
                self._config.port,
                keepalive=self._config.keepalive,
            )

            # Start the paho network loop in a background thread
            self._client.loop_start()
            logger.info("MQTT subscriber started")

        except Exception:
            logger.exception("Failed to connect to MQTT broker at %s:%d", self._config.host, self._config.port)
            raise

    async def stop(self) -> None:
        """Stop the MQTT subscriber."""
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()
            self._connected = False
            logger.info("MQTT subscriber stopped")
