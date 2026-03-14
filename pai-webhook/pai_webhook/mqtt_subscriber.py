import asyncio
from datetime import datetime, timezone
import json
import logging
import uuid

from paho.mqtt.client import Client, CallbackAPIVersion

logger = logging.getLogger("pai-webhook").getChild("mqtt")


class MQTTSubscriber:
    def __init__(self, config, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        self.config = config
        self.queue = queue
        self.loop = loop
        self.connected = False

        self.client = Client(
            callback_api_version=CallbackAPIVersion.VERSION2,
            client_id=config["client_id"],
            clean_session=config["clean_session"],
        )

        if config.get("username"):
            self.client.username_pw_set(config["username"], config.get("password"))

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, connect_flags, reason_code, properties=None):
        logger.info("Connected to MQTT broker (rc=%s)", reason_code)
        self.connected = True
        for topic in self.config["topics"]:
            client.subscribe(topic, qos=self.config["qos"])
            logger.info("Subscribed to %s (QoS %d)", topic, self.config["qos"])

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties=None):
        logger.warning("Disconnected from MQTT broker (rc=%s)", reason_code)
        self.connected = False

    def _on_message(self, client, userdata, message):
        payload_raw = message.payload.decode("utf-8", errors="replace")

        try:
            payload = json.loads(payload_raw)
        except (json.JSONDecodeError, ValueError):
            payload = payload_raw

        envelope = {
            "source": "pai",
            "id": str(uuid.uuid4()),
            "topic": message.topic,
            "payload": payload,
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }

        logger.debug("MQTT message: %s", message.topic)

        try:
            self.loop.call_soon_threadsafe(self._enqueue, envelope)
        except RuntimeError:
            logger.warning("Event loop closed, dropping message")

    def _enqueue(self, envelope):
        try:
            self.queue.put_nowait(envelope)
        except asyncio.QueueFull:
            try:
                dropped = self.queue.get_nowait()
                logger.warning("Queue full, dropped oldest message: %s", dropped.get("topic"))
                self.queue.put_nowait(envelope)
            except asyncio.QueueEmpty:
                self.queue.put_nowait(envelope)

    def start(self):
        self.client.connect(
            self.config["host"],
            self.config["port"],
            keepalive=60,
        )
        self.client.loop_start()
        logger.info("MQTT subscriber started")

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()
        logger.info("MQTT subscriber stopped")
