"""Health server for monitoring PAI Webhook Bridge status."""

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from aiohttp import web

from .config import HealthConfig
from .mqtt_subscriber import MQTTSubscriber
from .webhook_dispatcher import WebhookDispatcher

logger = logging.getLogger(__name__)


class HealthServer:
    """HTTP health server exposing status endpoints."""

    def __init__(
        self,
        config: HealthConfig,
        mqtt_subscriber: MQTTSubscriber,
        webhook_dispatcher: WebhookDispatcher,
        message_queue: asyncio.Queue[dict[str, Any]],
    ) -> None:
        """Initialize health server.

        Args:
            config: Health server configuration.
            mqtt_subscriber: MQTT subscriber to check connection status.
            webhook_dispatcher: Webhook dispatcher to check delivery status.
            message_queue: Message queue to check size.
        """
        self._config = config
        self._mqtt_subscriber = mqtt_subscriber
        self._webhook_dispatcher = webhook_dispatcher
        self._queue = message_queue
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None

    async def _health_handler(self, request: web.Request) -> web.Response:
        """Handle GET /health requests.

        Returns JSON with:
        - mqtt_connected: bool
        - queue_size: int
        - last_webhook_success: ISO8601 timestamp or null
        - last_webhook_failure: ISO8601 timestamp or null
        """
        def format_datetime(dt: datetime | None) -> str | None:
            if dt is None:
                return None
            return dt.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"

        status: dict[str, Any] = {
            "mqtt_connected": self._mqtt_subscriber.connected,
            "queue_size": self._queue.qsize(),
            "last_webhook_success": format_datetime(self._webhook_dispatcher.last_success),
            "last_webhook_failure": format_datetime(self._webhook_dispatcher.last_failure),
        }

        return web.Response(
            text=json.dumps(status),
            content_type="application/json",
        )

    async def start(self) -> None:
        """Start the health server."""
        self._app = web.Application()
        self._app.router.add_get("/health", self._health_handler)

        self._runner = web.AppRunner(self._app)
        await self._runner.setup()

        site = web.TCPSite(self._runner, self._config.host, self._config.port)
        await site.start()

        logger.info("Health server started on %s:%d", self._config.host, self._config.port)

    async def stop(self) -> None:
        """Stop the health server."""
        if self._runner:
            await self._runner.cleanup()
            logger.info("Health server stopped")
