"""Webhook dispatcher that reads from queue and POSTs to webhook URL."""

import asyncio
import json
import logging
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import aiohttp

from .config import WebhookConfig

logger = logging.getLogger(__name__)


class WebhookDispatcher:
    """Dispatches MQTT messages to a webhook endpoint with retry logic."""

    def __init__(
        self,
        config: WebhookConfig,
        message_queue: asyncio.Queue[dict[str, Any]],
    ) -> None:
        """Initialize webhook dispatcher.

        Args:
            config: Webhook configuration.
            message_queue: Queue to read messages from.
        """
        self._config = config
        self._queue = message_queue
        self._running = False
        self._last_success: datetime | None = None
        self._last_failure: datetime | None = None
        self._session: aiohttp.ClientSession | None = None

    @property
    def last_success(self) -> datetime | None:
        """Return timestamp of last successful webhook delivery."""
        return self._last_success

    @property
    def last_failure(self) -> datetime | None:
        """Return timestamp of last failed webhook delivery."""
        return self._last_failure

    def _create_payload(self, message: dict[str, Any]) -> dict[str, Any]:
        """Create webhook payload from MQTT message.

        Payload format:
        {
            "source": "pai",
            "id": <uuid4>,
            "topic": <string>,
            "payload": <string|object>,
            "timestamp": <ISO8601 UTC with microseconds>
        }
        """
        now = datetime.now(timezone.utc)
        return {
            "source": "pai",
            "id": str(uuid.uuid4()),
            "topic": message["topic"],
            "payload": message["payload"],
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z",
        }

    def _calculate_backoff(self, attempt: int) -> float:
        """Calculate backoff delay for retry attempt.

        Uses backoff_base^(attempt-1) + random jitter 0-1s.

        Args:
            attempt: Current attempt number (1-indexed).

        Returns:
            Delay in seconds.
        """
        base_delay = self._config.backoff_base ** (attempt - 1)
        jitter = random.uniform(0, 1)  # noqa: S311 - non-crypto use is fine
        return base_delay + jitter

    async def _send_webhook(self, payload: dict[str, Any]) -> bool:
        """Send webhook with retry logic.

        Args:
            payload: The webhook payload to send.

        Returns:
            True if webhook was sent successfully, False if all retries exhausted.
        """
        headers = {
            "Content-Type": "application/json",
            **self._config.headers,
        }

        # Note: Content-Type is always set and not overridable via config
        headers["Content-Type"] = "application/json"

        for attempt in range(1, self._config.max_attempts + 1):
            try:
                async with self._session.post(
                    self._config.url,
                    data=json.dumps(payload),
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self._config.timeout),
                ) as response:
                    if 200 <= response.status < 300:
                        self._last_success = datetime.now(timezone.utc)
                        logger.debug(
                            "Webhook delivered successfully (attempt %d/%d), id=%s",
                            attempt,
                            self._config.max_attempts,
                            payload["id"],
                        )
                        return True
                    else:
                        logger.warning(
                            "Webhook returned status %d (attempt %d/%d), id=%s",
                            response.status,
                            attempt,
                            self._config.max_attempts,
                            payload["id"],
                        )
            except asyncio.TimeoutError:
                logger.warning(
                    "Webhook timeout (attempt %d/%d), id=%s",
                    attempt,
                    self._config.max_attempts,
                    payload["id"],
                )
            except aiohttp.ClientError:
                logger.warning(
                    "Webhook client error (attempt %d/%d), id=%s",
                    attempt,
                    self._config.max_attempts,
                    payload["id"],
                    exc_info=True,
                )

            # Check if we should retry
            if attempt < self._config.max_attempts:
                backoff = self._calculate_backoff(attempt)
                logger.info(
                    "Retrying webhook in %.2fs (attempt %d/%d), id=%s",
                    backoff,
                    attempt + 1,
                    self._config.max_attempts,
                    payload["id"],
                )
                await asyncio.sleep(backoff)

        # All retries exhausted
        self._last_failure = datetime.now(timezone.utc)
        logger.error(
            "Webhook delivery failed after %d attempts, dropping message, id=%s",
            self._config.max_attempts,
            payload["id"],
        )
        return False

    async def _handle_queue_overflow(self) -> dict[str, Any] | None:
        """Handle queue overflow by dropping oldest message.

        Uses get_nowait + put_nowait to make room in queue.

        Returns:
            The dropped message if any, None if queue was not full.
        """
        try:
            # Try to see if queue is full
            if self._queue.full():
                # Drop oldest message
                dropped = self._queue.get_nowait()
                logger.warning(
                    "Queue overflow, dropping oldest message: topic=%s",
                    dropped.get("topic"),
                )
                return dropped
        except asyncio.QueueEmpty:
            pass
        return None

    async def start(self) -> None:
        """Start the webhook dispatcher."""
        self._running = True
        self._session = aiohttp.ClientSession()

        logger.info("Webhook dispatcher started, target: %s", self._config.url)

        try:
            while self._running:
                try:
                    # Wait for message with a timeout to allow checking running flag
                    message = await asyncio.wait_for(self._queue.get(), timeout=0.1)

                    payload = self._create_payload(message)
                    await self._send_webhook(payload)

                    # Mark task as done
                    self._queue.task_done()

                except asyncio.TimeoutError:
                    # No message available, continue loop
                    continue
                except asyncio.CancelledError:
                    logger.info("Webhook dispatcher cancelled")
                    break
                except Exception:
                    logger.exception("Unexpected error in webhook dispatcher")
                    await asyncio.sleep(1)  # Prevent tight error loop

        finally:
            await self._session.close()
            self._session = None
            logger.info("Webhook dispatcher stopped")

    async def stop(self) -> None:
        """Signal the dispatcher to stop."""
        self._running = False

    async def drain(self, timeout: float) -> int:
        """Drain remaining messages in queue before shutdown.

        Args:
            timeout: Maximum time to spend draining.

        Returns:
            Number of messages remaining in queue (should be 0 if drain completed).
        """
        logger.info("Draining queue, %d messages remaining", self._queue.qsize())

        start_time = asyncio.get_event_loop().time()

        while not self._queue.empty():
            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= timeout:
                logger.warning("Drain timeout reached, %d messages remaining", self._queue.qsize())
                break

            try:
                message = self._queue.get_nowait()
                payload = self._create_payload(message)
                await self._send_webhook(payload)
                self._queue.task_done()
            except asyncio.QueueEmpty:
                break
            except Exception:
                logger.exception("Error during drain")
                break

        remaining = self._queue.qsize()
        if remaining > 0:
            logger.warning("Drain completed with %d messages remaining", remaining)
        else:
            logger.info("Queue drained successfully")

        return remaining
