import asyncio
from datetime import datetime, timezone
import json
import logging
import random

import aiohttp

logger = logging.getLogger("pai-webhook").getChild("webhook")


class WebhookDispatcher:
    def __init__(self, config, queue: asyncio.Queue):
        self.config = config
        self.queue = queue
        self.session = None
        self._running = False

        self.last_success = None
        self.last_failure = None

    async def start(self):
        self.session = aiohttp.ClientSession()
        self._running = True
        logger.info("Webhook dispatcher started -> %s", self.config["url"])
        await self._consume()

    async def stop(self, drain_timeout=30):
        self._running = False
        if not self.queue.empty():
            logger.info("Draining %d queued messages (timeout=%ds)", self.queue.qsize(), drain_timeout)
            try:
                await asyncio.wait_for(self._drain(), timeout=drain_timeout)
            except asyncio.TimeoutError:
                remaining = self.queue.qsize()
                logger.warning("Drain timeout, discarding %d messages", remaining)

        if self.session:
            await self.session.close()
        logger.info("Webhook dispatcher stopped")

    async def _consume(self):
        while self._running:
            try:
                envelope = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._deliver(envelope)

    async def _drain(self):
        while not self.queue.empty():
            try:
                envelope = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            await self._deliver(envelope)

    async def _deliver(self, envelope):
        max_attempts = self.config["retry"]["max_attempts"]
        backoff_base = self.config["retry"]["backoff_base"]

        headers = {"Content-Type": "application/json"}
        headers.update(self.config.get("headers", {}))
        headers["Content-Type"] = "application/json"  # enforce, not overridable

        for attempt in range(1, max_attempts + 1):
            try:
                async with self.session.post(
                    self.config["url"],
                    data=json.dumps(envelope, ensure_ascii=False),
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.config["timeout"]),
                ) as resp:
                    if 200 <= resp.status < 300:
                        self.last_success = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                        logger.info("Delivered %s (status=%d)", envelope["topic"], resp.status)
                        return
                    else:
                        body = await resp.text()
                        logger.warning(
                            "Webhook returned %d for %s (attempt %d/%d): %s",
                            resp.status, envelope["topic"], attempt, max_attempts, body[:200],
                        )
            except Exception as e:
                logger.warning(
                    "Webhook delivery failed for %s (attempt %d/%d): %s",
                    envelope["topic"], attempt, max_attempts, e,
                )

            if attempt < max_attempts:
                delay = (backoff_base ** (attempt - 1)) + random.uniform(0, 1)
                logger.debug("Retrying in %.1fs", delay)
                await asyncio.sleep(delay)

        self.last_failure = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        logger.warning("Dropped message after %d attempts: %s", max_attempts, envelope["topic"])
