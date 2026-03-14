import json
import logging

from aiohttp import web

logger = logging.getLogger("pai-webhook").getChild("health")


class HealthServer:
    def __init__(self, port, mqtt_subscriber, queue, dispatcher):
        self.port = port
        self.mqtt = mqtt_subscriber
        self.queue = queue
        self.dispatcher = dispatcher
        self.runner = None

    async def start(self):
        app = web.Application()
        app.router.add_get("/health", self._handle_health)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "0.0.0.0", self.port)
        await site.start()
        logger.info("Health server listening on port %d", self.port)

    async def stop(self):
        if self.runner:
            await self.runner.cleanup()

    async def _handle_health(self, request):
        connected = self.mqtt.connected
        status_code = 200 if connected else 503

        body = {
            "status": "ok" if connected else "unhealthy",
            "mqtt_connected": connected,
            "queue_size": self.queue.qsize(),
            "last_webhook_success": self.dispatcher.last_success,
            "last_webhook_failure": self.dispatcher.last_failure,
        }

        return web.Response(
            status=status_code,
            body=json.dumps(body),
            content_type="application/json",
        )
