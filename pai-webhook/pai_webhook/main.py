import asyncio
import logging
import signal
import sys

from pai_webhook.config import load_config
from pai_webhook.health import HealthServer
from pai_webhook.mqtt_subscriber import MQTTSubscriber
from pai_webhook.webhook_dispatcher import WebhookDispatcher

logger = logging.getLogger("pai-webhook")


def setup_logging(level_name):
    level = getattr(logging, level_name.upper(), logging.INFO)
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)-8s - %(name)s - %(message)s",
        stream=sys.stdout,
    )


async def run(config):
    loop = asyncio.get_running_loop()
    queue = asyncio.Queue(maxsize=10000)

    subscriber = MQTTSubscriber(config["mqtt"], queue, loop)
    dispatcher = WebhookDispatcher(config["webhook"], queue)
    health = HealthServer(config["health"]["port"], subscriber, queue, dispatcher)

    shutdown_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received")
        shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    subscriber.start()
    await health.start()

    dispatcher_task = asyncio.create_task(dispatcher.start())

    await shutdown_event.wait()

    logger.info("Shutting down...")
    subscriber.stop()
    await dispatcher.stop(drain_timeout=30)
    await health.stop()
    dispatcher_task.cancel()
    try:
        await dispatcher_task
    except asyncio.CancelledError:
        pass


def main():
    config = load_config()
    setup_logging(config["logging"]["level"])
    logger.info("PAI Webhook Bridge v0.1.0 starting")
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
