"""Main entry point for PAI Webhook Bridge."""

import asyncio
import logging
import signal
import sys
from typing import Any

from .config import Config, load_config
from .health import HealthServer
from .mqtt_subscriber import MQTTSubscriber
from .webhook_dispatcher import WebhookDispatcher

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


async def run_bridge(config: Config) -> None:
    """Run the PAI Webhook Bridge.

    Args:
        config: Application configuration.
    """
    # Create the message queue
    message_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=config.queue_maxsize)
    logger.info("Message queue created with maxsize=%d", config.queue_maxsize)

    # Get the event loop for thread-safe operations
    loop = asyncio.get_event_loop()

    # Create components
    mqtt_subscriber = MQTTSubscriber(
        config=config.mqtt,
        message_queue=message_queue,
        loop=loop,
    )

    webhook_dispatcher = WebhookDispatcher(
        config=config.webhook,
        message_queue=message_queue,
    )

    health_server = HealthServer(
        config=config.health,
        mqtt_subscriber=mqtt_subscriber,
        webhook_dispatcher=webhook_dispatcher,
        message_queue=message_queue,
    )

    # Track running state
    shutdown_event = asyncio.Event()

    # Setup signal handlers
    def signal_handler(sig: signal.Signals) -> None:
        logger.info("Received signal %s, initiating graceful shutdown", sig.name)
        shutdown_event.set()

    # Register signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: signal_handler(s))

    try:
        # Start health server first
        await health_server.start()

        # Start MQTT subscriber
        await mqtt_subscriber.start()

        # Start webhook dispatcher in background
        dispatcher_task = asyncio.create_task(webhook_dispatcher.start())

        # Wait for shutdown signal
        logger.info("PAI Webhook Bridge started, waiting for messages...")
        await shutdown_event.wait()

        logger.info("Beginning graceful shutdown with %.1fs drain timeout", config.drain_timeout)

        # Stop accepting new messages
        await mqtt_subscriber.stop()

        # Stop the dispatcher (it will finish current message)
        await webhook_dispatcher.stop()

        # Wait for dispatcher to stop with timeout
        try:
            await asyncio.wait_for(dispatcher_task, timeout=config.drain_timeout)
        except asyncio.TimeoutError:
            logger.warning("Dispatcher did not stop within drain timeout")

        # Drain remaining queue
        await webhook_dispatcher.drain(config.drain_timeout)

    except Exception:
        logger.exception("Fatal error in PAI Webhook Bridge")
        raise
    finally:
        # Stop health server
        await health_server.stop()
        logger.info("PAI Webhook Bridge shutdown complete")


def main() -> None:
    """Main entry point."""
    try:
        config = load_config()
        logger.info("Configuration loaded successfully")
    except FileNotFoundError as e:
        logger.error("Configuration file not found: %s", e)
        sys.exit(1)
    except ValueError as e:
        logger.error("Configuration validation error: %s", e)
        sys.exit(1)
    except Exception:
        logger.exception("Failed to load configuration")
        sys.exit(1)

    try:
        asyncio.run(run_bridge(config))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception:
        logger.exception("Application error")
        sys.exit(1)


if __name__ == "__main__":
    main()
