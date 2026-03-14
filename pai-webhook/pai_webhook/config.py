"""Configuration loader for PAI Webhook Bridge."""

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class MQTTConfig:
    """MQTT connection configuration."""

    host: str = "localhost"
    port: int = 1883
    username: str | None = None
    password: str | None = None
    client_id: str = "pai-webhook-bridge"
    qos: int = 1
    clean_session: bool = True
    keepalive: int = 60
    topics: list[str] = field(default_factory=lambda: ["paradox/#"])


@dataclass
class WebhookConfig:
    """Webhook dispatcher configuration."""

    url: str = ""  # Required - must be set in config
    timeout: float = 30.0
    max_attempts: int = 5
    backoff_base: float = 2.0
    headers: dict[str, str] = field(default_factory=dict)


@dataclass
class HealthConfig:
    """Health server configuration."""

    host: str = "0.0.0.0"
    port: int = 8080


@dataclass
class Config:
    """Main configuration container."""

    mqtt: MQTTConfig = field(default_factory=MQTTConfig)
    webhook: WebhookConfig = field(default_factory=WebhookConfig)
    health: HealthConfig = field(default_factory=HealthConfig)
    queue_maxsize: int = 10000
    drain_timeout: float = 30.0


def _deep_update(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Recursively update a dictionary."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_update(result[key], value)
        else:
            result[key] = value
    return result


def load_config(config_path: str | None = None) -> Config:
    """Load configuration from YAML file.

    Args:
        config_path: Path to config file. If None, uses PAI_WEBHOOK_CONFIG env var
                    or defaults to /app/config.yaml.

    Returns:
        Loaded and validated Config object.

    Raises:
        FileNotFoundError: If config file doesn't exist.
        ValueError: If required fields are missing or invalid.
    """
    if config_path is None:
        config_path = os.environ.get("PAI_WEBHOOK_CONFIG", "/app/config.yaml")

    path = Path(config_path)

    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    with open(path) as f:
        raw_config = yaml.safe_load(f) or {}

    # Start with defaults
    config = Config()

    # Apply overrides from file
    if "mqtt" in raw_config:
        mqtt_data = raw_config["mqtt"]
        config.mqtt = MQTTConfig(
            host=mqtt_data.get("host", config.mqtt.host),
            port=mqtt_data.get("port", config.mqtt.port),
            username=mqtt_data.get("username"),
            password=mqtt_data.get("password"),
            client_id=mqtt_data.get("client_id", config.mqtt.client_id),
            qos=mqtt_data.get("qos", config.mqtt.qos),
            clean_session=mqtt_data.get("clean_session", config.mqtt.clean_session),
            keepalive=mqtt_data.get("keepalive", config.mqtt.keepalive),
            topics=mqtt_data.get("topics", config.mqtt.topics),
        )

    if "webhook" in raw_config:
        webhook_data = raw_config["webhook"]
        config.webhook = WebhookConfig(
            url=webhook_data.get("url", ""),
            timeout=webhook_data.get("timeout", config.webhook.timeout),
            max_attempts=webhook_data.get("max_attempts", config.webhook.max_attempts),
            backoff_base=webhook_data.get("backoff_base", config.webhook.backoff_base),
            headers=webhook_data.get("headers", {}),
        )

    if "health" in raw_config:
        health_data = raw_config["health"]
        config.health = HealthConfig(
            host=health_data.get("host", config.health.host),
            port=health_data.get("port", config.health.port),
        )

    # Top-level config options
    if "queue_maxsize" in raw_config:
        config.queue_maxsize = raw_config["queue_maxsize"]
    if "drain_timeout" in raw_config:
        config.drain_timeout = raw_config["drain_timeout"]

    # Validate required fields
    _validate_config(config)

    return config


def _validate_config(config: Config) -> None:
    """Validate configuration values.

    Args:
        config: Config to validate.

    Raises:
        ValueError: If validation fails.
    """
    if not config.webhook.url:
        raise ValueError("webhook.url is required and must not be empty")

    if not config.webhook.url.startswith(("http://", "https://")):
        raise ValueError("webhook.url must be a valid HTTP or HTTPS URL")

    if config.mqtt.qos not in (0, 1, 2):
        raise ValueError(f"mqtt.qos must be 0, 1, or 2, got {config.mqtt.qos}")

    if config.mqtt.port < 1 or config.mqtt.port > 65535:
        raise ValueError(f"mqtt.port must be between 1 and 65535, got {config.mqtt.port}")

    if config.webhook.max_attempts < 1:
        raise ValueError(f"webhook.max_attempts must be >= 1, got {config.webhook.max_attempts}")

    if config.webhook.backoff_base <= 0:
        raise ValueError(f"webhook.backoff_base must be > 0, got {config.webhook.backoff_base}")

    if config.queue_maxsize < 1:
        raise ValueError(f"queue_maxsize must be >= 1, got {config.queue_maxsize}")

    if config.drain_timeout < 0:
        raise ValueError(f"drain_timeout must be >= 0, got {config.drain_timeout}")
