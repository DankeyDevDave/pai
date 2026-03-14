import os
import sys

import yaml


def load_config(path=None):
    if path is None:
        path = os.environ.get("PAI_WEBHOOK_CONFIG", "/app/config.yaml")

    try:
        with open(path) as f:
            cfg = yaml.safe_load(f)
    except FileNotFoundError:
        print(f"Config file not found: {path}", file=sys.stderr)
        sys.exit(1)

    errors = []
    if not cfg.get("mqtt", {}).get("host"):
        errors.append("mqtt.host is required")
    if not cfg.get("mqtt", {}).get("topics"):
        errors.append("mqtt.topics is required (list of MQTT topic patterns)")
    if not cfg.get("webhook", {}).get("url"):
        errors.append("webhook.url is required")
    if errors:
        for e in errors:
            print(f"Config error: {e}", file=sys.stderr)
        sys.exit(1)

    mqtt = cfg.get("mqtt", {})
    webhook = cfg.get("webhook", {})
    retry = webhook.get("retry", {})
    health = cfg.get("health", {})
    logging_cfg = cfg.get("logging", {})

    return {
        "mqtt": {
            "host": mqtt["host"],
            "port": mqtt.get("port", 1883),
            "username": mqtt.get("username"),
            "password": mqtt.get("password"),
            "client_id": mqtt.get("client_id", "pai-webhook"),
            "qos": mqtt.get("qos", 1),
            "clean_session": mqtt.get("clean_session", True),
            "topics": mqtt["topics"],
        },
        "webhook": {
            "url": webhook["url"],
            "headers": webhook.get("headers", {}),
            "timeout": webhook.get("timeout", 10),
            "retry": {
                "max_attempts": retry.get("max_attempts", 3),
                "backoff_base": retry.get("backoff_base", 2),
            },
        },
        "health": {
            "port": health.get("port", 8080),
        },
        "logging": {
            "level": logging_cfg.get("level", "INFO"),
        },
    }
