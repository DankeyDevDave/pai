# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PAI (Paradox Alarm Interface) is a Python middleware that connects to Paradox alarm panels (EVO, Spectra, Magellan families) and exposes them through MQTT, Home Assistant, Signal, Pushbullet, Pushover, GSM, and IP interfaces. It supports serial, direct IP, local IP module, and cloud STUN connections.

## Build & Development Commands

```bash
# Install in development mode
pip install -e .

# Run the service
pai-service                         # uses /etc/pai/pai.conf by default
pai-service --config path/to.conf   # custom config

# Run all tests (multi-version via tox)
tox

# Run tests for current Python version
cd tests && pytest

# Run a single test file
cd tests && pytest test_config.py

# Run a single test
cd tests && pytest test_config.py::test_function_name -v

# Linting (run from repo root)
flake8 paradox/
black paradox/ tests/
isort paradox/ tests/

# Pre-commit hooks (all linting + checks)
pre-commit run --all-files

# Other utilities
pai-dump-memory                     # dump panel memory
ip150-connection-decrypt            # decrypt IP150 connection logs
```

## Code Style

- **Formatter**: Black (target Python 3.8+)
- **Import sorting**: isort (black profile, `force_sort_within_sections=true`)
- **Linter**: flake8 with flake8-bugbear (max-complexity=25, ignores E501/W503/E203/D202/W504/E128)
- **Minimum Python**: 3.8 (`pyupgrade --py38-plus` enforced by pre-commit)
- **Async testing**: pytest-asyncio with `asyncio_mode = "auto"`
- Logging uses hierarchy: `logging.getLogger("PAI").getChild(__name__)`

## Architecture

### Three-Layer Design

```
Connection Layer → Hardware Abstraction → Integration Interfaces
(paradox/connections/)   (paradox/hardware/)      (paradox/interfaces/)
```

**Connection Layer** (`paradox/connections/`): Handles physical communication with panels.
- `Connection` base class extends `AsyncMessageManager` for async send/receive
- Implementations: `SerialCommunication`, `BareIPConnection`, `LocalIPConnection`, `StunIPConnection`
- IP connections in `connections/ip/` handle encryption, STUN, and IP150 module protocols

**Hardware Abstraction** (`paradox/hardware/`): Panel-specific protocol handling.
- Two panel families: `evo/` (EVO48/96/192/HD/HD+) and `spectra_magellan/` (MG5000/5050, SP4000-7000)
- Each family has: `panel.py` (communication), `parsers.py` (binary protocol via `construct`), `event.py` (event maps), `property.py` (property maps), `adapters.py` (state parsing)
- Panel detection is automatic based on firmware response

**Integration Interfaces** (`paradox/interfaces/`): Output adapters for external systems.
- `InterfaceManager` dynamically loads enabled interfaces based on config flags
- Interfaces lazy-import their dependencies (e.g., Pushbullet only imports when enabled)
- MQTT interfaces in `mqtt/` with HomeAssistant entity autodiscovery in `mqtt/entities/`

### Event System

Global PubSub singleton in `paradox/lib/ps.py` connects all layers:
- Topics: `pai_events`, `pai_changes`, `pai_notifications`, `pai_status_update`
- `sendEvent()`, `sendChange()`, `sendNotification()` publish; interfaces subscribe
- Supports both sync callbacks and async coroutines

### Core Orchestrator

`Paradox` class in `paradox/paradox.py` is the main coordinator:
- Manages connection lifecycle with retry logic
- Runs the main async loop: connect → authenticate → fetch labels → poll status
- State stored in `MemoryStorage` (`paradox/data/memory_storage.py`)
- Element types: zones, partitions, outputs (PGMs), users, doors, modules, repeaters, keypads

### Configuration

`paradox/config.py` holds a `Config` class with 150+ options. Config loaded from file at startup (`pai.conf`). See `config/pai.conf.example` for all options with documentation.

### Key Data Types

`paradox/event.py` defines the event model:
- `Change` - property state change (e.g., zone open/closed)
- `Event` / `LiveEvent` - alarm events from panel
- `ChangeEvent` - wraps a Change as an Event
- `Notification` - outbound notification messages

### Binary Protocol

Uses the `construct` library extensively for parsing/building binary messages to/from panels. Parser definitions are in `hardware/*/parsers.py` and `connections/ip/parsers.py`.

## Exception Hierarchy

- `PAIException` → base
  - `PAIConnectionError` → connection issues (triggers reconnect)
  - `StatusRequestException` → status polling failure
  - `PAICriticalException` → fatal errors (stops PAI)
    - `AuthenticationFailed`, `CodeLockout`, `PanelNotDetected`, `SerialConnectionOpenFailed`

## Docker

Multi-arch image (i386, amd64, armv6/v7, arm64) based on `python:3.14-alpine`. Default ports: 10000 (IP interface), config via environment variables or mounted `/etc/pai/pai.conf`.
