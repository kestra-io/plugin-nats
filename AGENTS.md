# Kestra Nats Plugin

## What

description project.description Exposes 9 plugin components (tasks, triggers, and/or conditions).

## Why

Enables Kestra workflows to interact with NATS, allowing orchestration of NATS-based operations as part of data pipelines and automation workflows.

## How

### Architecture

Single-module plugin. Source packages under `io.kestra.plugin`:

- `nats`

Infrastructure dependencies (Docker Compose services):

- `nats`

### Key Plugin Classes

- `io.kestra.plugin.nats.core.Consume`
- `io.kestra.plugin.nats.core.Produce`
- `io.kestra.plugin.nats.core.RealtimeTrigger`
- `io.kestra.plugin.nats.core.Request`
- `io.kestra.plugin.nats.core.Trigger`
- `io.kestra.plugin.nats.kv.CreateBucket`
- `io.kestra.plugin.nats.kv.Delete`
- `io.kestra.plugin.nats.kv.Get`
- `io.kestra.plugin.nats.kv.Put`

### Project Structure

```
plugin-nats/
├── src/main/java/io/kestra/plugin/nats/kv/
├── src/test/java/io/kestra/plugin/nats/kv/
├── build.gradle
└── README.md
```

### Important Commands

```bash
# Build the plugin
./gradlew shadowJar

# Run tests
./gradlew test

# Build without tests
./gradlew shadowJar -x test
```

### Configuration

All tasks and triggers accept standard Kestra plugin properties. Credentials should use
`{{ secret('SECRET_NAME') }}` — never hardcode real values.

## Agents

**IMPORTANT:** This is a Kestra plugin repository (prefixed by `plugin-`, `storage-`, or `secret-`). You **MUST** delegate all coding tasks to the `kestra-plugin-developer` agent. Do NOT implement code changes directly — always use this agent.
