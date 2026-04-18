# Kestra Nats Plugin

## What

- Provides plugin components under `io.kestra.plugin.nats`.
- Includes classes such as `Request`, `Consume`, `Produce`, `Trigger`.

## Why

- This plugin integrates Kestra with NATS Core.
- It provides tasks that publish and subscribe to NATS subjects with triggers.

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

## References

- https://kestra.io/docs/plugin-developer-guide
- https://kestra.io/docs/plugin-developer-guide/contribution-guidelines
