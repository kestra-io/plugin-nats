# Kestra Nats Plugin

## What

- Provides plugin components under `io.kestra.plugin.nats`.
- Includes classes such as `Request`, `Consume`, `Produce`, `Trigger`.

## Why

- What user problem does this solve? Teams need to publish, subscribe, and work with NATS messaging from orchestrated workflows instead of relying on manual console work, ad hoc scripts, or disconnected schedulers.
- Why would a team adopt this plugin in a workflow? It keeps NATS steps in the same Kestra flow as upstream preparation, approvals, retries, notifications, and downstream systems.
- What operational/business outcome does it enable? It reduces manual handoffs and fragmented tooling while improving reliability, traceability, and delivery speed for processes that depend on NATS.

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
