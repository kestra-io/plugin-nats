# How to use the NATS plugin

Publish and consume messages on NATS subjects, make request-reply calls, and manage JetStream key-value buckets from Kestra flows.

## Common properties

Set `url` to your NATS server URL (e.g. `nats://localhost:4222`). For authentication, use one of: `username` and `password` for plaintext auth, `token` for token auth, or `creds` for a NATS credentials file. Store secrets in [secrets](https://kestra.io/docs/concepts/secret) and apply connection properties globally with [plugin defaults](https://kestra.io/docs/workflow-components/plugin-defaults).

## Tasks

`core.Produce` publishes messages to a `subject` — pass message data via `from`. Messages can include a `headers` map alongside the `data` payload.

`core.Consume` reads messages from a JetStream `subject` with explicit acknowledgement. Set `durableId` to name the consumer for resumable consumption. Control where consumption starts with `deliverPolicy` (e.g. `All`, `New`, `Last`) and bound the batch with `maxRecords` or `maxDuration`.

`core.Request` performs a NATS request-reply — set `subject` and `from`, and read the reply from the `response` output. Set `requestTimeout` to control how long to wait for a response (default 5 seconds).

The `kv` tasks manage JetStream key-value buckets: `kv.CreateBucket` creates a bucket by `name`; `kv.Put` writes key-value pairs from a `values` map to a `bucketName`; `kv.Get` reads keys by name or specific revision; `kv.Delete` removes keys.

`core.Trigger` polls on a schedule (default 60 seconds) and starts one execution per batch. `core.RealtimeTrigger` starts one execution per message as it arrives.
