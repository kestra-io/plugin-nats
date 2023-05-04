#!/bin/ash

ash -c "while ! echo 'exit' | curl -o /dev/null -s -f telnet://localhost:4222; do sleep 1; done"

nats str add --user kestra --password k3stra --config=stream.conf