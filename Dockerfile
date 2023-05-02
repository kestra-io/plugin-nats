FROM nats:alpine

COPY --from=golang:alpine /usr/local/go/ /usr/local/go/
ENV GOPATH="/usr/local/go"
ENV PATH="/usr/local/go/bin:${PATH}"
RUN go install github.com/nats-io/natscli/nats@latest
RUN apk add curl

COPY js.conf /js.conf
COPY stream.conf /stream.conf
COPY init-nats.sh /

