package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import jakarta.validation.constraints.NotNull;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a request to a NATS subject and wait for a reply."
)
@Plugin(
    examples = {
        @Example(
            title = "Send a request to the subject and wait for the reply (using username/password authentication).",
            full = true,
            code = """
                id: nats_request_reply
                namespace: company.team

                tasks:
                  - id: request
                    type: io.kestra.plugin.nats.Request
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: "greet.bob"
                    from:
                      headers:
                        someHeaderKey: someHeaderValue
                      data: "Hello from Kestra!"
                    requestTimeout: 2000
                """
        )
    }
)
public class Request extends NatsConnection implements RunnableTask<Request.Output> {
    @Schema(
        title = "Subject to send the request to"
    )
    @NotNull
    private Property<String> subject;

    @Schema(
        title = "Source of message(s) for the request",
        description = """
            If this is:
            - A plain string => entire string is the data
            - A kestra:// URI => entire file content is read into the data
            - A list with exactly one item => that item must be a map with optional headers + data
            - A map => optional 'headers' + 'data' keys
        """
    )
    @NotNull
    private Property<Object> from;

    @Schema(
        title = "Timeout in milliseconds to wait for a response.",
        description = "Defaults to 5000 ms."
    )
    @Builder.Default
    @NotNull
    private Property<Duration> requestTimeout = Property.of(Duration.ofMillis(5000));

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Connection connection = this.connect(runContext)) {
            // 2) Interpolate the subject (if it has placeholders like {{ ... }})
            String renderedSubject = runContext.render(this.subject).as(String.class).orElse(null);

            // 3) Retrieve a single "message map" (headers + data)
            Map<String, Object> messageMap = retrieveMessage(runContext);

            // 4) Build the NATS Message
            Message natsMessage = buildRequestMessage(renderedSubject, messageMap);

            // 5) Execute request-reply with the configured timeout
            Duration timeoutDuration = runContext.render(this.requestTimeout).as(Duration.class).orElse(null);
            Message reply = connection.request(natsMessage, timeoutDuration);

            // 6) Convert the reply (if any) to a UTF-8 string
            String response = (reply == null) ? null : new String(reply.getData(), StandardCharsets.UTF_8);

            connection.close();

            return Output.builder()
                .response(response)
                .build();
        } catch (Exception e) {
            runContext.logger().error("Unable to send request to NATS", e);
            throw e;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> retrieveMessage(RunContext runContext) throws Exception {
        Object from = runContext.render(this.from).as(Object.class).orElse(null);

        // CASE 1: "from" is a String
        if (from instanceof String fromStr) {
            // If it starts with kestra://, read entire file content into data
            if (fromStr.startsWith("kestra://")) {
                URI fromUri = new URI(fromStr);

                if (!"kestra".equalsIgnoreCase(fromUri.getScheme())) {
                    throw new IllegalArgumentException("Invalid 'from': must be a kestra:// URI or a plain string.");
                }

                try (InputStream is = runContext.storage().getFile(fromUri)) {
                    String content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    return Map.of("data", content);
                }
            } else {
                // Otherwise, treat the string itself as data
                return Map.of("data", fromStr);
            }
        }

        // CASE 2: "from" is a Map
        if (from instanceof Map<?, ?> fromMap) {
            return (Map<String, Object>) fromMap;
        }

        // CASE 3: Not supported
        throw new IllegalArgumentException(
            "Unsupported 'from' type. Must be: a String, or a Map. Got: " + (from != null ? from.getClass().getName() : "null")
        );
    }

    @SuppressWarnings("unchecked")
    private Message buildRequestMessage(String subject, Map<String, Object> msgMap) {
        // Build NATS headers if present
        Headers headers = new Headers();
        Object headersObj = msgMap.getOrDefault("headers", Collections.emptyMap());
        if (headersObj instanceof Map<?, ?> mapHeaders) {
            mapHeaders.forEach((key, value) -> {
                if (value instanceof Collection<?> multiValues) {
                    // Multi-value header
                    headers.add(key.toString(), (Collection<String>) multiValues);
                } else {
                    // Single-value header
                    headers.add(key.toString(), String.valueOf(value));
                }
            });
        }

        // Data defaults to an empty string if not present
        String data = String.valueOf(msgMap.getOrDefault("data", ""));

        return NatsMessage.builder()
            .subject(subject)
            .headers(headers)
            .data(data, StandardCharsets.UTF_8)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Response received from the request, or null if timed out/no responders."
        )
        private final String response;
    }
}