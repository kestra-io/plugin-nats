package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.io.BufferedReader;
import java.io.InputStreamReader;
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
    @PluginProperty(dynamic = true)
    @NotBlank
    @NotNull
    private String subject;

    @Schema(
        title = "Source of message(s) for the request",
        description = """
            Can be an internal storage URI, a map, or a list of maps.
            If a list, only the first item is used.
            Each map may have keys 'headers' and 'data' (which can contain Kestra variables).
        """
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    @Schema(
        title = "Timeout in milliseconds to wait for a response.",
        description = "Defaults to 5000 ms."
    )
    @Builder.Default
    private Integer requestTimeout = 5000;

    @Override
    public Output run(RunContext runContext) throws Exception {
        // 1) Connect to NATS
        Connection connection = this.connect(runContext);

        // 2) Interpolate the subject (if it has placeholders like {{ ... }})
        String renderedSubject = runContext.render(this.subject);

        // 3) Get the raw message from 'from' and then interpolate the entire map, including headers
        Map<String, Object> rawMessage = retrieveSingleMessage(runContext);
        Map<String, Object> renderedMessage = runContext.render(rawMessage);

        // 4) Build the NATS message with the fully rendered subject + map
        Message natsMessage = buildRequestMessage(renderedSubject, renderedMessage);

        // 5) Execute request-reply with the configured timeout
        Duration timeoutDuration = Duration.ofMillis(this.requestTimeout);
        Message reply = connection.request(natsMessage, timeoutDuration);

        // 6) Convert the reply (if any) to a UTF-8 string
        String response = (reply == null) ? null : new String(reply.getData(), StandardCharsets.UTF_8);

        connection.close();

        return Output.builder()
            .response(response)
            .build();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> retrieveSingleMessage(RunContext runContext) throws Exception {
        switch (this.from) {
            // A single Kestra storage URI
            case String fromStr -> {
                URI fromUri = new URI(runContext.render(fromStr));
                if (!"kestra".equalsIgnoreCase(fromUri.getScheme())) {
                    throw new IllegalArgumentException("Invalid 'from': must be a kestra:// URI.");
                }

                // Parse the first record from the Ion/JSON file
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(runContext.storage().getFile(fromUri)))) {
                    Flux<Object> flux = FileSerde.readAll(reader);
                    Object first = flux.blockFirst(); // read the first object
                    if (first == null) {
                        throw new IllegalArgumentException("'from' file is empty; expected exactly one map.");
                    }
                    if (!(first instanceof Map)) {
                        throw new IllegalArgumentException("'from' file must contain a map for the request message.");
                    }
                    return (Map<String, Object>) first;
                }
            }

            // A list => only the first item is used
            case List<?> fromList -> {
                if (fromList.isEmpty()) {
                    throw new IllegalArgumentException("'from' list is empty; need at least one map.");
                }
                Object first = fromList.get(0);
                if (!(first instanceof Map)) {
                    throw new IllegalArgumentException("The first item in 'from' list must be a map.");
                }
                return (Map<String, Object>) first;
            }

            // A single map => directly use it
            case Map<?, ?> fromMap -> {
                return (Map<String, Object>) fromMap;
            }

            // Any other type => not supported
            default -> throw new IllegalArgumentException(
                "Unsupported 'from' type. Must be String (kestra URI), Map, or List<Map>."
            );
        }
    }

    @SuppressWarnings("unchecked")
    private Message buildRequestMessage(String subject, Map<String, Object> msgMap) {
        // Build NATS headers
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