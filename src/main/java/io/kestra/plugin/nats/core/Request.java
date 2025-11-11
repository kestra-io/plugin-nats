package io.kestra.plugin.nats.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.nats.core.NatsConnection;
import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Send a request to a NATS subject and wait for a reply."
)
@Plugin(
    aliases = { "io.kestra.plugin.nats.Request"},
    examples = {
        @Example(
            title = "Send a request to the subject and wait for the reply (using username/password authentication).",
            full = true,
            code = """
                id: nats_request_reply
                namespace: company.team

                tasks:
                  - id: request
                    type: io.kestra.plugin.nats.core.Request
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
public class Request extends NatsConnection implements RunnableTask<Request.Output>, Data.From {
    @Schema(
        title = "Subject to send the request to"
    )
    @NotNull
    private Property<String> subject;

    @Schema(
        title = io.kestra.core.models.property.Data.From.TITLE,
        description = io.kestra.core.models.property.Data.From.DESCRIPTION
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Timeout in milliseconds to wait for a response.",
        description = "Defaults to 5000 ms."
    )
    @Builder.Default
    @NotNull
    private Property<Duration> requestTimeout = Property.ofValue(Duration.ofMillis(5000));

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Connection connection = this.connect(runContext)) {
            // 1) Interpolate the subject (if it has placeholders like {{ ... }})
            String renderedSubject = runContext.render(this.subject).as(String.class).orElse(null);

            // 2) Retrieve a single "message map" (headers + data)
            Map<String, Object> messageMap = retrieveMessage(runContext);

            // 3) Build the NATS Message
            Message natsMessage = buildRequestMessage(renderedSubject, messageMap);

            // 4) Execute request-reply with the configured timeout
            Duration timeoutDuration = runContext.render(this.requestTimeout).as(Duration.class).orElse(null);
            Message reply = connection.request(natsMessage, timeoutDuration);

            // 5) Convert the reply (if any) to a UTF-8 string
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

    private Map<String, Object> retrieveMessage(RunContext runContext) throws Exception {
        return Data.from(this.from).read(runContext).blockFirst();
    }

    @SuppressWarnings("unchecked")
    private Message buildRequestMessage(String subject, Map<String, Object> messageMap) throws JsonProcessingException {
        // Build NATS headers if present
        Headers headers = new Headers();
        Object headersObj = messageMap.getOrDefault("headers", Collections.emptyMap());
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

        String data;

        if (messageMap.get("data") instanceof String dataStr) {
            data = dataStr;
        } else {
            data = JacksonMapper.ofJson().writeValueAsString(messageMap.get("data"));
        }

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