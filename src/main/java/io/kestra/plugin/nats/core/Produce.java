package io.kestra.plugin.nats.core;

import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Data;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;

import io.nats.client.Connection;
import io.nats.client.Message;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;
import reactor.core.publisher.Flux;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Publish messages to a NATS subject",
    description = "Publishes one or more messages to the rendered subject using headers and data provided via `from`. Supports lists, maps, or storage files; flushes before closing and returns the number of messages sent."
)
@Plugin(
    aliases = { "io.kestra.plugin.nats.Produce" },
    examples = {
        @Example(
            title = "Produce a single message to kestra.publish subject, using user password authentication.",
            full = true,
            code = """
                id: nats_produce_single_message
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.nats.core.Produce
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: kestra.publish
                    from:
                      headers:
                        someHeaderKey: someHeaderValue
                      data: Some message
                """
        ),
        @Example(
            title = "Produce 2 messages to kestra.publish subject, using user password authentication.",
            full = true,
            code = """
                id: nats_produce_two_messages
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.nats.core.Produce
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: kestra.publish
                    from:
                      - headers:
                          someHeaderKey: someHeaderValue
                        data: Some message
                      - data: Another message
                """
        ),
        @Example(
            title = "Produce messages (1 / row) from an internal storage file to kestra.publish subject, using user password authentication.",
            full = true,
            code = """
                id: nats_produce_messages_from_file
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.nats.core.Produce
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: kestra.publish
                    from: "{{ outputs.some_task_with_output_file.uri }}"
                """
        ),
    }
)
public class Produce extends NatsConnection implements RunnableTask<Produce.Output>, Data.From {
    @Schema(
        title = "Subject to publish to",
        description = "Rendered subject or wildcard where messages are sent."
    )
    @NotBlank
    @NotNull
    private String subject;

    @Schema(
        title = io.kestra.core.models.property.Data.From.TITLE,
        description = io.kestra.core.models.property.Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @NotNull
    private Object from;

    @Schema(
        title = "Serialization type",
        description = "How to encode the data field. STRING sends as UTF-8 string; BASE64 expects the data field to be a base64 string which is decoded to raw bytes before publishing."
    )
    @Builder.Default
    private Property<SerializationType> serializationType = Property.ofValue(SerializationType.STRING);

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);
        SerializationType st = runContext.render(serializationType).as(SerializationType.class).orElse(SerializationType.STRING);
        int messagesCount = Data.from(this.from).read(runContext)
            .map(throwFunction(object ->
            {
                connection.publish(
                    this.producerMessage(
                        runContext.render(this.subject),
                        runContext.render((Map<String, Object>) object),
                        st,
                        runContext
                    )
                );
                return 1;
            }))
            .reduce(Integer::sum)
            .blockOptional()
            .orElse(0);

        connection.flushBuffer();
        connection.close();

        return Output.builder()
            .messagesCount(messagesCount)
            .build();
    }

    private Message producerMessage(String subject, Map<String, Object> message, SerializationType st, RunContext runContext) throws Exception {
        Headers headers = new Headers();
        ((Map<String, Object>) message.getOrDefault("headers", Collections.emptyMap())).forEach((headerKey, headerValue) ->
        {
            if (headerValue instanceof Collection<?> headerValues) {
                headers.add(headerKey, (Collection<String>) headerValues);
            } else {
                headers.add(headerKey, (String) headerValue);
            }
        });

        byte[] dataBytes;
        if (st == SerializationType.BINARY) {
            URI uri = URI.create((String) message.get("data"));
            try (InputStream is = runContext.storage().getFile(uri)) {
                dataBytes = is.readAllBytes();
            }
        } else if (st == SerializationType.BASE64) {
            dataBytes = Base64.getDecoder().decode((String) message.get("data"));
        } else {
            dataBytes = ((String) message.get("data")).getBytes(StandardCharsets.UTF_8);
        }

        return NatsMessage.builder()
            .subject(subject)
            .headers(headers)
            .data(dataBytes)
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Messages produced",
            description = "Total messages published during this run."
        )
        private final Integer messagesCount;
    }
}
