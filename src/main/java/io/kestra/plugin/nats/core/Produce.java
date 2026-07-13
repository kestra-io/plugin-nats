package io.kestra.plugin.nats.core;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Data;
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
import io.kestra.core.models.annotations.PluginProperty;

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
                    password: "{{ secret('NATS_PASSWORD') }}"
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
                    password: "{{ secret('NATS_PASSWORD') }}"
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
                    password: "{{ secret('NATS_PASSWORD') }}"
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
    @PluginProperty(group = "main")
    private String subject;

    @Schema(
        title = io.kestra.core.models.property.Data.From.TITLE,
        description = io.kestra.core.models.property.Data.From.DESCRIPTION,
        anyOf = { String.class, List.class, Map.class }
    )
    @NotNull
    @PluginProperty(group = "main")
    private Object from;

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);
        int messagesCount = Data.from(this.from).read(runContext)
            .map(throwFunction(object ->
            {
                connection.publish(
                    this.producerMessage(
                        runContext.render(this.subject),
                        runContext.render((Map<String, Object>) object)
                    )
                );
                return 1;
            }))
            .reduce(Integer::sum)
            .blockOptional()
            .orElse(0);

        // flush(Duration) sends a PING and blocks for the PONG, guaranteeing the server has
        // processed every prior PUB (and, since a connection is processed in order, that
        // JetStream has ingested them) before we close. flushBuffer() only pushes bytes into
        // the socket and can race with close(), silently dropping the last message.
        connection.flush(Duration.ofSeconds(5));
        connection.close();

        return Output.builder()
            .messagesCount(messagesCount)
            .build();
    }

    private Integer publish(RunContext runContext, Connection connection, Flux<Object> messagesFlowable) throws IllegalVariableEvaluationException {
        return messagesFlowable.map(throwFunction(object ->
        {
            connection.publish(this.producerMessage(runContext.render(this.subject), runContext.render((Map<String, Object>) object)));
            return 1;
        })).reduce(Integer::sum)
            .block();
    }

    private Message producerMessage(String subject, Map<String, Object> message) {
        Headers headers = new Headers();
        ((Map<String, Object>) message.getOrDefault("headers", Collections.emptyMap())).forEach((headerKey, headerValue) ->
        {
            if (headerValue instanceof Collection<?> headerValues) {
                headers.add(headerKey, (Collection<String>) headerValues);
            } else {
                headers.add(headerKey, (String) headerValue);
            }
        });

        return NatsMessage.builder()
            .subject(subject)
            .headers(headers)
            .data((String) message.get("data"))
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
