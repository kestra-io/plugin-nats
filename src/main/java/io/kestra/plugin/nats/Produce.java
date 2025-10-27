package io.kestra.plugin.nats;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Data;
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

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Produce messages in a NATS subject on a NATS server."
)
@Plugin(
    examples = {
        @Example(
            title = "Produce a single message to kestra.publish subject, using user password authentication.",
            full = true,
            code = """
                id: nats_produce_single_message
                namespace: company.team

                tasks:
                  - id: produce
                    type: io.kestra.plugin.nats.Produce
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
                    type: io.kestra.plugin.nats.Produce
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
                    type: io.kestra.plugin.nats.Produce
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: kestra.publish
                    from: "{{ outputs.some_task_with_output_file.uri }}"
                """
        ),
    }
)
public class Produce extends NatsConnection implements RunnableTask<Produce.Output>, Data.from {
    @Schema(
        title = "Subject to produce message to"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    @NotNull
    private String subject;

    @Schema(
        title = "Source of message(s) to send",
        description = "Can be an internal storage uri, a map or a list." +
            "with the following format: headers, data",
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    private Object from;

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);

        Flux<Object> messagesFlowable = Data.from(this.from).read(runContext);
+
+       int messagesCount = Data.from(this.from).read(runContext)
+            .map(throwFunction(object -> {
+                connection.publish(
+                    this.producerMessage(
+                        runContext.render(this.subject),
+                        runContext.render((Map<String, Object>) object)
+                    )
+                );
+                return 1;
+            }))
+            .reduce(Integer::sum)
+            .blockOptional()
+            .orElse(0);

        connection.flushBuffer();
        connection.close();

        return Output.builder()
            .messagesCount(messagesCount)
            .build();
    }

    private Integer publish(RunContext runContext, Connection connection, Flux<Object> messagesFlowable) throws IllegalVariableEvaluationException {
        return messagesFlowable.map(throwFunction(object -> {
                connection.publish(this.producerMessage(runContext.render(this.subject), runContext.render((Map<String, Object>) object)));
                return 1;
            })).reduce(Integer::sum)
            .block();
    }

    private Message producerMessage(String subject, Map<String, Object> message) {
        Headers headers = new Headers();
        ((Map<String, Object>) message.getOrDefault("headers", Collections.emptyMap())).forEach((headerKey, headerValue) -> {
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
            title = "Number of messages produced"
        )
        private final Integer messagesCount;
    }
}
