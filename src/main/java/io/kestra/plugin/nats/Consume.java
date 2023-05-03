package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from a NATS subject on a JetStream-enabled NATS server",
    description = """
        Please note that for it to work the server you run it against must have JetStream enabled.
        It should also have a stream configured to match the given subject"""
)
@Plugin(
    examples = {
        @Example(
            title = "Consume messages from any topic subject matching the kestra.> wildcard, using user password authentication",
            code = {
                "url: nats://localhost:4222",
                "username: nats_user",
                "password: nats_passwd",
                "subject: kestra.>",
                "durableId: someDurableId",
                "pollDuration: PT5S"
            }
        ),
    }
)
public class Consume extends NatsConnection implements RunnableTask<Consume.Output>, ConsumeInterface {
    private String subject;
    private String durableId;
    private String since;
    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(2);
    @Builder.Default
    private Integer maxRecords = 200;
    private Duration maxDuration;
    @Builder.Default
    private DeliverPolicy deliverPolicy = DeliverPolicy.All;
    @Builder.Default
    private Deserializer valueDeserializer = Deserializer.TO_STRING;

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);
        List<Message> fetchedMessages = connection.jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS).subscribe(
                runContext.render(subject),
                PullSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.Explicit)
                        .deliverPolicy(deliverPolicy)
                        .startTime(Optional.ofNullable(since).map(throwFunction(ignored -> ZonedDateTime.parse(runContext.render(since)))).orElse(null))
                        .build())
                    .durable(runContext.render(durableId)).build()
            )
            .fetch(Optional.ofNullable(maxRecords).orElse(Integer.MAX_VALUE), pollDuration);

        File outputFile = runContext.tempFile(".ion").toFile();
        try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            fetchedMessages.forEach(throwConsumer(message -> {
                Map<Object, Object> map = new HashMap<>();

                map.put("subject", message.getSubject());
                map.put("headers", Map.ofEntries(
                    Optional.ofNullable(message.getHeaders())
                        .map(headers -> headers.entrySet().toArray(Map.Entry[]::new))
                        .orElse(new Map.Entry[0])
                ));
                map.put("data", valueDeserializer.apply(message.getData()));
                map.put("timestamp", message.metaData().timestamp().toInstant());

                FileSerde.write(output, map);

                message.ack();
            }));
        }

        connection.close();

        return Output.builder()
            .messagesCount(fetchedMessages.size())
            .uri(runContext.putTempFile(outputFile))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of messages consumed"
        )
        private final Integer messagesCount;

        @Schema(
            title = "URI of a Kestra internal storage file"
        )
        private URI uri;
    }
}
