package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.nats.client.*;
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
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Consume messages from a NATS subject on a JetStream-enabled NATS server.",
    description = """
        Please note that the server you run it against must have JetStream enabled for it to work.
        It should also have a stream configured to match the given subject."""
)
@Plugin(
    examples = {
        @Example(
            title = "Consume messages from any topic subject matching the kestra.> wildcard, using user password authentication.",
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
    private Integer batchSize = 10;
    private Integer maxRecords;
    private Duration maxDuration;
    @Builder.Default
    private DeliverPolicy deliverPolicy = DeliverPolicy.All;

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);
        JetStreamSubscription subscription = connection.jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS).subscribe(
            runContext.render(subject),
            PullSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.Explicit)
                    .deliverPolicy(deliverPolicy)
                    .startTime(Optional.ofNullable(since).map(throwFunction(sinceDate -> ZonedDateTime.parse(runContext.render(sinceDate)))).orElse(null))
                    .build())
                .durable(runContext.render(durableId)).build()
        );

        Instant pollStart = Instant.now();
        List<Message> messages;
        AtomicInteger total = new AtomicInteger();
        File outputFile = runContext.tempFile(".ion").toFile();
        try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            AtomicReference<Integer> maxMessagesRemainingRef = new AtomicReference<>();
            do {
                Integer maxMessagesRemaining = Optional.ofNullable(maxRecords).map(max -> max - total.get()).orElse(null);
                maxMessagesRemainingRef.set(maxMessagesRemaining);

                batchSize = Optional.ofNullable(maxMessagesRemaining).map(max -> Math.min(batchSize, max)).orElse(batchSize);
                messages = subscription.fetch(batchSize, pollDuration);

                messages.forEach(throwConsumer(message -> {
                    Map<Object, Object> map = new HashMap<>();

                    map.put("subject", message.getSubject());
                    map.put("headers", Map.ofEntries(
                        Optional.ofNullable(message.getHeaders())
                            .map(headers -> headers.entrySet().toArray(Map.Entry[]::new))
                            .orElse(new Map.Entry[0])
                    ));
                    map.put("data", Base64.getEncoder().encodeToString(message.getData()));
                    map.put("timestamp", message.metaData().timestamp().toInstant());

                    FileSerde.write(output, map);

                    message.ack();
                    total.incrementAndGet();
                }));
            } while (
                !isEnded(messages, maxMessagesRemainingRef.get(), pollStart)
            );
        } finally {
            connection.close();
        }

        return Output.builder()
            .messagesCount(total.get())
            .uri(runContext.storage().putFile(outputFile))
            .build();
    }

    @SuppressWarnings("RedundantIfStatement")
    private boolean isEnded(List<Message> messages, Integer maxMessagesRemaining, Instant pollStart) {
        if (messages.isEmpty()) {
            return true;
        }

        if (Optional.ofNullable(maxMessagesRemaining).map(max -> max <= 0).orElse(false)) {
            return true;
        }

        if (Optional.ofNullable(maxDuration).map(max -> Instant.now().isBefore(pollStart.plus(max))).orElse(false)) {
            return true;
        }

        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(
            title = "Number of messages consumed."
        )
        private final Integer messagesCount;

        @Schema(
            title = "URI of a Kestra internal storage file."
        )
        private URI uri;
    }
}
