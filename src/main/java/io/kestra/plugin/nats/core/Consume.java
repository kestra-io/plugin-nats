package io.kestra.plugin.nats.core;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.nats.ConsumeInterface;
import io.kestra.plugin.nats.core.NatsConnection;
import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
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
    title = "Consume NATS JetStream messages",
    description = "Pulls messages from a JetStream subject with explicit acks and writes them to Kestra internal storage. Requires a stream matching the rendered subject; defaults: deliverPolicy=All, pollDuration=PT2S, batchSize=10. Stops when no messages, maxRecords, or maxDuration is reached."
)
@Plugin(
    aliases = { "io.kestra.plugin.nats.Consume"},
    examples = {
        @Example(
            title = "Consume messages from any topic subject matching the kestra.> wildcard, using user password authentication.",
            full = true,
            code = """
                id: nats_consume_messages
                namespace: company.team

                tasks:
                  - id: consume
                    type: io.kestra.plugin.nats.core.Consume
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: kestra.>
                    durableId: someDurableId
                    pollDuration: PT5S
                """
        ),
    }
)
public class Consume extends NatsConnection implements RunnableTask<Consume.Output>, ConsumeInterface, SubscribeInterface {

    @Schema(
        title = "Subject to consume",
        description = "Rendered subject or wildcard the JetStream stream is bound to."
    )
    private String subject;

    @Schema(
        title = "Durable consumer name",
        description = "Optional durable name to resume position between runs."
    )
    private Property<String> durableId;

    @Schema(
        title = "Start time",
        description = "ISO-8601 date-time rendered and parsed to set the deliver start; ignored if null."
    )
    private Property<String> since;

    @Schema(
        title = "Poll duration",
        description = "Wait time per fetch; defaults to PT2S."
    )
    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(2));

    @Schema(
        title = "Batch size",
        description = "Maximum messages fetched per pull; defaults to 10."
    )
    @Builder.Default
    private Integer batchSize = 10;

    @Schema(
        title = "Max records",
        description = "Optional cap on total messages; stops once reached."
    )
    private Property<Integer> maxRecords;

    @Schema(
        title = "Max duration",
        description = "Optional wall-clock duration after which polling stops."
    )
    private Property<Duration> maxDuration;

    @Schema(
        title = "Deliver policy",
        description = "JetStream deliver policy; defaults to All."
    )
    @Builder.Default
    private Property<DeliverPolicy> deliverPolicy = Property.ofValue(DeliverPolicy.All);

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);
        JetStreamSubscription subscription = connection.jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS).subscribe(
            runContext.render(subject),
            PullSubscribeOptions.builder()
                .configuration(ConsumerConfiguration.builder()
                    .ackPolicy(AckPolicy.Explicit)
                    .deliverPolicy(runContext.render(deliverPolicy).as(DeliverPolicy.class).orElseThrow())
                    .startTime(runContext.render(since).as(String.class).map(ZonedDateTime::parse).orElse(null))
                    .build())
                .durable(runContext.render(durableId).as(String.class).orElse(null)).build()
        );

        Instant pollStart = Instant.now();
        List<Message> messages;
        AtomicInteger total = new AtomicInteger();
        File outputFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            AtomicReference<Integer> maxMessagesRemainingRef = new AtomicReference<>();
            do {
                Integer maxMessagesRemaining = runContext.render(maxRecords).as(Integer.class)
                    .map(max -> max - total.get())
                    .orElse(null);

                maxMessagesRemainingRef.set(maxMessagesRemaining);

                batchSize = Optional.ofNullable(maxMessagesRemaining).map(max -> Math.min(batchSize, max)).orElse(batchSize);
                messages = subscription.fetch(batchSize, runContext.render(pollDuration).as(Duration.class).orElseThrow());

                messages.forEach(throwConsumer(message -> {
                    Map<Object, Object> map = new HashMap<>();

                    map.put("subject", message.getSubject());
                    map.put("headers", Map.ofEntries(
                        Optional.ofNullable(message.getHeaders())
                            .map(headers -> headers.entrySet().toArray(Map.Entry[]::new))
                            .orElse(new Map.Entry[0])
                    ));
                    map.put("data", new String(message.getData()));
                    map.put("timestamp", message.metaData().timestamp().toInstant());

                    FileSerde.write(output, map);

                    message.ack();
                    total.incrementAndGet();
                }));
            } while (
                !isEnded(messages, maxMessagesRemainingRef.get(), pollStart, runContext)
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
    private boolean isEnded(List<Message> messages, Integer maxMessagesRemaining, Instant pollStart, RunContext runContext) throws IllegalVariableEvaluationException {
        if (messages.isEmpty()) {
            return true;
        }

        if (Optional.ofNullable(maxMessagesRemaining).map(max -> max <= 0).orElse(false)) {
            return true;
        }

        if (runContext.render(maxDuration).as(Duration.class).map(max -> Instant.now().isBefore(pollStart.plus(max))).orElse(false)) {
            return true;
        }

        return false;
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Messages consumed",
            description = "Total acknowledged messages during this run."
        )
        private final Integer messagesCount;

        @Schema(
            title = "Output file URI",
            description = "Kestra internal storage URI of the ION file containing consumed messages."
        )
        private URI uri;

    }


    @Getter
    @Builder
    public static class NatsMessageOutput implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Subject",
            description = "Subject of the consumed message."
        )
        private String subject;

        @Schema(
            title = "Headers",
            description = "Message headers grouped by key."
        )
        private Map<String, List<String>> headers;

        @Schema(
            title = "Data",
            description = "Message payload as UTF-8 string."
        )
        private String data;

        @Schema(
            title = "Timestamp",
            description = "JetStream message timestamp."
        )
        private Instant timestamp;

    }

}
