package io.kestra.plugin.nats.core;

import java.io.*;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.LoggerFactory;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.kestra.plugin.nats.ConsumeInterface;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import io.kestra.core.models.annotations.PluginProperty;

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
    aliases = { "io.kestra.plugin.nats.Consume" },
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
                    password: "{{ secret('NATS_PASSWORD') }}"
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
    @PluginProperty(group = "main")
    private String subject;

    @Schema(
        title = "Durable consumer name",
        description = "Optional durable name to resume position between runs."
    )
    @PluginProperty(group = "advanced")
    private Property<String> durableId;

    @Schema(
        title = "Start time",
        description = "ISO-8601 date-time rendered and parsed to set the deliver start; ignored if null."
    )
    @PluginProperty(group = "advanced")
    private Property<String> since;

    @Schema(
        title = "Poll duration",
        description = "Wait time per fetch; defaults to PT2S."
    )
    @Builder.Default
    @PluginProperty(group = "execution")
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(2));

    @Schema(
        title = "Batch size",
        description = "Maximum messages fetched per pull; defaults to 10."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Integer batchSize = 10;

    @Schema(
        title = "Max records",
        description = "Optional cap on total messages; stops once reached."
    )
    @PluginProperty(group = "advanced")
    private Property<Integer> maxRecords;

    @Schema(
        title = "Max duration",
        description = "Optional wall-clock duration after which polling stops."
    )
    @PluginProperty(group = "execution")
    private Property<Duration> maxDuration;

    @Schema(
        title = "Deliver policy",
        description = "JetStream deliver policy; defaults to All."
    )
    @Builder.Default
    @PluginProperty(group = "advanced")
    private Property<DeliverPolicy> deliverPolicy = Property.ofValue(DeliverPolicy.All);

    // Lifecycle state for kill()/stop() support, mirroring RealtimeTrigger. Not a plugin property:
    // must stay out of the JSON schema, Jackson (de)serialization, and trigger equality/toString,
    // since the polling Trigger builds a fresh Consume per evaluate() cycle and compares/serializes
    // the Trigger itself, not this transient task instance.
    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicBoolean isActive = new AtomicBoolean(true);

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicReference<Connection> connectionRef = new AtomicReference<>();

    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient CountDownLatch waitForTermination = new CountDownLatch(1);

    // Ceiling on kill()'s wait for the connection to close. Core dispatches kill() to every running
    // task/trigger on the node from a single synchronized block (DefaultWorker), so an unbounded
    // await() here would stall kill processing for everything else on the worker if teardown hangs.
    private static final Duration TERMINATION_AWAIT_TIMEOUT = Duration.ofSeconds(30);

    public Output run(RunContext runContext) throws Exception {
        Connection connection = connect(runContext);
        connectionRef.set(connection);
        JetStreamSubscription subscription = connection.jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS).subscribe(
            runContext.render(subject),
            PullSubscribeOptions.builder()
                .configuration(
                    ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.Explicit)
                        .deliverPolicy(runContext.render(deliverPolicy).as(DeliverPolicy.class).orElseThrow())
                        .startTime(runContext.render(since).as(String.class).map(ZonedDateTime::parse).orElse(null))
                        .build()
                )
                .durable(runContext.render(durableId).as(String.class).orElse(null)).build()
        );

        Instant pollStart = Instant.now();
        List<Message> messages;
        AtomicInteger total = new AtomicInteger();
        File outputFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream output = new BufferedOutputStream(new FileOutputStream(outputFile))) {
            AtomicReference<Integer> maxMessagesRemainingRef = new AtomicReference<>();
            do {
                // Closes the window where kill()/stop() lands after connect() returns but before (or
                // between) fetch() calls: check isActive before every poll, including the first,
                // instead of relying solely on the while condition below.
                if (!isActive.get()) {
                    break;
                }

                Integer maxMessagesRemaining = runContext.render(maxRecords).as(Integer.class)
                    .map(max -> max - total.get())
                    .orElse(null);

                maxMessagesRemainingRef.set(maxMessagesRemaining);

                batchSize = Optional.ofNullable(maxMessagesRemaining).map(max -> Math.min(batchSize, max)).orElse(batchSize);
                try {
                    messages = subscription.fetch(batchSize, runContext.render(pollDuration).as(Duration.class).orElseThrow());
                } catch (IllegalStateException e) {
                    // A kill()/stop() racing in on another thread closes the tracked connection, which
                    // makes an in-flight fetch() throw. Expected in that case; a real error otherwise.
                    if (!isActive.get()) {
                        break;
                    }
                    throw e;
                }

                messages.forEach(throwConsumer(message ->
                {
                    // A kill() landing mid-batch must not ack messages that will never reach the
                    // output file, so they remain available for redelivery (at-least-once).
                    if (!isActive.get()) {
                        return;
                    }

                    Map<Object, Object> map = new HashMap<>();

                    map.put("subject", message.getSubject());
                    map.put(
                        "headers", Map.ofEntries(
                            Optional.ofNullable(message.getHeaders())
                                .map(headers -> headers.entrySet().toArray(Map.Entry[]::new))
                                .orElse(new Map.Entry[0])
                        )
                    );
                    map.put("data", new String(message.getData()));
                    map.put("timestamp", message.metaData().timestamp().toInstant());

                    FileSerde.write(output, map);

                    message.ack();
                    total.incrementAndGet();
                }));
            } while (
                isActive.get() && !isEnded(messages, maxMessagesRemainingRef.get(), pollStart, runContext)
            );
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                // Closing the connection from the killer thread makes this close() (and the one
                // triggered by kill()/stop() itself) throw; tolerate that noise, but not a genuine
                // close failure while the task is still active.
                if (isActive.get()) {
                    throw e;
                }
            } finally {
                waitForTermination.countDown();
            }
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

    /**
     * Forwarded from the owning polling {@link Trigger}'s {@code kill()}. Blocks (up to a bounded
     * timeout) until the poll loop has observed {@code isActive} and torn down its connection, so
     * the caller can rely on the task being fully stopped once this returns.
     */
    public void kill() {
        stop(true);
    }

    /**
     * Forwarded from the owning polling {@link Trigger}'s {@code stop()}. Must be non-blocking:
     * unlike {@link #kill()}, callers do not wait for teardown to complete.
     */
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        // jnats has no wakeup() primitive to interrupt a blocked fetch(); closing the tracked
        // connection is the only way to unblock it.
        Optional.ofNullable(connectionRef.get()).ifPresent(connection -> {
            try {
                connection.close();
            } catch (Exception e) {
                LoggerFactory.getLogger(Consume.class)
                    .debug("Failed to close NATS connection while stopping consume task id={}", this.id, e);
            }
        });

        if (wait) {
            try {
                if (!waitForTermination.await(TERMINATION_AWAIT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
                    LoggerFactory.getLogger(Consume.class).debug(
                        "NATS consume task id={} did not terminate within {} of kill(); returning to avoid stalling the worker's kill dispatch",
                        this.id, TERMINATION_AWAIT_TIMEOUT);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
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
        @PluginProperty(group = "main")
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
