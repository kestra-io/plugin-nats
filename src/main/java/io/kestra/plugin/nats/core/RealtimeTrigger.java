package io.kestra.plugin.nats.core;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.core.NatsConnection;
import io.nats.client.Connection;
import io.nats.client.JetStreamOptions;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on message consumption in real-time from a NATS subject on a JetStream-enabled NATS server.",
    description = "If you would like to consume multiple messages processed within a given time frame and process them in batch, you can use the [io.kestra.plugin.nats.Trigger](https://kestra.io/plugins/plugin-nats/triggers/io.kestra.plugin.nats.trigger) instead."
)
@Plugin(
    aliases = { "io.kestra.plugin.nats.RealtimeTrigger"},
    examples = {
        @Example(
            title = "Subscribe to a NATS subject, getting every message from the beginning of the subject on first trigger execution.",
            full = true,
            code = {
                """
                id: nats
                namespace: company.team

                tasks:
                  - id: log
                    type: io.kestra.plugin.core.log.Log
                    message: "{{ trigger.data }}"

                triggers:
                  - id: watch
                    type: io.kestra.plugin.nats.core.RealtimeTrigger
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_password
                    subject: kestra.trigger
                    durableId: natsTrigger
                    deliverPolicy: All
                """
            }
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.NatsMessageOutput>, NatsConnectionInterface, SubscribeInterface {
    private String url;
    private Property<String> username;
    private Property<String> password;
    private Property<String> token;
    private Property<String> creds;
    private String subject;
    private Property<String> durableId;
    private Property<String> since;

    @Builder.Default
    private Integer batchSize = 10;

    @Builder.Default
    private Property<DeliverPolicy> deliverPolicy = Property.ofValue(DeliverPolicy.All);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    @Builder.Default
    @Getter(AccessLevel.NONE)
    private final CountDownLatch waitForTermination = new CountDownLatch(1);

    @Override
    public Publisher<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        Consume task = Consume.builder()
            .id(id)
            .type(Consume.class.getName())
            .url(url)
            .username(username)
            .password(password)
            .creds(creds)
            .token(token)
            .subject(subject)
            .durableId(durableId)
            .since(since)
            .batchSize(batchSize)
            .deliverPolicy(deliverPolicy)
            .build();

        return Flux
            .from(publisher(task, conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, conditionContext, context, record));
    }

    public Publisher<Consume.NatsMessageOutput> publisher(final Consume task, final RunContext runContext) throws Exception {

        final String subject = runContext.render(this.subject);
        final String durableId = runContext.render(this.durableId).as(String.class).orElse(null);
        final ZonedDateTime startTime = Optional.ofNullable(since)
            .map(throwFunction(sinceDate -> ZonedDateTime.parse(runContext.render(sinceDate).as(String.class).orElse(null))))
            .orElse(null);

        return Flux.create(emitter -> {
            try (Connection connection = task.connect(runContext)) {
                // create options
                PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.Explicit)
                        .deliverPolicy(runContext.render(deliverPolicy).as(DeliverPolicy.class).orElseThrow())
                        .startTime(startTime)
                        .build()
                    )
                    .durable(durableId)
                    .build();

                // create subscription
                JetStreamSubscription subscription = connection
                    .jetStream(JetStreamOptions.DEFAULT_JS_OPTIONS)
                    .subscribe(subject, options);

                // fetch
                while (isActive.get()) {
                    List<Message> messages = subscription.fetch(batchSize, Duration.ofMillis(100));

                    messages.forEach(message -> {
                        Map<String, List<String>> headers;
                        if (message.getHeaders() == null) {
                            headers = Collections.emptyMap();
                        } else {
                            headers = message.getHeaders()
                                .entrySet()
                                .stream()
                                .collect(
                                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                                );
                        }

                        Consume.NatsMessageOutput output = Consume.NatsMessageOutput.builder()
                            .subject(message.getSubject())
                            .headers(headers)
                            .data(new String(message.getData()))
                            .timestamp(message.metaData().timestamp().toInstant())
                            .build();

                        emitter.next(output);
                        message.ack(); // AckPolicy.Explicit
                    });
                    // The JetStreamSubscription#fetch method catches any thrown InterruptedException.
                    // Let's check if the thread was interrupted, and if we need to stop.
                    if (Thread.currentThread().isInterrupted()) {
                        isActive.set(false);
                    }
                }
                emitter.complete();
            } catch (Exception throwable) {
                emitter.error(throwable);
            } finally {
                waitForTermination.countDown();
            }
        });
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        stop(true);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        stop(false); // must be non-blocking
    }

    private void stop(boolean wait) {
        if (!isActive.compareAndSet(true, false)) {
            return;
        }

        if (wait) {
            try {
                this.waitForTermination.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
