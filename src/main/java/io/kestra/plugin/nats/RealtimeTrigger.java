package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.AbstractTrigger;
import io.kestra.core.models.triggers.RealtimeTriggerInterface;
import io.kestra.core.models.triggers.TriggerContext;
import io.kestra.core.models.triggers.TriggerOutput;
import io.kestra.core.models.triggers.TriggerService;
import io.kestra.core.runners.RunContext;
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
    title = "Consume a message in real-time from a NATS subject on a JetStream-enabled NATS server and create one execution per message."
)
@Plugin(
    examples = {
        @Example(
            title = "Subscribe to a NATS subject, getting every message from the beginning of the subject on first trigger execution.",
            full = true,
            code = {
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.nats.RealtimeTrigger",
                "    url: nats://localhost:4222",
                "    username: kestra",
                "    password: k3stra",
                "    subject: kestra.trigger",
                "    durableId: natsTrigger",
                "    deliverPolicy: All"
            }
        )
    },
    beta = true
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.Output>, NatsConnectionInterface, SubscribeInterface {
    private String url;
    private String username;
    private String password;
    private String subject;
    private String durableId;
    private String since;
    @Builder.Default
    private Duration pollDuration = Duration.ofSeconds(2);

    @Builder.Default
    private Integer batchSize = 10;

    @Builder.Default
    private DeliverPolicy deliverPolicy = DeliverPolicy.All;

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
            .subject(subject)
            .durableId(durableId)
            .since(since)
            .pollDuration(pollDuration)
            .batchSize(batchSize)
            .deliverPolicy(deliverPolicy)
            .build();

        return Flux
            .from(publisher(task, conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, context, record));
    }

    public Publisher<Consume.NatsMessageOutput> publisher(final Consume task, final RunContext runContext) throws Exception {

        final String subject = runContext.render(this.subject);
        final String durableId = runContext.render(this.durableId);
        final ZonedDateTime startTime = Optional.ofNullable(since)
            .map(throwFunction(sinceDate -> ZonedDateTime.parse(runContext.render(sinceDate))))
            .orElse(null);

        return Flux.create(emitter -> {
            try (Connection connection = task.connect(runContext)) {
                // create options
                PullSubscribeOptions options = PullSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder()
                        .ackPolicy(AckPolicy.Explicit)
                        .deliverPolicy(deliverPolicy)
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
                    List<Message> messages = subscription.fetch(batchSize, pollDuration);

                    messages.forEach(message -> {
                        Map<String, List<String>> headerMap;
                        if (message.getHeaders() == null) {
                            headerMap = Collections.emptyMap();
                        } else {
                            headerMap = message.getHeaders()
                                .entrySet()
                                .stream()
                                .collect(
                                    Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)
                                );
                        }

                        Consume.NatsMessageOutput natsMessage = Consume.NatsMessageOutput.builder()
                            .subject(message.getSubject())
                            .headers(headerMap)
                            .data(new String(message.getData()))
                            .timestamp(message.metaData().timestamp().toInstant())
                            .build();

                        emitter.next(natsMessage);
                    });
                    // The JetStreamSubscription#fetch method catches any thrown InterruptedException.
                    // Let's check if the thread was interrupted, and if we need to stop.
                    if (Thread.currentThread().isInterrupted()) {
                        isActive.set(false);
                    }
                }
            } catch (Exception throwable) {
                emitter.error(throwable);
            } finally {
                emitter.complete();
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
