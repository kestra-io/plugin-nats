package io.kestra.plugin.nats.core;

import io.kestra.core.models.annotations.PluginProperty;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.ConsumeInterface;

import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger on polled NATS messages",
    description = "Polls a JetStream subject on a schedule (default every 60s) and starts one execution per batch. Defaults: deliverPolicy=All, pollDuration=PT2S, batchSize=10. Use the realtime [io.kestra.plugin.nats.RealtimeTrigger](https://kestra.io/plugins/plugin-nats/triggers/io.kestra.plugin.nats.realtimetrigger) to emit one execution per message."
)
@Plugin(
    aliases = { "io.kestra.plugin.nats.Trigger" },
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
                        type: io.kestra.plugin.nats.core.Trigger
                        url: nats://localhost:4222
                        username: nats_user
                        password: "{{ secret('NATS_PASSWORD') }}"
                        subject: kestra.trigger
                        durableId: natsTrigger
                        deliverPolicy: All
                        maxRecords: 1
                    """
            }
        )
    }
)
public class Trigger extends AbstractTrigger implements PollingTriggerInterface, TriggerOutput<Consume.Output>, NatsConnectionInterface, ConsumeInterface, SubscribeInterface {
    private String url;
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> username;
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> password;
    @ToString.Exclude
    @PluginProperty(secret = true, group = "connection")
    private Property<String> token;
    @ToString.Exclude
    @PluginProperty(group = "connection", secret = true)
    private Property<String> creds;
    private String subject;
    private Property<String> durableId;
    private Property<String> since;
    @Builder.Default
    private Property<Duration> pollDuration = Property.ofValue(Duration.ofSeconds(2));
    @Builder.Default
    private Integer batchSize = 10;
    private Property<Integer> maxRecords;
    private Property<Duration> maxDuration;
    @Builder.Default
    private Property<DeliverPolicy> deliverPolicy = Property.ofValue(DeliverPolicy.All);
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

    // Holds the Consume instance built by the current evaluate() call so kill()/stop() can be
    // forwarded to it. A fresh Consume is built on every evaluate(), so this must be an
    // AtomicReference rather than a plain field: a killed evaluation must not poison later ones,
    // and kill() may arrive before or after any evaluation has started.
    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicReference<Consume> activeConsumeTask = new AtomicReference<>();

    // Sticky flag: kill()/stop() may arrive in the gap between a cycle finishing (activeConsumeTask
    // reset to null) and the next evaluate() publishing its freshly built Consume, where the signal
    // would otherwise find no task to forward to and be silently dropped. Never reset: once a trigger
    // is killed or stopped, no further evaluate() cycle should be allowed to start.
    @Getter(AccessLevel.NONE)
    @JsonIgnore
    @ToString.Exclude
    @EqualsAndHashCode.Exclude
    private final transient AtomicBoolean killedOrStopped = new AtomicBoolean(false);

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

        if (killedOrStopped.get()) {
            logger.debug("NATS polling trigger id={} received kill()/stop() before this evaluation cycle started; skipping poll", this.id);
            return Optional.empty();
        }

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
            .pollDuration(pollDuration)
            .batchSize(batchSize)
            .maxRecords(maxRecords)
            .maxDuration(maxDuration)
            .deliverPolicy(deliverPolicy)
            .build();

        this.activeConsumeTask.set(task);
        // Re-check right after publishing: closes the gap between build() and set() above, where a
        // kill()/stop() landing in between finds the previous cycle's (null) activeConsumeTask and
        // is otherwise silently dropped, letting the freshly built task run a full poll cycle.
        if (killedOrStopped.get()) {
            this.activeConsumeTask.compareAndSet(task, null);
            return Optional.empty();
        }

        Consume.Output run;
        try {
            run = task.run(runContext);
        } finally {
            this.activeConsumeTask.compareAndSet(task, null);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages from '{}'", run.getMessagesCount(), runContext.render(subject));
        }

        // A kill()/stop() landing mid-poll can still let task.run() return normally with some
        // messages acked+written before teardown; re-check the flag here so a killed/stopped
        // trigger never fires an execution off that partial batch.
        if (killedOrStopped.get() || run.getMessagesCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void kill() {
        killedOrStopped.set(true);
        Optional.ofNullable(this.activeConsumeTask.get()).ifPresent(Consume::kill);
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void stop() {
        killedOrStopped.set(true);
        Optional.ofNullable(this.activeConsumeTask.get()).ifPresent(Consume::stop);
    }
}
