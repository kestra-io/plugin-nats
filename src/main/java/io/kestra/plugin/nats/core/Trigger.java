package io.kestra.plugin.nats.core;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.core.NatsConnection;
import io.kestra.plugin.nats.ConsumeInterface;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.slf4j.Logger;

import java.time.Duration;
import java.util.Optional;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Trigger a flow on periodic message consumption from a NATS subject on a JetStream-enabled NATS server.",
    description = "If you would like to consume each message from a NATS subject in real-time and create one execution per message, you can use the [io.kestra.plugin.nats.RealtimeTrigger](https://kestra.io/plugins/plugin-nats/triggers/io.kestra.plugin.nats.realtimetrigger) instead."
)
@Plugin(
    aliases = {"io.kestra.plugin.nats.Trigger"},
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
                    password: nats_password
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
    private Property<String> username;
    private Property<String> password;
    private Property<String> token;
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

    @Override
    public Optional<Execution> evaluate(ConditionContext conditionContext, TriggerContext context) throws Exception {
        RunContext runContext = conditionContext.getRunContext();
        Logger logger = runContext.logger();

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
        Consume.Output run = task.run(runContext);

        if (logger.isDebugEnabled()) {
            logger.debug("Found '{}' messages from '{}'", run.getMessagesCount(), runContext.render(subject));
        }

        if (run.getMessagesCount() == 0) {
            return Optional.empty();
        }

        Execution execution = TriggerService.generateExecution(this, conditionContext, context, run);

        return Optional.of(execution);
    }
}
