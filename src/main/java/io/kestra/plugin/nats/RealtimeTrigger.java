package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.triggers.*;
import io.kestra.core.runners.RunContext;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.time.Duration;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "React to and consume messages from a NATS subject on a JetStream-enabled NATS server to trigger execution on results."
)
@Plugin(
    examples = {
        @Example(
            title = "Subscribe to a NATS subject, getting every message from the beginning of the subject on first trigger execution.",
            full = true,
            code = {
                "triggers:",
                "  - id: watch",
                "    type: io.kestra.plugin.nats.Trigger",
                "    url: nats://localhost:4222",
                "    username: kestra",
                "    password: k3stra",
                "    subject: kestra.trigger",
                "    durableId: natsTrigger",
                "    deliverPolicy: All"
            }
        )
    }
)
public class RealtimeTrigger extends AbstractTrigger implements RealtimeTriggerInterface, TriggerOutput<Consume.Output>, NatsConnectionInterface, ConsumeInterface {
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
    private Integer maxRecords;
    private Duration maxDuration;
    @Builder.Default
    private DeliverPolicy deliverPolicy = DeliverPolicy.All;
    @Builder.Default
    private final Duration interval = Duration.ofSeconds(60);

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
            .maxRecords(maxRecords)
            .maxDuration(maxDuration)
            .deliverPolicy(deliverPolicy)
            .build();

        return Flux.from(task.stream(conditionContext.getRunContext()))
            .map(record -> TriggerService.generateRealtimeExecution(this, context, record))
            .next();
    }
}
