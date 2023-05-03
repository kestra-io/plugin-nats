package io.kestra.plugin.nats;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.schedulers.DefaultScheduler;
import io.kestra.core.schedulers.SchedulerExecutionStateInterface;
import io.kestra.core.schedulers.SchedulerTriggerStateInterface;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.services.FlowListenersInterface;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.plugin.nats.ProduceTest.SOME_HEADER_KEY;
import static io.kestra.plugin.nats.ProduceTest.SOME_HEADER_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class TriggerTest extends NatsTest {
    @Inject
    private ApplicationContext applicationContext;
    @Inject
    private FlowListenersInterface flowListenersService;
    @Inject
    private SchedulerExecutionStateInterface executionState;
    @Inject
    private SchedulerTriggerStateInterface triggerState;
    @Inject
    @Named(QueueFactoryInterface.EXECUTION_NAMED)
    private QueueInterface<Execution> executionQueue;
    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void simpleConsumeTrigger() throws Exception {
        Produce.builder()
            .url("localhost:4222")
            .username("kestra")
            .password("k3stra")
            .subject("kestra.trigger")
            .from(Map.of(
                "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                "data", "Hello Kestra From Produce Task"
            ))
            .build()
            .run(runContextFactory.of());

        Execution execution = triggerFlow();


        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(URI.create((String) execution.getTrigger().getVariables().get("uri")))));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(execution.getTrigger().getVariables().get("messagesCount"), is(1));
        assertThat(result.size(), is(1));
        assertThat(result, Matchers.contains(
            Matchers.allOf(
                Matchers.hasEntry("subject", "kestra.trigger"),
                Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))),
                Matchers.hasEntry("data", "Hello Kestra From Produce Task")
            )
        ));
    }

    protected Execution triggerFlow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (var scheduler = new DefaultScheduler(this.applicationContext, this.flowListenersService, this.executionState, this.triggerState)) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(execution -> {
                last.set(execution);

                queueCount.countDown();
                assertThat(execution.getFlowId(), is("nats-listen"));
            });

            scheduler.run();

            localFlowRepositoryLoader.load(this.getClass().getClassLoader().getResource("flows/nats-listen.yml"));

            queueCount.await(1, TimeUnit.MINUTES);

            return last.get();
        }
    }
}
