package io.kestra.plugin.nats;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.schedulers.DefaultScheduler;
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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.plugin.nats.ProduceTest.SOME_HEADER_KEY;
import static io.kestra.plugin.nats.ProduceTest.SOME_HEADER_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class RealtimeTriggerTest extends NatsTest {
    @Inject
    private ApplicationContext applicationContext;
    @Inject
    private FlowListenersInterface flowListenersService;
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
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (
            Worker worker = applicationContext.createBean(Worker.class, UUID.randomUUID().toString(), 8, null);
            AbstractScheduler scheduler = new DefaultScheduler(
                this.applicationContext,
                this.flowListenersService,
                this.triggerState
            );
        ) {
            AtomicReference<Execution> last = new AtomicReference<>();

            // wait for execution
            executionQueue.receive(RealtimeTriggerTest.class, execution -> {
                last.set(execution.getLeft());

                queueCount.countDown();
                assertThat(execution.getLeft().getFlowId(), is("nats-realtime"));
            });

            worker.run();
            scheduler.run();

            localFlowRepositoryLoader.load(this.getClass().getClassLoader().getResource("flows/nats-realtime.yml"));

            Produce.builder()
                .url("localhost:4222")
                .username("kestra")
                .password("k3stra")
                .subject("kestra.realtime.trigger")
                .from(Map.of(
                    "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                    "data", "Hello Kestra From Produce Task"
                ))
                .build()
                .run(runContextFactory.of());

            queueCount.await(1, TimeUnit.MINUTES);

            Execution execution = last.get();

            Map<String, Object> result = execution.getTrigger().getVariables();
            assertThat(result.size(), is(4)); // expect 4 variables
            assertThat(result.get("subject"), is("kestra.realtime.trigger"));
            assertThat(result.get("headers"), is(new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))));
            assertThat(result.get("data"), is("Hello Kestra From Produce Task"));
            assertThat(result.get("timestamp"), notNullValue());
        }
    }
}
