package io.kestra.plugin.nats.core;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.services.FlowListenersInterface;
import io.kestra.core.storages.StorageInterface;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.scheduler.AbstractScheduler;
import io.kestra.worker.DefaultWorker;

import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import java.util.concurrent.atomic.AtomicReference;

import static io.kestra.plugin.nats.core.ProduceTest.SOME_HEADER_KEY;
import static io.kestra.plugin.nats.core.ProduceTest.SOME_HEADER_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class RealtimeTriggerTest extends NatsTest {
    @Inject
    private ApplicationContext applicationContext;
    @Inject
    private FlowListenersInterface flowListenersService;
    @Inject
    private DispatchQueueInterface<Execution> executionQueue;
    @Inject
    private LocalFlowRepositoryLoader localFlowRepositoryLoader;
    @Inject
    private RunContextFactory runContextFactory;

    public RealtimeTriggerTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void simpleConsumeTrigger() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);
        AtomicReference<Execution> last = new AtomicReference<>();

        // scheduler
        try (
            DefaultWorker worker = applicationContext.createBean(DefaultWorker.class, UUID.randomUUID().toString(), 8, null);
            AbstractScheduler scheduler = new JdbcScheduler(
                this.applicationContext,
                this.flowListenersService
            );
        ) {
            // wait for execution
            executionQueue.addListener(execution -> {
                last.set(execution);
                queueCount.countDown();
                assertThat(execution.getFlowId(), is("nats-realtime"));
            });

            worker.run();
            scheduler.run();

            localFlowRepositoryLoader.load(this.getClass().getClassLoader().getResource("flows/nats-realtime.yml"));

            Produce.builder()
                .url("localhost:4222")
                .username(Property.ofValue("kestra"))
                .password(Property.ofValue("k3stra"))
                .subject("kestra.realtime.trigger")
                .from(
                    Map.of(
                        "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                        "data", "Hello Kestra From Produce Task"
                    )
                )
                .build()
                .run(runContextFactory.of());

            boolean await = queueCount.await(1, TimeUnit.MINUTES);
            assertThat(await, is(true));

            Map<String, Object> result = last.get().getTrigger().getVariables();
            assertThat(result.size(), is(4)); // expect 4 variables
            assertThat(result.get("subject"), is("kestra.realtime.trigger"));
            assertThat(result.get("headers"), is(new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))));
            assertThat(result.get("data"), is("Hello Kestra From Produce Task"));
            assertThat(result.get("timestamp"), notNullValue());
        }
    }
}
