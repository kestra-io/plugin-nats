package io.kestra.plugin.nats.core;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.queues.DispatchQueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;

import jakarta.inject.Inject;

import static io.kestra.plugin.nats.core.ProduceTest.SOME_HEADER_KEY;
import static io.kestra.plugin.nats.core.ProduceTest.SOME_HEADER_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@KestraTest(startRunner = true, startScheduler = true)
class RealtimeTriggerTest extends NatsTest {
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
        var queueCount = new CountDownLatch(1);
        var last = new AtomicReference<Execution>();

        executionQueue.addListener(execution -> {
            last.set(execution);
            queueCount.countDown();
            assertThat(execution.getFlowId(), is("nats-realtime"));
        });

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

        assertThat(queueCount.await(1, TimeUnit.MINUTES), is(true));

        var result = last.get().getTrigger().getVariables();
        assertThat(result.size(), is(4));
        assertThat(result.get("subject"), is("kestra.realtime.trigger"));
        assertThat(result.get("headers"), is(new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))));
        assertThat(result.get("data"), is("Hello Kestra From Produce Task"));
        assertThat(result.get("timestamp"), notNullValue());
    }
}
