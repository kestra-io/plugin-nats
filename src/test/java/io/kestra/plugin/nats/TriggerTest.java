package io.kestra.plugin.nats;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.queues.QueueFactoryInterface;
import io.kestra.core.queues.QueueInterface;
import io.kestra.core.repositories.LocalFlowRepositoryLoader;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.runners.Worker;
import io.kestra.core.schedulers.AbstractScheduler;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;
import io.kestra.jdbc.runner.JdbcScheduler;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.services.FlowListenersInterface;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Inject;
import jakarta.inject.Named;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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


        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(null, URI.create((String) execution.getTrigger().getVariables().get("uri")))));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(execution.getTrigger().getVariables().get("messagesCount"), is(1));
        assertThat(result.size(), is(1));
        assertThat(result, Matchers.contains(
            Matchers.allOf(
                Matchers.hasEntry("subject", "kestra.trigger"),
                Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))),
                Matchers.hasEntry("data", base64Encoded("Hello Kestra From Produce Task"))
            )
        ));
    }

    protected Execution triggerFlow() throws Exception {
        // mock flow listeners
        CountDownLatch queueCount = new CountDownLatch(1);

        // scheduler
        try (Worker worker = applicationContext.createBean(Worker.class, IdUtils.create(), 8, null)) {
            try (
                AbstractScheduler scheduler = new JdbcScheduler(
                    this.applicationContext,
                    this.flowListenersService
                );
            ) {
                // wait for execution
                Flux<Execution> receive = TestsUtils.receive(executionQueue, execution -> {
                    queueCount.countDown();
                    assertThat(execution.getLeft().getFlowId(), is("nats-listen"));
                });

                worker.run();
                scheduler.run();

                localFlowRepositoryLoader.load(Objects.requireNonNull(this.getClass().getClassLoader().getResource("flows/nats-listen.yml")));

                boolean await = queueCount.await(1, TimeUnit.MINUTES);
                assertThat(await, is(true));

                return receive.blockLast();
            }
        }
    }
}
