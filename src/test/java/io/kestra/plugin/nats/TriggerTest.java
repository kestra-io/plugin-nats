package io.kestra.plugin.nats.core;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import io.kestra.core.junit.annotations.EvaluateTrigger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;


import jakarta.inject.Inject;

import static io.kestra.plugin.nats.core.ProduceTest.SOME_HEADER_KEY;
import static io.kestra.plugin.nats.core.ProduceTest.SOME_HEADER_VALUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class TriggerTest extends io.kestra.plugin.nats.core.NatsTest {
    @Inject
    private RunContextFactory runContextFactory;

    public TriggerTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    @EvaluateTrigger(flow = "flows/nats-listen.yml", triggerId = "watch")
    void simpleConsumeTrigger(Optional<Execution> optionalExecution) throws Exception {
        assertThat(optionalExecution.isPresent(), is(true));

        Execution execution = optionalExecution.get();

        Produce.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject("kestra.trigger")
            .from(
                Map.of(
                    "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                    "data", "Hello Kestra From Produce Task"
                )
            )
            .build()
            .run(runContextFactory.of());

        BufferedReader inputStream = new BufferedReader(
            new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, URI.create((String) execution.getTrigger().getVariables().get("uri"))))
        );
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));

        assertThat(execution.getTrigger().getVariables().get("messagesCount"), is(1));
        assertThat(result.size(), is(1));
        assertThat(
            result, Matchers.contains(
                Matchers.allOf(
                    Matchers.hasEntry("subject", "kestra.trigger"),
                    Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))),
                    Matchers.hasEntry("data", "Hello Kestra From Produce Task")
                )
            )
        );
    }
}
