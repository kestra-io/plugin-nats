package io.kestra.plugin.nats;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.Rethrow;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.impl.Headers;
import jakarta.inject.Inject;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ConsumeTest extends NatsTest {
    @Inject
    protected RunContextFactory runContextFactory;

    public ConsumeTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void consumeMessageFromSubject() throws Exception {
        Connection connection = Nats.connect(Options.builder().server("localhost:4222").userInfo("kestra", "k3stra").build());

        AtomicReference<Instant> messageInstant = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        JetStreamSubscription subscription = connection.jetStream().subscribe("kestra.consumeMessageFromSubject.topic", PullSubscribeOptions.builder()
            .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.New).build())
            .build()
        );

        Executors.newSingleThreadExecutor().submit(Rethrow.throwRunnable(() ->
            {
                List<Message> messages = subscription.fetch(1, Duration.ofSeconds(2));
                messageInstant.set(
                    // Compulsory to match ION-serialized instant precision
                    Instant.ofEpochMilli(messages.get(0).metaData().timestamp().toInstant().toEpochMilli())
                );
                countDownLatch.countDown();
                connection.close();
            }
        ));

        Headers headers = new Headers();
        String expectedHeaderKey = "someHeaderKey";
        String expectedHeaderValue = "someHeaderValue";
        headers.add(expectedHeaderKey, expectedHeaderValue);
        connection.publish("kestra.consumeMessageFromSubject.topic", headers, "Hello Kestra".getBytes());
        connection.publish("kestra.consumeMessageFromSubject.anotherTopic", "Hello Again".getBytes());

        Consume.Output output = Consume.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject("kestra.consumeMessageFromSubject.>")
            .durableId(Property.ofValue("consumeMessageFromSubject-" + UUID.randomUUID()))
            .deliverPolicy(Property.ofValue(DeliverPolicy.LastPerSubject))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .batchSize(1)
            .build()
            .run(runContextFactory.of());

        List<Map<String, Object>> result = toMessages(output);

        countDownLatch.await();

        assertThat(output.getMessagesCount(), is(2));
        assertThat(result.size(), is(2));
        assertThat(result, Matchers.contains(
            Matchers.<Map<String, Object>>allOf(
                Matchers.hasEntry("subject", "kestra.consumeMessageFromSubject.topic"),
                Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(expectedHeaderKey), contains(expectedHeaderValue)))),
                Matchers.hasEntry("data", "Hello Kestra"),
                Matchers.hasEntry("timestamp", messageInstant.get())
            )
            ,
            Matchers.<Map<String, Object>>allOf(
                Matchers.hasEntry("subject", "kestra.consumeMessageFromSubject.anotherTopic"),
                Matchers.hasEntry(is("headers"), new HeaderMatcher(anEmptyMap())),
                Matchers.hasEntry("data", "Hello Again")
            )
        ));
    }

    @Test
    void consumeSince() throws Exception {
        Connection connection = Nats.connect(Options.builder().server("localhost:4222").userInfo("kestra", "k3stra").build());

        JetStreamSubscription subscription = connection.jetStream().subscribe("kestra.consumeSince.>", PullSubscribeOptions.builder()
            .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.New).build())
            .build()
        );

        AtomicReference<ZonedDateTime> messageTimestamp = new AtomicReference<>();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().submit(Rethrow.throwRunnable(() ->
            {
                List<Message> messages = subscription.fetch(2, Duration.ofSeconds(2));
                messageTimestamp.set(
                    // Compulsory to match ION-serialized instant precision
                    messages.get(1).metaData().timestamp()
                );
                countDownLatch.countDown();
                connection.close();
            }
        ));

        connection.publish("kestra.consumeSince.topic", "First message".getBytes());
        Thread.sleep(5);
        connection.publish("kestra.consumeSince.anotherTopic", "Second message".getBytes());

        countDownLatch.await();

        Consume.Output output = Consume.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject("kestra.consumeSince.>")
            .durableId(Property.ofValue("consumeSince-" + UUID.randomUUID()))
            .deliverPolicy(Property.ofValue(DeliverPolicy.ByStartTime))
            .pollDuration(Property.ofValue(Duration.ofSeconds(1)))
            .since(Property.ofValue(messageTimestamp.get().minus(1, ChronoUnit.MILLIS).toString()))
            .build()
            .run(runContextFactory.of());

        List<Map<String, Object>> result = toMessages(output);

        assertThat(output.getMessagesCount(), is(1));
        assertThat(result.size(), is(1));
        Assertions.assertEquals("Second message", result.get(0).get("data"));
    }
}
