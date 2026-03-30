package io.kestra.plugin.nats.core;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;

import io.nats.client.Connection;
import io.nats.client.JetStream;
import io.nats.client.JetStreamSubscription;
import io.nats.client.Message;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PullSubscribeOptions;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import jakarta.inject.Inject;

import static io.kestra.core.utils.Rethrow.throwConsumer;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class ProduceTest extends NatsTest {
    private static final String BASE_SUBJECT = "kestra.produce";
    public static final String SOME_HEADER_KEY = "someHeaderKey";
    public static final String SOME_HEADER_VALUE = "someHeaderValue";

    @Inject
    protected RunContextFactory runContextFactory;
    @Inject
    protected StorageInterface storageInterface;

    public ProduceTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void produceMessage() throws Exception {
        String subject = generateSubject();
        Produce.Output produceOutput = Produce.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .from(
                Map.of(
                    "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                    "data", "Hello Kestra From Produce Task"
                )
            )
            .build()
            .run(runContextFactory.of());

        Consume.Output consumeOutput = Consume.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .durableId(Property.ofValue("produceMessage-" + UUID.randomUUID()))
            .deliverPolicy(Property.ofValue(DeliverPolicy.All))
            .pollDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build()
            .run(runContextFactory.of());

        List<Map<String, Object>> result = toMessages(consumeOutput);

        assertThat(produceOutput.getMessagesCount(), is(1));
        assertThat(result.size(), is(1));
        assertThat(
            result, Matchers.contains(
                Matchers.allOf(
                    Matchers.hasEntry("subject", subject),
                    Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))),
                    Matchers.hasEntry("data", "Hello Kestra From Produce Task")
                )
            )
        );
    }

    @Test
    void produceMultipleMessages() throws Exception {
        String subject = generateSubject();
        Produce.Output produceOutput = Produce.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .from(
                List.of(
                    Map.of(
                        "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                        "data", "Hello Kestra From Produce Task"
                    ),
                    Map.of(
                        "data", "Hello Again From Another Produce Task"
                    )
                )
            )
            .build()
            .run(runContextFactory.of());

        Consume.Output consumeOutput = Consume.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .durableId(Property.ofValue("produceMultipleMessages-" + UUID.randomUUID()))
            .deliverPolicy(Property.ofValue(DeliverPolicy.All))
            .pollDuration(Property.ofValue(Duration.ofSeconds(3)))
            .build()
            .run(runContextFactory.of());

        List<Map<String, Object>> result = toMessages(consumeOutput);

        assertThat(produceOutput.getMessagesCount(), is(2));
        assertThat(result.size(), is(2));
        assertThat(
            result, Matchers.contains(
                Matchers.allOf(
                    Matchers.hasEntry("subject", subject),
                    Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))),
                    Matchers.hasEntry("data", "Hello Kestra From Produce Task")
                ),
                Matchers.allOf(
                    Matchers.hasEntry("subject", subject),
                    Matchers.hasEntry(is("headers"), new HeaderMatcher(anEmptyMap())),
                    Matchers.hasEntry("data", "Hello Again From Another Produce Task")
                )
            )
        );
    }

    @Test
    void produceMultipleMessagesFromInternalStorage() throws Exception {
        String subject = generateSubject();
        List<Map<String, ?>> messages = List.of(
            Map.of(
                "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                "data", "Hello Kestra From Produce Task"
            ),
            Map.of(
                "data", "Hello Again From Another Produce Task"
            )
        );

        RunContext runContext = runContextFactory.of();

        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            messages.forEach(throwConsumer(message -> FileSerde.write(outputStream, message)));
        }
        URI uri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".ion"), new FileInputStream(tempFile));

        Produce.Output produceOutput = Produce.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .from(uri.toString())
            .build()
            .run(runContext);

        // Core NATS publish is fire-and-forget; JetStream needs time to index the messages
        // before the consumer can fetch them. Without this delay, the consumer may see only
        // a subset of messages because JetStream hasn't finished persisting them yet.
        Thread.sleep(1000);

        Consume.Output consumeOutput = Consume.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .durableId(Property.ofValue("produceMultipleMessages-" + UUID.randomUUID()))
            .deliverPolicy(Property.ofValue(DeliverPolicy.All))
            .pollDuration(Property.ofValue(Duration.ofSeconds(10)))
            .maxRecords(Property.ofValue(2))
            .build()
            .run(runContextFactory.of());

        List<Map<String, Object>> result = toMessages(consumeOutput);

        assertThat(produceOutput.getMessagesCount(), is(2));
        assertThat(result.size(), is(2));
        assertThat(
            result, Matchers.contains(
                Matchers.allOf(
                    Matchers.hasEntry("subject", subject),
                    Matchers.hasEntry(is("headers"), new HeaderMatcher(hasEntry(is(SOME_HEADER_KEY), contains(SOME_HEADER_VALUE)))),
                    Matchers.hasEntry("data", "Hello Kestra From Produce Task")
                ),
                Matchers.allOf(
                    Matchers.hasEntry("subject", subject),
                    Matchers.hasEntry(is("headers"), new HeaderMatcher(anEmptyMap())),
                    Matchers.hasEntry("data", "Hello Again From Another Produce Task")
                )
            )
        );
    }

    @Test
    void produceBinaryData() throws Exception {
        byte[] binary = {0x00, (byte) 0xFF, 0x42, 0x7F};
        String base64Payload = Base64.getEncoder().encodeToString(binary);
        String subject = generateSubject();

        try (Connection connection = Nats.connect(Options.builder().server("localhost:4222").userInfo("kestra", "k3stra").build())) {
            JetStream jetStream = connection.jetStream();
            JetStreamSubscription subscription = jetStream.subscribe(
                subject,
                PullSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.New).build())
                    .build()
            );

            Produce.builder()
                .url("localhost:4222")
                .username(Property.ofValue("kestra"))
                .password(Property.ofValue("k3stra"))
                .subject(subject)
                .from(Map.of("data", base64Payload))
                .serializationType(Property.ofValue(SerializationType.BASE64))
                .build()
                .run(runContextFactory.of());

            List<Message> messages = subscription.fetch(1, Duration.ofSeconds(3));
            assertThat(messages.size(), is(1));
            assertThat(Arrays.equals(messages.get(0).getData(), binary), is(true));
        }
    }

    @Test
    void produceBinaryMode() throws Exception {
        byte[] binary = {0x00, (byte) 0xFF, 0x42, 0x7F};
        String subject = generateSubject();

        URI storageUri = storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("kestra:///test/binary.bin"), new ByteArrayInputStream(binary));

        try (Connection connection = Nats.connect(Options.builder().server("localhost:4222").userInfo("kestra", "k3stra").build())) {
            JetStream jetStream = connection.jetStream();
            JetStreamSubscription subscription = jetStream.subscribe(
                subject,
                PullSubscribeOptions.builder()
                    .configuration(ConsumerConfiguration.builder().deliverPolicy(DeliverPolicy.New).build())
                    .build()
            );

            Produce.builder()
                .url("localhost:4222")
                .username(Property.ofValue("kestra"))
                .password(Property.ofValue("k3stra"))
                .subject(subject)
                .from(Map.of("data", storageUri.toString()))
                .serializationType(Property.ofValue(SerializationType.BINARY))
                .build()
                .run(runContextFactory.of());

            List<Message> messages = subscription.fetch(1, Duration.ofSeconds(3));
            assertThat(messages.size(), is(1));
            assertThat(Arrays.equals(messages.get(0).getData(), binary), is(true));
        }
    }

    private static String generateSubject() {
        return BASE_SUBJECT + "." + UUID.randomUUID();
    }
}
