package io.kestra.plugin.nats;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests the NATS "Request" plugin in a pure request-reply manner.

 * Scenarios:
 *   1) No responder => The request times out => returns null
 *   2) With responder => The request returns the actual reply from the subscriber
 *   3) Reading the request from a file in Kestra's internal storage (must be a single top-level Map)

 * Uses helper methods to keep the code DRY.
 */
class RequestTest extends NatsTest {
    private static final String BASE_SUBJECT = "kestra.request";
    public static final String SOME_HEADER_KEY = "someHeaderKey";
    public static final String SOME_HEADER_VALUE = "someHeaderValue";

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    // ---------------------------------------------------------
    // 1) NO RESPONDER => returns null
    // ---------------------------------------------------------

    @Test
    void requestMessageNoResponder() throws Exception {
        String subject = generateSubject();

        Map<String, Object> singleMessage = Map.of(
            "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
            "data", "Hello Kestra From Request Task"
        );

        // Run request with no local responder
        var output = runRequest(subject, singleMessage, Duration.ofMillis(1000));
        assertThat(output.getResponse(), nullValue());
    }

    @Test
    void requestFromFileNoResponder() throws Exception {
        String subject = generateSubject();
        RunContext runContext = runContextFactory.of();

        Map<String, Object> singleMessage = Map.of(
            "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
            "data", "A single record from file"
        );

        // Store the single map in a file -> get URI
        URI fileUri = storeSingleMapInStorage(runContext, singleMessage);

        // Run request with no responder
        var output = runRequest(subject, fileUri.toString(), Duration.ofMillis(1000));
        assertThat(output.getResponse(), nullValue());
    }

    // ---------------------------------------------------------
    // 2) WITH RESPONDER => returns actual response
    // ---------------------------------------------------------

    @Test
    void requestMessageWithResponder() throws Exception {
        String subject = generateSubject();

        // ephemeral subscription that replies "Hello from local responder!"
        try (Connection conn = this.natsConnection()) {
            setupLocalResponder(conn, subject, "Hello from local responder!");

            // Now run the request
            Map<String, Object> singleMessage = Map.of(
                "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                "data", "Hello from requestWithResponder test"
            );

            var output = runRequest(subject, singleMessage, Duration.ofMillis(2000));

            // Expect real user-level reply
            assertThat(output.getResponse(), is("Hello from local responder!"));
        }
    }

    @Test
    void requestFileWithResponder() throws Exception {
        String subject = generateSubject();
        RunContext runContext = runContextFactory.of();

        Map<String, Object> singleMessage = Map.of(
            "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
            "data", "Request data from file"
        );

        // Store the single map -> get URI
        URI fileUri = storeSingleMapInStorage(runContext, singleMessage);

        // ephemeral subscription that replies "Response from file-based request"
        try (Connection conn = this.natsConnection()) {
            setupLocalResponder(conn, subject, "Response from file-based request");

            // Now run request
            var output = runRequest(subject, fileUri.toString(), Duration.ofMillis(3000));
            assertThat(output.getResponse(), is("Response from file-based request"));
        }
    }

    // ---------------------------------------------------------
    // HELPER METHODS
    // ---------------------------------------------------------

    /**
     * A helper to run the Request plugin for a given subject, "from" source, and timeout in ms.
     * Returns the plugin's Output.
     */
    private Request.Output runRequest(String subject, Object from, Duration timeoutMs) throws Exception {
        return Request.builder()
            .url("localhost:4222")
            .username(Property.of("kestra"))
            .password(Property.of("k3stra"))
            .subject(subject)
            .from(from)
            .requestTimeout(timeoutMs)
            .build()
            .run(runContextFactory.of());
    }

    /**
     * A helper to store a single map in Kestra's internal storage as .ion,
     * then return the resulting storage URI.
     */
    private URI storeSingleMapInStorage(RunContext runContext, Map<String, Object> singleMap) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".ion").toFile();

        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            FileSerde.write(outputStream, singleMap);
        }

        return storageInterface.put(
            null,
            null,
            URI.create("/" + IdUtils.create() + ".ion"),
            new FileInputStream(tempFile)
        );
    }

    /**
     * A helper to create a local ephemeral subscription that replies with the given text.
     * The subscription automatically responds to messages on the given subject.
     */
    private void setupLocalResponder(Connection connection, String subject, String replyText) {
        Dispatcher dispatcher = connection.createDispatcher(msg -> {
            byte[] replyData = replyText.getBytes(StandardCharsets.UTF_8);
            connection.publish(msg.getReplyTo(), replyData);
        });

        dispatcher.subscribe(subject);
    }

    /**
     * Generates a unique subject for each test.
     */
    private static String generateSubject() {
        return BASE_SUBJECT + "." + UUID.randomUUID();
    }
}