package io.kestra.plugin.nats;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.IdUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Tests the NATS "Request" plugin in a pure request-reply manner.
 *
 * Scenarios:
 *   1) No responder => The request times out => returns null
 *   2) With responder => The request returns the actual reply from the subscriber
 *   3) Reading the request from a file in Kestra's internal storage => entire file => data
 *   4) List-based scenarios => single item vs. multiple items
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

        // Instead of storing a map with headers & data,
        // we'll store plain text. The plugin reads entire file => data.
        String fileContent = "A single record from file";

        // Store the text file => produce a kestra:// URI
        URI fileUri = storeStringInStorage(runContext, fileContent);

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
            assertThat(output.getResponse(), is("Hello from local responder!"));
        }
    }

    @Test
    void requestFileWithResponder() throws Exception {
        String subject = generateSubject();
        RunContext runContext = runContextFactory.of();

        // We'll store plain text in the file
        String fileContent = "Request data from file";

        // ephemeral subscription that replies "Response from file-based request"
        try (Connection conn = this.natsConnection()) {
            setupLocalResponder(conn, subject, "Response from file-based request");

            // Put text file in storage => produce a kestra:// URI
            URI fileUri = storeStringInStorage(runContext, fileContent);

            // Now run request
            var output = runRequest(subject, fileUri.toString(), Duration.ofMillis(3000));
            assertThat(output.getResponse(), is("Response from file-based request"));
        }
    }

    // ---------------------------------------------------------
    // 3) LIST SCENARIOS
    // ---------------------------------------------------------

    @Test
    void requestSingleItemListSuccess() throws Exception {
        String subject = generateSubject();

        try (Connection conn = this.natsConnection()) {
            setupLocalResponder(conn, subject, "List-based reply");

            // fromList with exactly one item, a valid Map
            Map<String, Object> singleItem = Map.of(
                "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                "data", "Hello from single-item list"
            );

            var output = runRequest(subject, List.of(singleItem), Duration.ofMillis(2000));
            assertThat(output.getResponse(), is("List-based reply"));
        }
    }

    @Test
    void requestMultipleItemsInListShouldFail() {
        String subject = generateSubject();

        // fromList with multiple maps => should fail
        var fromList = List.of(
            Map.of("data", "Message 1"),
            Map.of("data", "Message 2")
        );

        // We expect an IllegalArgumentException because it’s more than 1 item
        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            runRequest(subject, fromList, Duration.ofMillis(1000));
        });
    }

    @Test
    void requestEmptyListShouldFail() {
        String subject = generateSubject();

        // fromList is empty => should fail
        var fromList = List.of();

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            runRequest(subject, fromList, Duration.ofMillis(1000));
        });
    }

    @Test
    void requestSingleItemListNonMapShouldFail() {
        String subject = generateSubject();

        // fromList with exactly one item, but it's not a Map
        var fromList = List.of("I am not a Map!");

        Assertions.assertThrows(IllegalArgumentException.class, () -> {
            runRequest(subject, fromList, Duration.ofMillis(1000));
        });
    }

    // ---------------------------------------------------------
    // HELPER METHODS
    // ---------------------------------------------------------

    /**
     * A helper to run the Request plugin for a given subject, "from" source, and timeout.
     * Returns the plugin's Output.
     */
    private Request.Output runRequest(String subject, Object from, Duration timeout) throws Exception {
        return Request.builder()
            .url("localhost:4222")
            .username(Property.of("kestra"))
            .password(Property.of("k3stra"))
            .subject(subject)
            .from(from)
            .requestTimeout(timeout)
            .build()
            .run(runContextFactory.of());
    }

    /**
     * Store the given String as a file in Kestra's internal storage.
     * Returns the resulting storage URI (like kestra://...).
     */
    private URI storeStringInStorage(RunContext runContext, String content) throws Exception {
        File tempFile = runContext.workingDir().createTempFile(".txt").toFile();

        // Write the raw text
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
        }

        // Upload into Kestra storage => returns a kestra:// URI
        return storageInterface.put(
            null,
            null,
            URI.create("/" + IdUtils.create() + ".txt"),
            new FileInputStream(tempFile)
        );
    }

    /**
     * Create a local ephemeral subscription that replies with the given text.
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