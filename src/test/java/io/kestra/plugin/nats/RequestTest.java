package io.kestra.plugin.nats;

import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;
import io.kestra.core.utils.IdUtils;
import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Nats;
import io.nats.client.Options;
import jakarta.inject.Inject;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

class RequestTest extends NatsTest {
    private static final String BASE_SUBJECT = "_unit.request.reply";
    public static final String SOME_HEADER_KEY = "someHeaderKey";
    public static final String SOME_HEADER_VALUE = "someHeaderValue";

    @Inject
    protected RunContextFactory runContextFactory;

    @Inject
    protected StorageInterface storageInterface;

    public RequestTest(StorageInterface storageInterface) {
        super(storageInterface);
    }

    @Test
    void requestMessageNoResponder() throws Exception {
        String subject = generateSubject();

        Map<String, Object> from = Map.of(
            "headers",Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
            "data", "Hello Kestra From Request Task"
        );

        // Run request with no local responder
        var output = runRequest(subject, from, Duration.ofMillis(1000));
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

   @Test
    void requestMessageWithResponder() throws Exception {
        String subject = generateSubject();

        // ephemeral subscription that replies "Hello from local responder!"
        try (Connection conn = Nats.connect(Options.builder().server("localhost:4222").userInfo("kestra", "k3stra").build());) {
            setupLocalResponder(conn, subject, "Hello from local responder!");

            // Now run the request
            Map<String, Object> from = Map.of(
                "headers", Map.of(SOME_HEADER_KEY, SOME_HEADER_VALUE),
                "data", "Hello from requestWithResponder test"
            );

            var output = runRequest(subject, from, Duration.ofMillis(2000));
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
        try (Connection conn = Nats.connect(Options.builder().server("localhost:4222").userInfo("kestra", "k3stra").build());) {
            setupLocalResponder(conn, subject, "Response from file-based request");

            // Put text file in storage => produce a kestra:// URI
            URI fileUri = storeStringInStorage(runContext, fileContent);

            // Now run request
            var output = runRequest(subject, fileUri.toString(), Duration.ofMillis(3000));
            assertThat(output.getResponse(), is("Response from file-based request"));
        }
    }

    /**
     * A helper to run the Request plugin for a given subject, "from" source, and timeout.
     * Returns the plugin's Output.
     */
    private Request.Output runRequest(String subject, Object from, Duration timeout) throws Exception {
        return Request.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(Property.ofValue(subject))
            .from(Property.ofValue(from))
            .requestTimeout(Property.ofValue(timeout))
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
        return storageInterface.put(TenantService.MAIN_TENANT, null, URI.create("/" + IdUtils.create() + ".txt"), new FileInputStream(tempFile));
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