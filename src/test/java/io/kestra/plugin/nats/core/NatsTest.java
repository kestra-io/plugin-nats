package io.kestra.plugin.nats.core;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.tenant.TenantService;

import jakarta.inject.Inject;

@KestraTest
class NatsTest {
    protected final StorageInterface storageInterface;

    @Inject // Micronaut supports constructor injection
    public NatsTest(StorageInterface storageInterface) {
        this.storageInterface = storageInterface;
    }

    private static final Pattern NUM_CONNECTIONS = Pattern.compile("\"num_connections\":\\s*(\\d+)");

    // Client connections open on the NATS server (monitoring endpoint -m 8222).
    protected static int serverConnections() throws Exception {
        try (HttpClient client = HttpClient.newHttpClient()) {
            var body = client.send(
                HttpRequest.newBuilder(URI.create("http://localhost:8222/connz")).build(),
                HttpResponse.BodyHandlers.ofString()
            ).body();
            var matcher = NUM_CONNECTIONS.matcher(body);
            if (!matcher.find()) {
                throw new IllegalStateException("num_connections not found in /connz response: " + body);
            }
            return Integer.parseInt(matcher.group(1));
        }
    }

    // Waits up to 5s for connections to drop to expected or below (close is async).
    protected static int awaitServerConnections(int expected) throws Exception {
        var current = serverConnections();
        for (int i = 0; i < 50 && current > expected; i++) {
            Thread.sleep(100);
            current = serverConnections();
        }
        return current;
    }

    protected List<Map<String, Object>> toMessages(Consume.Output output) throws IOException {
        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(TenantService.MAIN_TENANT, null, output.getUri())));
        List<Map<String, Object>> result = new ArrayList<>();
        FileSerde.reader(inputStream, r -> result.add((Map<String, Object>) r));
        return result;
    }

    protected static class HeaderMatcher extends BaseMatcher<Object> {
        private final Matcher<Map<? extends String, ? extends Iterable<? extends String>>> mapMatcher;

        public HeaderMatcher(Matcher<Map<? extends String, ? extends Iterable<? extends String>>> mapMatcher) {
            this.mapMatcher = mapMatcher;
        }

        @Override
        public boolean matches(Object actual) {
            return mapMatcher.matches(actual);
        }

        @Override
        public void describeTo(Description description) {
            description.appendDescriptionOf(mapMatcher);
        }
    }
}
