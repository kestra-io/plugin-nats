package io.kestra.plugin.nats;

import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.junit.annotations.KestraTest;
import jakarta.inject.Inject;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@KestraTest
class NatsTest {
    protected final StorageInterface storageInterface;

    @Inject // Micronaut supports constructor injection
    public NatsTest(StorageInterface storageInterface) {
        this.storageInterface = storageInterface;
    }

    protected Connection connection;

    @AfterEach
    void tearDown() throws Exception {
        if (connection != null && connection.getStatus() == Connection.Status.CONNECTED) {
            connection.close();
        }
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

    protected Connection natsConnection() throws Exception {
        Options options = new Options.Builder()
            .server("nats://localhost:4222")
            .userInfo("kestra", "k3stra")
            .build();

        this.connection = Nats.connect(options);
        return this.connection;
    }
}
