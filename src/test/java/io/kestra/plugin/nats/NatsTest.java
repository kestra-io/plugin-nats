
package io.kestra.plugin.nats;

import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.serializers.FileSerde;
import io.kestra.core.storages.StorageInterface;
import io.kestra.core.utils.Rethrow;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.nats.client.*;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.DeliverPolicy;
import io.nats.client.impl.Headers;
import jakarta.inject.Inject;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@MicronautTest
class NatsTest {
    @Inject
    protected StorageInterface storageInterface;

    protected List<Map<String, Object>> toMessages(Consume.Output output) throws IOException {
        BufferedReader inputStream = new BufferedReader(new InputStreamReader(storageInterface.get(output.getUri())));
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
