package io.kestra.plugin.nats.kv;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;

@KestraTest
public class PutTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    public void putPair() throws Exception {
        String bucket = createBucket();

        Put.Output putOutput = Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of(
                        "key1", "value1",
                        "key2", 2L,
                        "key3", Map.of("subkey", "subvalue")
                    )
                )
            )
            .build()
            .run(runContextFactory.of());

        assertThat(putOutput.getRevisions(), notNullValue());
    }

    @Test
    public void putWithMatchingExpectedRevision() throws Exception {
        String bucket = createBucket();

        Put.Output initialPut = Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of("lock_key", "initial")
                )
            )
            .build()
            .run(runContextFactory.of());

        Long initialRev = initialPut.getRevisions().get("lock_key");
        assertThat(initialRev, notNullValue());

        Put.Output conditionalPut = Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of("lock_key", "updated")
                )
            )
            .expectedRevision(Property.ofValue(initialRev))
            .build()
            .run(runContextFactory.of());

        assertThat(conditionalPut.getRevisions().get("lock_key"), greaterThan(initialRev));
    }

    @Test
    public void putWithMismatchedExpectedRevisionThrowsException() throws Exception {
        String bucket = createBucket();

        Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of("lock_key", "initial")
                )
            )
            .build()
            .run(runContextFactory.of());

        assertThrows(Exception.class, () -> {
            Put.builder()
                .url("localhost:4222")
                .username(Property.ofValue("kestra"))
                .password(Property.ofValue("k3stra"))
                .bucketName(bucket)
                .values(
                    Property.ofValue(
                        Map.of("lock_key", "should_fail")
                    )
                )
                .expectedRevision(Property.ofValue(9999L))
                .build()
                .run(runContextFactory.of());
        });
    }

    public String createBucket() throws Exception {
        CreateBucket.Output bucketOutput = CreateBucket.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .name(IdUtils.create())
            .build()
            .run(runContextFactory.of());

        return bucketOutput.getBucket();
    }

}
