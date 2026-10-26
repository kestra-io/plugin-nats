package io.kestra.plugin.nats.kv;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;
import io.kestra.core.utils.TestsUtils;

import jakarta.inject.Inject;
import jakarta.validation.ConstraintViolationException;

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
        var bucket = createBucket();

        var initialPut = Put.builder()
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

        var initialRev = initialPut.getRevisions().get("lock_key");
        assertThat(initialRev, notNullValue());

        var conditionalPut = Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of("lock_key", "updated")
                )
            )
            // revisions usually come from a previous task output, so render it like a flow would
            .expectedRevision(Property.ofExpression("{{ revision }}"))
            .build()
            .run(runContextFactory.of(Map.of("revision", initialRev)));

        assertThat(conditionalPut.getRevisions().get("lock_key"), greaterThan(initialRev));
    }

    @Test
    public void putWithMismatchedExpectedRevisionThrowsException() throws Exception {
        var bucket = createBucket();

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

        var exception = assertThrows(IllegalStateException.class, () ->
        {
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
        assertThat(exception.getMessage(), containsString("Key 'lock_key' in bucket '%s' is not at revision 9999".formatted(bucket)));

        var getOutput = Get.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(Property.ofValue(List.of("lock_key")))
            .build()
            .run(runContextFactory.of());
        assertThat(getOutput.getOutput().get("lock_key"), is("initial"));
    }

    @Test
    public void putNegativeExpectedRevisionIsRejected() {
        var task = Put.builder()
            .id("put-%s".formatted(IdUtils.create()))
            .type(Put.class.getName())
            .url("localhost:4222")
            .bucketName("unused")
            .values(Property.ofValue(Map.of("lock_key", "locked")))
            .expectedRevision(Property.ofValue(-1L))
            .build();

        assertThrows(ConstraintViolationException.class, () -> task.run(TestsUtils.mockRunContext(runContextFactory, task, Map.of())));
    }

    @Test
    public void putWithRevisionZeroWritesOnlyMissingKeys() throws Exception {
        var bucket = createBucket();

        var created = conditionalPut(bucket, "lock_key", 0L);
        assertThat(created.getRevisions().get("lock_key"), notNullValue());

        var conflict = assertThrows(IllegalStateException.class, () -> conditionalPut(bucket, "lock_key", 0L));
        assertThat(conflict.getMessage(), containsString("already exists"));

        Delete.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(Property.ofValue(List.of("lock_key")))
            .build()
            .run(runContextFactory.of());

        // A deleted key only keeps a delete marker, so revision 0 can create it again
        assertThat(conditionalPut(bucket, "lock_key", 0L).getRevisions().get("lock_key"), greaterThan(created.getRevisions().get("lock_key")));
    }

    @Test
    public void putExpectedRevisionWithSeveralKeysIsRejected() throws Exception {
        var bucket = createBucket();

        var exception = assertThrows(
            IllegalArgumentException.class, () -> Put.builder()
                .url("localhost:4222")
                .username(Property.ofValue("kestra"))
                .password(Property.ofValue("k3stra"))
                .bucketName(bucket)
                .values(Property.ofValue(Map.of("first", "a", "second", "b")))
                .expectedRevision(Property.ofValue(1L))
                .build()
                .run(runContextFactory.of())
        );
        assertThat(exception.getMessage(), containsString("single key"));
    }

    private Put.Output conditionalPut(String bucket, String key, long expectedRevision) throws Exception {
        return Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(Property.ofValue(Map.of(key, "locked")))
            .expectedRevision(Property.ofValue(expectedRevision))
            .build()
            .run(runContextFactory.of());
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
