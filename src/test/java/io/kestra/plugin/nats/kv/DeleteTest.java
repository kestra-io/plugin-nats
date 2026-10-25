package io.kestra.plugin.nats.kv;

import java.util.ArrayList;
import java.util.List;
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
public class DeleteTest {

    @Inject
    protected RunContextFactory runContextFactory;

    @Test
    public void deletePair() throws Exception {
        String bucket = createBucket();
        Map<String, Object> keyValuePair = putPair(bucket);
        List<String> keys = new ArrayList<>(keyValuePair.keySet());

        Delete.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(
                Property.ofValue(
                    keys
                )
            )
            .build()
            .run(runContextFactory.of());

        Get.Output getOutput = Get.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(
                Property.ofValue(
                    new ArrayList<>(keyValuePair.keySet())
                )
            )
            .build()
            .run(runContextFactory.of());

        assertThat(getOutput.getOutput(), anEmptyMap());
    }

    @Test
    public void deleteWithMatchingExpectedRevision() throws Exception {
        var bucket = createBucket();

        var putOutput = Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of("key_to_delete", "val")
                )
            )
            .build()
            .run(runContextFactory.of());

        var initialRev = putOutput.getRevisions().get("key_to_delete");
        assertThat(initialRev, notNullValue());

        var deleteOutput = Delete.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(Property.ofValue(List.of("key_to_delete")))
            .expectedRevision(Property.ofValue(initialRev))
            .build()
            .run(runContextFactory.of());

        assertThat(deleteOutput.getDeletedCount(), is(1));

        var getOutput = Get.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(Property.ofValue(List.of("key_to_delete")))
            .build()
            .run(runContextFactory.of());

        assertThat(getOutput.getOutput(), anEmptyMap());
    }

    @Test
    public void deleteWithMismatchedExpectedRevisionThrowsException() throws Exception {
        var bucket = createBucket();

        Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    Map.of("key_to_delete", "val")
                )
            )
            .build()
            .run(runContextFactory.of());

        var exception = assertThrows(IllegalStateException.class, () -> {
            Delete.builder()
                .url("localhost:4222")
                .username(Property.ofValue("kestra"))
                .password(Property.ofValue("k3stra"))
                .bucketName(bucket)
                .keys(Property.ofValue(List.of("key_to_delete")))
                .expectedRevision(Property.ofValue(9999L))
                .build()
                .run(runContextFactory.of());
        });
        assertThat(exception.getMessage(), containsString("is not at revision 9999"));
    }

    public Map<String, Object> putPair(String bucket) throws Exception {
        Map<String, Object> keyValuePair = Map.of(
            "key1", "value1",
            "key2", "value2",
            "key3", "3"
        );

        Put.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .values(
                Property.ofValue(
                    keyValuePair
                )
            )
            .build()
            .run(runContextFactory.of());

        return keyValuePair;
    }

    @Test
    public void deleteExpectedRevisionWithSeveralKeysIsRejected() throws Exception {
        var bucket = createBucket();

        var exception = assertThrows(IllegalArgumentException.class, () -> Delete.builder()
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .bucketName(bucket)
            .keys(Property.ofValue(List.of("first", "second")))
            .expectedRevision(Property.ofValue(1L))
            .build()
            .run(runContextFactory.of()));
        assertThat(exception.getMessage(), containsString("single key"));
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
