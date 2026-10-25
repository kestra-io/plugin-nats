package io.kestra.plugin.nats.kv;

import java.util.List;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.core.NatsConnection;

import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.KeyValue;
import io.nats.client.support.NatsJetStreamConstants;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Delete keys from NATS Key/Value bucket",
    description = "Removes the latest value for each provided key and writes a delete marker. Bucket name and keys are rendered, and the bucket must already exist."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: nats_kv_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.nats.kv.Delete
                    url: nats://localhost:4222
                    username: nats_user
                    password: "{{ secret('NATS_PASSWORD') }}"
                    bucketName: my_bucket
                    keys:
                      - key1
                      - key2
                """
        ),
        @Example(
            title = "Conditional delete with expected revision",
            full = true,
            code = """
                id: nats_kv_conditional_delete
                namespace: company.team

                tasks:
                  - id: conditional_delete
                    type: io.kestra.plugin.nats.kv.Delete
                    url: nats://localhost:4222
                    username: nats_user
                    password: "{{ secret('NATS_PASSWORD') }}"
                    bucketName: my_bucket
                    keys:
                      - lock_key
                    expectedRevision: 2
                """
        ),
    }
)
public class Delete extends NatsConnection implements RunnableTask<Delete.Output> {

    @Schema(
        title = "Bucket name",
        description = "Rendered bucket identifier that must already exist."
    )
    @NotBlank
    @PluginProperty(dynamic = true, group = "main")
    private String bucketName;

    @Schema(
        title = "Keys to delete",
        description = "List of keys rendered before deletion; each delete creates a tombstone and increments the revision."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<List<String>> keys;

    @Schema(
        title = "Expected revision",
        description = """
            Delete only if the key is still at this revision, otherwise the task fails.
            Unset means the delete is unconditional. Only allowed with a single key, because revisions are unique across a bucket.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<@Min(1) Long> expectedRevision;

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValue keyValue = connection.keyValue(runContext.render(this.bucketName));

            var rExpectedRevision = runContext.render(this.expectedRevision).as(Long.class).orElse(null);
            var rKeys = runContext.render(this.keys).asList(String.class);
            if (rExpectedRevision != null && rKeys.size() > 1) {
                throw new IllegalArgumentException("`expectedRevision` applies to a single key, but %d keys were given. Use one Delete task per key.".formatted(rKeys.size()));
            }

            int count = 0;
            for (var key : rKeys) {
                if (rExpectedRevision == null) {
                    keyValue.delete(key);
                } else {
                    conditionalDelete(keyValue, key, rExpectedRevision);
                }
                count++;
            }

            return Output.builder().deletedCount(count).build();
        }
    }

    private void conditionalDelete(KeyValue keyValue, String key, long rExpectedRevision) throws Exception {
        try {
            keyValue.delete(key, rExpectedRevision);
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == NatsJetStreamConstants.JS_WRONG_LAST_SEQUENCE) {
                throw new IllegalStateException(
                    "Key '%s' in bucket '%s' is not at revision %d. Read it again and retry.".formatted(key, keyValue.getBucketName(), rExpectedRevision), e
                );
            }
            throw e;
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of keys deleted")
        private final int deletedCount;
    }
}
