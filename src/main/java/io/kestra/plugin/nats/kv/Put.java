package io.kestra.plugin.nats.kv;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
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
    title = "Put values into NATS Key/Value bucket",
    description = "Writes one or more Key/Value entries to an existing NATS bucket. Values are rendered, serialized to JSON, and the resulting revisions are returned."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: nats_kv_put
                namespace: company.team

                tasks:
                  - id: put
                    type: io.kestra.plugin.nats.kv.Put
                    url: nats://localhost:4222
                    username: nats_user
                    password: "{{ secret('NATS_PASSWORD') }}"
                    bucketName: my_bucket
                    values:
                      key1: value1
                      key2: value2
                      key3:
                        subKey1: some other value
                """
        ),
        @Example(
            title = "Conditional put with expected revision (compare-and-set)",
            full = true,
            code = """
                id: nats_kv_conditional_put
                namespace: company.team

                tasks:
                  - id: conditional_put
                    type: io.kestra.plugin.nats.kv.Put
                    url: nats://localhost:4222
                    username: nats_user
                    password: "{{ secret('NATS_PASSWORD') }}"
                    bucketName: my_bucket
                    values:
                      lock_key: acquired
                    expectedRevision: 1
                """
        ),
    }
)
public class Put extends NatsConnection implements RunnableTask<Put.Output> {

    private final static ObjectMapper mapper = JacksonMapper.ofJson();

    @Schema(
        title = "Bucket name",
        description = "Rendered bucket identifier that must already exist."
    )
    @NotBlank
    @PluginProperty(dynamic = true, group = "main")
    private String bucketName;

    @Schema(
        title = "Values to write",
        description = "Map of keys to values rendered then serialized to JSON before being stored."
    )
    @NotNull
    @PluginProperty(group = "main")
    private Property<Map<String, Object>> values;

    @Schema(
        title = "Expected revision",
        description = """
            Write only if the key is still at this revision (compare-and-set), otherwise the task fails. `0` writes only if the key does not exist.
            Unset means the write is unconditional. Only allowed with a single key, because revisions are unique across a bucket.
            """
    )
    @PluginProperty(group = "advanced")
    private Property<@Min(0) Long> expectedRevision;

    @Override
    public Put.Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValue keyValue = connection.keyValue(runContext.render(this.bucketName));

            var rExpectedRevision = runContext.render(this.expectedRevision).as(Long.class).orElse(null);
            var rValues = runContext.render(this.values).asMap(String.class, Object.class);
            if (rExpectedRevision != null && rValues.size() > 1) {
                throw new IllegalArgumentException("`expectedRevision` applies to a single key, but %d keys were given. Use one Put task per key.".formatted(rValues.size()));
            }

            Map<String, Long> revisions = new HashMap<>();
            for (var entry : rValues.entrySet()) {
                String key = entry.getKey();
                var bytes = mapper.writeValueAsString(entry.getValue()).getBytes();

                var revision = rExpectedRevision == null ? keyValue.put(key, bytes) : conditionalWrite(keyValue, key, bytes, rExpectedRevision);

                revisions.put(key, revision);
            }

            return Output.builder()
                .revisions(revisions)
                .build();
        }
    }

    // create() also succeeds when the key only has a delete marker, update(key, bytes, 0) does not
    private long conditionalWrite(KeyValue keyValue, String key, byte[] bytes, long rExpectedRevision) throws Exception {
        try {
            return rExpectedRevision == 0 ? keyValue.create(key, bytes) : keyValue.update(key, bytes, rExpectedRevision);
        } catch (JetStreamApiException e) {
            if (e.getApiErrorCode() == NatsJetStreamConstants.JS_WRONG_LAST_SEQUENCE) {
                var reason = rExpectedRevision == 0 ? "already exists" : "is not at revision %d".formatted(rExpectedRevision);
                throw new IllegalStateException("Key '%s' in bucket '%s' %s. Read it again and retry.".formatted(key, keyValue.getBucketName(), reason), e);
            }
            throw e;
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "Revision numbers",
            description = "Revision returned by NATS for each written key."
        )
        private Map<String, Long> revisions;

    }

}
