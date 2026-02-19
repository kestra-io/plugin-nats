package io.kestra.plugin.nats.kv;

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
import io.nats.client.KeyValue;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.Map;

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
                    password: nats_passwd
                    bucketName: my_bucket
                    values:
                      key1: value1
                      key2: value2
                      key3:
                        subKey1: some other value
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
    @PluginProperty(dynamic = true)
    private String bucketName;

    @Schema(
        title = "Values to write",
        description = "Map of keys to values rendered then serialized to JSON before being stored."
    )
    @NotNull
    private Property<Map<String, Object>> values;

    @Override
    public Put.Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValue keyValue = connection.keyValue(runContext.render(this.bucketName));

            Map<String, Long> revisions = new HashMap<>();
            for (Map.Entry<String, Object> entry : runContext.render(this.values).asMap(String.class, Object.class).entrySet()) {
                String key = entry.getKey();

                long revision = keyValue.put(
                    key,
                    mapper.writeValueAsString(entry.getValue()).getBytes()
                );

                revisions.put(key, revision);
            }

            return Output.builder()
                .revisions(revisions)
                .build();
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
