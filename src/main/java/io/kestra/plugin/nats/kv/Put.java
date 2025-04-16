package io.kestra.plugin.nats.kv;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.JacksonMapper;
import io.kestra.plugin.nats.NatsConnection;
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
    title = "Put a Key/Value pair into a NATS Key/Value bucket."
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
                      - key1: value1
                      - key2: value2
                      - key3:
                        - subKey1: some other value
                """
        ),
    }
)
public class Put extends NatsConnection implements RunnableTask<Put.Output> {

    private final static ObjectMapper mapper = JacksonMapper.ofJson();

    @Schema(
        title = "The name of the key value bucket."
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String bucketName;

    @Schema(
        title = "The Key/Value pairs."
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
            title = "The revision numbers for each key."
        )
        private Map<String, Long> revisions;

    }

}
