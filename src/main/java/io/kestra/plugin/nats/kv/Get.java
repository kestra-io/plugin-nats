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
import io.nats.client.api.KeyValueEntry;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static io.kestra.core.utils.Rethrow.throwFunction;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Get a value from a NATS Key/Value bucket."
)
@Plugin(
    examples = {
        @Example(
            title = "Gets a value from a NATS Key/Value bucket by keys.",
            full = true,
            code = """
                id: nats_kv_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.nats.kv.Get
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_passwd
                    bucketName: my_bucket
                    keys:
                      - key1
                      - key2
                """
        ),
        @Example(
            title = "Gets a value from a NATS Key/Value bucket by keys with revisions.",
            full = true,
            code = """
			    id: nats_kv_get
                namespace: company.team

                tasks:
                  - id: get
                    type: io.kestra.plugin.nats.kv.Get
				    url: nats://localhost:4222
                    username: nats_user
                    password: nats_passwd
                    bucketName: my_bucket
                    keyRevisions:
                      - key1: 1
                      - key2: 3
				"""
        ),
    }
)
public class Get extends NatsConnection implements RunnableTask<Get.Output> {

    private final static ObjectMapper mapper = JacksonMapper.ofJson();

    @Schema(
        title = "The name of the key value bucket."
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String bucketName;

    @Schema(
        title = "The keys of Key/Value pairs."
    )
    @NotNull
    private Property<List<String>> keys;

    @Schema(
        title = "The keys with revision of Key/Value pairs."
    )
    private Property<Map<String, Long>> keyRevisions;

    @Override
    public Get.Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValue keyValue = connection.keyValue(runContext.render(this.bucketName));

            List<KeyValueEntry> entries = new ArrayList<>();
            if (this.keyRevisions == null) {
                for (String key : runContext.render(this.keys).asList(String.class)) {
                    entries.add(keyValue.get(key));
                }
            } else {
                for (Map.Entry<String, Long> entry : runContext.render(this.keyRevisions).asMap(String.class, Long.class).entrySet()) {
                    entries.add(keyValue.get(entry.getKey(), entry.getValue()));
                }
            }

            Map<String, Object> result = entries.stream()
                .filter(Objects::nonNull)
                .collect(
                    Collectors.toMap(
                        KeyValueEntry::getKey,
                        throwFunction(entry -> mapper.readValue(entry.getValue(), Object.class))
                    )
                );

            return Output.builder()
                .output(result)
                .build();
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The Key/Value pairs."
        )
        private Map<String, Object> output;

    }

}
