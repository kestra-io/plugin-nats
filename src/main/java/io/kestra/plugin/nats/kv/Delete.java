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
import io.nats.client.KeyValue;
import io.swagger.v3.oas.annotations.media.Schema;
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
                    password: nats_passwd
                    bucketName: my_bucket
                    keys:
                      - key1
                      - key2
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

    @Override
    public Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValue keyValue = connection.keyValue(runContext.render(this.bucketName));

            int count = 0;
            for (String key : runContext.render(this.keys).asList(String.class)) {
                keyValue.delete(key);
                count++;
            }

            return Output.builder().deletedCount(count).build();
        }
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @Schema(title = "Number of keys deleted")
        private final int deletedCount;
    }
}
