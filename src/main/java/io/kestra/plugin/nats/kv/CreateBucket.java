package io.kestra.plugin.nats.kv;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.NatsConnection;
import io.nats.client.Connection;
import io.nats.client.KeyValueManagement;
import io.nats.client.api.KeyValueConfiguration;
import io.nats.client.api.KeyValueStatus;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Creates Key/Value bucket in NATS."
)
@Plugin(
    examples = {
        @Example(
            title = "Creates a new Key/Value bucket, with all required properties.",
            full = true,
            code = """
                id: nats_kv_create_bucket
                namespace: company.team

                tasks:
                  - id: create_bucket
                    type: io.kestra.plugin.nats.kv.CreateBucket
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_passwd
                    name: my_bucket
                """
        ),
        @Example(
            title = "Creates a new Key/Value bucket.",
            full = true,
            code = """
                id: nats_kv_create_bucket
                namespace: company.team

                tasks:
                  - id: create_bucket
                    type: io.kestra.plugin.nats.kv.CreateBucket
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_passwd
                    name: my_bucket
                    description: my bucket for special purposes
                    historyPerKey: 2
                    bucketSize: 1024
                    valueSize: 1024
                    metadata: {"key1":"value1","key2":"value2"}
                """
        ),
    }
)
public class CreateBucket extends NatsConnection implements RunnableTask<CreateBucket.Output> {

    @Schema(
        title = "The name of the key value bucket."
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String name;

    @Schema(
        title = "The description of the key value bucket."
    )
    @PluginProperty(dynamic = true)
    private String description;

    @Schema(
        title = "The metadata of the key value bucket."
    )
    @PluginProperty(dynamic = true)
    private Map<String,String> metadata;

    @Schema(
        title = "The maximum number of history for a key."
    )
    @Builder.Default
    @PluginProperty
    private Integer historyPerKey = 1;

    @Schema(
        title = "The maximum size in bytes for this bucket."
    )
    private Long bucketSize;

    @Schema(
        title = "The maximum size in bytes for an individual value in the bucket."
    )
    private Long valueSize;

    @Override
    public CreateBucket.Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValueManagement keyValueManagement = connection.keyValueManagement();

            KeyValueConfiguration.Builder builder = KeyValueConfiguration.builder()
                .name(runContext.render(this.name))
                .maxHistoryPerKey(this.historyPerKey);

            if (this.description != null) {
                builder.description(runContext.render(this.description));
            }

            if (this.metadata != null) {
                builder.metadata(runContext.renderMap(metadata));
            }

            if (this.bucketSize != null) {
                builder.maxBucketSize(this.bucketSize);
            }

            if (this.valueSize != null) {
                builder.maxValueSize(this.valueSize);
            }

            KeyValueConfiguration configuration = builder.build();

            KeyValueStatus keyValueStatus = keyValueManagement.create(configuration);

            return Output.builder()
                .bucket(keyValueStatus.getBucketName())
                .description(keyValueStatus.getDescription())
                .history(keyValueStatus.getMaxHistoryPerKey())
                .entryCount(keyValueStatus.getEntryCount())
                .bucketSize(keyValueStatus.getMaxBucketSize())
                .valueSize(keyValueStatus.getMaxValueSize())
                .metadata(keyValueStatus.getMetadata())
                .build();
        }
    }

    @Getter
    @Builder
    public static class Output implements io.kestra.core.models.tasks.Output {

        @Schema(
            title = "The name of the key value bucket."
        )
        private String bucket;

        @Schema(
            title = "The description of the bucket."
        )
        private String description;

        @Schema(
            title = "The maximum number of history for a key."
        )
        private long history;

        @Schema(
            title = "The number of total entries in the bucket, including historical entries."
        )
        private long entryCount;

        @Schema(
            title = "The maximum size in bytes for this bucket."
        )
        private long bucketSize;

        @Schema(
            title = "The maximum size in bytes for an individual value in the bucket."
        )
        private long valueSize;

        @Schema(
            title = "The metadata for the store"
        )
        private Map<String,String> metadata;

    }

}
