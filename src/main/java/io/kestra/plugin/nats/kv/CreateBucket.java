package io.kestra.plugin.nats.kv;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.core.NatsConnection;
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
    title = "Create NATS Key/Value bucket",
    description = "Creates a Key/Value bucket on the target NATS cluster. Renders name and metadata before creation, sets maxHistoryPerKey to 1 by default, and lets you cap bucket and value sizes in bytes."
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
        title = "Bucket name",
        description = "Rendered bucket identifier; must be unique within the NATS account."
    )
    @NotBlank
    @PluginProperty(dynamic = true)
    private String name;

    @Schema(
        title = "Bucket description",
        description = "Optional human-readable description stored with the bucket."
    )
    @PluginProperty(dynamic = true)
    private String description;

    @Schema(
        title = "Bucket metadata",
        description = "Optional string map persisted as NATS Key/Value metadata."
    )
    private Property<Map<String, String>> metadata;

    @Schema(
        title = "Max history per key",
        description = "Maximum revisions retained for each key; defaults to 1 to keep only the latest value."
    )
    @Builder.Default
    private Property<Integer> historyPerKey = Property.ofValue(1);

    @Schema(
        title = "Bucket size limit",
        description = "Optional maximum bucket size in bytes; leave unset to use the server default."
    )
    private Property<Long> bucketSize;

    @Schema(
        title = "Value size limit",
        description = "Optional maximum size in bytes for any single entry in the bucket."
    )
    private Property<Long> valueSize;

    @Override
    public CreateBucket.Output run(RunContext runContext) throws Exception {
        try (Connection connection = super.connect(runContext)) {
            KeyValueManagement keyValueManagement = connection.keyValueManagement();

            KeyValueConfiguration.Builder builder = KeyValueConfiguration.builder()
                .name(runContext.render(this.name))
                .maxHistoryPerKey(runContext.render(this.historyPerKey).as(Integer.class).orElseThrow());

            if (this.description != null) {
                builder.description(runContext.render(this.description));
            }

            if (this.metadata != null) {
                builder.metadata(runContext.render(metadata).asMap(String.class, String.class));
            }

            if (this.bucketSize != null) {
                builder.maxBucketSize(runContext.render(this.bucketSize).as(Long.class).orElseThrow());
            }

            if (this.valueSize != null) {
                builder.maxValueSize(runContext.render(this.valueSize).as(Long.class).orElseThrow());
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
            title = "Bucket name",
            description = "Name of the created bucket."
        )
        private String bucket;

        @Schema(
            title = "Bucket description",
            description = "Description stored alongside the bucket."
        )
        private String description;

        @Schema(
            title = "Max history per key",
            description = "Configured revision cap per key."
        )
        private long history;

        @Schema(
            title = "Total entries",
            description = "Number of entries including historical revisions."
        )
        private long entryCount;

        @Schema(
            title = "Bucket size limit",
            description = "Maximum bucket size in bytes if configured, otherwise zero."
        )
        private long bucketSize;

        @Schema(
            title = "Value size limit",
            description = "Maximum entry size in bytes if configured, otherwise zero."
        )
        private long valueSize;

        @Schema(
            title = "Bucket metadata",
            description = "Metadata map returned by NATS."
        )
        private Map<String,String> metadata;

    }

}
