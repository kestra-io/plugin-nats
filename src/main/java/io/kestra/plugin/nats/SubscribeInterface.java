package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.PluginProperty;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;


/**
 * Base interface to subscribe and consume a subject.
 */
public interface SubscribeInterface {

    @Schema(
        title = "Subject to subscribe to"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    @NotNull
    String getSubject();

    @Schema(
        title = "ID used to attach the subscription to a durable one, allowing the subscription to start back from a previous position"
    )
    @PluginProperty(dynamic = true)
    String getDurableId();

    @Schema(
        title = "Minimum message timestamp to start consumption from.",
        description = "By default, we consume all messages from the subjects starting from beginning of logs or " +
            "depending on the current durable id position. You can also provide an arbitrary start time to " +
            "get all messages since this date for a new durable id. Note that if you don't provide a durable id, " +
            "you will retrieve all messages starting from this date even after subsequent usage of this task." +
            "Must be a valid iso 8601 date."
    )
    @PluginProperty(dynamic = true)
    String getSince();

    @Schema(
        title = "Messages are fetched by batch of given size."
    )
    @PluginProperty
    @NotNull
    @Min(1)
    Integer getBatchSize();

    @Schema(
        title = "The point in the stream to receive messages from.",
        description= "Possible settings are:\n" +
            "- `All`: The default policy. The consumer will start receiving from the earliest available message.\n" +
            "- `Last`: When first consuming messages, the consumer will start receiving messages with the last message added to the stream, or the last message in the stream that matches the consumer's filter subject if defined.\n" +
            "- `New`: When first consuming messages, the consumer will only start receiving messages that were created after the consumer was created.\n" +
            "- `ByStartSequence`: When first consuming messages, start at the first message having the sequence number or the next one available.\n" +
            "- `ByStartTime`: When first consuming messages, start with messages on or after this time. The consumer is required to specify `since` which defines this start time.\n" +
            "- `LastPerSubject`: When first consuming messages, start with the latest one for each filtered subject currently in the stream.\n"
    )
    @PluginProperty
    @NotNull
    DeliverPolicy getDeliverPolicy();
}
