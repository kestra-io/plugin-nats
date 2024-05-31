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
        title = "Minimum message timestamp to start consumption from",
        description = "By default, we consume all messages from the subjects starting from beginning of logs or " +
            "depending on the current durable id position. You can also provide an arbitrary start time to " +
            "get all messages since this date for a new durable id. Note that if you don't provide a durable id, " +
            "you will retrieve all messages starting from this date even after subsequent usage of this task." +
            "Must be a valid iso 8601 date."
    )
    @PluginProperty(dynamic = true)
    String getSince();

    @Schema(
        title = "Messages are fetched by batch of given size"
    )
    @PluginProperty
    @NotNull
    @Min(1)
    Integer getBatchSize();

    @Schema(
        title = "The point in the stream to receive messages from. Either All, Last, New, StartSequence, StartTime, or LastPerSubject"
    )
    @PluginProperty
    @NotNull
    DeliverPolicy getDeliverPolicy();
}
