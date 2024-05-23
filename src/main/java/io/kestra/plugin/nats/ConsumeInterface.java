package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.PluginProperty;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public interface ConsumeInterface {

    @Schema(
        title = "The max number of rows to fetch before stopping"
    )
    @PluginProperty
    Integer getMaxRecords();

    @Schema(
        title = "The max duration before stopping the message polling",
        description = "It's not an hard limit and is evaluated every second"
    )
    @PluginProperty
    Duration getMaxDuration();

}
