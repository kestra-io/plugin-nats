package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.nats.client.api.DeliverPolicy;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.time.Duration;

public interface ConsumeInterface {

    @Schema(
        title = "Maximum records to consume",
        description = "Optional cap on total messages before stopping; implementations stop once this count is reached."
    )
    Property<Integer> getMaxRecords();

    @Schema(
        title = "Maximum polling duration",
        description = "Soft wall-clock limit evaluated between fetches; polling stops once exceeded."
    )
    Property<Duration> getMaxDuration();

    @Schema(
        title = "Fetch wait duration",
        description = "Max wait per fetch when no messages are available; implementations default to PT2S."
    )
    @NotNull
    Property<Duration> getPollDuration();
}
