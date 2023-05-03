package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;

public interface ProduceInterface {
    @Schema(
        title = "Subject to produce message to"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    @NotNull
    String getSubject();

    @Schema(
        title = "Source of message(s) to send",
        description = "Can be an internal storage uri, a map or a list." +
            "with the following format: headers, data",
        anyOf = {String.class, List.class, Map.class}
    )
    @NotNull
    @PluginProperty(dynamic = true)
    Object getFrom();
}
