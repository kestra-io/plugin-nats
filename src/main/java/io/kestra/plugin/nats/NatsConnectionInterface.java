package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.PluginProperty;
import io.swagger.v3.oas.annotations.media.Schema;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public interface NatsConnectionInterface {
    @Schema(
        title = "URL to connect to NATS server",
        description = "The format is (nats://)server_url:port. You can also provide a connection token like so: nats://token@server_url:port"
    )
    @PluginProperty(dynamic = true)
    @NotBlank
    @NotNull
    String getUrl();

    @Schema(
        title = "Plaintext authentication username"
    )
    @PluginProperty(dynamic = true)
    String getUsername();

    @Schema(
        title = "Plaintext authentication password"
    )
    @PluginProperty(dynamic = true)
    String getPassword();

    @Schema(
        title = "Token authentification"
    )
    @PluginProperty(dynamic = true)
    String getToken();
}
