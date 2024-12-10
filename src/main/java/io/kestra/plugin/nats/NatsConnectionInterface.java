package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
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
    Property<String> getUsername();

    @Schema(
        title = "Plaintext authentication password"
    )
    Property<String> getPassword();

    @Schema(
        title = "Token authentification"
    )
    Property<String> getToken();

    @Schema(
        title = "Credentials files authentification"
    )
    Property<String> getCreds();
}
