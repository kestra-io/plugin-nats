package io.kestra.plugin.nats;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;

import java.util.function.Function;

@AllArgsConstructor
@Schema(title = "Deserializer to use when retrieving NATS message data", description = "Data is in bytes format by default")
public enum Deserializer {
    TO_STRING(String::new),
    BYTES(Function.identity());

    private final Function<byte[], ?> converter;

    public Object apply(byte[] bytes) {
        return converter.apply(bytes);
    }
}
