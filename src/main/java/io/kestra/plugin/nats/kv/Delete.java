package io.kestra.plugin.nats.kv;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.property.Property;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.VoidOutput;
import io.kestra.core.runners.RunContext;
import io.kestra.plugin.nats.NatsConnection;
import io.nats.client.Connection;
import io.nats.client.KeyValue;
import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Deletes a pair from a NATS Key/Value bucket."
)
@Plugin(
    examples = {
        @Example(
            full = true,
            code = """
                id: nats_kv_delete
                namespace: company.team

                tasks:
                  - id: delete
                    type: io.kestra.plugin.nats.kv.Delete
                    url: nats://localhost:4222
                    username: nats_user
                    password: nats_passwd
                    bucketName: my_bucket
                     keys:
                      - key1
                      - key2
                """
        ),
    }
)
public class Delete extends NatsConnection implements RunnableTask<VoidOutput> {

     @Schema(
          title = "The name of the key value bucket."
     )
     @NotBlank
     @PluginProperty(dynamic = true)
     private String bucketName;

     @Schema(
          title = "The keys of Key/Value pairs."
     )
     @NotNull
     private Property<List<String>> keys;

     @Override
     public VoidOutput run(RunContext runContext) throws Exception {
          try (Connection connection = super.connect(runContext)) {
               KeyValue keyValue = connection.keyValue(runContext.render(this.bucketName));

               for (String key : runContext.render(this.keys).asList(String.class)) {
                    keyValue.delete(key);
               }

               return new VoidOutput();
          }
     }

}
