package io.kestra.plugin.nats;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.IOException;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class NatsConnection extends Task implements NatsConnectionInterface {
    protected String url;

    protected String username;

    protected String password;

    protected Connection connect(RunContext runContext) throws IOException, InterruptedException, IllegalVariableEvaluationException {
        Options.Builder connectOptions = Options.builder().server(runContext.render(url));
        if (username != null) {
            connectOptions.userInfo(runContext.render(username), runContext.render(password));
        }

        return Nats.connect(connectOptions.build());
    }
}
