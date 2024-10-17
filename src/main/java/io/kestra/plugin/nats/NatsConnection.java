package io.kestra.plugin.nats;

import io.kestra.core.exceptions.IllegalVariableEvaluationException;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;
import io.nats.client.AuthHandler;
import io.nats.client.Connection;
import io.nats.client.Nats;
import io.nats.client.Options;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public abstract class NatsConnection extends Task implements NatsConnectionInterface {
    protected String url;

    protected String username;

    protected String password;

    protected String token;

    protected String creds;

    protected Connection connect(RunContext runContext) throws IOException, InterruptedException, IllegalVariableEvaluationException {
        Options.Builder connectOptions = Options.builder().server(runContext.render(url));
        if (username != null && password != null) {
            connectOptions.userInfo(runContext.render(username), runContext.render(password));
        }

        if (token != null) {
            connectOptions.token(runContext.render(token).toCharArray());
        }

        if (this.creds != null) {
            File credsFiles = runContext.workingDir()
                .createTempFile(runContext.render(this.creds).getBytes(StandardCharsets.UTF_8), "creds")
                .toFile();

            AuthHandler ah = Nats.credentials(credsFiles.getAbsolutePath());
            connectOptions.authHandler(ah);
        }

        return Nats.connect(connectOptions.build());
    }
}
