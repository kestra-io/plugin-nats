package io.kestra.plugin.nats;

import io.kestra.core.models.annotations.Example;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.runners.RunContext;
import io.kestra.core.serializers.FileSerde;
import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.impl.Headers;
import io.nats.client.impl.NatsMessage;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;

import java.io.*;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.*;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Produce messages to a NATS subject on a NATS server"
)
@Plugin(
    examples = {
        @Example(
            title = "Produce a single message to kestra.publish subject, using user password authentication",
            code = {
                "url: nats://localhost:4222",
                "username: nats_user",
                "password: nats_passwd",
                "subject: kestra.publish",
                "from:",
                "  headers:",
                "    someHeaderKey: someHeaderValue",
                "  data: Some message"
            }
        ),
        @Example(
            title = "Produce 2 messages to kestra.publish subject, using user password authentication",
            code = {
                "url: nats://localhost:4222",
                "username: nats_user",
                "password: nats_passwd",
                "subject: kestra.publish",
                "from:",
                "  - headers:",
                "      someHeaderKey: someHeaderValue",
                "    data: Some message",
                "  - data: Another message",
            }
        ),
        @Example(
            title = "Produce messages (1 / row) from an internal storage file to kestra.publish subject, using user password authentication",
            code = {
                "url: nats://localhost:4222",
                "username: nats_user",
                "password: nats_passwd",
                "subject: kestra.publish",
                "from: {{outputs.someTaskWithOutputFile.uri}}"
            }
        ),
    }
)
public class Produce extends NatsConnection implements RunnableTask<Produce.Output>, ProduceInterface {
    private String subject;
    private Object from;

    public Output run(RunContext runContext) throws Exception {
        Options.Builder connectOptionsBuilder = Options.builder();
        if (username != null) {
            connectOptionsBuilder.userInfo(runContext.render(username), runContext.render(password));
        }
        Connection connection = Nats.connect(connectOptionsBuilder.build());

        int messagesCount;
        String renderedSubject = runContext.render(subject);

        if (this.from instanceof String || this.from instanceof List) {
            if (this.from instanceof String) {
                URI from = new URI(runContext.render((String) this.from));
                try (BufferedReader inputStream = new BufferedReader(new InputStreamReader(runContext.uriToInputStream(from)))) {
                    messagesCount = publish(runContext, connection, renderedSubject, Flowable.create(FileSerde.reader(inputStream), BackpressureStrategy.BUFFER));
                }
            } else {
                messagesCount = publish(runContext, connection, renderedSubject,  Flowable.fromArray(((List<?>) this.from).toArray()));
            }

        }else {
            connection.publish(this.producerMessage(renderedSubject, runContext.render((Map<String, Object>) this.from)));
            messagesCount = 1;
        }

        connection.close();

        return Output.builder()
            .messagesCount(messagesCount)
            .build();
    }

    private Integer publish(RunContext runContext, Connection connection, String renderedSubject, Flowable<Object> messagesFlowable) {
        return messagesFlowable.map(object -> {
                connection.publish(this.producerMessage(renderedSubject, runContext.render((Map<String, Object>) object)));
                return 1;
            }).reduce(Integer::sum)
            .blockingGet();
    }

    private Message producerMessage(String subject, Map<String, Object> message) {
        Headers headers = new Headers();
        ((Map<String, Object>) message.getOrDefault("headers", Collections.emptyMap())).entrySet().forEach(entry -> {
            String headerKey = entry.getKey();
            Object headerValue = entry.getValue();
            if(headerValue instanceof Collection<?> headerValues){
                headers.add(headerKey, (Collection<String>) headerValues);
            }else {
                headers.add(headerKey, new String[]{(String) headerValue});
            }
        });

        return NatsMessage.builder()
            .subject(subject)
            .headers(headers)
            .data((String) message.get("data"))
            .build();
    }

    @Builder
    @Getter
    public static class Output implements io.kestra.core.models.tasks.Output {
        @io.swagger.v3.oas.annotations.media.Schema(
            title = "Number of messages produced"
        )
        private final Integer messagesCount;
    }
}
