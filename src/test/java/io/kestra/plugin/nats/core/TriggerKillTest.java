package io.kestra.plugin.nats.core;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

import io.kestra.core.junit.annotations.KestraTest;
import io.kestra.core.models.conditions.ConditionContext;
import io.kestra.core.models.executions.Execution;
import io.kestra.core.models.property.Property;
import io.kestra.core.runners.RunContext;
import io.kestra.core.runners.RunContextFactory;
import io.kestra.core.utils.IdUtils;

import jakarta.inject.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

// Kept separate from TriggerTest: that class's @BeforeEach unconditionally publishes a message to
// the shared "kestra.trigger" subject/durable before every test method, which would pollute the
// durable consumer's backlog for these kill tests' unrelated, per-test-unique subjects and vice
// versa for TriggerTest's own messagesCount assertion.
@KestraTest
class TriggerKillTest {
    @Inject
    private RunContextFactory runContextFactory;

    @Test
    void shouldUnblockInFlightEvaluateOnKill() throws Exception {
        // Empty, never-published-to subject: the underlying Consume's first fetch() has nothing to
        // return and blocks for the full pollDuration unless kill() closes the connection to unblock it.
        String subject = "kestra.trigger.kill." + IdUtils.create();

        Trigger trigger = Trigger.builder()
            .id(TriggerKillTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .durableId(Property.ofValue("natsTriggerKill-" + IdUtils.create()))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        RunContext runContext = runContextFactory.of();
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();

        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Optional<Execution>> result = new AtomicReference<>();
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        Thread runner = new Thread(() -> {
            try {
                result.set(trigger.evaluate(conditionContext, null));
            } catch (Throwable t) {
                thrown.set(t);
            } finally {
                completed.countDown();
            }
        });
        runner.start();

        // Give evaluate() time to build its Consume task, connect, subscribe, and enter the blocking fetch().
        Thread.sleep(Duration.ofSeconds(3).toMillis());

        long killStart = System.currentTimeMillis();
        trigger.kill();
        long killElapsedMs = System.currentTimeMillis() - killStart;

        assertThat("Trigger.kill() must not block for the full pollDuration", killElapsedMs, lessThan(15000L));
        assertThat("evaluate() must return promptly after kill()", completed.await(15, TimeUnit.SECONDS), is(true));
        assertThat("A killed evaluate() must not fail the run", thrown.get(), nullValue());
        assertThat("A killed evaluate() must not fire an execution", result.get().isPresent(), is(false));
    }

    @Test
    void shouldSkipEvaluationWhenKillArrivesBeforeCycleStarts() throws Exception {
        // Reproduces the gap between cycles where activeConsumeTask is null (e.g. before the very
        // first evaluate() call): without the sticky killedOrStopped flag, kill() finds no task to
        // forward to and is silently dropped, letting the next evaluate() run a full poll cycle.
        String subject = "kestra.trigger.kill.early." + IdUtils.create();

        Trigger trigger = Trigger.builder()
            .id(TriggerKillTest.class.getSimpleName())
            .type(Trigger.class.getName())
            .url("localhost:4222")
            .username(Property.ofValue("kestra"))
            .password(Property.ofValue("k3stra"))
            .subject(subject)
            .durableId(Property.ofValue("natsTriggerKillEarly-" + IdUtils.create()))
            .pollDuration(Property.ofValue(Duration.ofSeconds(30)))
            .build();

        // No evaluation has run yet, so activeConsumeTask is still null: this is the exact window
        // the sticky killedOrStopped flag must cover.
        trigger.kill();

        RunContext runContext = runContextFactory.of();
        ConditionContext conditionContext = ConditionContext.builder()
            .runContext(runContext)
            .build();

        long start = System.currentTimeMillis();
        Optional<Execution> result = trigger.evaluate(conditionContext, null);
        long elapsedMs = System.currentTimeMillis() - start;

        assertThat("A pre-killed trigger must not fire an execution", result.isPresent(), is(false));
        assertThat("evaluate() must skip the poll cycle instead of blocking on a fresh Consume",
            elapsedMs, lessThan(15000L));
    }
}
