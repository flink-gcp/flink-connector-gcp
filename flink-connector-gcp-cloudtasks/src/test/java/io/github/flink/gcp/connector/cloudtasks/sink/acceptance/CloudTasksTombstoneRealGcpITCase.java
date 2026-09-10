/*
 * Copyright 2026 The flink-gcp authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.flink.gcp.connector.cloudtasks.sink.acceptance;

import com.google.cloud.tasks.v2.CreateTaskRequest;
import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.DefaultTaskCreatorFactory;
import io.grpc.Deadline;
import io.grpc.Status;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Real elapsed retention, with explicit positive and falsifying post-expiry creation controls. */
@Tag("gated")
@Tag("slow")
@EnabledIfEnvironmentVariable(named = "CLOUDTASKS_RECOVERY_ACCEPTANCE", matches = "approved")
@Timeout(value = 3, unit = TimeUnit.HOURS)
class CloudTasksTombstoneRealGcpITCase {
    @Test
    void observeBothTargetsAcrossRemovalAndActualTombstoneExpiry() throws Exception {
        try (var run = new CloudTasksAcceptanceRun("slow")) {
            run.provision();
            List<Control> controls = new ArrayList<>();
            for (boolean exhaustion : new boolean[] {false, true}) {
                for (boolean appEngine : new boolean[] {false, true}) {
                    if (exhaustion) {
                        run.configureExhaustion(run.queue(appEngine));
                    } else {
                        run.verifyRetries(run.queue(appEngine), false);
                    }
                }
                for (boolean appEngine : new boolean[] {false, true}) {
                    for (String removal :
                            exhaustion
                                    ? List.of("retry-exhaustion")
                                    : List.of("delete", "execute")) {
                        for (boolean old : new boolean[] {false, true}) {
                            String id = UUID.randomUUID().toString().replace("-", "");
                            String uri =
                                    removal.equals("retry-exhaustion")
                                            ? "/unavailable"
                                            : "/accepted";
                            Task.Builder task =
                                    StagedRecoveryAcceptance.task(
                                                    id,
                                                    appEngine,
                                                    System.currentTimeMillis() / 1000)
                                            .toBuilder()
                                            .setName(run.queue(appEngine) + "/tasks/" + id);
                            if (appEngine) {
                                task.getAppEngineHttpRequestBuilder().setRelativeUri(uri);
                            } else {
                                task.getHttpRequestBuilder()
                                        .setUrl(run.value("fixture.http-origin") + uri);
                            }
                            CreateTaskRequest request =
                                    CreateTaskRequest.newBuilder()
                                            .setParent(run.queue(appEngine))
                                            .setTask(task)
                                            .build();
                            Control control = new Control(request, appEngine, old, removal);
                            controls.add(control);
                            run.evidence.record(
                                    "control-start",
                                    Map.of(
                                            "name",
                                            task.getName(),
                                            "target",
                                            appEngine ? "ae" : "http",
                                            "removal",
                                            removal,
                                            "oldIncarnation",
                                            old));
                            Task created = create(run, control, false);
                            run.observe(created);
                            assertThat(created.getDispatchCount()).isZero();
                            if (removal.equals("delete")) {
                                run.delete(created.getName());
                                assertThat(run.get(created.getName())).isNull();
                                removed(run, control);
                            }
                        }
                    }
                }
                for (boolean appEngine : new boolean[] {false, true}) {
                    run.resume(run.queue(appEngine));
                }
                long removalLimit = System.nanoTime() + TimeUnit.MINUTES.toNanos(5);
                while (controls.stream().anyMatch(control -> control.removedMillis == 0)) {
                    if (System.nanoTime() >= removalLimit) {
                        throw new AssertionError(
                                "Removal did not complete; preserve partial evidence, no support verdict");
                    }
                    for (Control control : controls) {
                        if (control.removedMillis == 0) {
                            Task task = run.get(control.request.getTask().getName());
                            if (task == null) {
                                if (control.removal.equals("retry-exhaustion")) {
                                    assertThat(control.failedAttemptObserved)
                                            .as(
                                                    "a failed dispatch must precede retry-exhaustion removal")
                                            .isTrue();
                                }
                                removed(run, control);
                            } else if (task.hasLastAttempt()
                                    && task.getLastAttempt().hasResponseStatus()
                                    && task.getLastAttempt().getResponseStatus().getCode() != 0) {
                                control.failedAttemptObserved = true;
                            }
                        }
                    }
                    Thread.sleep(1_000);
                }
                for (boolean appEngine : new boolean[] {false, true}) {
                    run.pause(run.queue(appEngine));
                }
            }
            // Unlimited retries in the first phase exclude exhaustion as an execution-removal
            // cause.
            // Only retry_config changes between phases; retention stays fixed throughout.
            for (Control control : controls) {
                collision(run, control, "immediate-positive");
            }
            long positiveAt =
                    controls.stream()
                                    .mapToLong(control -> control.removedMillis)
                                    .min()
                                    .orElseThrow()
                            + Duration.ofMinutes(50).toMillis();
            waitUntil(run, positiveAt);
            for (Control control : controls) {
                collision(run, control, "retention-positive");
                run.ledger.expectRecreation(control.request.getTask().getName());
            }
            long earliestNegative =
                    controls.stream()
                                    .mapToLong(control -> control.removedMillis)
                                    .max()
                                    .orElseThrow()
                            + Duration.ofHours(1).toMillis()
                            + Duration.ofSeconds(5).toMillis();
            waitUntil(run, earliestNegative);
            long negativeLimit =
                    Math.min(
                            System.currentTimeMillis() + Duration.ofHours(1).toMillis(),
                            System.currentTimeMillis()
                                    + run.remainingMillis()
                                    - Duration.ofMinutes(30).toMillis());
            while (controls.stream().anyMatch(control -> !control.recreated)) {
                if (System.currentTimeMillis() >= negativeLimit) {
                    throw new AssertionError(
                            "Actual tombstone expiry not observed within the registered budget");
                }
                for (Control control : controls) {
                    if (control.recreated) {
                        continue;
                    }
                    try {
                        // A deliberate fresh RPC after expiry calibrates the oracle. It is not a
                        // production retry and does not reuse/extend an old absolute RPC deadline.
                        Task recreated = create(run, control, true);
                        run.observe(recreated);
                        run.ledger.assertRecreatedAndObserved(recreated.getName());
                        control.recreated = true;
                        run.evidence.record(
                                "control-pass",
                                Map.of("name", recreated.getName(), "oldIncarnation", control.old));
                    } catch (Exception error) {
                        if (Status.fromThrowable(error).getCode() != Status.Code.ALREADY_EXISTS) {
                            throw error;
                        }
                        run.evidence.record(
                                "negative-still-protected",
                                Map.of("name", control.request.getTask().getName()));
                    }
                }
                if (controls.stream().anyMatch(control -> !control.recreated)) {
                    Thread.sleep(30_000);
                }
            }
            for (boolean appEngine : new boolean[] {false, true}) {
                run.verifyPaused(run.queue(appEngine));
            }
            run.evidence.record("tombstone-pass", Map.of("controls", controls.size()));
        }
    }

    private static Task create(CloudTasksAcceptanceRun run, Control control, boolean negative)
            throws Exception {
        if (!control.old) {
            return run.create(control.request, Deadline.after(20, TimeUnit.SECONDS));
        }
        try (var proxy = new RecordingTaskProxy(run, run.evidence);
                var creator =
                        new DefaultTaskCreatorFactory(
                                        null,
                                        EmulatorEndpoint.parse(
                                                proxy.endpoint(), "acceptance proxy"),
                                        null)
                                .create()) {
            proxy.inject(RecordingTaskProxy.Fault.HOLD_RESPONSE, 1);
            var lost = creator.createTask(control.request, Deadline.after(20, TimeUnit.SECONDS));
            try {
                long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
                while (proxy.responses().isEmpty() && !lost.isDone() && System.nanoTime() < end) {
                    Thread.sleep(20);
                }
                if (proxy.responses().isEmpty()) {
                    try {
                        lost.get(1, TimeUnit.SECONDS);
                    } catch (java.util.concurrent.ExecutionException error) {
                        if (error.getCause() instanceof Exception) {
                            throw (Exception) error.getCause();
                        }
                        throw error;
                    }
                    throw new AssertionError("No recorded old-incarnation creation response");
                }
                Task created = proxy.responses().get(0);
                run.evidence.record(
                        "old-client-response-unavailable",
                        Map.of("name", created.getName(), "negativeControl", negative));
                assertThat(lost.isDone()).isFalse();
                return created;
            } finally {
                lost.cancel(true);
            }
        }
    }

    private static void collision(CloudTasksAcceptanceRun run, Control control, String stage)
            throws Exception {
        try {
            run.create(control.request, Deadline.after(20, TimeUnit.SECONDS));
            throw new AssertionError("Re-created inside the registered retention window");
        } catch (Exception error) {
            if (Status.fromThrowable(error).getCode() != Status.Code.ALREADY_EXISTS) {
                throw error;
            }
            run.evidence.record(
                    stage,
                    Map.of(
                            "name",
                            control.request.getTask().getName(),
                            "status",
                            "ALREADY_EXISTS"));
        }
    }

    private static void removed(CloudTasksAcceptanceRun run, Control control) {
        control.removedMillis = System.currentTimeMillis();
        run.ledger.removed(control.request.getTask().getName());
        run.evidence.record(
                "removal-observed",
                Map.of(
                        "name",
                        control.request.getTask().getName(),
                        "removal",
                        control.removal,
                        "observationMillis",
                        control.removedMillis));
    }

    private static void waitUntil(CloudTasksAcceptanceRun run, long epochMillis)
            throws InterruptedException {
        while (System.currentTimeMillis() < epochMillis) {
            if (run.remainingMillis() < Duration.ofMinutes(30).toMillis()) {
                throw new AssertionError(
                        "Insufficient lifetime for retention observation and cleanup");
            }
            Thread.sleep(Math.min(30_000, epochMillis - System.currentTimeMillis()));
        }
    }

    private static final class Control {
        final CreateTaskRequest request;
        final boolean appEngine;
        final boolean old;
        final String removal;
        long removedMillis;
        boolean recreated;
        boolean failedAttemptObserved;

        Control(CreateTaskRequest request, boolean appEngine, boolean old, String removal) {
            this.request = request;
            this.appEngine = appEngine;
            this.old = old;
            this.removal = removal;
        }
    }
}
