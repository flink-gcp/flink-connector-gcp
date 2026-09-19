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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import org.apache.flink.annotation.Internal;

import com.google.api.gax.rpc.ApiException;
import io.grpc.Status;

import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.function.Consumer;

/** Raw per-attempt evidence for external sequence, task-name and attempt reconciliation. */
@Internal
final class ObservationLog implements Consumer<ObservedTaskCreator.Observation> {
    private final MeasurementOptions options;
    private final UUID incarnation = UUID.randomUUID();

    ObservationLog(MeasurementOptions options) {
        this.options = options;
    }

    @Override
    public synchronized void accept(ObservedTaskCreator.Observation event) {
        String status;
        if (event.failure() == null) {
            if (!event.result().getName().startsWith(options.queue + "/tasks/")
                    || (!event.requestedName().isEmpty()
                            && !event.result().getName().equals(event.requestedName()))
                    || event.result().getDispatchCount() != 0) {
                throw new IllegalStateException(
                        "Successful measurement create lacks a name or has dispatched");
            }
            status = "OK";
        } else if (event.failure() instanceof CancellationException) {
            status = "CANCELLED";
        } else if (event.failure() instanceof ApiException) {
            status = ((ApiException) event.failure()).getStatusCode().getCode().name();
        } else {
            status = Status.fromThrowable(event.failure()).getCode().name();
        }
        long latency =
                event.origin().elapsedNanos(MeasurementPayload.process(), event.completedNanos());
        // All free text is fixed vocabulary, a validated label, a UUID or a service task path.
        // Task bodies, target URLs and exception messages never enter evidence logs.
        String name = event.result() == null ? event.requestedName() : event.result().getName();
        System.out.println(
                "CT1246,"
                        + options.runId
                        + ","
                        + options.cellId
                        + ","
                        + options.arm
                        + ","
                        + incarnation
                        + ","
                        + MeasurementPayload.process()
                        + ","
                        + event.origin().process()
                        + ","
                        + event.origin().sequence()
                        + ","
                        + event.attempt()
                        + ","
                        + event.origin().millis()
                        + ","
                        + event.origin().nanos()
                        + ","
                        + event.startedNanos()
                        + ","
                        + event.completedNanos()
                        + ","
                        + latency
                        + ","
                        + status
                        + ","
                        + name);
        if (System.out.checkError()) {
            throw new IllegalStateException("Measurement evidence output failed");
        }
    }
}
