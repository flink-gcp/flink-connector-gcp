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

import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.Timestamp;
import io.grpc.Status;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/** Calibrates the complete recovery experiment locally; this model is not service evidence. */
class CloudTasksRecoveryHarnessITCase extends StagedRecoveryAcceptance {
    private final Map<String, Task> live = new ConcurrentHashMap<>();

    @Override
    RecordingTaskProxy.Service service() {
        return (request, deadline) -> {
            Task task =
                    request.getTask().toBuilder()
                            .setCreateTime(
                                    Timestamp.newBuilder()
                                            .setSeconds(System.currentTimeMillis() / 1000))
                            .build();
            if (live.putIfAbsent(task.getName(), task) != null) {
                throw Status.ALREADY_EXISTS.asRuntimeException();
            }
            return task;
        };
    }

    @Override
    String queue(boolean appEngine) {
        return "projects/p/locations/l/queues/" + (appEngine ? "ae" : "http");
    }

    @Override
    void observe(Task task) {
        assertThat(live.get(task.getName())).isEqualTo(task);
    }

    @Override
    AcceptanceEvidence openEvidence() throws Exception {
        return new AcceptanceEvidence(temporary.resolve("evidence.jsonl"));
    }

    @Override
    void closeEvidence() throws Exception {
        if (evidence != null) {
            evidence.close();
        }
    }
}
