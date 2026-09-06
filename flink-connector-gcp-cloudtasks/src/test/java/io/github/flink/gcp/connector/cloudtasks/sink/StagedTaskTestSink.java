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

package io.github.flink.gcp.connector.cloudtasks.sink;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.WriterInitContext;

import com.google.cloud.tasks.v2.HttpMethod;
import com.google.cloud.tasks.v2.HttpRequest;
import com.google.cloud.tasks.v2.Task;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.CloudTasksStagedWriter;
import io.github.flink.gcp.connector.cloudtasks.sink.writer.TimeSource;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Production writer and envelope with a recording-only committer, never a service implementation.
 */
final class StagedTaskTestSink extends CloudTasksStagedCreateTaskSink<String> {
    private static final long serialVersionUID = 1L;
    static final Map<String, Probe> PROBES = new ConcurrentHashMap<>();
    private final String runId;

    StagedTaskTestSink(String runId) {
        super(config(runId), new CloudTasksStagingConfig());
        this.runId = runId;
    }

    private static CloudTasksSinkConfig<String> config(String runId) {
        return ((CloudTasksCreateTaskSink<String>)
                        CloudTasksSink.<String>builder()
                                .queue(QueueDestination.of("p", "l", "q"))
                                // Production createWriter tests configure an endpoint, even though
                                // staging opens no client.
                                .emulatorEndpoint("localhost:1")
                                .serializer(
                                        element -> {
                                            PROBES.get(runId).serialized++;
                                            return Task.newBuilder()
                                                    .setHttpRequest(
                                                            HttpRequest.newBuilder()
                                                                    .setUrl(
                                                                            "https://example.com/task")
                                                                    .setHttpMethod(HttpMethod.POST)
                                                                    .putHeaders(
                                                                            "Authorization",
                                                                            "private-header")
                                                                    .setBody(
                                                                            ByteString.copyFromUtf8(
                                                                                    element)))
                                                    .build();
                                        })
                                .build())
                .getConfig();
    }

    static final class Probe {
        final List<CloudTasksCommittable> committed = new ArrayList<>();
        long clockMillis = 1000;
        int serialized;
        long identities;
    }

    @Override
    protected CloudTasksStagedWriter<String> createStagedWriter(
            CloudTasksSinkConfig<String> config,
            CloudTasksStagingConfig staging,
            WriterInitContext context) {
        Probe probe = PROBES.get(runId);
        return new CloudTasksStagedWriter<>(
                config,
                staging,
                context.metricGroup(),
                new TimeSource() {
                    @Override
                    public long currentTimeMillis() {
                        return probe.clockMillis;
                    }

                    @Override
                    public void sleep(long millis) {
                        throw new AssertionError("Staging must never wait");
                    }
                },
                new SecureRandom() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void nextBytes(byte[] bytes) {
                        ByteBuffer.wrap(bytes).putLong(0).putLong(++probe.identities);
                    }
                });
    }

    @Override
    public Committer<CloudTasksCommittable> createCommitter(CommitterInitContext context) {
        return new Committer<>() {
            @Override
            public void commit(Collection<CommitRequest<CloudTasksCommittable>> requests) {
                for (var request : requests) {
                    PROBES.get(runId).committed.add(request.getCommittable());
                }
            }

            @Override
            public void close() {}
        };
    }
}
