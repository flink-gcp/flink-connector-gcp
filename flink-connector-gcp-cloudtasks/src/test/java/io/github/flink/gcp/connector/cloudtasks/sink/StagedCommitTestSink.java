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
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A service-free Sink V2 probe for ADR-0158. This deliberately has no Cloud Tasks implementation:
 * strings are checkpoint payloads, and the shared probe observes Flink's real operators. Like the
 * staged sink the ADR defines, the writer holds no Flink state; every staged payload leaves it
 * through {@code prepareCommit()}.
 */
final class StagedCommitTestSink implements CrossVersionSink<String>, SupportsCommitter<String> {

    private static final long serialVersionUID = 1L;
    static final Map<String, Probe> PROBES = new ConcurrentHashMap<>();

    private final String runId;

    StagedCommitTestSink(String runId) {
        this.runId = runId;
    }

    enum Outcome {
        SUCCESS,
        RETRY
    }

    static final class Probe {
        final List<String> events = new CopyOnWriteArrayList<>();
        final List<String> created = new CopyOnWriteArrayList<>();
        final List<String> restored = new CopyOnWriteArrayList<>();
        final List<Integer> retries = new CopyOnWriteArrayList<>();
        final List<Thread> callbackThreads = new CopyOnWriteArrayList<>();
        volatile Outcome outcome = Outcome.SUCCESS;
    }

    @Override
    public CommittingSinkWriter<String, String> createWriter(WriterInitContext context) {
        return new Writer(PROBES.get(runId));
    }

    @Override
    public Committer<String> createCommitter(CommitterInitContext context) {
        Probe probe = PROBES.get(runId);
        boolean restored = context.getRestoredCheckpointId().isPresent();
        return new Committer<>() {
            @Override
            public void commit(Collection<CommitRequest<String>> requests) {
                probe.callbackThreads.add(Thread.currentThread());
                probe.events.add("commit");
                for (CommitRequest<String> request : requests) {
                    String envelope = request.getCommittable();
                    if (restored) {
                        probe.restored.add(envelope);
                    }
                }
                for (CommitRequest<String> request : requests) {
                    probe.retries.add(request.getNumberOfRetries());
                    if (probe.outcome == Outcome.RETRY) {
                        request.retryLater();
                    } else {
                        probe.created.add(request.getCommittable());
                    }
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public SimpleVersionedSerializer<String> getCommittableSerializer() {
        return new StringSerializer();
    }

    private static final class StringSerializer implements SimpleVersionedSerializer<String> {
        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(String value) {
            return value.getBytes(StandardCharsets.UTF_8);
        }

        @Override
        public String deserialize(int version, byte[] bytes) throws IOException {
            if (version != 1) {
                throw new IOException("Unsupported probe state version: " + version);
            }
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static final class Writer implements CommittingSinkWriter<String, String> {
        private final Probe probe;
        private final List<String> pending = new ArrayList<>();

        private Writer(Probe probe) {
            this.probe = probe;
        }

        @Override
        public void write(String element, Context context) {
            pending.add(element);
        }

        @Override
        public void flush(boolean endOfInput) {
            probe.events.add("flush:" + endOfInput);
        }

        @Override
        public Collection<String> prepareCommit() {
            probe.events.add("prepare");
            List<String> result = List.copyOf(pending);
            pending.clear();
            return result;
        }

        @Override
        public void close() {}
    }
}
