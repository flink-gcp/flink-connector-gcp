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

package io.github.flink.gcp.connector.testutils.lineage;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.execution.JobStatusChangedListener;
import org.apache.flink.core.execution.JobStatusChangedListenerFactory;
import org.apache.flink.streaming.runtime.execution.JobCreatedEvent;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Captures a real Flink 2.x job-created event through the configured listener factory. Each capture
 * owns one registry entry, removed on close; tests never share one global last event. This fixture
 * contains no connector metadata implementation and has no dependency on base.
 */
@Internal
public final class LineageListenerCapture implements AutoCloseable {
    private static final String CAPTURE_ID = "flink.gcp.tests.lineage.capture-id";
    private static final ConcurrentMap<String, CompletableFuture<JobCreatedEvent>> CAPTURES =
            new ConcurrentHashMap<>();

    private final String id = UUID.randomUUID().toString();
    private final CompletableFuture<JobCreatedEvent> created = new CompletableFuture<>();

    /** Registers a capture for a single job submission. */
    public LineageListenerCapture() {
        CAPTURES.put(id, created);
    }

    /** Returns an independent configuration selecting this capture's listener factory. */
    public Configuration configuration() {
        Configuration configuration = new Configuration();
        configuration.setString(CAPTURE_ID, id);
        configuration.set(
                DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS, List.of(Factory.class.getName()));
        return configuration;
    }

    /** Awaits listener delivery with a bounded diagnostic timeout. */
    public JobCreatedEvent awaitCreated() throws Exception {
        return created.get(30, TimeUnit.SECONDS);
    }

    @Override
    public void close() {
        CAPTURES.remove(id);
    }

    /** Instantiated by Flink from the listener configuration, not called directly by tests. */
    @Internal
    public static final class Factory implements JobStatusChangedListenerFactory {
        /** Creates the factory through Flink's reflection-based listener loading. */
        public Factory() {}

        @Override
        public JobStatusChangedListener createListener(Context context) {
            String captureId = context.getConfiguration().getString(CAPTURE_ID, "");
            return event -> {
                CompletableFuture<JobCreatedEvent> capture = CAPTURES.get(captureId);
                if (capture != null && event instanceof JobCreatedEvent) {
                    capture.complete((JobCreatedEvent) event);
                }
            };
        }
    }
}
