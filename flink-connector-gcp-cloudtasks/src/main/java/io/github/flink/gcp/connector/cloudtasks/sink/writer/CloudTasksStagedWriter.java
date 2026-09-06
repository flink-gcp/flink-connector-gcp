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

package io.github.flink.gcp.connector.cloudtasks.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.Preconditions;

import com.google.cloud.tasks.v2.Task;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.cloudtasks.CloudTasksMetricNames;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksCommittable;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksSinkConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.CloudTasksStagingConfig;
import io.github.flink.gcp.connector.cloudtasks.sink.QueueDestination;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;

/**
 * Stages named tasks without owning a client or any Flink writer state. All methods run on the
 * writer operator's task thread. A checkpoint's committable collector becomes the sole recovery
 * owner of the drained list.
 *
 * @param <T> input record type
 */
@Internal
public final class CloudTasksStagedWriter<T>
        implements CommittingSinkWriter<T, CloudTasksCommittable> {
    private final CloudTasksSinkConfig<T> config;
    private final CloudTasksStagingConfig staging;
    private final TimeSource clock;
    private final SecureRandom random;
    private final MessageDigest digest;
    private final Counter recordsSkipped;
    private final Counter errors;
    private List<CloudTasksCommittable> staged = new ArrayList<>();
    private long stagedBytes;
    private boolean closed;

    /** Creates a staging writer using the wall clock and cryptographic random identities. */
    public CloudTasksStagedWriter(
            CloudTasksSinkConfig<T> config,
            CloudTasksStagingConfig staging,
            SinkWriterMetricGroup metrics) {
        this(config, staging, metrics, TimeSource.SYSTEM, new SecureRandom());
    }

    /**
     * Creates a writer against deterministic time and identity collaborators for lifecycle tests.
     */
    public CloudTasksStagedWriter(
            CloudTasksSinkConfig<T> config,
            CloudTasksStagingConfig staging,
            SinkWriterMetricGroup metrics,
            TimeSource clock,
            SecureRandom random) {
        this.config = Preconditions.checkNotNull(config, "config");
        this.staging = Preconditions.checkNotNull(staging, "staging");
        staging.validate();
        Preconditions.checkArgument(
                config.getFailedTaskHandler() == FailureHandler.failJob(),
                "Cloud Tasks staging requires FailureHandler.failJob()");
        this.clock = Preconditions.checkNotNull(clock, "clock");
        this.random = Preconditions.checkNotNull(random, "random");
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the Java platform.", e);
        }
        recordsSkipped = metrics.counter(CloudTasksMetricNames.RECORDS_SKIPPED);
        errors = metrics.getNumRecordsSendErrorsCounter();
        metrics.gauge(CloudTasksMetricNames.STAGED_TASKS, this::getStagedTasks);
        metrics.gauge(CloudTasksMetricNames.STAGED_BYTES, this::getStagedBytes);
    }

    @Override
    public void write(T element, Context context) throws IOException {
        checkOpen();
        final QueueDestination destination;
        try {
            destination = config.getDestinationResolver().resolve(element, context);
        } catch (RuntimeException e) {
            throw new IOException("The Cloud Tasks destination resolver failed for a record.");
        }
        if (destination == null) {
            throw new IOException("The destination resolver returned null for a record.");
        }
        final Task task;
        try {
            task = config.getSerializer().serialize(element);
        } catch (IOException | RuntimeException e) {
            errors.inc();
            // User exceptions may contain bodies, headers or tokens. Do not chain them.
            throw new IOException("Cloud Tasks staging serialization failed for a record.");
        }
        if (task == null) {
            recordsSkipped.inc();
            return;
        }
        if (!task.getName().isEmpty()) {
            throw new IOException(
                    "The serializer returned an already named Cloud Tasks task;"
                            + " task names must be assigned by the sink.");
        }
        String id;
        if (config.getTaskIdExtractor() == null) {
            byte[] identity = new byte[16];
            random.nextBytes(identity);
            id = HexFormat.of().formatHex(identity);
        } else {
            final String key;
            try {
                key = config.getTaskIdExtractor().extractTaskId(element);
            } catch (RuntimeException e) {
                errors.inc();
                throw new IOException("The Cloud Tasks task id extractor failed for a record.");
            }
            if (key == null || key.isEmpty()) {
                throw new IOException(
                        "The Cloud Tasks task id extractor returned "
                                + (key == null ? "null" : "an empty key")
                                + "; every record needs a key when taskIdExtractor is configured.");
            }
            id = HexFormat.of().formatHex(digest.digest(key.getBytes(StandardCharsets.UTF_8)));
        }
        Task named = task.toBuilder().setName(destination.toQueuePath() + "/tasks/" + id).build();
        int wireBytes = named.getSerializedSize();
        if (wireBytes <= 0 || wireBytes > CloudTasksCommittable.MAX_TASK_BYTES) {
            errors.inc();
            throw new IOException(
                    "Cloud Tasks staging serialization failed: named Task wire size "
                            + wireBytes
                            + " exceeds the "
                            + CloudTasksCommittable.MAX_TASK_BYTES
                            + " byte limit.");
        }
        long additionalBytes = (long) wireBytes + CloudTasksCommittable.ACCOUNTING_OVERHEAD_BYTES;
        if (staged.size() >= staging.getMaxStagedTasks()
                || additionalBytes > staging.getMaxStagedBytes() - stagedBytes) {
            throw new IOException(
                    "Cloud Tasks staging capacity exceeded: maxStagedTasks="
                            + staging.getMaxStagedTasks()
                            + ", maxStagedBytes="
                            + staging.getMaxStagedBytes()
                            + ", stagedTasks="
                            + staged.size()
                            + ", stagedBytes="
                            + stagedBytes
                            + ", additionalBytes="
                            + additionalBytes
                            + ". Shorten the checkpoint interval, increase both the cap and heap, or reduce"
                            + " parallelism per host. A four-representation heap probe measured up to"
                            + " 7x accounted bytes; this is not a runtime heap bound (see ADR-0158)."
                            + " These caps bound one writer batch, not the committer collector.");
        }
        long origin = clock.currentTimeMillis();
        final long deadline;
        try {
            deadline = staging.authorizationDeadlineMillis(origin);
        } catch (ArithmeticException e) {
            throw new IOException(
                    "Cloud Tasks staging authorization deadline overflows epoch milliseconds.");
        }
        final CloudTasksCommittable envelope;
        try {
            envelope =
                    CloudTasksCommittable.fromTask(
                            destination.toQueuePath(), origin, deadline, named);
        } catch (IOException e) {
            errors.inc();
            throw new IOException("Cloud Tasks staging serialization failed: " + e.getMessage());
        }
        staged.add(envelope);
        stagedBytes += additionalBytes;
    }

    @Override
    public void flush(boolean endOfInput) throws IOException {
        checkOpen();
        // Both flush paths leave ownership with prepareCommit; neither can reach a service.
    }

    @Override
    public Collection<CloudTasksCommittable> prepareCommit() throws IOException {
        checkOpen();
        List<CloudTasksCommittable> owned = staged;
        staged = new ArrayList<>();
        stagedBytes = 0;
        return Collections.unmodifiableList(owned);
    }

    @Override
    public void close() {
        closed = true;
        staged.clear();
        stagedBytes = 0;
    }

    private void checkOpen() throws IOException {
        if (closed) {
            throw new IOException("Cloud Tasks staged writer is closed.");
        }
    }

    /** Returns the records held by the writer, excluding emitted committables. */
    public int getStagedTasks() {
        return staged.size();
    }

    /** Returns the accounted bytes held by the writer, excluding emitted committables. */
    public long getStagedBytes() {
        return stagedBytes;
    }
}
