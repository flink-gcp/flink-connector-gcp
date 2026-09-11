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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;

import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;
import io.github.flink.gcp.connector.bigtable.BigtableMetricNames;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.BigtableSinkConfig;
import io.github.flink.gcp.connector.bigtable.sink.BigtableStagedOptions;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.BigtableCommittable;

import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Stages immutable requests without opening a service client or waiting for downstream capacity.
 */
@Internal
public final class BigtableStagedWriter<T> implements CommittingSinkWriter<T, BigtableCommittable> {
    private final BigtableSinkConfig<T> config;
    private final BigtableStagedOptions options;
    private final SecureRandom random = new SecureRandom();
    private final List<BigtableCommittable> pending = new ArrayList<>();
    private final Counter skipped;
    private volatile long stagedBytes;
    private volatile int stagedEntries;
    private boolean closed;

    /** Creates the writer after its serializer has been initialized by the sink. */
    public BigtableStagedWriter(
            BigtableSinkConfig<T> config,
            BigtableStagedOptions options,
            SinkWriterMetricGroup metrics) {
        this.config = config;
        this.options = options;
        options.validate();
        skipped = metrics.counter(BigtableMetricNames.RECORDS_SKIPPED);
        metrics.gauge(BigtableMetricNames.STAGED_ENTRIES, () -> stagedEntries);
        metrics.gauge(BigtableMetricNames.STAGED_BYTES, () -> stagedBytes);
    }

    @Override
    public void write(T element, Context context) throws IOException {
        if (closed) {
            throw new IOException("Bigtable staging writer is closed");
        }
        TableDestination destination = config.getDestinationResolver().resolve(element, context);
        if (destination == null) {
            throw new IOException("Bigtable destination resolver returned null");
        }
        RowMutationEntry entry = config.getSerializer().serialize(element, context);
        if (entry == null) {
            skipped.inc();
            return;
        }
        BigtableCommittable value =
                BigtableCommittable.stage(
                        destination,
                        config.getAppProfileId(),
                        options.getMarkerFamily(),
                        entry.toProto(),
                        random);
        if (pending.size() >= options.getMaxStagedEntries()
                || value.getStagedBytes() > options.getMaxStagedBytes() - stagedBytes) {
            throw new IOException(
                    "Bigtable staging capacity exceeded: maxStagedEntries="
                            + options.getMaxStagedEntries()
                            + ", maxStagedBytes="
                            + options.getMaxStagedBytes()
                            + ", retainedEntries="
                            + pending.size()
                            + ", retainedBytes="
                            + stagedBytes
                            + ". Size the checkpoint interval and pending committable heap for this workload.");
        }
        pending.add(value);
        stagedEntries = pending.size();
        stagedBytes += value.getStagedBytes();
    }

    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public Collection<BigtableCommittable> prepareCommit() {
        List<BigtableCommittable> result = new ArrayList<>(pending);
        pending.clear();
        stagedEntries = 0;
        stagedBytes = 0;
        return result;
    }

    @Override
    public void close() {
        closed = true;
        pending.clear();
        stagedEntries = 0;
        stagedBytes = 0;
    }
}
