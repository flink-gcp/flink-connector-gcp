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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.Struct;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceConfig;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlan;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStream;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Iterator;

/**
 * The bounded read Flink's full lookup cache loads the table through: the bounded scan's plan and
 * partition reads, run start to finish by one input split.
 *
 * <p>Each load is one batch read. Opening the split begins a batch read-only transaction at the
 * scan's timestamp bound, so every partition of one load reads the same snapshot and the next load
 * reads a newer one. The partitions are read one after another on the loading thread through one
 * reader client, and {@link #close()} releases the transaction's session through the planner, the
 * same order the bounded scan's enumerator and readers follow. A partition that fails fails the
 * load: Flink keeps the cache it had and fails the lookups that follow.
 *
 * <p>One split rather than one per partition, because Flink clones this format for every split of
 * every load and the clones share nothing: each would build its own client and could not release
 * the session the others still read through.
 *
 * <p>No client metrics are exported to Flink: Flink's full cache configures and opens its copy of
 * this format with no runtime context, so no metric group reaches it (Flink 2.2.1, {@code
 * InputFormatCacheLoader}). The Cloud Monitoring choice still reaches the clients the scan
 * configuration builds.
 */
@Internal
final class SpannerFullCacheInputFormat extends GenericInputFormat<RowData> {

    private static final long serialVersionUID = 1L;

    private final SpannerSourceConfig<RowData> config;

    @Nullable private transient PartitionPlanner planner;
    @Nullable private transient StructStreamOpener opener;
    @Nullable private transient PartitionPlan plan;
    @Nullable private transient StructStream stream;
    private transient Iterator<Partition> remaining = Collections.emptyIterator();
    private transient ArrayDeque<RowData> pending = new ArrayDeque<>();

    SpannerFullCacheInputFormat(SpannerSourceConfig<RowData> config) {
        this.config = Preconditions.checkNotNull(config, "config must not be null");
    }

    /** The scan configuration each load reads with, for the module's tests. */
    @VisibleForTesting
    SpannerSourceConfig<RowData> getConfig() {
        return config;
    }

    /**
     * Returns the one split a load reads, whatever count is asked for. Flink's loader asks for one,
     * and a second split would plan and read the whole table again.
     */
    @Override
    public GenericInputSplit[] createInputSplits(int numSplits) {
        return new GenericInputSplit[] {new GenericInputSplit(0, 1)};
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);
        pending = new ArrayDeque<>();
        remaining = Collections.emptyIterator();
        try {
            GoogleCredentials credentials =
                    SpannerCredentials.load(config.getServiceAccountKeyFile());
            planner = config.getPlannerFactory().create();
            planner.useCredentials(credentials);
            planner.useClientMetrics(null);
            opener = config.getOpener();
            opener.useCredentials(credentials);
            opener.useClientMetrics(null);
            plan =
                    planner.plan(
                            config.getReadOperation(),
                            config.getTimestampBound(),
                            config.getPartitionOptions(),
                            config.isDataBoostEnabled(),
                            config.getRpcPriority());
            remaining = plan.getPartitions().iterator();
        } catch (Throwable e) {
            // Flink's loader also closes the format after a failed open; closing here keeps this
            // method from relying on that, and close() is idempotent because it clears every
            // field before releasing what they held.
            Closers.closeAllSuppressing(e, this::close);
            throw e;
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        while (pending.isEmpty()) {
            Struct row = stream == null ? null : stream.next();
            if (row != null) {
                config.getDeserializer().deserialize(row, new PendingRows(pending));
                continue;
            }
            closeStream();
            if (!remaining.hasNext()) {
                return true;
            }
            stream = opener.open(plan.getBatchTransactionId(), remaining.next());
        }
        return false;
    }

    @Override
    public RowData nextRecord(RowData reuse) throws IOException {
        return reachedEnd() ? null : pending.poll();
    }

    @Override
    public void close() throws IOException {
        StructStream openStream = stream;
        StructStreamOpener openOpener = opener;
        PartitionPlanner openPlanner = planner;
        stream = null;
        opener = null;
        planner = null;
        plan = null;
        remaining = Collections.emptyIterator();
        pending = new ArrayDeque<>();
        try {
            // The stream before its client, and the planner last: closing the planner releases
            // the session every partition of this load reads through.
            Closers.closeAll(openStream, openOpener, openPlanner);
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            // An IOException whatever the cause: Flink's loader logs one from close() and keeps
            // the rows it read, so a release failure after a complete read does not discard the
            // load, as an unchecked exception escaping its finally block would.
            throw new IOException("Failed to release the Spanner full-cache load.", e);
        }
    }

    private void closeStream() {
        StructStream openStream = stream;
        stream = null;
        if (openStream != null) {
            openStream.close();
        }
    }

    /** Collects what the deserializer emits for one row into the pending queue. */
    private static final class PendingRows implements Collector<RowData> {

        private final ArrayDeque<RowData> pending;

        private PendingRows(ArrayDeque<RowData> pending) {
            this.pending = pending;
        }

        @Override
        public void collect(RowData record) {
            pending.add(record);
        }

        @Override
        public void close() {}
    }
}
