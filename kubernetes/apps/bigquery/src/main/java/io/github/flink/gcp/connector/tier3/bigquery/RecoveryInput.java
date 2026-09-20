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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/** Verifies source replay position and records the restored trial identity. */
@Internal
final class RecoveryInput extends RichMapFunction<Long, Long>
        implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RecoveryInput.class);
    private final RecoveryOptions options;
    private transient ListState<Tuple3<String, String, Long>> state;
    private transient String lineage;
    private transient long next;
    private transient boolean restored;
    private transient boolean reported;

    RecoveryInput(RecoveryOptions options) {
        this.options = options;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "bq1312-input-v1",
                                        Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
        restored = context.isRestored();
        if (options.requireRestored && !restored) {
            throw new IllegalStateException("BigQuery trial requires restored state");
        }
        if (restored) {
            var entries = state.get().iterator();
            if (!entries.hasNext()) {
                throw new IllegalStateException("BigQuery input state is missing");
            }
            var entry = entries.next();
            if (entries.hasNext()
                    || !options.identity().equals(entry.f0)
                    || entry.f1 == null
                    || !entry.f1.matches("[a-f0-9-]{36}")
                    || entry.f2 == null
                    || entry.f2 < 0
                    || entry.f2 > options.records) {
                throw new IllegalStateException(
                        "BigQuery input state belongs to another trial or configuration");
            }
            lineage = entry.f1;
            next = entry.f2;
        } else {
            lineage = UUID.randomUUID().toString();
            next = 0;
        }
    }

    @Override
    public Long map(Long sequence) {
        if (sequence != next || next >= options.records) {
            throw new IllegalStateException(
                    "BigQuery input sequence mismatch: expected=" + next + " actual=" + sequence);
        }
        next++;
        if (!reported
                || next % Math.max(1, options.bytesPerSecond / options.mode.rowBytes * 30) == 0
                || next == options.records) {
            LOG.info(
                    "event=bigquery-progress run_id={} phase={} lineage={} restored={} processed={} sequence={}",
                    options.runId,
                    options.phase,
                    lineage,
                    restored,
                    next,
                    sequence);
            reported = true;
        }
        return sequence;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.update(List.of(Tuple3.of(options.identity(), lineage, next)));
        LOG.info(
                "event=bigquery-snapshot run_id={} phase={} lineage={} checkpoint_id={} processed={}",
                options.runId,
                options.phase,
                lineage,
                context.getCheckpointId(),
                next);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.info(
                "event=bigquery-checkpoint-complete run_id={} phase={} lineage={} checkpoint_id={}",
                options.runId,
                options.phase,
                lineage,
                checkpointId);
    }
}
