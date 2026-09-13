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

package io.github.flink.gcp.connector.tier3.smoke;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.UUID;

/** Checks each key's sequence and records evidence of the state used by an attempt. */
@Internal
final class SmokeVerifier extends KeyedProcessFunction<Integer, Long, String>
        implements CheckpointedFunction, CheckpointListener {

    static final int KEYS = 4;
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(SmokeVerifier.class);

    private final SmokeOptions options;
    private transient ValueState<Long> next;
    private transient ListState<Tuple3<String, String, Long>> lineageState;
    private transient String lineage;
    private transient long processed;
    private transient boolean restored;
    private transient boolean reportedAttempt;

    SmokeVerifier(SmokeOptions options) {
        this.options = options;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        lineageState =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(
                                        "smoke-lineage-v1",
                                        Types.TUPLE(Types.STRING, Types.STRING, Types.LONG)));
        restored = context.isRestored();
        if (options.requireRestored && !restored) {
            throw new IllegalStateException(
                    "Smoke --require-restored=true requires checkpoint or savepoint state");
        }
        if (restored) {
            Iterator<Tuple3<String, String, Long>> entries = lineageState.get().iterator();
            if (!entries.hasNext()) {
                throw new IllegalStateException("Restored smoke lineage state is missing");
            }
            Tuple3<String, String, Long> entry = entries.next();
            if (entries.hasNext()
                    || !options.runId.equals(entry.f0)
                    || entry.f2 < 0
                    || entry.f2 > options.records) {
                throw new IllegalStateException(
                        "Restored smoke state must belong to this --run-id and a single verifier");
            }
            lineage = entry.f1;
            processed = entry.f2;
        } else {
            lineage = UUID.randomUUID().toString();
            processed = 0;
        }
    }

    @Override
    public void open(OpenContext context) {
        next =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("smoke-next-v1", Types.LONG));
    }

    @Override
    public void processElement(Long value, Context context, Collector<String> output)
            throws Exception {
        Long saved = next.value();
        long expected = saved == null ? context.getCurrentKey() : saved;
        if (value != expected) {
            throw new IllegalStateException(
                    "Smoke sequence mismatch: key="
                            + context.getCurrentKey()
                            + " expected="
                            + expected
                            + " actual="
                            + value);
        }
        next.update(value + KEYS);
        processed++;
        if (!reportedAttempt || processed % 100 == 0 || processed == options.records) {
            output.collect(
                    "event=smoke-progress run_id="
                            + options.runId
                            + " phase="
                            + options.phase
                            + " lineage="
                            + lineage
                            + " restored="
                            + restored
                            + " processed="
                            + processed
                            + " sequence="
                            + value);
            reportedAttempt = true;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        lineageState.update(List.of(Tuple3.of(options.runId, lineage, processed)));
        LOG.info(
                "event=smoke-snapshot run_id={} phase={} lineage={} checkpoint_id={} processed={}",
                options.runId,
                options.phase,
                lineage,
                context.getCheckpointId(),
                processed);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.info(
                "event=smoke-checkpoint-complete run_id={} phase={} lineage={} checkpoint_id={}",
                options.runId,
                options.phase,
                lineage,
                checkpointId);
    }
}
