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

package io.github.flink.gcp.connector.tier3.pubsub;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/** Guards run identity across rescaling and emits distinct per-processing observations. */
@Internal
final class RecoveryObserver extends RichMapFunction<String, String>
        implements CheckpointedFunction, CheckpointListener {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(RecoveryObserver.class);
    // How long an attempt that keeps processing goes between two progress lines; it logs its first
    // record too, so a restored attempt shows it runs as soon as it processes anything, and the
    // latest line dates its latest processing to within this interval.
    static final long PROGRESS_MILLIS = 10_000;
    private final RecoveryOptions options;
    private transient ListState<String> identity;
    private transient boolean restored;
    private transient String attempt;
    // The checkpoint this attempt restored, or zero, and the latest checkpoint whose barrier it
    // has passed. Barriers are aligned, so a record carrying an epoch below a checkpoint's id was
    // emitted before that checkpoint's barrier and is covered by it.
    private transient long origin;
    private transient long epoch;
    // How many records this attempt has processed, which its progress lines report, and when it
    // logged the last line.
    private transient long processed;
    private transient long progressAt;

    RecoveryObserver(RecoveryOptions options) {
        this.options = options;
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        identity =
                context.getOperatorStateStore()
                        .getUnionListState(
                                new ListStateDescriptor<>("pubsub-run-v1", Types.STRING));
        restored = context.isRestored();
        verifyIdentity(options, restored, identity.get());
        origin = restored ? context.getRestoredCheckpointId().orElse(0L) : 0L;
        if (restored && origin < 1) {
            throw new IllegalStateException("Restored Pub/Sub state names no checkpoint");
        }
        epoch = origin;
        attempt = UUID.randomUUID().toString();
        LOG.info(
                "event=pubsub-attempt run_id={} phase={} attempt={} restored={} origin={}",
                options.runId,
                options.phase,
                attempt,
                restored,
                origin);
    }

    static void verifyIdentity(
            RecoveryOptions options, boolean restored, Iterable<String> entries) {
        if (options.requireRestored && !restored) {
            throw new IllegalStateException(
                    "--require-restored=true requires checkpoint or savepoint state");
        }
        int count = 0;
        for (String entry : entries) {
            if (!options.identity().equals(entry)) {
                throw new IllegalStateException(
                        "Restored Pub/Sub state belongs to another run or input domain");
            }
            count++;
        }
        if (restored && count == 0) {
            throw new IllegalStateException("Restored Pub/Sub run identity is missing");
        }
    }

    @Override
    public String map(String value) throws InterruptedException {
        if (options.recordDelayMillis > 0) {
            // The observer is chained to the source, so this paces the input: records the
            // subscription has already delivered wait in the source's buffers, leased and
            // unacknowledged, until their turn comes.
            Thread.sleep(options.recordDelayMillis);
        }
        processed++;
        long now = System.currentTimeMillis();
        if (logsProgress(processed, now, progressAt)) {
            progressAt = now;
            LOG.info("{}", progressLine(options, attempt, restored, origin, processed));
        }
        return RecoveryPayload.observation(
                options,
                value,
                attempt,
                restored,
                origin,
                epoch,
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                getRuntimeContext().getTaskInfo().getAttemptNumber());
    }

    /** Whether an attempt logs its progress at this record: its first, then every interval. */
    static boolean logsProgress(long processed, long now, long lastLoggedAt) {
        return processed == 1 || now - lastLoggedAt >= PROGRESS_MILLIS;
    }

    /**
     * The progress line the supervisor's fault sequence parses, `PROGRESS` in {@code
     * flink_tier3/pubsub/exercise.py}; a test on each side pins the same example.
     */
    static String progressLine(
            RecoveryOptions options,
            String attempt,
            boolean restored,
            long origin,
            long processed) {
        return "event=pubsub-progress run_id="
                + options.runId
                + " phase="
                + options.phase
                + " attempt="
                + attempt
                + " restored="
                + restored
                + " origin="
                + origin
                + " processed="
                + processed;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        identity.update(List.of(options.identity()));
        epoch = context.getCheckpointId();
        LOG.info(
                "event=pubsub-snapshot run_id={} phase={} attempt={} checkpoint_id={}",
                options.runId,
                options.phase,
                attempt,
                context.getCheckpointId());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        LOG.info(
                "event=pubsub-checkpoint-complete run_id={} phase={} attempt={} checkpoint_id={}",
                options.runId,
                options.phase,
                attempt,
                checkpointId);
    }
}
