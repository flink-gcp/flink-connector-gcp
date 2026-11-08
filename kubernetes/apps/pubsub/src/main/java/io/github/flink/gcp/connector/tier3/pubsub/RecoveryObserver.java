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
import org.apache.flink.annotation.VisibleForTesting;
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
    private final RecoveryOptions options;
    private transient ListState<String> identity;
    private transient boolean restored;
    private transient String attempt;
    // The checkpoint this attempt restored, or zero, and the latest checkpoint whose barrier it
    // has passed. Barriers are aligned, so a record carrying an epoch below a checkpoint's id was
    // emitted before that checkpoint's barrier and is covered by it.
    private transient long origin;
    private transient long epoch;
    // Set by a snapshot of an attempt that restored nothing, and cleared by the first
    // replay-cohort record after it, which is held; see map.
    private transient boolean holdArmed;

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
        long hold = holdFor(value);
        if (hold > 0) {
            // On the DataStream entry point the observer is chained to the source, and the
            // checkpoint's completion notification reaches the reader as a mail on this task
            // thread, which runs only between records. Holding the first replay-cohort record
            // after the snapshot keeps that mail queued for the hold's length, so a restart that
            // the checkpoint's completion triggers, or a TaskManager loss within the hold, always
            // finds it unrun: the case restore deduplication exists for, produced on purpose
            // rather than left to how the race falls.
            LOG.info(
                    "event=pubsub-hold run_id={} phase={} attempt={} checkpoint_id={} millis={}",
                    options.runId,
                    options.phase,
                    attempt,
                    epoch,
                    hold);
            Thread.sleep(hold);
        }
        if (options.replayDelayMillis > 0 && options.inReplayCohort(value)) {
            // On the DataStream entry point the observer is chained to the source, so this paces
            // the replay cohort: it is still flowing, with records buffered behind the one in
            // progress, when the checkpoint that the trial's fault waits for is taken, so that
            // checkpoint covers some of it and a hold follows its snapshot at once. The pace
            // alone does not decide the race with a cancellation; the hold does.
            Thread.sleep(options.replayDelayMillis);
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

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        identity.update(List.of(options.identity()));
        checkpointTaken(context.getCheckpointId());
        LOG.info(
                "event=pubsub-snapshot run_id={} phase={} attempt={} checkpoint_id={}",
                options.runId,
                options.phase,
                attempt,
                context.getCheckpointId());
    }

    /** Marks this attempt as one that restored state, as {@code initializeState} does. */
    @VisibleForTesting
    void markRestored() {
        restored = true;
    }

    /** Moves the epoch past a snapshot's barrier and arms the hold that follows it. */
    void checkpointTaken(long checkpointId) {
        epoch = checkpointId;
        // Only the attempts that ran before the fault hold: a restored attempt has nothing left to
        // prove, and holding there would only slow its recovery.
        holdArmed = options.holdAfterCheckpointMillis > 0 && !restored;
    }

    /**
     * How long to hold this record: the configured hold for the first replay-cohort record after a
     * snapshot, and nothing for any other record.
     */
    long holdFor(String value) {
        if (!holdArmed || !options.inReplayCohort(value)) {
            return 0;
        }
        holdArmed = false;
        return options.holdAfterCheckpointMillis;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (checkpointId >= epoch) {
            // The notification of the checkpoint the hold was armed for ran before any replay
            // record came, so holding now would log a hold for a notification that is no longer
            // queued.
            holdArmed = false;
        }
        LOG.info(
                "event=pubsub-checkpoint-complete run_id={} phase={} attempt={} checkpoint_id={}",
                options.runId,
                options.phase,
                attempt,
                checkpointId);
    }
}
