/*
 * Copyright 2023 Google LLC
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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.core.io.InputStatus;

import com.google.pubsub.v1.PubsubMessage;
import io.github.flink.gcp.connector.pubsub.source.SubscriptionDestination;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerChunkEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.LedgerUploadedEvent;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.MessageIdLedgerCodec;
import io.github.flink.gcp.connector.pubsub.source.streamingpull.SubscriptionSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Reader subtask consuming its assigned subscription splits.
 *
 * <p>The split itself is the split state: splits carry no progress, so there is nothing to mutate
 * while reading and nothing to convert when snapshotting.
 *
 * <p>The reader owns the two ends of the acknowledgement lifecycle that only it can see — binding
 * staged messages to the checkpoint being taken, and acknowledging them once that checkpoint
 * completes. It is therefore also the only place that sees checkpoints happen at all, which it
 * reports to the {@link MissingCheckpointDetector} it shares with the split reader.
 *
 * <p>With restore deduplication on (#2049), the reader also carries the restore-deduplication
 * ledger through checkpoints. Its snapshot holds ledger chunks — the ids of messages the checkpoint
 * covers that are not acknowledged yet — and never a consuming split. On a restore, the chunks it
 * is handed are released at once, so their split outputs hold back no watermark, and uploaded to
 * the enumerator, which forwards each subscription's chunks to the readers that consume it before
 * assigning any split. A chunk the enumerator forwards lands in the {@link RestoredLedger} the
 * record emitter consults.
 *
 * <p>Adapted from the Flink connector in <a
 * href="https://github.com/GoogleCloudPlatform/pubsub">GoogleCloudPlatform/pubsub</a> (Apache-2.0),
 * which wraps the split in a separate state class and passes a fresh {@code Configuration} instead
 * of the job's.
 *
 * @param <T> type of the records produced by the source
 */
@Internal
public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                PubsubMessage, T, SubscriptionSplit, SubscriptionSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceReader.class);

    /**
     * The size at which a ledger chunk is closed. Far below the RPC frame size (10 MiB by default),
     * because the enumerator forwards each chunk as one event and the frame also carries the
     * event's envelope.
     */
    @VisibleForTesting static final int LEDGER_CHUNK_BYTES = 1 << 20;

    private final AckTracker ackTracker;
    private final MissingCheckpointDetector checkpointDetector;
    @Nullable private final RestoredLedger restoredLedger;

    private int ledgerChunkBytes = LEDGER_CHUNK_BYTES;

    /** The subscription of every consuming split this reader was handed, by split id. */
    private final Map<String, SubscriptionDestination> subscriptions = new HashMap<>();

    /** Restored ledger chunks awaiting upload in {@link #start()}. */
    private final List<SubscriptionSplit> restoredChunks = new ArrayList<>();

    /** Ledger chunk split ids whose source outputs have not been released yet. */
    private final List<String> chunkOutputsToRelease = new ArrayList<>();

    /**
     * Creates the reader.
     *
     * @param splitReaderSupplier supplies the split reader multiplexing the subscribers
     * @param recordEmitter deserializes and emits received messages
     * @param config the TaskManager configuration, which carries the source reader options
     * @param context the reader context
     * @param ackTracker tracks the acknowledgement lifecycle of received messages
     * @param checkpointDetector the detector this reader feeds checkpoint signals to; the split
     *     reader it shares with is what evaluates them
     * @param restoredLedger the ledger the record emitter consults, or {@code null} when restore
     *     deduplication is off
     */
    public PubSubSourceReader(
            Supplier<SplitReader<PubsubMessage, SubscriptionSplit>> splitReaderSupplier,
            RecordEmitter<PubsubMessage, T, SubscriptionSplit> recordEmitter,
            Configuration config,
            SourceReaderContext context,
            AckTracker ackTracker,
            MissingCheckpointDetector checkpointDetector,
            @Nullable RestoredLedger restoredLedger) {
        super(splitReaderSupplier, recordEmitter, config, context);
        this.ackTracker = ackTracker;
        this.checkpointDetector = checkpointDetector;
        this.restoredLedger = restoredLedger;
    }

    @Override
    public void start() {
        super.start();
        if (restoredLedger == null) {
            return;
        }
        // Registration precedes start(), so the enumerator already knows this reader; sending here
        // is what SourceOperator allows. The completion report goes even when nothing was restored:
        // the enumerator cannot tell "no chunks" from "chunks still on the way" otherwise.
        for (SubscriptionSplit chunk : restoredChunks) {
            context.sendSourceEventToCoordinator(new LedgerChunkEvent(chunk));
        }
        context.sendSourceEventToCoordinator(new LedgerUploadedEvent());
        if (!restoredChunks.isEmpty()) {
            LOG.info(
                    "Uploaded {} restored deduplication ledger chunk(s) to the enumerator.",
                    restoredChunks.size());
        }
        restoredChunks.clear();
    }

    @Override
    public void addSplits(List<SubscriptionSplit> splits) {
        List<SubscriptionSplit> consuming = new ArrayList<>(splits.size());
        for (SubscriptionSplit split : splits) {
            if (split.isLedgerChunk()) {
                // SourceOperator opened an output for the restored chunk; release it on the next
                // poll, or a split that never emits would pin the watermark.
                chunkOutputsToRelease.add(split.splitId());
                // Taken out of the split object, which SourceOperator keeps for the execution's
                // lifetime; once uploaded, nothing here references the payload any more.
                SubscriptionSplit chunk = split.takeLedgerChunk();
                if (restoredLedger != null) {
                    restoredChunks.add(chunk);
                } else {
                    LOG.info(
                            "Discarding restored deduplication ledger chunk {}: restore"
                                    + " deduplication is off.",
                            split.splitId());
                }
            } else {
                subscriptions.put(split.splitId(), split.getSubscription());
                consuming.add(split);
            }
        }
        if (!consuming.isEmpty()) {
            if (restoredLedger != null) {
                restoredLedger.startClock();
            }
            super.addSplits(consuming);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        if (!chunkOutputsToRelease.isEmpty()) {
            for (String splitId : chunkOutputsToRelease) {
                output.releaseOutputForSplit(splitId);
            }
            chunkOutputsToRelease.clear();
        }
        return super.pollNext(output);
    }

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {
        if (!(sourceEvent instanceof LedgerChunkEvent)) {
            super.handleSourceEvents(sourceEvent);
            return;
        }
        SubscriptionSplit chunk = ((LedgerChunkEvent) sourceEvent).chunk();
        if (restoredLedger == null) {
            return;
        }
        try {
            restoredLedger.add(
                    chunk.getSubscription(), MessageIdLedgerCodec.decode(chunk.getLedgerChunk()));
        } catch (IOException | RuntimeException e) {
            // The chunk came out of a checkpoint, so failing here would fail every restore of it.
            // Without the chunk its messages are only emitted again, which is the duplicate this
            // ledger exists to prevent, not a loss.
            LOG.warn(
                    "Ignoring unreadable deduplication ledger chunk {}; its messages may be"
                            + " emitted again.",
                    chunk.splitId(),
                    e);
        }
    }

    @Override
    public List<SubscriptionSplit> snapshotState(long checkpointId) {
        checkpointDetector.checkpointTaken();

        // Everything emitted before the barrier belongs to this checkpoint; anything emitted after
        // it belongs to the next one.
        ackTracker.addCheckpoint(checkpointId);

        // Deliberately checkpoint no consuming split. SourceOperator restores the reader's splits
        // from operator state and hands them to addSplits, and the enumerator separately assigns
        // the recomputed plan on addReader — the two are unioned, never reconciled. Since splits
        // carry
        // no progress state and the plan is deterministic, reporting none makes the enumerator the
        // only source of split ownership. Reporting them instead would let a rescale or an edited
        // subscription list leave a subscription consumed by two subtasks at once, which is exactly
        // what PER_KEY exists to prevent, or keep consuming a subscription that was removed from
        // the configuration. Ledger chunks consume nothing, so they are safe to hand out anywhere.
        if (restoredLedger == null) {
            return Collections.emptyList();
        }
        restoredLedger.expireIfDue();
        return ledgerChunks(checkpointId);
    }

    /**
     * Encodes the ids the checkpoint covers but has not acknowledged, split by split. Each chunk
     * carries its split's subscription; the plan never gives one subtask two splits of the same
     * subscription, and two chunks of one subscription would be just as valid if it did.
     */
    private List<SubscriptionSplit> ledgerChunks(long checkpointId) {
        List<SubscriptionSplit> chunks = new ArrayList<>();
        int subtask = context.getIndexOfSubtask();
        for (Map.Entry<String, List<String>> entry :
                ackTracker.checkpointedMessageIds().entrySet()) {
            SubscriptionDestination subscription = subscriptions.get(entry.getKey());
            if (subscription == null) {
                // Every tracked split was added through addSplits, so this cannot happen; skipping
                // costs at most a duplicate.
                continue;
            }
            for (byte[] chunk : MessageIdLedgerCodec.encode(entry.getValue(), ledgerChunkBytes)) {
                // Unique across the job: the checkpoint, the subtask, and the chunk's position.
                chunks.add(
                        SubscriptionSplit.ledgerChunk(
                                subscription,
                                checkpointId + "-" + subtask + "-" + chunks.size(),
                                chunk));
            }
        }
        return chunks;
    }

    /** Lowers the chunk size so that a test can produce several chunks from a few ids. */
    @VisibleForTesting
    void setLedgerChunkBytes(int ledgerChunkBytes) {
        this.ledgerChunkBytes = ledgerChunkBytes;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        ackTracker.notifyCheckpointComplete(checkpointId);
    }

    @Override
    protected SubscriptionSplit initializedState(SubscriptionSplit split) {
        return split;
    }

    @Override
    protected SubscriptionSplit toSplitType(String splitId, SubscriptionSplit splitState) {
        return splitState;
    }

    @Override
    protected void onSplitFinished(Map<String, SubscriptionSplit> finishedSplits) {
        throw new IllegalStateException(
                "A Pub/Sub subscription split never finishes, but the reader was told that "
                        + finishedSplits.keySet()
                        + " did.");
    }
}
