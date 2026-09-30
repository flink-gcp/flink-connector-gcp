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

package io.github.flink.gcp.connector.firestore.sink.writer;

import com.google.api.core.ApiFuture;
import com.google.api.core.SettableApiFuture;
import com.google.cloud.firestore.BulkWriterException;
import com.google.cloud.firestore.WriteResult;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWrite;
import io.grpc.Status;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * A scripted {@link FirestoreDatabaseAccess} modelling what the writer relies on in the library's
 * {@code BulkWriter}: writes queue until {@link #sendOutstanding()} or a batch of {@value
 * #BATCH_SIZE}, every write of a sent batch is answered by the {@link Responder}, and a write
 * answered with a failure keeps its pending-operation slot until {@link #replaceBulkWriter()} —
 * past {@value FirestoreWriter#PENDING_OPERATION_LIMIT} occupied slots a submitted write is
 * stranded and never answered, as the library strands it.
 *
 * <p>Synchronized, because a test may drive the writer on one thread while answering held writes on
 * another.
 */
final class FakeFirestoreDatabaseAccess implements FirestoreDatabaseAccess {

    static final int BATCH_SIZE = 20;

    /** Answers one write of a sent batch: {@code null} applies it, a throwable fails it. */
    @FunctionalInterface
    interface Responder {
        @Nullable
        Throwable answer(FirestoreWrite write, List<FirestoreWrite> batch);
    }

    private final List<FirestoreWrite> queued = new ArrayList<>();
    private final List<SettableApiFuture<WriteResult>> queuedFutures = new ArrayList<>();
    private final List<List<FirestoreWrite>> sentBatches = new ArrayList<>();
    private final List<List<FirestoreWrite>> heldBatches = new ArrayList<>();
    private final List<List<SettableApiFuture<WriteResult>>> heldFutures = new ArrayList<>();

    private Responder responder = (write, batch) -> null;
    @Nullable private RuntimeException synchronousRefusal;
    private boolean answering = true;
    private int sendOutstandingCalls;
    private int replacements;
    private int leakedSlots;
    private int unanswered;
    private int stranded;
    private boolean closed;

    synchronized void respondWith(Responder responder) {
        this.responder = responder;
    }

    /** Makes the next {@link #submit} throw, as the library does for a value it cannot encode. */
    synchronized void refuseNextSubmit(RuntimeException refusal) {
        this.synchronousRefusal = refusal;
    }

    /** Stops answering sent batches until {@link #answerHeld()}; for writes held in flight. */
    synchronized void holdAnswers() {
        answering = false;
    }

    @Override
    public synchronized ApiFuture<WriteResult> submit(FirestoreWrite write) {
        if (closed) {
            throw new IllegalStateException("closed");
        }
        if (synchronousRefusal != null) {
            RuntimeException refusal = synchronousRefusal;
            synchronousRefusal = null;
            throw refusal;
        }
        SettableApiFuture<WriteResult> future = SettableApiFuture.create();
        if (leakedSlots + unanswered >= FirestoreWriter.PENDING_OPERATION_LIMIT) {
            // The library queues it behind slots that are never released: nothing sends it.
            stranded++;
            return future;
        }
        unanswered++;
        queued.add(write);
        queuedFutures.add(future);
        if (queued.size() >= BATCH_SIZE) {
            send();
        }
        return future;
    }

    @Override
    public synchronized void sendOutstanding() {
        sendOutstandingCalls++;
        send();
    }

    private void send() {
        if (queued.isEmpty()) {
            return;
        }
        List<FirestoreWrite> batch = new ArrayList<>(queued);
        List<SettableApiFuture<WriteResult>> futures = new ArrayList<>(queuedFutures);
        queued.clear();
        queuedFutures.clear();
        sentBatches.add(batch);
        if (!answering) {
            heldBatches.add(batch);
            heldFutures.add(futures);
            return;
        }
        answer(batch, futures);
    }

    /** Answers every batch held since {@link #holdAnswers()} and resumes answering. */
    synchronized void answerHeld() {
        answering = true;
        for (int i = 0; i < heldBatches.size(); i++) {
            answer(heldBatches.get(i), heldFutures.get(i));
        }
        heldBatches.clear();
        heldFutures.clear();
    }

    private void answer(List<FirestoreWrite> batch, List<SettableApiFuture<WriteResult>> futures) {
        for (int i = 0; i < batch.size(); i++) {
            Throwable failure = responder.answer(batch.get(i), batch);
            unanswered--;
            if (failure == null) {
                // The writer never reads the result, and the library's type has no public
                // constructor.
                futures.get(i).set(null);
            } else {
                leakedSlots++;
                futures.get(i).setException(failure);
            }
        }
    }

    @Override
    public synchronized void replaceBulkWriter() {
        if (unanswered > 0) {
            throw new IllegalStateException(
                    "replaceBulkWriter() with "
                            + unanswered
                            + " write(s) unanswered: the dropped BulkWriter would never send them");
        }
        replacements++;
        leakedSlots = 0;
    }

    @Override
    public synchronized void close() {
        closed = true;
    }

    synchronized List<List<FirestoreWrite>> sentBatches() {
        return new ArrayList<>(sentBatches);
    }

    synchronized int queuedWrites() {
        return queued.size();
    }

    synchronized int sendOutstandingCalls() {
        return sendOutstandingCalls;
    }

    synchronized int replacements() {
        return replacements;
    }

    /** Writes submitted past the pending ceiling, which the library would never answer. */
    synchronized int strandedWrites() {
        return stranded;
    }

    synchronized boolean isClosed() {
        return closed;
    }

    /** A failure the way the library reports one: a {@code BulkWriterException} with a status. */
    static BulkWriterException failure(Status status) {
        return new BulkWriterException(status, "scripted " + status.getCode(), null, null, 1);
    }
}
