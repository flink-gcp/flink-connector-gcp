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

package io.github.flink.gcp.connector.bigquery.sink.fileloads.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One writer's background staging uploader: a single thread that runs upload operations in the
 * order the task thread submits them. An upload operation is one full chunk to send, or a closed
 * staging file to finalize together with its last chunk, if any.
 *
 * <p>The task thread keeps appending, encoding and compressing into each file's next chunk while
 * the uploader sends earlier ones. Every upload operation takes one of {@code
 * maxInFlightOperations} permits, released when the uploader has run it, so the operations queued
 * or running behind the task thread are bounded per writer whatever the number of open
 * destinations. An uploader at its bound blocks the task thread, which is Flink's backpressure.
 *
 * <p>Each file's channel is touched by one thread at a time. Until the file is {@linkplain
 * UploadOutputStream#detach() detached}, only the uploader writes to it, and since the uploader is
 * one thread running tasks in submission order, a file's chunks reach the service in the order they
 * were written and its close follows its last chunk. A detach happens only after {@link #drain()},
 * so every earlier uploader write happens-before whatever the detached file does next, on whichever
 * thread.
 *
 * <p>The first failure on the uploader is kept, and every later task skips its work, so no file is
 * finalized after a failure the writer has not yet reported. The task thread reports it on its next
 * submission, drain or {@link #checkFailure()}: an {@link Error} as itself, anything else wrapped
 * in an {@link IOException}. {@link #close()} waits for nothing. Queued tasks are dropped, so their
 * files are never finalized and create no object; a task already running may still finalize its
 * file, which then belongs to no committable and is left to the staging bucket's lifecycle rule.
 */
@Internal
final class StagingUploader implements AutoCloseable {

    private final int chunkBytes;
    private final Semaphore permits;
    private final ExecutorService executor;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private volatile boolean closed;

    /**
     * Starts an uploader.
     *
     * @param chunkBytes the upload chunk size, matching the size each channel was opened with
     * @param maxInFlightOperations how many upload operations may be queued or running, positive
     * @param threadName the uploader thread's name
     */
    StagingUploader(int chunkBytes, int maxInFlightOperations, String threadName) {
        Preconditions.checkArgument(chunkBytes > 0, "chunkBytes must be positive: %s", chunkBytes);
        Preconditions.checkArgument(
                maxInFlightOperations > 0,
                "maxInFlightOperations must be positive: %s",
                maxInFlightOperations);
        this.chunkBytes = chunkBytes;
        this.permits = new Semaphore(maxInFlightOperations);
        // The permits bound the queue; it is unbounded here only so a barrier never waits on one.
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        this.executor =
                new ThreadPoolExecutor(
                        1,
                        1,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(),
                        runnable -> {
                            Thread thread = new Thread(runnable, threadName);
                            thread.setDaemon(true);
                            thread.setContextClassLoader(contextClassLoader);
                            return thread;
                        });
    }

    /**
     * Wraps a newly opened staging channel in a stream whose full chunks go to this uploader.
     *
     * @param channel the staging object's channel, not yet written
     * @return the stream the format writer writes to, confined to the task thread
     */
    UploadOutputStream open(WritableByteChannel channel) {
        return new UploadOutputStream(channel);
    }

    /**
     * Waits until the uploader has run every upload operation submitted so far, then reports the
     * first failure.
     *
     * @throws IOException if an uploader task failed
     * @throws InterruptedException if interrupted while waiting; the uploader keeps running
     */
    void drain() throws IOException, InterruptedException {
        checkFailure();
        CompletableFuture<Void> barrier = new CompletableFuture<>();
        try {
            executor.execute(() -> barrier.complete(null));
        } catch (RejectedExecutionException e) {
            throw new IOException("The staging uploader is closed", e);
        }
        try {
            barrier.get();
        } catch (ExecutionException e) {
            throw new IllegalStateException("A drain barrier cannot fail", e);
        }
        checkFailure();
    }

    /**
     * Reports the first uploader failure, if any.
     *
     * @throws IOException wrapping an uploader failure that is not an {@link Error}
     */
    void checkFailure() throws IOException {
        Throwable cause = failure.get();
        if (cause == null) {
            return;
        }
        if (cause instanceof Error) {
            throw (Error) cause;
        }
        throw new IOException("A staging upload failed on the staging uploader: " + cause, cause);
    }

    /**
     * Stops the uploader without waiting: queued tasks are dropped and the running one interrupted.
     *
     * <p>Dropped tasks keep their permits and never complete their futures. That is safe because
     * nothing submits after close: the writer clears its pending finalizations, and a file's {@code
     * abort()} never writes to or closes its stream. An {@code abort()} that did would block on a
     * permit here, so revisit this method before changing either.
     */
    @Override
    public void close() {
        closed = true;
        executor.shutdownNow();
    }

    /**
     * Submits one upload operation (a chunk, a close, or both), blocking while the uploader is at
     * its bound.
     *
     * @param chunk the bytes to send, flipped for reading, or {@code null}
     * @param close whether to close the channel after sending, finalizing the object
     * @param done completed when the task has run, or exceptionally when it failed or was skipped
     */
    private void submit(
            WritableByteChannel channel,
            @Nullable ByteBuffer chunk,
            boolean close,
            @Nullable CompletableFuture<Void> done)
            throws IOException {
        checkFailure();
        try {
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            InterruptedIOException interrupted =
                    new InterruptedIOException("Interrupted waiting for the staging uploader");
            interrupted.initCause(e);
            throw interrupted;
        }
        Runnable task =
                () -> {
                    try {
                        Throwable earlier = failure.get();
                        if (earlier != null || closed) {
                            if (done != null) {
                                done.completeExceptionally(
                                        new IOException(
                                                "Skipped after the staging uploader failed or closed",
                                                earlier));
                            }
                            return;
                        }
                        if (chunk != null) {
                            writeFully(channel, chunk);
                        }
                        if (close) {
                            channel.close();
                        }
                        if (done != null) {
                            done.complete(null);
                        }
                    } catch (Throwable t) {
                        // Every Throwable, JVM-fatal ones included: the uploader thread must
                        // survive to keep releasing permits, and the task thread rethrows an Error
                        // as itself.
                        failure.compareAndSet(null, t);
                        if (done != null) {
                            done.completeExceptionally(t);
                        }
                    } finally {
                        permits.release();
                    }
                };
        try {
            executor.execute(task);
        } catch (RejectedExecutionException e) {
            permits.release();
            throw new IOException("The staging uploader is closed", e);
        }
    }

    private static void writeFully(WritableByteChannel channel, ByteBuffer bytes)
            throws IOException {
        while (bytes.hasRemaining()) {
            channel.write(bytes);
        }
    }

    /**
     * The stream one staging file's format writer writes to. It fills one chunk-sized buffer and
     * submits each full one to the uploader as an upload operation, where the channel sends it
     * without copying. Closing it submits the last, partial chunk together with the channel's close
     * as one more.
     *
     * <p>Confined to the task thread until {@link #detach()}, and to one thread at a time after.
     */
    final class UploadOutputStream extends OutputStream {

        private final WritableByteChannel channel;
        private final CompletableFuture<Void> finalized = new CompletableFuture<>();
        @Nullable private ByteBuffer chunk;
        private boolean detached;
        private boolean closed;

        private UploadOutputStream(WritableByteChannel channel) {
            this.channel = channel;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b}, 0, 1);
        }

        @Override
        public void write(byte[] bytes, int offset, int length) throws IOException {
            if (closed) {
                throw new IOException("The staging stream is closed");
            }
            while (length > 0) {
                if (chunk == null) {
                    chunk = ByteBuffer.allocate(chunkBytes);
                }
                int copied = Math.min(length, chunk.remaining());
                chunk.put(bytes, offset, copied);
                offset += copied;
                length -= copied;
                if (!chunk.hasRemaining()) {
                    ByteBuffer full = chunk;
                    chunk = null;
                    full.flip();
                    send(full, false);
                }
            }
        }

        /**
         * Sends nothing: a partial chunk is sent only by {@link #close()}, as the channel would.
         */
        @Override
        public void flush() {}

        /**
         * Finalizes the object: on the uploader thread, or on the calling thread once detached.
         * There the call returns once the close is submitted, and {@link #finalized()} completes
         * when the uploader has run it.
         */
        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }
            closed = true;
            ByteBuffer last = chunk;
            chunk = null;
            if (last != null) {
                last.flip();
            }
            send(last, true);
        }

        /**
         * Moves this file off the uploader thread: every later chunk and the close run on the
         * calling thread. The caller must have {@linkplain #drain() drained} the uploader first.
         *
         * <p>The buffered partial chunk stays here until the close, rather than being copied into
         * the channel now. The channel copies a partial chunk into a buffer of its own, so copying
         * at the close holds two chunks only for the files closing at that moment, not for every
         * open file at once.
         */
        void detach() {
            detached = true;
        }

        /** Sends a chunk, a close, or both: on the uploader thread, or here once detached. */
        private void send(@Nullable ByteBuffer bytes, boolean close) throws IOException {
            if (!detached) {
                submit(channel, bytes, close, close ? finalized : null);
                return;
            }
            // A detached file is finished by its caller, which reports a failure directly, so
            // nothing here completes the finalization future.
            if (bytes != null) {
                writeFully(channel, bytes);
            }
            if (close) {
                channel.close();
            }
        }

        /**
         * Completes when the uploader has finalized the object, or exceptionally if that failed or
         * was skipped. A file closed after {@link #detach()} never completes it.
         */
        CompletableFuture<Void> finalized() {
            return finalized;
        }
    }
}
