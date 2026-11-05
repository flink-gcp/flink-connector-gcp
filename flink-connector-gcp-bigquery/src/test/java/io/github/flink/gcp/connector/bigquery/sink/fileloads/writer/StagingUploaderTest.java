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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StagingUploader}. */
class StagingUploaderTest {

    private static final String UPLOADER_THREAD = "test staging uploader";

    private final List<String> events = Collections.synchronizedList(new ArrayList<>());
    private StagingUploader uploader;

    @AfterEach
    void closeUploader() {
        if (uploader != null) {
            uploader.close();
        }
    }

    @Test
    void sendsFullChunksAndTheCloseOnTheUploaderInWriteOrder() throws Exception {
        uploader = new StagingUploader(4, 8, UPLOADER_THREAD);
        RecordingChannel channel = new RecordingChannel("a");
        StagingUploader.UploadOutputStream stream = uploader.open(channel);

        stream.write(bytes("0123456789"));
        stream.flush();
        stream.close();
        stream.finalized().get(5, TimeUnit.SECONDS);
        uploader.drain();

        // Full chunks go over whole, so the channel can send them without copying; the partial
        // last chunk goes with the close, as a flush never sends it.
        assertThat(events)
                .containsExactly(
                        "a write 4 on " + UPLOADER_THREAD,
                        "a write 4 on " + UPLOADER_THREAD,
                        "a write 2 on " + UPLOADER_THREAD,
                        "a close on " + UPLOADER_THREAD);
        assertThat(channel.content()).isEqualTo("0123456789");
    }

    @Test
    void runsUploadOperationsInOrderAcrossChannels() throws Exception {
        uploader = new StagingUploader(2, 8, UPLOADER_THREAD);
        RecordingChannel firstChannel = new RecordingChannel("a");
        firstChannel.holdWrites();
        StagingUploader.UploadOutputStream first = uploader.open(firstChannel);
        StagingUploader.UploadOutputStream second = uploader.open(new RecordingChannel("b"));

        // Everything after the first chunk queues behind it, so an uploader that ran tasks out of
        // order, or on more than one thread, would record "b" first.
        first.write(bytes("aa"));
        assertThat(firstChannel.awaitHeld()).isTrue();
        second.write(bytes("bb"));
        first.close();
        second.close();
        firstChannel.release();
        uploader.drain();

        assertThat(events)
                .containsExactly(
                        "a write 2 on " + UPLOADER_THREAD,
                        "b write 2 on " + UPLOADER_THREAD,
                        "a close on " + UPLOADER_THREAD,
                        "b close on " + UPLOADER_THREAD);
        assertThat(first.finalized()).isCompleted();
        assertThat(second.finalized()).isCompleted();
    }

    @Test
    void blocksTheWriterOnceTheBoundIsSubmitted() throws Exception {
        uploader = new StagingUploader(2, 1, UPLOADER_THREAD);
        RecordingChannel channel = new RecordingChannel("a");
        channel.holdWrites();
        StagingUploader.UploadOutputStream stream = uploader.open(channel);
        // The first chunk takes the only permit and holds it while the uploader is held in it.
        stream.write(bytes("aa"));
        assertThat(channel.awaitHeld()).isTrue();

        BackgroundWrite second = BackgroundWrite.start(stream, bytes("bb"));

        // A wrong bound fails this fast; a right one only costs the timeout.
        assertThat(second.awaitReturned(200)).isFalse();
        channel.release();
        assertThat(second.awaitReturned(5_000)).isTrue();
        assertThat(second.failure()).isNull();
        uploader.drain();
        assertThat(channel.content()).isEqualTo("aabb");
    }

    @Test
    void aFailedTaskReleasesItsPermitToAWriterWaitingForIt() throws Exception {
        uploader = new StagingUploader(2, 1, UPLOADER_THREAD);
        RecordingChannel failing = new RecordingChannel("a");
        IOException cause = new IOException("chunk rejected");
        failing.failWrites(cause);
        failing.holdWrites();
        RecordingChannel later = new RecordingChannel("b");
        StagingUploader.UploadOutputStream first = uploader.open(failing);
        StagingUploader.UploadOutputStream second = uploader.open(later);
        first.write(bytes("aa"));
        assertThat(failing.awaitHeld()).isTrue();

        // Waits for the only permit, which the failing task still holds.
        BackgroundWrite waiting = BackgroundWrite.start(second, bytes("bb"));
        assertThat(waiting.awaitReturned(200)).isFalse();
        failing.release();

        // The failed task released the permit, so the waiting upload operation went in, and the
        // uploader then skipped it rather than send a chunk after an unreported failure.
        assertThat(waiting.awaitReturned(5_000)).isTrue();
        assertThat(waiting.failure()).isNull();
        assertThatThrownBy(uploader::drain).isInstanceOf(IOException.class).hasCause(cause);
        assertThat(events).noneMatch(event -> event.startsWith("b "));
    }

    @Test
    void aFailureSkipsLaterWorkAndIsReportedOnTheCallingThread() throws Exception {
        uploader = new StagingUploader(2, 8, UPLOADER_THREAD);
        RecordingChannel failing = new RecordingChannel("a");
        IOException cause = new IOException("chunk rejected");
        failing.failWrites(cause);
        // Held until the later file's close is queued, so the failure lands ahead of it.
        failing.holdWrites();
        RecordingChannel later = new RecordingChannel("b");
        StagingUploader.UploadOutputStream first = uploader.open(failing);
        StagingUploader.UploadOutputStream second = uploader.open(later);

        first.write(bytes("aa"));
        assertThat(failing.awaitHeld()).isTrue();
        second.write(bytes("b"));
        second.close();
        failing.release();

        assertThatThrownBy(uploader::drain).isInstanceOf(IOException.class).hasCause(cause);
        // drain() may report the failure before the uploader reaches the queued close, so wait for
        // the
        // close itself: it is skipped, and the later file is never finalized after a failure the
        // writer has not reported yet.
        assertThatThrownBy(() -> second.finalized().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IOException.class);
        assertThat(later.isClosed()).isFalse();
        assertThatThrownBy(() -> first.write(bytes("aa")))
                .isInstanceOf(IOException.class)
                .hasCause(cause);
    }

    @Test
    void anErrorOnTheUploaderIsRethrownAsItselfAndTheUploaderKeepsRunning() throws Exception {
        uploader = new StagingUploader(2, 8, UPLOADER_THREAD);
        RecordingChannel failing = new RecordingChannel("a");
        NoClassDefFoundError error = new NoClassDefFoundError("uploader blew up");
        failing.failWrites(error);
        // Held until the later file's close is queued, so it waits behind the Error.
        failing.holdWrites();
        StagingUploader.UploadOutputStream first = uploader.open(failing);
        StagingUploader.UploadOutputStream second = uploader.open(new RecordingChannel("b"));
        first.write(bytes("aa"));
        assertThat(failing.awaitHeld()).isTrue();
        second.write(bytes("b"));
        second.close();
        failing.release();

        // The uploader went on to run the queued close after the Error, which skipped its work.
        assertThatThrownBy(() -> second.finalized().get(5, TimeUnit.SECONDS))
                .hasCauseInstanceOf(IOException.class);
        assertThatThrownBy(uploader::drain).isSameAs(error);
    }

    @Test
    void anInterruptWhileWaitingForAPermitIsReportedAndKept() throws Exception {
        uploader = new StagingUploader(2, 1, UPLOADER_THREAD);
        RecordingChannel channel = new RecordingChannel("a");
        channel.holdWrites();
        StagingUploader.UploadOutputStream stream = uploader.open(channel);
        stream.write(bytes("aa"));
        assertThat(channel.awaitHeld()).isTrue();

        BackgroundWrite waiting = BackgroundWrite.start(stream, bytes("bb"));
        assertThat(waiting.awaitReturned(200)).isFalse();
        waiting.thread.interrupt();

        assertThat(waiting.awaitReturned(5_000)).isTrue();
        assertThat(waiting.failure()).isInstanceOf(InterruptedIOException.class);
        assertThat(waiting.interruptedAfterwards()).isTrue();
        channel.release();
        uploader.drain();
        assertThat(channel.content()).isEqualTo("aa");
    }

    @Test
    void closeDropsQueuedWorkWithoutWaitingForTheRunningTask() throws Exception {
        uploader = new StagingUploader(2, 8, UPLOADER_THREAD);
        RecordingChannel running = new RecordingChannel("a");
        // Ignores the interrupt close() sends, so only a close that waits could see it end.
        running.holdWritesUninterruptibly();
        RecordingChannel queued = new RecordingChannel("b");
        StagingUploader.UploadOutputStream first = uploader.open(running);
        StagingUploader.UploadOutputStream second = uploader.open(queued);
        first.write(bytes("aa"));
        assertThat(running.awaitHeld()).isTrue();
        second.write(bytes("b"));
        second.close();

        uploader.close();

        assertThat(running.hasExited()).isFalse();
        running.release();
        assertThat(running.awaitExited()).isTrue();
        // Once the thread has ended, a queued close that had survived the close would have run.
        awaitUploaderThreadsEnded(UPLOADER_THREAD);
        // The running chunk finished; the queued close never ran, so its object was never
        // finalized.
        assertThat(events).containsExactly("a write 2 on " + UPLOADER_THREAD);
        assertThat(queued.isClosed()).isFalse();
    }

    @Test
    void aSubmissionAfterCloseIsRefused() throws Exception {
        uploader = new StagingUploader(2, 1, UPLOADER_THREAD);
        StagingUploader.UploadOutputStream stream = uploader.open(new RecordingChannel("a"));
        uploader.close();

        assertThatThrownBy(() -> stream.write(bytes("aa")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        assertThatThrownBy(uploader::drain)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("closed");
        assertThat(events).isEmpty();
    }

    @Test
    void aDetachedFileSendsItsChunksAndClosesOnTheCallingThread() throws Exception {
        uploader = new StagingUploader(4, 8, UPLOADER_THREAD);
        RecordingChannel channel = new RecordingChannel("a");
        StagingUploader.UploadOutputStream stream = uploader.open(channel);
        stream.write(bytes("012345"));
        uploader.drain();
        String caller = Thread.currentThread().getName();

        stream.detach();
        stream.write(bytes("6789"));
        stream.close();

        // The partial chunk stayed in the stream through the detach and goes out with the next
        // full chunk and the close, all on the caller.
        assertThat(events)
                .containsExactly(
                        "a write 4 on " + UPLOADER_THREAD,
                        "a write 4 on " + caller,
                        "a write 2 on " + caller,
                        "a close on " + caller);
        assertThat(channel.content()).isEqualTo("0123456789");
        assertThat(stream.finalized()).isNotDone();
    }

    /**
     * Waits for every uploader thread with this name prefix to end, so that anything a stopped
     * uploader still ran has happened before the assertions that follow.
     */
    private static void awaitUploaderThreadsEnded(String prefix) throws InterruptedException {
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.getName().startsWith(prefix)) {
                thread.join(TimeUnit.SECONDS.toMillis(5));
                assertThat(thread.isAlive()).as(thread.getName()).isFalse();
            }
        }
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.US_ASCII);
    }

    /** One write on another thread, observed from the test thread. */
    private static final class BackgroundWrite {
        private final Thread thread;
        private final CountDownLatch returned = new CountDownLatch(1);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicBoolean interruptedAfterwards = new AtomicBoolean();

        private BackgroundWrite(StagingUploader.UploadOutputStream stream, byte[] bytes) {
            this.thread =
                    new Thread(
                            () -> {
                                try {
                                    stream.write(bytes);
                                } catch (Throwable t) {
                                    failure.set(t);
                                } finally {
                                    interruptedAfterwards.set(
                                            Thread.currentThread().isInterrupted());
                                    returned.countDown();
                                }
                            });
        }

        static BackgroundWrite start(StagingUploader.UploadOutputStream stream, byte[] bytes) {
            BackgroundWrite write = new BackgroundWrite(stream, bytes);
            write.thread.start();
            return write;
        }

        boolean awaitReturned(long millis) throws InterruptedException {
            return returned.await(millis, TimeUnit.MILLISECONDS);
        }

        Throwable failure() {
            return failure.get();
        }

        boolean interruptedAfterwards() {
            return interruptedAfterwards.get();
        }
    }

    /** Records every write and close with its thread; can hold or fail its writes. */
    private final class RecordingChannel implements WritableByteChannel {
        private final String name;
        private final ByteArrayOutputStream content = new ByteArrayOutputStream();
        private final CountDownLatch held = new CountDownLatch(1);
        private final CountDownLatch exited = new CountDownLatch(1);
        private volatile CountDownLatch gate;
        private volatile boolean uninterruptible;
        private volatile Throwable writeFailure;
        private volatile boolean closed;

        RecordingChannel(String name) {
            this.name = name;
        }

        void holdWrites() {
            gate = new CountDownLatch(1);
        }

        void holdWritesUninterruptibly() {
            uninterruptible = true;
            holdWrites();
        }

        void release() {
            gate.countDown();
        }

        boolean awaitHeld() throws InterruptedException {
            return held.await(5, TimeUnit.SECONDS);
        }

        boolean awaitExited() throws InterruptedException {
            return exited.await(5, TimeUnit.SECONDS);
        }

        boolean hasExited() {
            return exited.getCount() == 0;
        }

        void failWrites(Throwable failure) {
            writeFailure = failure;
        }

        boolean isClosed() {
            return closed;
        }

        synchronized String content() {
            return new String(content.toByteArray(), StandardCharsets.US_ASCII);
        }

        @Override
        public int write(ByteBuffer source) throws IOException {
            try {
                awaitGate();
                Throwable failure = writeFailure;
                if (failure instanceof IOException) {
                    throw (IOException) failure;
                }
                if (failure instanceof Error) {
                    throw (Error) failure;
                }
                int length = source.remaining();
                byte[] copy = new byte[length];
                source.get(copy);
                synchronized (this) {
                    content.write(copy, 0, length);
                }
                events.add(name + " write " + length + " on " + Thread.currentThread().getName());
                return length;
            } finally {
                exited.countDown();
            }
        }

        private void awaitGate() throws IOException {
            CountDownLatch current = gate;
            if (current == null) {
                return;
            }
            held.countDown();
            boolean interrupted = false;
            try {
                while (true) {
                    try {
                        if (!current.await(5, TimeUnit.SECONDS)) {
                            throw new IOException("Timed out holding a write");
                        }
                        return;
                    } catch (InterruptedException e) {
                        if (!uninterruptible) {
                            Thread.currentThread().interrupt();
                            throw new IOException("Interrupted holding a write", e);
                        }
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public boolean isOpen() {
            return !closed;
        }

        @Override
        public void close() {
            closed = true;
            events.add(name + " close on " + Thread.currentThread().getName());
        }
    }
}
