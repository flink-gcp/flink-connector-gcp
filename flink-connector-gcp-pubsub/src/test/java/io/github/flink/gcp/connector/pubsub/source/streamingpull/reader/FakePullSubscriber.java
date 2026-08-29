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

package io.github.flink.gcp.connector.pubsub.source.streamingpull.reader;

import org.apache.flink.util.ExceptionUtils;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

/**
 * Scriptable {@link PullSubscriber} standing in for a real streaming-pull client.
 *
 * <p>Everything touching the buffer is {@code synchronized}, as the production subscriber's is: the
 * SPI requires {@link #bufferUsage()} to be safe from any thread, and a fake that read the deque
 * unguarded would let a test sampling the buffer gauges while a fetcher drains fail inside {@code
 * ArrayDeque} rather than on an assertion.
 */
final class FakePullSubscriber implements PullSubscriber {

    private final Runnable dataAvailableSignal;
    private final Deque<PubsubMessage> messages = new ArrayDeque<>();

    @Nullable private IOException failure;
    private boolean closed;
    private boolean stopRequested;
    private boolean shutdownRequested;
    @Nullable private Throwable closeFailure;
    @Nullable private RuntimeException shutdownFailure;
    private Runnable onShutdown = () -> {};

    /** Set by {@link #recordCallsInto}; shared across the subscribers of one test. */
    @Nullable private List<String> calls;

    private String name = "";

    FakePullSubscriber(Runnable dataAvailableSignal) {
        this.dataAvailableSignal = dataAvailableSignal;
    }

    /** Names this subscriber in the recorded call order. */
    FakePullSubscriber named(String name) {
        this.name = name;
        return this;
    }

    /** Buffers a message as the client library would, and wakes a blocked fetch. */
    synchronized void deliver(PubsubMessage... delivered) {
        Collections.addAll(messages, delivered);
        dataAvailableSignal.run();
    }

    /**
     * Buffers {@code count} messages carrying a payload of {@code payloadBytes}, and returns what
     * they added to {@link #bufferUsage()}.
     *
     * <p>Returns the size rather than taking one, because the serialized size a payload costs
     * includes protobuf's own framing: a test that needs a byte bound the buffer crosses should
     * derive the bound from what was actually delivered, not from a number it hoped for.
     */
    synchronized long deliverSized(int count, int payloadBytes) {
        String payload = String.join("", Collections.nCopies(payloadBytes, "x"));
        long delivered = 0;
        for (int index = 0; index < count; index++) {
            PubsubMessage message =
                    PubsubMessage.newBuilder()
                            .setMessageId("sized-" + index)
                            .setData(ByteString.copyFromUtf8(payload))
                            .build();
            messages.addLast(message);
            delivered += message.getSerializedSize();
        }
        dataAvailableSignal.run();
        return delivered;
    }

    /**
     * Makes this subscriber report a permanent client failure. Sticky, as the real one is: {@code
     * permanentError} is never cleared, so every later {@link #pullMessages} and {@link
     * #checkFailure} reports the same failure.
     */
    void failWith(IOException failure) {
        this.failure = failure;
        dataAvailableSignal.run();
    }

    void failOnClose() {
        failOnClose(new IOException("close failed"));
    }

    /** Makes {@link #close()} throw the given failure — an {@code Error} is thrown as itself. */
    void failOnClose(Throwable closeFailure) {
        this.closeFailure = closeFailure;
    }

    /**
     * Makes {@link #shutdown()} throw. Unchecked only, because the SPI method declares nothing —
     * which is the whole reason #297 was a defect rather than a compile error.
     */
    void failOnShutdown(RuntimeException shutdownFailure) {
        this.shutdownFailure = shutdownFailure;
    }

    void runOnShutdown(Runnable onShutdown) {
        this.onShutdown = onShutdown;
    }

    boolean isClosed() {
        return closed;
    }

    boolean isShutdownRequested() {
        return shutdownRequested;
    }

    boolean isStopRequested() {
        return stopRequested;
    }

    /**
     * Records the order shutdowns and closes happened in, shared by every subscriber of one test.
     */
    void recordCallsInto(List<String> calls) {
        this.calls = calls;
    }

    @Override
    public synchronized List<PubsubMessage> pullMessages(int maxMessages) throws IOException {
        checkFailure();
        List<PubsubMessage> drained = new ArrayList<>();
        while (drained.size() < maxMessages && !messages.isEmpty()) {
            drained.add(messages.pollFirst());
        }
        return drained;
    }

    @Override
    public void checkFailure() throws IOException {
        if (failure != null) {
            throw failure;
        }
    }

    @Override
    public synchronized BufferUsage bufferUsage() {
        long bytes = 0;
        for (PubsubMessage message : messages) {
            bytes += message.getSerializedSize();
        }
        return BufferUsage.of(messages.size(), bytes);
    }

    @Override
    public synchronized void requestStop() {
        if (stopRequested) {
            return;
        }
        stopRequested = true;
        record("requestStop");
    }

    @Override
    public synchronized void shutdown() {
        if (shutdownRequested) {
            return;
        }
        shutdownRequested = true;
        record("shutdown");
        onShutdown.run();
        if (shutdownFailure != null) {
            // After the record, mirroring the production subscriber: it flips its own closed flag
            // and nacks before anything that could fail, so a failure here is a shutdown that
            // happened and then threw, not one that never started.
            throw shutdownFailure;
        }
    }

    @Override
    public void close() throws Exception {
        shutdown();
        closed = true;
        record("close");
        if (closeFailure != null) {
            // rethrowException, not rethrow: close() may throw a checked exception, and the
            // default failure is an IOException the existing test asserts on by type.
            ExceptionUtils.rethrowException(closeFailure);
        }
    }

    private void record(String call) {
        if (calls != null) {
            calls.add(call + ":" + name);
        }
    }
}
