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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.function.Consumer;

/**
 * Runs one source deserialization call with a direct, non-null collector.
 *
 * <p>The collector forwards each record immediately instead of buffering it. The deserializer must
 * call it synchronously on the task thread and must not retain it. The downstream function is
 * cleared in {@code finally}, so retained use after the call neither emits a record nor retains
 * Flink's output or an input-specific timestamp or metadata closure.
 */
@Internal
public final class SynchronousDeserializationCollector {

    private static final String LIFECYCLE_VIOLATION_MESSAGE =
            "A source deserializer may collect only during its synchronous deserialize call.";
    private static final String NULL_RECORD_MESSAGE =
            "A source deserializer must not collect null; skip an input by collecting no records.";

    /** One deserialization call that emits through the supplied collector. */
    @FunctionalInterface
    public interface Invocation<T, E extends Exception> {
        void deserialize(Collector<T> out) throws E;
    }

    private SynchronousDeserializationCollector() {}

    /**
     * Runs one deserialization call and returns the number of records forwarded successfully.
     *
     * <p>The caller owns failure classification and source-progress updates. This method does not
     * catch or retain downstream failures.
     */
    public static <T, E extends Exception> long deserialize(
            Consumer<T> output, Invocation<T, E> invocation) throws E {
        Preconditions.checkNotNull(invocation, "invocation must not be null");
        DirectCollector<T> collector = new DirectCollector<>(output);
        try {
            invocation.deserialize(collector);
            return collector.emittedCount;
        } finally {
            collector.release();
        }
    }

    private static final class DirectCollector<T> implements Collector<T> {

        @Nullable private Consumer<T> output;
        private long emittedCount;

        private DirectCollector(Consumer<T> output) {
            this.output = Preconditions.checkNotNull(output, "output must not be null");
        }

        @Override
        public void collect(T record) {
            Consumer<T> currentOutput = output;
            Preconditions.checkState(currentOutput != null, LIFECYCLE_VIOLATION_MESSAGE);
            Preconditions.checkNotNull(record, NULL_RECORD_MESSAGE);
            currentOutput.accept(record);
            emittedCount++;
        }

        @Override
        public void close() {}

        private void release() {
            output = null;
        }
    }
}
