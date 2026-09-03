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

package io.github.flink.gcp.connector.bigtable.sink.singlerow.writer;

import org.apache.flink.annotation.Internal;

import com.google.api.core.ApiFuture;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.sink.singlerow.RowOperation;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One accepted request: what a runtime needs to attribute its answer, cancel it at teardown, and
 * make sure it is released exactly once.
 *
 * <p>Identity-compared, never by value: two requests for the same row are two handles.
 */
@Internal
final class RequestHandle {

    final DestinationState state;
    final RowOperation operation;
    final ByteString rowKey;
    private final ApiFuture<?> future;

    /**
     * Whether an outcome has claimed this request. The async function has two racing outcomes — the
     * client's answer and Flink's operator timeout — and only the first may release and report it;
     * the sink writer has one, and settles for uniformity.
     */
    private final AtomicBoolean settled = new AtomicBoolean();

    RequestHandle(
            DestinationState state,
            RowOperation operation,
            ByteString rowKey,
            ApiFuture<?> future) {
        this.state = state;
        this.operation = operation;
        this.rowKey = rowKey;
        this.future = future;
    }

    /**
     * Claims the request for one outcome.
     *
     * @return {@code true} for the first caller only
     */
    boolean settle() {
        return settled.compareAndSet(false, true);
    }

    /**
     * Cancels the request. The client's future runs its failure callback synchronously inside this
     * call, with a {@code CancellationException}, so a caller that has settled the handle first
     * sees that callback do nothing.
     */
    void cancel() {
        future.cancel(true);
    }
}
