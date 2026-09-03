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
import org.apache.flink.util.Preconditions;

import com.google.api.core.AbstractApiFuture;
import com.google.api.core.ApiFunction;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;

import java.util.concurrent.CancellationException;

/**
 * The client's answer with a conversion applied on the thread that delivers it, owning {@code
 * cancel} so that a cancel reaches the client's future through the client's own {@code cancel}.
 *
 * <p>{@code ApiFutures.transform} cannot be used here. api-common unwraps an {@code
 * AbstractApiFuture} input to its internal Guava future before transforming it, and Guava's cancel
 * propagation marks that internal future cancelled directly. The client's answer is an {@code
 * AbstractApiFuture} — {@code BigtableUnaryOperationCallable.UnaryFuture} — whose override of
 * {@code cancel} is the only path to cancelling the RPC on the wire, and it does not override
 * {@code interruptTask}; so a cancel through the transform ends the transformed future and leaves
 * the RPC running to completion or to its deadline. Forwarding from a listener would not repair it
 * either: Guava runs the cancel propagation before the listeners, so the client's {@code cancel}
 * would find its future already resolved and do nothing. The forward has to run from this future's
 * own {@code cancel}, before its state resolves anything downstream.
 *
 * <p>The conversion runs where the answer arrives — the gax thread — so no executor sits between
 * the client and the runtime's callback; a conversion that throws fails the answer with what it
 * threw, and the client's own cancellation reads as a cancellation here rather than as a failure
 * with a {@link CancellationException} cause.
 */
@Internal
final class ConvertedAnswer<S, R> extends AbstractApiFuture<R> {

    private final ApiFuture<S> answer;

    /**
     * Creates the converted answer.
     *
     * @param answer the client's answer
     * @param conversion the conversion, run on the thread that delivers the answer
     */
    ConvertedAnswer(ApiFuture<S> answer, ApiFunction<? super S, ? extends R> conversion) {
        this.answer = Preconditions.checkNotNull(answer, "answer must not be null");
        Preconditions.checkNotNull(conversion, "conversion must not be null");
        ApiFutures.addCallback(
                answer,
                new ApiFutureCallback<S>() {
                    @Override
                    public void onSuccess(S value) {
                        R converted;
                        try {
                            converted = conversion.apply(value);
                        } catch (RuntimeException e) {
                            setException(e);
                            return;
                        }
                        set(converted);
                    }

                    @Override
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof CancellationException) {
                            cancel(false);
                        } else {
                            setException(throwable);
                        }
                    }
                },
                Runnable::run);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        // Resolve this future first, so the client's cancellation callback — which arrives here
        // synchronously through onFailure — finds nothing left to do; then reach the client
        // through its own cancel. A cancel that lost the race to an answer forwards nothing, as
        // the client's future has already resolved too.
        if (!super.cancel(mayInterruptIfRunning)) {
            return false;
        }
        answer.cancel(mayInterruptIfRunning);
        return true;
    }
}
