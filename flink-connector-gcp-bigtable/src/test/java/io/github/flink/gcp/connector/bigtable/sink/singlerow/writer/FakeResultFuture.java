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

import org.apache.flink.streaming.api.functions.async.ResultFuture;

import javax.annotation.Nullable;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link ResultFuture} that records how it was completed, readable from the test thread after a
 * completion from a client thread.
 *
 * <p>A reflective proxy rather than an implementation: Flink 2.x added an abstract {@code
 * complete(CollectionSupplier)} overload that 1.20 does not have, so no one class implements the
 * interface on both supported lines. The function under test calls only the two overloads the lines
 * share; the proxy refuses the third. One proxy per fake, as the operator hands one handler to both
 * {@code asyncInvoke} and {@code timeout}: the function's ledger is keyed by that identity.
 *
 * <p>{@link #rejectCompletions()} makes every completion throw {@link RejectedExecutionException},
 * as Flink's does once the task mailbox is quiesced or closed: the operator's result handler hands
 * the completion to the mailbox, and the executor throws to the completing thread.
 */
final class FakeResultFuture<OUT> {

    private final AtomicInteger completions = new AtomicInteger();
    private final ResultFuture<OUT> proxy = newProxy();
    private volatile boolean rejecting;
    @Nullable private volatile List<OUT> results;
    @Nullable private volatile Throwable failure;

    ResultFuture<OUT> asResultFuture() {
        return proxy;
    }

    @SuppressWarnings("unchecked")
    private ResultFuture<OUT> newProxy() {
        return (ResultFuture<OUT>)
                Proxy.newProxyInstance(
                        ResultFuture.class.getClassLoader(),
                        new Class<?>[] {ResultFuture.class},
                        (proxy, method, arguments) -> {
                            switch (method.getName()) {
                                case "complete":
                                    rejectIfClosed();
                                    if (arguments[0] instanceof Collection) {
                                        completions.incrementAndGet();
                                        results = new ArrayList<>((Collection<OUT>) arguments[0]);
                                        return null;
                                    }
                                    throw new UnsupportedOperationException(
                                            "The function completes with a Collection.");
                                case "completeExceptionally":
                                    rejectIfClosed();
                                    completions.incrementAndGet();
                                    failure = (Throwable) arguments[0];
                                    return null;
                                case "toString":
                                    return "FakeResultFuture";
                                case "hashCode":
                                    return System.identityHashCode(proxy);
                                case "equals":
                                    return proxy == arguments[0];
                                default:
                                    throw new UnsupportedOperationException(
                                            "Unexpected ResultFuture call: " + method.getName());
                            }
                        });
    }

    /** From now on, every completion throws as the mailbox of a finishing or failing task does. */
    void rejectCompletions() {
        rejecting = true;
    }

    private void rejectIfClosed() {
        if (rejecting) {
            throw new RejectedExecutionException("MailboxExecutor is shut down.");
        }
    }

    boolean isDone() {
        return completions.get() > 0;
    }

    /** How many times the function completed this result; more than once is a defect. */
    int completions() {
        return completions.get();
    }

    @Nullable
    List<OUT> results() {
        return results;
    }

    @Nullable
    Throwable failure() {
        return failure;
    }
}
