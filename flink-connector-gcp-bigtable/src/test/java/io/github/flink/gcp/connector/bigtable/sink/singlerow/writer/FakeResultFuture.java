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
import org.apache.flink.util.function.ThrowingRunnable;

import javax.annotation.Nullable;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

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
 * <p>The first completion is the one that counts, as in the operator: Flink's result handler guards
 * itself with a compare-and-set and drops every later completion silently, and its retry delegator
 * drops a repeated completion of one attempt through its awaiting flag until a retry or the timeout
 * resets it. So {@link #results()}, {@link #failure()} and {@link #isDone()} read the first
 * completion, {@link #completions()} counts them all, and {@link #failures()} lists the exceptional
 * ones in order, which is what lets a test assert what a second completion carried.
 *
 * <p>{@link #rejectCompletions()} makes every completion throw {@link RejectedExecutionException},
 * as Flink's does once the task mailbox is quiesced or closed: the operator's result handler hands
 * the completion to the mailbox, and the executor throws to the completing thread. {@link
 * #onNextCompletion(ThrowingRunnable)} runs a hook inside the next completion, before it is
 * recorded, to interleave a timeout with an answer's hand-off.
 */
final class FakeResultFuture<OUT> {

    private final List<Completion<OUT>> completions =
            Collections.synchronizedList(new ArrayList<>());
    private final ResultFuture<OUT> proxy = newProxy();
    private volatile boolean rejecting;
    @Nullable private volatile ThrowingRunnable<Exception> hook;

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
                                    if (arguments[0] instanceof Collection) {
                                        record(
                                                new ArrayList<>((Collection<OUT>) arguments[0]),
                                                null);
                                        return null;
                                    }
                                    throw new UnsupportedOperationException(
                                            "The function completes with a Collection.");
                                case "completeExceptionally":
                                    record(null, (Throwable) arguments[0]);
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

    private void record(@Nullable List<OUT> results, @Nullable Throwable failure) {
        if (rejecting) {
            throw new RejectedExecutionException("MailboxExecutor is shut down.");
        }
        ThrowingRunnable<Exception> interleaved = hook;
        if (interleaved != null) {
            // One-shot, and cleared before it runs: a hook that completes this result re-enters
            // here and must not run itself again.
            hook = null;
            try {
                interleaved.run();
            } catch (Exception e) {
                throw new IllegalStateException("The completion hook failed.", e);
            }
        }
        completions.add(new Completion<>(results, failure));
    }

    /** From now on, every completion throws as the mailbox of a finishing or failing task does. */
    void rejectCompletions() {
        rejecting = true;
    }

    /**
     * Runs {@code hook} inside the next completion of this result, before that completion is
     * recorded — on the completing thread, with the function's hand-off in progress.
     */
    void onNextCompletion(ThrowingRunnable<Exception> hook) {
        this.hook = hook;
    }

    boolean isDone() {
        return !completions.isEmpty();
    }

    /**
     * How many times the function completed this result, whichever completion the operator kept.
     */
    int completions() {
        return completions.size();
    }

    /** The results of the first completion, or {@code null} if there is none or it failed. */
    @Nullable
    List<OUT> results() {
        return completions.isEmpty() ? null : completions.get(0).results;
    }

    /** The failure of the first completion, or {@code null} if there is none or it succeeded. */
    @Nullable
    Throwable failure() {
        return completions.isEmpty() ? null : completions.get(0).failure;
    }

    /** Every exceptional completion, in order, including those the operator would drop. */
    List<Throwable> failures() {
        List<Throwable> failures = new ArrayList<>();
        synchronized (completions) {
            for (Completion<OUT> completion : completions) {
                if (completion.failure != null) {
                    failures.add(completion.failure);
                }
            }
        }
        return failures;
    }

    private static final class Completion<OUT> {

        @Nullable private final List<OUT> results;
        @Nullable private final Throwable failure;

        private Completion(@Nullable List<OUT> results, @Nullable Throwable failure) {
            this.results = results;
            this.failure = failure;
        }
    }
}
