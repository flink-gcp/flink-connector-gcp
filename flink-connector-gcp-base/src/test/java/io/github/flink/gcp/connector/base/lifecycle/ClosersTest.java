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

package io.github.flink.gcp.connector.base.lifecycle;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link Closers}. */
class ClosersTest {

    @Test
    void closeAllClosesEveryResourceAndSkipsNulls() throws Exception {
        List<String> closed = new ArrayList<>();

        Closers.closeAll(List.<AutoCloseable>of(() -> closed.add("a"), () -> closed.add("b")));
        Closers.closeAll(() -> closed.add("c"), null, () -> closed.add("d"));

        assertThat(closed).containsExactly("a", "b", "c", "d");
    }

    @Test
    void closeAllReportsTheFirstFailureWithTheLaterOnesSuppressedOntoIt() {
        // Every resource is closed before anything is reported: a writer puts its failure handler
        // last precisely because an earlier resource refusing to close must not skip it.
        List<String> closed = new ArrayList<>();
        IllegalStateException first = new IllegalStateException("first refuses");
        IllegalStateException second = new IllegalStateException("second refuses");

        assertThatThrownBy(
                        () ->
                                Closers.closeAll(
                                        () -> {
                                            throw first;
                                        },
                                        () -> {
                                            throw second;
                                        },
                                        () -> closed.add("third")))
                .isSameAs(first)
                .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(second));
        assertThat(closed).containsExactly("third");
    }

    @Test
    void closeAllRethrowsAnErrorAsAnErrorHavingClosedTheRest() {
        // The case this loop exists for, and it discriminates against both alternatives. Flink's
        // IOUtils.closeAll with Exception.class — what its one-argument form passes — rethrows the
        // Error from inside the loop, leaving the second resource open (#276). With
        // Throwable.class it closes everything but hands back new Exception(e), and Flink's
        // Task.preProcessException tests the throwable itself, so a wrapped OutOfMemoryError from
        // a teardown fails the task into a restart loop instead of halting the JVM.
        List<String> closed = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                Closers.closeAll(
                                        () -> {
                                            throw new NoClassDefFoundError("first blows up");
                                        },
                                        () -> closed.add("second")))
                .isInstanceOf(NoClassDefFoundError.class)
                .hasMessage("first blows up");
        assertThat(closed).containsExactly("second");
    }

    @Test
    void closesEveryResourceAndSkipsNulls() {
        List<String> closed = new ArrayList<>();
        IOException failure = new IOException("original");

        Closers.closeAllSuppressing(failure, () -> closed.add("a"), null, () -> closed.add("b"));

        assertThat(closed).containsExactly("a", "b");
        assertThat(failure.getSuppressed()).isEmpty();
    }

    @Test
    void aCloseFailureIsSuppressedOntoTheFailureRatherThanReplacingIt() {
        IOException failure = new IOException("original");
        IllegalStateException closeFailure = new IllegalStateException("close blew up");

        Closers.closeAllSuppressing(
                failure,
                () -> {
                    throw closeFailure;
                });

        // The caller rethrows its own exception: what went wrong is why the resources are being
        // released, so a close failure must not take its place.
        assertThat(failure).hasMessage("original");
        assertThat(failure.getSuppressed()).containsExactly(closeFailure);
    }

    @Test
    void aFailingCloseDoesNotStopTheOnesAfterIt() {
        // The property this borrows from closeAll and relies on: a resource is not left open
        // because an earlier one refused to close. Both leak otherwise.
        List<String> closed = new ArrayList<>();
        IOException failure = new IOException("original");

        Closers.closeAllSuppressing(
                failure,
                () -> {
                    throw new IllegalStateException("first refuses");
                },
                () -> closed.add("second"));

        assertThat(closed).containsExactly("second");
        assertThat(failure.getSuppressed()).hasSize(1);
    }

    @Test
    void anErrorFromACloseStillLeavesNothingOpenAndDoesNotEscape() {
        // The case that decides catching Throwable here: closeAll rethrows an Error as an Error,
        // so catching Exception would let it escape — the second resource stays open, this method
        // never returns, and the caller's rethrow is skipped with its own failure replaced. A
        // NoClassDefFoundError from a client's first classload is the realistic shape, and it
        // repeats on every restart attempt.
        List<String> closed = new ArrayList<>();
        IOException failure = new IOException("original");

        Closers.closeAllSuppressing(
                failure,
                () -> {
                    throw new NoClassDefFoundError("first blows up");
                },
                () -> closed.add("second"));

        assertThat(closed).containsExactly("second");
        assertThat(failure).hasMessage("original");
        // The Error arrives as itself rather than wrapped: closeAll preserves its type, and
        // nothing here has a reason to take it away again.
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0]).isInstanceOf(NoClassDefFoundError.class);
    }

    @Test
    void aJvmFatalCloseFailureTakesTheTopSlotSoFlinkStillSeesIt() {
        // The one exception to "the caller's failure is never replaced". Task.preProcessException
        // inspects only the throwable it is handed, so an OutOfMemoryError arriving as a
        // suppressed entry is one nothing halts on — which silently overrides whatever the
        // operator set taskmanager.jvm-exit-on-oom to. The test above is the other side of the
        // same rule: NoClassDefFoundError is not in Flink's fatal set and stays suppressed, so
        // the escalation is narrow rather than "any Error wins".
        List<String> closed = new ArrayList<>();
        IOException failure = new IOException("original");
        OutOfMemoryError fatal = new OutOfMemoryError("heap gone");

        assertThatThrownBy(
                        () ->
                                Closers.closeAllSuppressing(
                                        failure,
                                        () -> {
                                            throw fatal;
                                        },
                                        () -> closed.add("second")))
                .isSameAs(fatal)
                .satisfies(e -> assertThat(e.getSuppressed()).containsExactly(failure));
        // Escalated after the loop, not from inside it, so nothing is left open by it either.
        assertThat(closed).containsExactly("second");
    }
}
