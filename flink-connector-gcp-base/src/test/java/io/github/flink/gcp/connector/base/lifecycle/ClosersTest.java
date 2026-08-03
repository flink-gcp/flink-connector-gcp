/*
 * Copyright 2026 laughingman7743
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

/** Tests for {@link Closers}. */
class ClosersTest {

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
        // The property this borrows from IOUtils.closeAll and relies on: a resource is not left
        // open because an earlier one refused to close. Both leak otherwise.
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
        // The case that decides Throwable.class over the one-argument closeAll: with
        // Exception.class an Error is rethrown from inside the loop, so the second resource stays
        // open and this method never returns — the caller's rethrow is skipped and its own failure
        // is replaced. A NoClassDefFoundError from a client's first classload is the realistic
        // shape, and it repeats on every restart attempt.
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
        assertThat(failure.getSuppressed()).hasSize(1);
        assertThat(failure.getSuppressed()[0]).hasCauseInstanceOf(NoClassDefFoundError.class);
    }
}
