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

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The branches of {@link ConvertedAnswer} that a request-type test does not enter. */
class ConvertedAnswerTest {

    @Test
    void aConversionThatThrowsFailsTheAnswerWithWhatItThrew() {
        FakeAnswerFuture<String> answer = new FakeAnswerFuture<>();
        IllegalStateException failure = new IllegalStateException("not convertible");
        ConvertedAnswer<String, Integer> converted =
                new ConvertedAnswer<>(
                        answer,
                        value -> {
                            throw failure;
                        });

        answer.set("x");

        assertThat(converted.isDone()).isTrue();
        assertThat(converted.isCancelled()).isFalse();
        assertThatThrownBy(converted::get).isInstanceOf(ExecutionException.class).hasCause(failure);
    }

    @Test
    void aFailedAnswerFailsTheConversionWithTheSameCause() throws Exception {
        FakeAnswerFuture<String> answer = new FakeAnswerFuture<>();
        RuntimeException failure = new RuntimeException("service said no");
        ConvertedAnswer<String, Integer> converted = new ConvertedAnswer<>(answer, String::length);

        answer.setException(failure);

        assertThatThrownBy(converted::get).isInstanceOf(ExecutionException.class).hasCause(failure);
    }

    @Test
    void aCancelThatLostTheRaceToTheAnswerReturnsFalseAndForwardsNothing() throws Exception {
        FakeAnswerFuture<String> answer = new FakeAnswerFuture<>();
        ConvertedAnswer<String, Integer> converted = new ConvertedAnswer<>(answer, String::length);
        answer.set("four");

        assertThat(converted.cancel(true)).isFalse();

        assertThat(converted.get()).isEqualTo(4);
        assertThat(answer.upstreamCancelled()).isFalse();
    }

    @Test
    void aSecondCancelForwardsNothingMore() {
        // The client's own cancellation callback re-enters cancel(false) from inside the forward;
        // the forward must have resolved this future first, so that re-entry is the no-op below.
        FakeAnswerFuture<String> answer = new FakeAnswerFuture<>();
        ConvertedAnswer<String, Integer> converted = new ConvertedAnswer<>(answer, String::length);

        assertThat(converted.cancel(true)).isTrue();
        assertThat(converted.cancel(true)).isFalse();

        assertThat(answer.upstreamCancelled()).isTrue();
        assertThat(converted.isCancelled()).isTrue();
    }
}
