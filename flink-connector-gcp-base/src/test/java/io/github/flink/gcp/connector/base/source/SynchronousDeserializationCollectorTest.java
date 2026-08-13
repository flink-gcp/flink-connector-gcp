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

package io.github.flink.gcp.connector.base.source;

import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SynchronousDeserializationCollectorTest {

    @Test
    void forwardsEachRecordDirectlyAndReturnsTheSuccessfulCount() throws Exception {
        List<String> events = new ArrayList<>();
        AtomicReference<Collector<String>> retained = new AtomicReference<>();

        long emittedCount =
                SynchronousDeserializationCollector.<String, Exception>deserialize(
                        record -> events.add("output:" + record),
                        out -> {
                            retained.set(out);
                            events.add("before-one");
                            out.collect("one");
                            events.add("before-two");
                            out.collect("two");
                            events.add("after-two");
                        });

        assertThat(events)
                .containsExactly(
                        "before-one", "output:one", "before-two", "output:two", "after-two");
        assertThat(emittedCount).isEqualTo(2);
        assertThatThrownBy(() -> retained.get().collect("late"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
    }

    @Test
    void returnsZeroWhenTheDeserializerEmitsNothing() throws Exception {
        long emittedCount =
                SynchronousDeserializationCollector.<String, Exception>deserialize(
                        ignored -> {
                            throw new AssertionError("output must not be called");
                        },
                        out -> {});

        assertThat(emittedCount).isZero();
    }

    @Test
    void rejectsNullBeforeCallingTheOutput() {
        List<String> records = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                SynchronousDeserializationCollector
                                        .<String, RuntimeException>deserialize(
                                                records::add, out -> out.collect(null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("must not collect null");
        assertThat(records).isEmpty();
    }

    @Test
    void propagatesADownstreamFailureWithoutBufferingLaterRecords() {
        IllegalStateException failure = new IllegalStateException("downstream");
        List<String> attempted = new ArrayList<>();

        assertThatThrownBy(
                        () ->
                                SynchronousDeserializationCollector
                                        .<String, RuntimeException>deserialize(
                                                record -> {
                                                    attempted.add(record);
                                                    throw failure;
                                                },
                                                out -> {
                                                    out.collect("one");
                                                    out.collect("two");
                                                }))
                .isSameAs(failure);
        assertThat(attempted).containsExactly("one");
    }

    @Test
    void countsOnlyOutputsThatTheDownstreamAccepted() throws Exception {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();

        long emittedCount =
                SynchronousDeserializationCollector.<String, Exception>deserialize(
                        ignored -> {
                            throw new IllegalStateException("downstream");
                        },
                        out -> {
                            retained.set(out);
                            try {
                                out.collect("rejected");
                            } catch (IllegalStateException ignored) {
                                // The caller owns failure classification, including a schema that
                                // suppresses a downstream exception.
                            }
                        });

        assertThat(emittedCount).isZero();
        assertThatThrownBy(() -> retained.get().collect("late"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
    }

    @Test
    void releasesTheDownstreamReferenceAfterFailure() {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();

        assertThatThrownBy(
                        () ->
                                SynchronousDeserializationCollector.<String, Exception>deserialize(
                                        ignored -> {},
                                        out -> {
                                            retained.set(out);
                                            throw new Exception("schema");
                                        }))
                .hasMessage("schema");

        assertThatThrownBy(() -> retained.get().collect("late"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("only during its synchronous deserialize call");
    }
}
