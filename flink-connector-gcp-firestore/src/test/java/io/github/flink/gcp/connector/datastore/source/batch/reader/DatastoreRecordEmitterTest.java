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

package io.github.flink.gcp.connector.datastore.source.batch.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.util.Collector;

import com.google.cloud.Timestamp;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;
import io.github.flink.gcp.connector.datastore.source.batch.FetchedEntity;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplit;
import io.github.flink.gcp.connector.datastore.source.batch.QuerySplitState;
import io.github.flink.gcp.connector.datastore.source.batch.enumerator.ScriptedQueryPlanner;
import io.github.flink.gcp.connector.datastore.source.serializer.DatastoreEntityDeserializationSchema;
import io.github.flink.gcp.connector.testutils.CollectingSourceOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link DatastoreRecordEmitter}. */
@Timeout(30)
class DatastoreRecordEmitterTest {

    private static final Timestamp READ_TIME = Timestamp.ofTimeSecondsAndNanos(1_700_000_000L, 0);

    private final TestReaderMetrics metrics = new TestReaderMetrics();

    private static QuerySplitState state() {
        return new QuerySplitState(
                new QuerySplit("0", ScriptedQueryPlanner.request("K"), READ_TIME));
    }

    private static FetchedEntity entity(String name) {
        return new FetchedEntity(
                Entity.newBuilder(Key.newBuilder("p", "K", name).build()).build(),
                ScriptedQueryPageReader.cursorAfter(name));
    }

    private DatastoreRecordEmitter<String> emitter(Deserializer deserializer) {
        return new DatastoreRecordEmitter<>(deserializer, metrics.metrics());
    }

    @Test
    void emitsTheRecordsAnEntityProducedAndAdvancesTheSplit() throws Exception {
        CollectingSourceOutput<String> output = new CollectingSourceOutput<>();
        QuerySplitState state = state();

        emitter(
                        (entity, out) -> {
                            out.collect(entity.getKey().getName() + "#0");
                            out.collect(entity.getKey().getName() + "#1");
                        })
                .emitRecord(entity("a"), output, state);

        assertThat(output.records()).containsExactly("a#0", "a#1");
        assertThat(state.getEmitted()).isOne();
        assertThat(metrics.counter(DatastoreMetricNames.RECORDS_SKIPPED)).isZero();
    }

    @Test
    void anEntityThatProducedNothingIsSkippedAndStillPassed() throws Exception {
        QuerySplitState state = state();

        emitter((entity, out) -> {}).emitRecord(entity("a"), new CollectingSourceOutput<>(), state);

        assertThat(metrics.counter(DatastoreMetricNames.RECORDS_SKIPPED)).isOne();
        assertThat(state.getEmitted())
                .as("the resume point moves past a skipped entity too")
                .isOne();
        assertThat(state.toSplit().getRequest().getQuery().getStartCursor())
                .isEqualTo(ScriptedQueryPageReader.cursorAfter("a"));
    }

    @Test
    void leavesTheSplitWhereItWasWhenTheDeserializerThrows() {
        QuerySplitState state = state();

        assertThatThrownBy(
                        () ->
                                emitter(
                                                (entity, out) -> {
                                                    throw new IOException("bad entity");
                                                })
                                        .emitRecord(
                                                entity("a"), new CollectingSourceOutput<>(), state))
                .isInstanceOf(IOException.class);
        assertThat(state.getEmitted()).isZero();
    }

    @Test
    void refusesACollectorUsedOutsideTheCallItWasHandedTo() throws Exception {
        AtomicReference<Collector<String>> retained = new AtomicReference<>();
        DatastoreRecordEmitter<String> emitter =
                emitter(
                        (entity, out) -> {
                            if (retained.get() == null) {
                                retained.set(out);
                            } else {
                                retained.get().collect("late");
                            }
                        });
        emitter.emitRecord(entity("a"), new CollectingSourceOutput<>(), state());

        assertThatThrownBy(
                        () ->
                                emitter.emitRecord(
                                        entity("b"), new CollectingSourceOutput<>(), state()))
                .isInstanceOf(IllegalStateException.class);
    }

    /** A deserializer written as a lambda. */
    @FunctionalInterface
    private interface Deserializer extends DatastoreEntityDeserializationSchema<String> {

        @Override
        default TypeInformation<String> getProducedType() {
            return Types.STRING;
        }

        @Override
        void deserialize(Entity entity, Collector<String> out) throws IOException;
    }
}
