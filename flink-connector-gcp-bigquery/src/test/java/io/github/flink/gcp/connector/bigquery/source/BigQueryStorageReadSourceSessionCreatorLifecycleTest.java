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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.connector.source.SplitEnumerator;

import io.github.flink.gcp.connector.bigquery.source.enumerator.BigQueryReadEnumeratorState;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ScriptedReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins that one read session creator belongs to one enumerator.
 *
 * <p>The JobManager keeps one source object for a job's whole life, and a coordinator reset builds
 * the next enumerator from it — so a creator carried on the source configuration would already have
 * been closed by the enumerator before it. Issue #990 was that in the Spanner batch source, and
 * this source had the same shape.
 *
 * <p>These tests therefore do the one thing nothing else does: two enumerators over <em>one</em>
 * source object, with a teardown between them. {@code BigQuerySourceBuilderTest} comes close and
 * misses it by cloning the source first, which hands each enumerator a fresh configuration.
 */
class BigQueryStorageReadSourceSessionCreatorLifecycleTest {

    @SuppressWarnings("unchecked")
    private static BigQueryStorageReadSource<GenericRecord> source(
            ScriptedReadSessionCreator.Factory creators) {
        return (BigQueryStorageReadSource<GenericRecord>)
                BigQuerySource.<GenericRecord>builder()
                        .table(TestSources.TABLE)
                        .deserializer(BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON))
                        .emulatorEndpoint("localhost:1")
                        .sessionCreatorFactory(creators)
                        .build();
    }

    @Test
    void aSecondEnumeratorCreatesItsSessionThroughItsOwnCreator() throws Exception {
        ScriptedReadSessionCreator.Factory creators =
                ScriptedReadSessionCreator.Factory.withStreams(2);
        BigQueryStorageReadSource<GenericRecord> source = source(creators);

        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> firstContext =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BigQueryReadStreamSplit, BigQueryReadEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }

        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BigQueryReadStreamSplit, BigQueryReadEnumeratorState> second =
                source.createEnumerator(context)) {
            second.start();
            context.runAsyncCalls();

            assertThat(second.snapshotState(1L).isInitialized())
                    .as(
                            "the second enumerator creates a session instead of meeting a closed"
                                    + " creator")
                    .isTrue();
        }
        assertThat(creators.minted()).hasSize(2);
        assertThat(creators.minted().get(0).creations())
                .as("the first enumerator created its session through its own creator")
                .isOne();
        assertThat(creators.minted().get(1).creations())
                .as("and the second through its own, not the first's")
                .isOne();
    }

    @Test
    void eachEnumeratorGetsItsOwnCreator() throws Exception {
        ScriptedReadSessionCreator.Factory creators =
                ScriptedReadSessionCreator.Factory.withStreams(1);
        BigQueryStorageReadSource<GenericRecord> source = source(creators);

        source.createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();
        SplitEnumerator<BigQueryReadStreamSplit, BigQueryReadEnumeratorState> second =
                source.createEnumerator(new FakeSplitEnumeratorContext<>(1));

        assertThat(creators.minted()).hasSize(2);
        assertThat(creators.minted().get(0)).isNotSameAs(creators.minted().get(1));
        assertThat(creators.minted().get(0).closes())
                .as("one teardown closed it once, rather than twice")
                .isOne();
        assertThat(creators.minted().get(0).isClosed())
                .as("the first enumerator's creator is the one its teardown ended")
                .isTrue();
        assertThat(creators.minted().get(1).isClosed())
                .as("the second enumerator's creator is untouched by that teardown")
                .isFalse();

        second.close();
    }

    @Test
    void aRestoreFromAnInitializedCheckpointMintsItsOwnCreatorAndOpensNothing() throws Exception {
        // The call the real restore makes, with the state it really carries. Passing a null
        // checkpoint would make restoreEnumerator behave exactly like createEnumerator, so it
        // would assert nothing the test above does not.
        ScriptedReadSessionCreator.Factory creators =
                ScriptedReadSessionCreator.Factory.withStreams(1);
        BigQueryStorageReadSource<GenericRecord> source = source(creators);

        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> firstContext =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BigQueryReadStreamSplit, BigQueryReadEnumeratorState> first =
                source.createEnumerator(firstContext)) {
            first.start();
            firstContext.runAsyncCalls();
        }

        BigQueryReadEnumeratorState initialized =
                new BigQueryReadEnumeratorState(
                        true,
                        ScriptedReadSessionCreator.SESSION,
                        Instant.parse("2099-01-01T00:00:00Z"),
                        Collections.emptyList());
        FakeSplitEnumeratorContext<BigQueryReadStreamSplit> context =
                new FakeSplitEnumeratorContext<>(1);
        try (SplitEnumerator<BigQueryReadStreamSplit, BigQueryReadEnumeratorState> restored =
                source.restoreEnumerator(context, initialized)) {
            restored.start();
            context.runAsyncCalls();

            assertThat(restored.snapshotState(1L).isInitialized())
                    .as("the restore adopts the checkpointed session")
                    .isTrue();
        }

        assertThat(creators.minted())
                .as("the restore mints a creator of its own rather than reusing a closed one")
                .hasSize(2);
        assertThat(creators.minted().get(1).creations())
                .as("and creates no session through it: only the first enumerator did")
                .isZero();
        assertThat(creators.minted().get(1).closes())
                .as("the creator it minted and never used is still closed exactly once")
                .isOne();
    }

    /**
     * Pins the window minting opens: between {@code create()} and the enumerator taking ownership,
     * the source is the only thing that can release the creator.
     */
    @Test
    void theSourceClosesACreatorItCouldNotHandOver() {
        ScriptedReadSessionCreator.Factory creators =
                ScriptedReadSessionCreator.Factory.withStreams(1);
        BigQueryStorageReadSource<GenericRecord> source = source(creators);

        assertThatThrownBy(() -> source.createEnumerator(null))
                .isInstanceOf(NullPointerException.class);

        assertThat(creators.minted()).hasSize(1);
        assertThat(creators.only().isClosed())
                .as("a creator the enumerator never took is released by the source")
                .isTrue();
    }
}
