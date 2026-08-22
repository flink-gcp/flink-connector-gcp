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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import com.google.cloud.bigtable.data.v2.models.ChangeStreamRecordAdapter;
import com.google.cloud.bigtable.data.v2.models.Value;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for what {@link BigtableChangeStreamMutationConverter}'s entry chains are written against.
 */
class BigtableChangeStreamSdkEntrySurfaceTest {

    @Test
    void theEntryKindsTheConverterIsWrittenAgainstAreStillTheOnesTheClientBuilds() {
        // The connector's own entry hierarchy dispatches through a visitor (ADR-0126), so a subtype
        // added there breaks the build. Nothing holds the *client's* side: the converter takes
        // ChangeStreamMutation.getEntries() and branches on the SDK's Entry with `instanceof`,
        // ending in a throw, so an entry kind the client grows is a running job's failure rather
        // than a build's.
        //
        // The SDK's Entry is a marker interface with no methods, so its implementations cannot be
        // enumerated without scanning the classpath — but they do not have to be. Every entry kind
        // the client can build is a method on this builder, which is the surface
        // ChangeStreamMutation is assembled through, so a new kind arrives here first. Pinning the
        // whole interface rather than the five entry-building methods is deliberate: any change to
        // what the reader's input surface offers is worth a look, and the module reference asks for
        // that surface to be reread on every client upgrade.
        //
        // This is not hypothetical, and the growth it would have caught is measurable: by javap,
        // the interface carries eleven methods at 2.20.1 and thirteen at 2.45.1 and 2.81.0, the
        // two added being addToCell and mergeToCell — exactly the entry kinds whose absence from
        // the converter would have been a runtime failure. The same release grew Mutation
        // .MutationCase from five constants to seven, which BigtableWriterMutationCaseTest pins
        // for the sink; this is the reader's half of that one bump.
        //
        // The arrival vector is a libraries-bom bump, which is where this assertion spends itself
        // instead of a job doing so. If it fires, extend BigtableChangeStreamMutationConverter's
        // chains — familyName, qualifier, validateEntry and convertEntry, all four — and decide
        // whether the connector-owned model needs the new kind too.
        assertThat(ChangeStreamRecordAdapter.ChangeStreamRecordBuilder.class.getDeclaredMethods())
                .extracting(Method::getName)
                .containsExactlyInAnyOrder(
                        // Builds one entry each; these are the five the converter branches on.
                        "deleteFamily",
                        "deleteCells",
                        "addToCell",
                        "mergeToCell",
                        "startCell",
                        // SetCell arrives across three calls, of which startCell is the first.
                        "cellValue",
                        "finishCell",
                        // Record and mutation framing, not entries.
                        "onHeartbeat",
                        "onCloseStream",
                        "startUserMutation",
                        "startGcMutation",
                        "finishChangeStreamMutation",
                        "reset");
    }

    @Test
    void theAggregateValueTypesTheConverterIsWrittenAgainstAreStillTheOnesTheClientHas() {
        // The client's aggregate Value, unlike its Entry, does carry a discriminator, so this half
        // needs no proxy: three constants at 2.45.1, where the hierarchy first appeared, and three
        // at 2.81.0. AddToCell and MergeToCell carry three Values each, and the converter reads
        // every one of them by `instanceof`.
        assertThat(Value.ValueType.values())
                .extracting(Enum::name)
                .containsExactlyInAnyOrder("RawValue", "RawTimestamp", "Int64");
    }
}
