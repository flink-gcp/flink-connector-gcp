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

package io.github.flink.gcp.connector.datastore.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import com.google.api.core.SettableApiFuture;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.datastore.v1.Key;
import com.google.datastore.v1.LookupResponse;
import com.google.datastore.v1.Value;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.table.DatastoreTableSchema;
import io.github.flink.gcp.connector.datastore.table.TypeMismatchPolicy;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Drives both lookup functions over a scripted {@link DatastoreEntityLookup}. */
class DatastoreRowDataLookupFunctionTest {

    private static final DatabaseDestination DATABASE = DatabaseDestination.of("p", "db");

    private static RowType type(boolean id) {
        return (RowType)
                DataTypes.ROW(
                                DataTypes.FIELD(
                                        "id",
                                        id
                                                ? DataTypes.BIGINT().notNull()
                                                : DataTypes.STRING().notNull()),
                                DataTypes.FIELD("n", DataTypes.BIGINT()))
                        .getLogicalType();
    }

    private static DatastoreLookupKeys keys(boolean id) {
        return new DatastoreLookupKeys(DATABASE, "ns", "K", id);
    }

    private static RowDataDeserializationSchema deserializer(boolean id, List<String> metadata) {
        RowType type = type(id);
        RowType produced = type;
        if (!metadata.isEmpty()) {
            produced =
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("id", DataTypes.STRING().notNull()),
                                            DataTypes.FIELD("n", DataTypes.BIGINT()),
                                            DataTypes.FIELD("version", DataTypes.BIGINT()),
                                            DataTypes.FIELD("created", DataTypes.TIMESTAMP_LTZ(6)),
                                            DataTypes.FIELD("updated", DataTypes.TIMESTAMP_LTZ(6)),
                                            DataTypes.FIELD("read", DataTypes.TIMESTAMP_LTZ(6)))
                                    .getLogicalType();
        }
        return new RowDataDeserializationSchema(
                DatastoreTableSchema.of(type, new int[] {0}, List.of()),
                new int[] {0, 1},
                metadata,
                TypeMismatchPolicy.FAIL,
                InternalTypeInfo.of(produced));
    }

    private static Key named(String name) {
        return keys(false).key(nameRow(name));
    }

    private static RowData nameRow(String name) {
        return GenericRowData.of(name == null ? null : StringData.fromString(name));
    }

    private static RowData idRow(Long id) {
        return GenericRowData.of(id);
    }

    private static DatastoreRowDataLookupFunction sync(
            boolean id, List<String> metadata, int maxRetries, FakeEntityLookup lookup)
            throws Exception {
        DatastoreRowDataLookupFunction function =
                new DatastoreRowDataLookupFunction(
                        deserializer(id, metadata), keys(id), maxRetries, lookup);
        function.open(null);
        return function;
    }

    private static DatastoreRowDataLookupFunction sync(int maxRetries, FakeEntityLookup lookup)
            throws Exception {
        return sync(false, List.of(), maxRetries, lookup);
    }

    private static DatastoreRowDataAsyncLookupFunction async(
            boolean id, int maxRetries, FakeEntityLookup lookup) throws Exception {
        DatastoreRowDataAsyncLookupFunction function =
                new DatastoreRowDataAsyncLookupFunction(
                        deserializer(id, List.of()), keys(id), maxRetries, lookup);
        function.open(null);
        return function;
    }

    private static DatastoreRowDataAsyncLookupFunction async(
            int maxRetries, FakeEntityLookup lookup) throws Exception {
        return async(false, maxRetries, lookup);
    }

    private static LookupResponse found(String name, long n) {
        return FakeEntityLookup.found(named(name), Value.newBuilder().setIntegerValue(n).build());
    }

    @Test
    void theFoundEntityIsReadAsItsRowWithTheAnswersMetadata() throws Exception {
        // n is 1 and the version 7, so a swapped column shows.
        FakeEntityLookup lookup = new FakeEntityLookup().then(found("a", 1L));

        assertThat(
                        sync(
                                        false,
                                        List.of(
                                                "version",
                                                "create-time",
                                                "update-time",
                                                "read-time"),
                                        0,
                                        lookup)
                                .lookup(nameRow("a")))
                .containsExactly(
                        GenericRowData.of(
                                StringData.fromString("a"),
                                1L,
                                7L,
                                TimestampData.fromInstant(Instant.ofEpochSecond(1_700_000_000L)),
                                TimestampData.fromInstant(
                                        Instant.ofEpochSecond(1_700_000_001L, 123_456_000)),
                                TimestampData.fromInstant(Instant.ofEpochSecond(1_700_000_002L))));
        assertThat(lookup.opened()).isTrue();
    }

    @Test
    void theKeyIsARootKeyOfTheKindInTheNamespaceAndDatabase() throws Exception {
        FakeEntityLookup names = new FakeEntityLookup().then(found("a", 1L));
        FakeEntityLookup ids =
                new FakeEntityLookup().then(FakeEntityLookup.missing(keys(true).key(idRow(-5L))));

        sync(0, names).lookup(nameRow("a"));
        async(true, 0, ids).asyncLookup(idRow(-5L)).get(10, SECONDS);

        Key name = names.lookups().get(0);
        assertThat(name.getPartitionId().getProjectId()).isEqualTo("p");
        assertThat(name.getPartitionId().getDatabaseId()).isEqualTo("db");
        assertThat(name.getPartitionId().getNamespaceId()).isEqualTo("ns");
        assertThat(name.getPathList()).hasSize(1);
        assertThat(name.getPath(0).getKind()).isEqualTo("K");
        assertThat(name.getPath(0).getName()).isEqualTo("a");
        assertThat(ids.lookups().get(0).getPath(0).getId()).isEqualTo(-5L);
    }

    @Test
    void aMissingEntityJoinsNoRow() throws Exception {
        FakeEntityLookup syncLookup =
                new FakeEntityLookup().then(FakeEntityLookup.missing(named("a")));
        FakeEntityLookup asyncLookup =
                new FakeEntityLookup().then(FakeEntityLookup.missing(named("a")));

        assertThat(sync(0, syncLookup).lookup(nameRow("a"))).isEmpty();
        assertThat(async(0, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS)).isEmpty();
    }

    @Test
    void aKeyThatCanNameNoEntityIsNeverRead() throws Exception {
        FakeEntityLookup lookup = new FakeEntityLookup();
        DatastoreRowDataLookupFunction sync = sync(0, lookup);
        DatastoreRowDataAsyncLookupFunction async = async(0, lookup);

        // 751 two-byte characters are 1,502 UTF-8 bytes: the limit counts bytes, not characters.
        for (String name :
                new String[] {null, "", "__x__", "_____", "x".repeat(1501), "é".repeat(751)}) {
            assertThat(sync.lookup(nameRow(name))).as("%s", name).isEmpty();
            assertThat(async.asyncLookup(nameRow(name)).get(10, SECONDS)).as("%s", name).isEmpty();
        }
        DatastoreRowDataLookupFunction syncId = sync(true, List.of(), 0, lookup);
        DatastoreRowDataAsyncLookupFunction asyncId = async(true, 0, lookup);
        for (Long id : new Long[] {null, 0L}) {
            assertThat(syncId.lookup(idRow(id))).as("%s", id).isEmpty();
            assertThat(asyncId.asyncLookup(idRow(id)).get(10, SECONDS)).as("%s", id).isEmpty();
        }
        assertThat(lookup.lookups()).isEmpty();
    }

    @Test
    void aKeyTheServiceCanStoreIsRead() throws Exception {
        String longest = "é".repeat(750); // 1,500 UTF-8 bytes
        String[] names = {"____", "__x", "x__", "a/b", " ", longest};
        FakeEntityLookup lookup = new FakeEntityLookup();
        for (String name : names) {
            lookup.then(FakeEntityLookup.missing(named(name)));
        }
        lookup.then(FakeEntityLookup.missing(keys(true).key(idRow(-5L))));
        lookup.then(FakeEntityLookup.missing(keys(true).key(idRow(Long.MIN_VALUE))));
        DatastoreRowDataLookupFunction sync = sync(0, lookup);
        DatastoreRowDataLookupFunction syncId = sync(true, List.of(), 0, lookup);

        for (String name : names) {
            sync.lookup(nameRow(name));
        }
        syncId.lookup(idRow(-5L));
        syncId.lookup(idRow(Long.MIN_VALUE));
        assertThat(lookup.lookups()).hasSize(names.length + 2);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"UNAVAILABLE", "DEADLINE_EXCEEDED"})
    void aTransientFailureIsReadAgainWithinTheBudget(StatusCode.Code code) throws Exception {
        FakeEntityLookup syncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.failure(code))
                        .then(FakeEntityLookup.failure(code))
                        .then(found("a", 1L));
        FakeEntityLookup asyncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.failure(code))
                        .then(FakeEntityLookup.failure(code))
                        .then(found("a", 1L));

        assertThat(sync(2, syncLookup).lookup(nameRow("a"))).hasSize(1);
        assertThat(async(2, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS)).hasSize(1);
        assertThat(syncLookup.lookups()).hasSize(3);
        assertThat(asyncLookup.lookups()).hasSize(3);
    }

    @Test
    void aDeferralIsSentAgainWithinTheBudget() throws Exception {
        FakeEntityLookup syncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.deferred(named("a")))
                        .then(found("a", 1L));
        FakeEntityLookup asyncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.deferred(named("a")))
                        .then(found("a", 1L));

        assertThat(sync(1, syncLookup).lookup(nameRow("a"))).hasSize(1);
        assertThat(async(1, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS)).hasSize(1);
        assertThat(syncLookup.lookups()).hasSize(2);
        assertThat(asyncLookup.lookups()).hasSize(2);
    }

    @Test
    void aDeferralThatOutlastsTheBudgetFailsTheJoinNamingTheOption() throws Exception {
        FakeEntityLookup syncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.deferred(named("a")))
                        .then(FakeEntityLookup.deferred(named("a")));
        FakeEntityLookup asyncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.deferred(named("a")))
                        .then(FakeEntityLookup.deferred(named("a")));

        assertThatThrownBy(() -> sync(1, syncLookup).lookup(nameRow("a")))
                .isInstanceOf(DatastoreEntityLookups.DeferredException.class)
                .hasMessageContaining("Datastore deferred the lookup of the K entity named 'a'")
                .hasMessageContaining("'lookup.max-retries'");
        assertThatThrownBy(() -> async(1, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(DatastoreEntityLookups.DeferredException.class);
        assertThat(syncLookup.lookups()).hasSize(2);
        assertThat(asyncLookup.lookups()).hasSize(2);
    }

    @Test
    void anExhaustedBudgetSurfacesTheLastFailure() throws Exception {
        FakeEntityLookup syncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.failure(StatusCode.Code.UNAVAILABLE))
                        .then(FakeEntityLookup.failure(StatusCode.Code.DEADLINE_EXCEEDED));
        FakeEntityLookup asyncLookup =
                new FakeEntityLookup()
                        .then(FakeEntityLookup.failure(StatusCode.Code.UNAVAILABLE))
                        .then(FakeEntityLookup.failure(StatusCode.Code.DEADLINE_EXCEEDED));

        // The blocking read rethrows the library's exception itself, not an ExecutionException.
        assertThatThrownBy(() -> sync(1, syncLookup).lookup(nameRow("a")))
                .isInstanceOf(ApiException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED");
        assertThatThrownBy(() -> async(1, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("DEADLINE_EXCEEDED");
        assertThat(syncLookup.lookups()).hasSize(2);
        assertThat(asyncLookup.lookups()).hasSize(2);
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"INTERNAL", "RESOURCE_EXHAUSTED", "PERMISSION_DENIED", "INVALID_ARGUMENT"})
    void aPermanentFailureIsNotReadAgain(StatusCode.Code code) throws Exception {
        FakeEntityLookup syncLookup = new FakeEntityLookup().then(FakeEntityLookup.failure(code));
        FakeEntityLookup asyncLookup = new FakeEntityLookup().then(FakeEntityLookup.failure(code));

        assertThatThrownBy(() -> sync(3, syncLookup).lookup(nameRow("a")))
                .hasMessageContaining(code.name());
        assertThatThrownBy(() -> async(3, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS))
                .hasMessageContaining(code.name());
        assertThat(syncLookup.lookups()).hasSize(1);
        assertThat(asyncLookup.lookups()).hasSize(1);
    }

    @Test
    void aMismatchFailsTheLookupNamingTheEntity() throws Exception {
        LookupResponse text =
                FakeEntityLookup.found(
                        named("bad"), Value.newBuilder().setStringValue("text").build());
        FakeEntityLookup syncLookup = new FakeEntityLookup().then(text);
        FakeEntityLookup asyncLookup = new FakeEntityLookup().then(text);

        assertThatThrownBy(() -> sync(3, syncLookup).lookup(nameRow("bad")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("kind=K")
                .hasMessageContaining("name=bad")
                .hasMessageContaining("cannot be read into the table");
        assertThatThrownBy(() -> async(3, asyncLookup).asyncLookup(nameRow("bad")).get(10, SECONDS))
                .hasCauseInstanceOf(IOException.class)
                .hasMessageContaining("name=bad")
                .hasMessageContaining("cannot be read into the table");
        assertThat(syncLookup.lookups()).hasSize(1);
        assertThat(asyncLookup.lookups()).hasSize(1);
    }

    @Test
    void anAnswerThatNeitherFindsNorMissesTheKeyFailsTheLookup() throws Exception {
        for (LookupResponse answer :
                new LookupResponse[] {
                    LookupResponse.getDefaultInstance(),
                    found("a", 1L).toBuilder()
                            .addAllMissing(FakeEntityLookup.missing(named("a")).getMissingList())
                            .build()
                }) {
            FakeEntityLookup syncLookup = new FakeEntityLookup().then(answer);
            FakeEntityLookup asyncLookup = new FakeEntityLookup().then(answer);
            assertThatThrownBy(() -> sync(3, syncLookup).lookup(nameRow("a")))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("where one lookup of one key has exactly one");
            assertThatThrownBy(
                            () -> async(3, asyncLookup).asyncLookup(nameRow("a")).get(10, SECONDS))
                    .hasCauseInstanceOf(IOException.class)
                    .hasMessageContaining("where one lookup of one key has exactly one");
            assertThat(syncLookup.lookups()).hasSize(1);
            assertThat(asyncLookup.lookups()).hasSize(1);
        }
    }

    @Test
    void anAnswerWithoutItsReadTimeFailsOnlyAReadOfThatMetadata() throws Exception {
        LookupResponse noReadTime = found("a", 1L).toBuilder().clearReadTime().build();

        assertThat(sync(0, new FakeEntityLookup().then(noReadTime)).lookup(nameRow("a")))
                .hasSize(1);
        assertThatThrownBy(
                        () ->
                                sync(
                                                false,
                                                List.of(
                                                        "version",
                                                        "create-time",
                                                        "update-time",
                                                        "read-time"),
                                                0,
                                                new FakeEntityLookup().then(noReadTime))
                                        .lookup(nameRow("a")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("without its 'read-time'");
    }

    @Test
    void aReadThatThrowsAtOnceIsRetriedLikeAFailedOne() throws Exception {
        FakeEntityLookup lookup =
                new FakeEntityLookup()
                        .then(
                                new FakeEntityLookup.Thrown(
                                        FakeEntityLookup.failure(StatusCode.Code.UNAVAILABLE)))
                        .then(found("a", 1L));

        FakeEntityLookup syncLookup =
                new FakeEntityLookup()
                        .then(
                                new FakeEntityLookup.Thrown(
                                        FakeEntityLookup.failure(StatusCode.Code.UNAVAILABLE)))
                        .then(found("a", 1L));

        assertThat(async(1, lookup).asyncLookup(nameRow("a")).get(10, SECONDS)).hasSize(1);
        assertThat(sync(1, syncLookup).lookup(nameRow("a"))).hasSize(1);
        assertThat(lookup.lookups()).hasSize(2);
        assertThat(syncLookup.lookups()).hasSize(2);
    }

    @Test
    void theAsynchronousLookupReturnsBeforeTheReadCompletes() throws Exception {
        SettableApiFuture<LookupResponse> pending = SettableApiFuture.create();
        FakeEntityLookup lookup = new FakeEntityLookup().then(pending);

        CompletableFuture<Collection<RowData>> rows = async(0, lookup).asyncLookup(nameRow("a"));

        assertThat(rows).isNotDone();
        pending.set(found("a", 1L));
        assertThat(rows.get(10, SECONDS)).hasSize(1);
    }

    @Test
    void closingEitherFunctionClosesItsLookup() throws Exception {
        FakeEntityLookup syncLookup = new FakeEntityLookup();
        FakeEntityLookup asyncLookup = new FakeEntityLookup();
        sync(0, syncLookup).close();
        async(0, asyncLookup).close();

        assertThat(syncLookup.closed()).isTrue();
        assertThat(asyncLookup.closed()).isTrue();
    }
}
