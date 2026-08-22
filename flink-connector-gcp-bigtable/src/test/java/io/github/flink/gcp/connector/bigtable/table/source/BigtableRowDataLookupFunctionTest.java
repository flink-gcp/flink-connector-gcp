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

package io.github.flink.gcp.connector.bigtable.table.source;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import io.github.flink.gcp.connector.bigtable.table.TrailingBytes;
import io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigtableRowDataLookupFunctionTest {

    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf1",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.STRING()))))
                                    .getLogicalType());

    private static GenericRowData key(@Nullable String value) {
        return GenericRowData.of(value == null ? null : StringData.fromString(value));
    }

    private static Row row(String key, String value) {
        RowCell cell =
                RowCell.create(
                        "cf1",
                        ByteString.copyFromUtf8("q"),
                        1_000L,
                        Collections.emptyList(),
                        ByteString.copyFromUtf8(value));
        return Row.create(ByteString.copyFromUtf8(key), Collections.singletonList(cell));
    }

    private static RuntimeException failure(StatusCode.Code code) {
        return ApiExceptionFactory.createException(
                new RuntimeException("scripted " + code),
                GrpcStatusCode.of(Status.Code.valueOf(code.name())),
                false);
    }

    private static BigtableRowDataLookupFunction sync(FakeRowLookup lookup, int maxRetries) {
        return new BigtableRowDataLookupFunction(
                SCHEMA, null, "null", TrailingBytes.IGNORE, maxRetries, lookup);
    }

    private static BigtableRowDataAsyncLookupFunction async(FakeRowLookup lookup, int maxRetries) {
        return new BigtableRowDataAsyncLookupFunction(
                SCHEMA, null, "null", TrailingBytes.IGNORE, maxRetries, lookup);
    }

    @Test
    void synchronousLookupEncodesTheKeyAndConvertsAHit() throws Exception {
        FakeRowLookup lookup = new FakeRowLookup().answer(row("k1", "value"));
        BigtableRowDataLookupFunction function = sync(lookup, 3);
        function.open(null);

        Collection<RowData> result = function.lookup(key("k1"));

        assertThat(lookup.keys).containsExactly(ByteString.copyFromUtf8("k1"));
        assertThat(result)
                .containsExactly(
                        GenericRowData.of(
                                StringData.fromString("k1"),
                                GenericRowData.of(StringData.fromString("value"))));
        function.close();
        assertThat(lookup.opened).isTrue();
        assertThat(lookup.closed).isTrue();
    }

    @Test
    void asynchronousLookupBridgesTheApiFutureAndConvertsAHit() throws Exception {
        FakeRowLookup lookup = new FakeRowLookup().answer(row("k1", "value"));
        BigtableRowDataAsyncLookupFunction function = async(lookup, 3);
        function.open(null);

        assertThat(function.asyncLookup(key("k1")).join())
                .containsExactly(
                        GenericRowData.of(
                                StringData.fromString("k1"),
                                GenericRowData.of(StringData.fromString("value"))));
        assertThat(lookup.keys).containsExactly(ByteString.copyFromUtf8("k1"));
        function.close();
        assertThat(lookup.closed).isTrue();
    }

    @Test
    void asynchronousLookupWaitsForALaterCallbackAndPropagatesCancellation() throws Exception {
        SettableApiFuture<Row> pending = SettableApiFuture.create();
        FakeRowLookup lookup = new FakeRowLookup().answer(pending);
        BigtableRowDataAsyncLookupFunction function = async(lookup, 3);
        function.open(null);

        CompletableFuture<Collection<RowData>> result = function.asyncLookup(key("k1"));
        assertThat(result).isNotDone();
        pending.set(row("k1", "value"));
        assertThat(result.join()).hasSize(1);

        SettableApiFuture<Row> cancelledCall = SettableApiFuture.create();
        FakeRowLookup cancelledLookup = new FakeRowLookup().answer(cancelledCall);
        CompletableFuture<Collection<RowData>> cancelled =
                async(cancelledLookup, 3).asyncLookup(key("k2"));
        cancelled.cancel(true);
        assertThat(cancelledCall.isCancelled()).isTrue();
        assertThat(cancelledLookup.keys).hasSize(1);
    }

    @Test
    void anAbsentRowAndANullJoinKeyAreMisses() throws Exception {
        FakeRowLookup syncLookup = new FakeRowLookup().miss();
        BigtableRowDataLookupFunction sync = sync(syncLookup, 3);
        sync.open(null);

        assertThat(sync.lookup(key("missing"))).isEmpty();
        assertThat(sync.lookup(key(null))).isEmpty();
        assertThat(syncLookup.keys).containsExactly(ByteString.copyFromUtf8("missing"));

        FakeRowLookup asyncLookup = new FakeRowLookup().miss();
        BigtableRowDataAsyncLookupFunction async = async(asyncLookup, 3);
        async.open(null);
        assertThat(async.asyncLookup(key("missing")).join()).isEmpty();
        assertThat(async.asyncLookup(key(null)).join()).isEmpty();
        assertThat(asyncLookup.keys).containsExactly(ByteString.copyFromUtf8("missing"));
    }

    @ParameterizedTest
    @EnumSource(
            value = StatusCode.Code.class,
            names = {"DEADLINE_EXCEEDED", "UNAVAILABLE", "ABORTED"})
    void retriesOnlyThePointReadTransientCodes(StatusCode.Code code) throws Exception {
        FakeRowLookup syncLookup =
                new FakeRowLookup()
                        .answer(failure(code))
                        .answer(failure(code))
                        .answer(row("k", "v"));
        BigtableRowDataLookupFunction sync = sync(syncLookup, 2);
        sync.open(null);
        assertThat(sync.lookup(key("k"))).hasSize(1);
        assertThat(syncLookup.keys).hasSize(3);

        FakeRowLookup asyncLookup =
                new FakeRowLookup()
                        .answer(failure(code))
                        .answer(failure(code))
                        .answer(row("k", "v"));
        BigtableRowDataAsyncLookupFunction async = async(asyncLookup, 2);
        async.open(null);
        assertThat(async.asyncLookup(key("k")).join()).hasSize(1);
        assertThat(asyncLookup.keys).hasSize(3);
    }

    @Test
    void stopsAfterTheConfiguredRetriesAndDoesNotRetryAPermanentFailure() throws Exception {
        RuntimeException transientFailure = failure(StatusCode.Code.UNAVAILABLE);
        FakeRowLookup syncLookup =
                new FakeRowLookup()
                        .answer(transientFailure)
                        .answer(transientFailure)
                        .answer(row("never", "read"));
        BigtableRowDataLookupFunction sync = sync(syncLookup, 1);
        sync.open(null);
        assertThatThrownBy(() -> sync.lookup(key("k"))).isSameAs(transientFailure);
        assertThat(syncLookup.keys).hasSize(2);

        FakeRowLookup exhaustedAsyncLookup =
                new FakeRowLookup()
                        .answer(transientFailure)
                        .answer(transientFailure)
                        .answer(row("never", "read"));
        BigtableRowDataAsyncLookupFunction exhaustedAsync = async(exhaustedAsyncLookup, 1);
        exhaustedAsync.open(null);
        assertThatThrownBy(() -> exhaustedAsync.asyncLookup(key("k")).join())
                .isInstanceOf(CompletionException.class)
                .hasCause(transientFailure);
        assertThat(exhaustedAsyncLookup.keys).hasSize(2);

        RuntimeException permanentFailure = failure(StatusCode.Code.PERMISSION_DENIED);
        FakeRowLookup asyncLookup =
                new FakeRowLookup().answer(permanentFailure).answer(row("never", "read"));
        BigtableRowDataAsyncLookupFunction async = async(asyncLookup, 3);
        async.open(null);
        assertThatThrownBy(() -> async.asyncLookup(key("k")).join())
                .isInstanceOf(CompletionException.class)
                .hasCause(permanentFailure);
        assertThat(asyncLookup.keys).hasSize(1);
    }

    @Test
    void immediateAsyncFailuresDoNotGrowTheTaskThreadStack() throws Exception {
        int retries = 5_000;
        RuntimeException transientFailure = failure(StatusCode.Code.UNAVAILABLE);
        FakeRowLookup lookup = new FakeRowLookup();
        for (int i = 0; i < retries; i++) {
            lookup.answer(transientFailure);
        }
        lookup.answer(row("k", "v"));

        assertThat(async(lookup, retries).asyncLookup(key("k")).join()).hasSize(1);
        assertThat(lookup.keys).hasSize(retries + 1);
    }

    @Test
    void encodesNonStringRowKeysWithTheHBaseLayout() throws Exception {
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("rowkey", DataTypes.BIGINT()),
                                                DataTypes.FIELD(
                                                        "cf1",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "q", DataTypes.STRING()))))
                                        .getLogicalType());
        FakeRowLookup lookup = new FakeRowLookup().miss();
        BigtableRowDataLookupFunction function =
                new BigtableRowDataLookupFunction(
                        schema, null, "null", TrailingBytes.IGNORE, 0, lookup);
        function.open(null);

        assertThat(function.lookup(GenericRowData.of(7L))).isEmpty();
        assertThat(lookup.keys)
                .containsExactly(ByteString.copyFrom(new byte[] {0, 0, 0, 0, 0, 0, 0, 7}));
    }

    @Test
    void asynchronousConversionFailureCompletesTheReturnedFutureExceptionally() throws Exception {
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "cf1",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "q", DataTypes.BIGINT()))))
                                        .getLogicalType());
        Row malformed =
                Row.create(
                        ByteString.copyFromUtf8("k"),
                        Collections.singletonList(
                                RowCell.create(
                                        "cf1",
                                        ByteString.copyFromUtf8("q"),
                                        1_000L,
                                        Collections.emptyList(),
                                        ByteString.copyFrom(new byte[] {1, 2, 3}))));
        BigtableRowDataAsyncLookupFunction function =
                new BigtableRowDataAsyncLookupFunction(
                        schema,
                        null,
                        "null",
                        TrailingBytes.IGNORE,
                        (int) 0,
                        new FakeRowLookup().answer(malformed));

        CompletableFuture<Collection<RowData>> result = function.asyncLookup(key("k"));
        assertThatThrownBy(result::join)
                .isInstanceOf(CompletionException.class)
                .hasCauseInstanceOf(IllegalStateException.class);
        assertThat(result).isCompletedExceptionally();
    }

    @Test
    void theLookupPathHonoursTheRejectPolicy() throws Exception {
        // The lookup decode runs the same converter as the scan, so the policy must reach it too;
        // without this, hardcoding IGNORE anywhere along the lookup plumbing survives the suite.
        BigtableTableSchema schema =
                BigtableTableSchema.of(
                        (RowType)
                                DataTypes.ROW(
                                                DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                                DataTypes.FIELD(
                                                        "cf1",
                                                        DataTypes.ROW(
                                                                DataTypes.FIELD(
                                                                        "q", DataTypes.BIGINT()))))
                                        .getLogicalType());
        Row overlong =
                Row.create(
                        ByteString.copyFromUtf8("k"),
                        Collections.singletonList(
                                RowCell.create(
                                        "cf1",
                                        ByteString.copyFromUtf8("q"),
                                        1_000L,
                                        Collections.emptyList(),
                                        ByteString.copyFrom(
                                                new byte[] {0, 0, 0, 0, 0, 0, 0, 7, 0x7f}))));
        BigtableRowDataLookupFunction function =
                new BigtableRowDataLookupFunction(
                        schema,
                        null,
                        "null",
                        TrailingBytes.REJECT,
                        0,
                        new FakeRowLookup().answer(overlong));
        function.open(null);

        assertThatThrownBy(() -> function.lookup(key("k")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("9 byte(s)")
                .hasCauseInstanceOf(IllegalArgumentException.class);
    }

    private static final class FakeRowLookup implements BigtableRowLookup {

        private static final long serialVersionUID = 1L;

        private final Deque<Object> answers = new ArrayDeque<>();
        private final java.util.List<ByteString> keys = new java.util.ArrayList<>();
        private boolean opened;
        private boolean closed;

        private FakeRowLookup answer(Row answer) {
            answers.add(answer);
            return this;
        }

        private FakeRowLookup miss() {
            answers.add(Missing.INSTANCE);
            return this;
        }

        private FakeRowLookup answer(RuntimeException failure) {
            answers.add(failure);
            return this;
        }

        private FakeRowLookup answer(ApiFuture<Row> future) {
            answers.add(future);
            return this;
        }

        @Override
        public void open() {
            opened = true;
        }

        @Override
        @Nullable
        public Row read(ByteString rowKey) {
            keys.add(rowKey);
            Object answer = answers.removeFirst();
            if (answer instanceof RuntimeException) {
                throw (RuntimeException) answer;
            }
            return answer == Missing.INSTANCE ? null : (Row) answer;
        }

        @Override
        public ApiFuture<Row> readAsync(ByteString rowKey) {
            if (answers.peekFirst() instanceof ApiFuture) {
                keys.add(rowKey);
                @SuppressWarnings("unchecked")
                ApiFuture<Row> future = (ApiFuture<Row>) answers.removeFirst();
                return future;
            }
            try {
                return ApiFutures.immediateFuture(read(rowKey));
            } catch (RuntimeException failure) {
                return ApiFutures.immediateFailedFuture(failure);
            }
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private enum Missing {
        INSTANCE
    }
}
