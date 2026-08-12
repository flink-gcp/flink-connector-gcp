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

package io.github.flink.gcp.connector.spanner.sink.writer;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.MutationGroup;
import com.google.cloud.spanner.ResultSets;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.rpc.Status;
import com.google.spanner.v1.BatchWriteResponse;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link SpannerServiceAdapter} — the one class that translates the client library's
 * stream shape into what the writer consumes.
 *
 * <p>{@link FakeSpannerDatabaseAccess} sits <em>below</em> this adapter, so nothing else in the
 * module exercises the translation. The case that matters most is the one that fake structurally
 * cannot produce: a single {@code BatchWriteResponse} deciding several groups at once.
 */
class SpannerServiceAdapterTest {

    private static final Type INDEX_ROW =
            Type.struct(
                    Type.StructField.of("s", Type.string()),
                    Type.StructField.of("t", Type.string()),
                    Type.StructField.of("c", Type.string()),
                    Type.StructField.of("i", Type.string()));

    @Test
    void oneResponseCanDecideSeveralGroups() {
        // The wire shape: `indexes` is repeated, so the service may report one status for a run of
        // groups. Reading only the first would leave the rest undecided, and the writer would
        // re-send mutations that were already applied — silent duplicates from an at-least-once
        // sink.
        BatchWriteResponse response =
                BatchWriteResponse.newBuilder()
                        .addAllIndexes(Arrays.asList(0, 2))
                        .setStatus(status(0))
                        .build();
        SpannerServiceAdapter adapter = adapter(groups -> List.of(response));

        Map<Integer, Status> outcomes = new LinkedHashMap<>();
        adapter.batchWrite(groups(3), outcomes::put);

        assertThat(outcomes).containsOnlyKeys(0, 2);
    }

    @Test
    void reportsEveryResponseOfTheStreamInOrder() {
        SpannerServiceAdapter adapter =
                adapter(
                        groups ->
                                List.of(
                                        BatchWriteResponse.newBuilder()
                                                .addIndexes(1)
                                                .setStatus(status(6))
                                                .build(),
                                        BatchWriteResponse.newBuilder()
                                                .addIndexes(0)
                                                .setStatus(status(0))
                                                .build()));

        Map<Integer, Status> outcomes = new LinkedHashMap<>();
        adapter.batchWrite(groups(2), outcomes::put);

        assertThat(outcomes).containsOnlyKeys(0, 1);
        assertThat(outcomes.get(1).getCode()).isEqualTo(6);
        assertThat(outcomes.get(0).getCode()).isZero();
    }

    @Test
    void passesTheGroupsStraightThrough() {
        List<List<MutationGroup>> sent = new ArrayList<>();
        SpannerServiceAdapter adapter =
                adapter(
                        groups -> {
                            sent.add(groups);
                            return List.of();
                        });
        List<MutationGroup> groups = groups(2);

        adapter.batchWrite(groups, (index, status) -> {});

        assertThat(sent).containsExactly(groups);
    }

    @Test
    void aFailingStreamPropagatesAfterWhateverItReported() {
        Map<Integer, Status> outcomes = new LinkedHashMap<>();
        SpannerServiceAdapter adapter =
                adapter(
                        groups ->
                                () ->
                                        new FailingIterator(
                                                BatchWriteResponse.newBuilder()
                                                        .addIndexes(0)
                                                        .setStatus(status(0))
                                                        .build()));

        assertThatThrownBy(() -> adapter.batchWrite(groups(2), outcomes::put))
                .hasMessageContaining("stream broke");
        // The partial progress survives the failure, which is what lets the writer re-send only
        // the groups it never heard about.
        assertThat(outcomes).containsOnlyKeys(0);
    }

    @Test
    void readsTheWeightsWithTheQueryTheDialectAsksFor() throws Exception {
        List<String> queries = new ArrayList<>();
        AtomicInteger dialectReads = new AtomicInteger();
        SpannerServiceAdapter adapter =
                new SpannerServiceAdapter(
                        "db",
                        () -> {
                            dialectReads.incrementAndGet();
                            return Dialect.POSTGRESQL;
                        },
                        sql -> {
                            queries.add(sql);
                            return ResultSets.forRows(
                                    INDEX_ROW,
                                    List.of(
                                            Struct.newBuilder()
                                                    .set("s")
                                                    .to("public")
                                                    .set("t")
                                                    .to("orders")
                                                    .set("c")
                                                    .to("name")
                                                    .set("i")
                                                    .to("by_name")
                                                    .build()));
                        },
                        groups -> List.of(),
                        () -> {});

        CellWeights weights = adapter.readCellWeights();

        assertThat(queries)
                .containsExactly(InformationSchemaCellWeights.queryFor(Dialect.POSTGRESQL));
        assertThat(dialectReads).hasValue(1);
        assertThat(weights.knows("orders")).isTrue();
    }

    @Test
    void aFailedDialectReadNamesTheDatabase() {
        SpannerServiceAdapter adapter =
                new SpannerServiceAdapter(
                        "projects/p/instances/i/databases/d",
                        () -> {
                            throw SpannerExceptionFactory.newSpannerException(
                                    ErrorCode.UNAVAILABLE, "backend down");
                        },
                        sql -> {
                            throw new AssertionError("the query must not be reached");
                        },
                        groups -> List.of(),
                        () -> {});

        assertThatThrownBy(adapter::readCellWeights)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("dialect")
                .hasMessageContaining("projects/p/instances/i/databases/d");
    }

    @Test
    void aFailedWeightsReadSaysWhatThePermissionIsFor() {
        SpannerServiceAdapter adapter =
                new SpannerServiceAdapter(
                        "projects/p/instances/i/databases/d",
                        () -> Dialect.GOOGLE_STANDARD_SQL,
                        sql -> {
                            throw SpannerExceptionFactory.newSpannerException(
                                    ErrorCode.PERMISSION_DENIED, "no select");
                        },
                        groups -> List.of(),
                        () -> {});

        // This is the message a misconfigured user actually meets, so it is asserted where it is
        // produced rather than where a test fixture could restate it.
        assertThatThrownBy(adapter::readCellWeights)
                .isInstanceOf(IOException.class)
                .hasMessageContaining("INFORMATION_SCHEMA")
                .hasMessageContaining("secondary indexes")
                .hasMessageContaining("projects/p/instances/i/databases/d");
    }

    @Test
    void closingReleasesTheServiceHandle() {
        AtomicInteger closes = new AtomicInteger();
        SpannerServiceAdapter adapter =
                new SpannerServiceAdapter(
                        "db",
                        () -> Dialect.GOOGLE_STANDARD_SQL,
                        sql -> {
                            throw new AssertionError("not reached");
                        },
                        groups -> List.of(),
                        closes::incrementAndGet);

        adapter.close();

        assertThat(closes.get()).isEqualTo(1);
    }

    // ---------------------------------------------------------------- helpers

    private static SpannerServiceAdapter adapter(
            java.util.function.Function<List<MutationGroup>, Iterable<BatchWriteResponse>>
                    batchWrite) {
        return new SpannerServiceAdapter(
                "db",
                () -> Dialect.GOOGLE_STANDARD_SQL,
                sql -> {
                    throw new AssertionError("not reached");
                },
                batchWrite,
                () -> {});
    }

    private static List<MutationGroup> groups(int count) {
        List<MutationGroup> groups = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            groups.add(
                    MutationGroup.of(
                            Mutation.newInsertOrUpdateBuilder("orders")
                                    .set("id")
                                    .to((long) i)
                                    .build()));
        }
        return groups;
    }

    private static Status status(int code) {
        return Status.newBuilder().setCode(code).build();
    }

    /** Yields one response and then breaks, as a server stream failing part-way through does. */
    private static final class FailingIterator implements java.util.Iterator<BatchWriteResponse> {

        private final BatchWriteResponse first;
        private boolean served;

        private FailingIterator(BatchWriteResponse first) {
            this.first = first;
        }

        @Override
        public boolean hasNext() {
            if (served) {
                throw SpannerExceptionFactory.newSpannerException(
                        ErrorCode.UNAVAILABLE, "the stream broke");
            }
            return true;
        }

        @Override
        public BatchWriteResponse next() {
            served = true;
            return first;
        }
    }
}
