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

package io.github.flink.gcp.connector.datastore.sink.writer;

import org.apache.flink.api.connector.sink2.SinkWriter;

import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.datastore.Entity;
import com.google.cloud.datastore.IncompleteKey;
import com.google.cloud.datastore.Key;
import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.DatastoreMetricNames;
import io.github.flink.gcp.connector.datastore.sink.DatastoreCommitSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreKeyAllocator;
import io.github.flink.gcp.connector.datastore.sink.DatastoreMutation;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSink;
import io.github.flink.gcp.connector.datastore.sink.DatastoreSinkConfig;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.sink.FailedMutation;
import io.github.flink.gcp.connector.datastore.sink.serializer.DatastoreMutationSerializationSchema;
import io.github.flink.gcp.connector.datastore.sink.serializer.KeyAllocatingSerializationSchema;
import io.github.flink.gcp.connector.testutils.TestContexts;
import io.github.flink.gcp.connector.testutils.TestSinkWriterMetricGroup;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.Field;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DatastoreWriterTest {

    private static final String PROJECT = "p";
    private static final String KIND = "Order";
    private static final DatabaseDestination DATABASE = DatabaseDestination.of(PROJECT);

    private final FakeDatastoreDatabaseAccess access = new FakeDatastoreDatabaseAccess();
    private final TestSinkWriterMetricGroup metrics = TestSinkWriterMetricGroup.create();
    private final RecordingHandler handler = new RecordingHandler();

    // --- the write path ---

    @Test
    void appliesTheBatchInOneCommitAtFlushAndCountsEachRecordOnce() throws Exception {
        DatastoreWriter<DatastoreMutation> writer = writer(handler);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(DatastoreMutation.delete(key("b")), TestContexts.NO_OP);
        assertThat(access.requests()).isEmpty();
        assertThat((Integer) metrics.gaugeValue(DatastoreMetricNames.BUFFERED_MUTATIONS))
                .isEqualTo(2);

        writer.flush(false);

        assertThat(access.requests()).containsExactly(List.of(key("a"), key("b")));
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_RECORDS_SEND)).isEqualTo(2);
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_BYTES_SEND))
                .isEqualTo(
                        MutationSizeEstimator.sizeOf(upsert("a"))
                                + MutationSizeEstimator.sizeOf(DatastoreMutation.delete(key("b"))));
        assertThat(metrics.counterValue(DatastoreMetricNames.BATCHES_SENT)).isEqualTo(1);
        assertThat((Integer) metrics.gaugeValue(DatastoreMetricNames.BUFFERED_MUTATIONS)).isZero();
        assertThat((Long) metrics.gaugeValue(DatastoreMetricNames.BUFFERED_BYTES)).isZero();
        assertThat(handler.events).containsExactly("flush");
    }

    @Test
    void aNullFromTheSerializerSkipsTheRecord() throws Exception {
        DatastoreWriter<String> writer =
                writer((element, context) -> null, DatastoreWriterOptions.defaults(), handler);
        writer.write("ignored", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).isEmpty();
        assertThat(handler.routed).isEmpty();
        assertThat(metrics.counterValue(DatastoreMetricNames.RECORDS_SKIPPED)).isEqualTo(1);
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_RECORDS_SEND)).isZero();
    }

    @Test
    void aSerializerFailureIsRoutedWithoutAWrite() throws Exception {
        DatastoreWriter<String> writer =
                writer(
                        (element, context) -> {
                            throw new IOException("unparseable");
                        },
                        DatastoreWriterOptions.defaults(),
                        handler);
        writer.write("bad", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).isEmpty();
        assertThat(handler.routed).hasSize(1);
        assertThat(handler.routed.get(0).getMutation()).isNull();
        assertThat(handler.routed.get(0).getCause()).hasMessage("unparseable");
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_RECORDS_SEND_ERRORS))
                .isEqualTo(1);
    }

    // --- service-allocated keys ---

    /** Upserts each record under the first key of a batch it allocates for it. */
    private static final class AllocatingSerializer
            implements KeyAllocatingSerializationSchema<String> {
        private static final long serialVersionUID = 1L;
        private transient DatastoreKeyAllocator allocator;

        @Override
        public void setKeyAllocator(DatastoreKeyAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public DatastoreMutation serialize(String element, SinkWriter.Context context)
                throws IOException {
            Key key = allocator.allocate(IncompleteKey.newBuilder(PROJECT, KIND).build()).get(0);
            return DatastoreMutation.upsert(Entity.newBuilder(key).set("v", element).build());
        }
    }

    @Test
    void aKeyAllocatingSerializerAllocatesThroughTheWritersAccess() throws Exception {
        DatastoreWriter<String> writer =
                writer(
                        new AllocatingSerializer(),
                        DatastoreWriterOptions.builder().idAllocationBatchSize(3).build(),
                        handler);
        writer.write("a", TestContexts.NO_OP);
        writer.write("b", TestContexts.NO_OP);
        writer.flush(false);

        // Each call asks for the configured batch.
        assertThat(access.allocations()).containsExactly(3, 3);
        assertThat(access.applied().keySet())
                .extracting(Key::getId)
                .containsExactlyInAnyOrder(1L, 4L);
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void theDefaultAllocationBatchIsAThousandIds() throws Exception {
        DatastoreWriter<String> writer =
                writer(new AllocatingSerializer(), DatastoreWriterOptions.defaults(), handler);
        writer.write("a", TestContexts.NO_OP);

        assertThat(access.allocations()).containsExactly(1000);
    }

    @Test
    void aTransientAllocationFailureIsRetriedWithinTheRecoveryBudget() throws Exception {
        access.failNextAllocations(StatusCode.Code.UNAVAILABLE, StatusCode.Code.DEADLINE_EXCEEDED);
        DatastoreWriter<String> writer =
                writer(new AllocatingSerializer(), fastRetries(3), handler);
        writer.write("a", TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.allocations()).hasSize(3);
        assertThat(access.applied()).hasSize(1);
        assertThat(handler.routed).isEmpty();
        assertThat(errorClass("UNAVAILABLE")).isEqualTo(1);
        assertThat(errorClass("DEADLINE_EXCEEDED")).isEqualTo(1);
    }

    @Test
    void anAllocationTheBudgetCannotFinishFailsTheJobWithoutRoutingTheRecord() throws Exception {
        access.failNextAllocations(StatusCode.Code.UNAVAILABLE, StatusCode.Code.UNAVAILABLE);
        DatastoreWriter<String> writer =
                writer(new AllocatingSerializer(), fastRetries(2), handler);

        assertThatThrownBy(() -> writer.write("a", TestContexts.NO_OP))
                .isInstanceOf(DatastoreWriter.AllocationFailure.class)
                .hasMessageContaining("after 2 attempt(s)");
        assertThat(access.allocations()).hasSize(2);
        assertThat(access.requests()).isEmpty();
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aRefusedAllocationFailsTheJobAtOnce() throws Exception {
        access.failNextAllocations(StatusCode.Code.PERMISSION_DENIED);
        DatastoreWriter<String> writer =
                writer(new AllocatingSerializer(), fastRetries(3), handler);

        assertThatThrownBy(() -> writer.write("a", TestContexts.NO_OP))
                .isInstanceOf(DatastoreWriter.AllocationFailure.class)
                .hasMessageContaining("after 1 attempt(s)")
                .hasMessageContaining("PERMISSION_DENIED");
        assertThat(handler.routed).isEmpty();
        assertThat(errorClass("PERMISSION_DENIED")).isEqualTo(1);
    }

    @Test
    void anAllocationFailureAWrappingSerializerRethrowsStillFailsTheJob() throws Exception {
        access.failNextAllocations(StatusCode.Code.PERMISSION_DENIED);
        AllocatingSerializer inner = new AllocatingSerializer();
        KeyAllocatingSerializationSchema<String> wrapping =
                new KeyAllocatingSerializationSchema<>() {
                    @Override
                    public void setKeyAllocator(DatastoreKeyAllocator allocator) {
                        inner.setKeyAllocator(allocator);
                    }

                    @Override
                    public DatastoreMutation serialize(String element, SinkWriter.Context context)
                            throws IOException {
                        try {
                            return inner.serialize(element, context);
                        } catch (IOException e) {
                            throw new IOException("wrapped", e);
                        }
                    }
                };
        DatastoreWriter<String> writer = writer(wrapping, fastRetries(3), handler);

        assertThatThrownBy(() -> writer.write("a", TestContexts.NO_OP))
                .isInstanceOf(DatastoreWriter.AllocationFailure.class);
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aShortAllocationAnswerFailsTheJob() throws Exception {
        access.answerAllocationsShort();
        DatastoreWriter<String> writer =
                writer(
                        new AllocatingSerializer(),
                        DatastoreWriterOptions.builder().idAllocationBatchSize(3).build(),
                        handler);

        assertThatThrownBy(() -> writer.write("a", TestContexts.NO_OP))
                .isInstanceOf(DatastoreWriter.AllocationFailure.class)
                .hasMessageContaining("allocated 2 ids for 3 keys");
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void anInterruptDuringTheAllocationBackoffFailsTheJobAndKeepsTheFlag() throws Exception {
        access.failNextAllocations(StatusCode.Code.UNAVAILABLE);
        DatastoreWriter<String> writer =
                writer(new AllocatingSerializer(), fastRetries(3), handler);
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> writer.write("a", TestContexts.NO_OP))
                    .isInstanceOf(DatastoreWriter.AllocationFailure.class)
                    .hasMessageContaining("Interrupted");
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aKeyOfAnotherProjectOrDatabaseIsRoutedBeforeItIsSent() throws Exception {
        DatastoreWriter<DatastoreMutation> writer = writer(handler);
        DatastoreMutation otherProject =
                DatastoreMutation.delete(Key.newBuilder("other", KIND, "x").build());
        DatastoreMutation otherDatabase =
                DatastoreMutation.delete(Key.newBuilder(PROJECT, KIND, "x", "named").build());
        writer.write(otherProject, TestContexts.NO_OP);
        writer.write(otherDatabase, TestContexts.NO_OP);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).containsExactly(List.of(key("a")));
        assertThat(handler.routed)
                .extracting(FailedMutation::getMutation)
                .containsExactly(otherProject, otherDatabase);
        assertThat(handler.routed.get(0).getErrorMessage())
                .contains("addresses project 'other'")
                .contains("projects/p/databases/(default)");
        assertThat(handler.routed.get(1).getErrorMessage())
                .contains("database id 'named'")
                .contains("Key.newBuilder(project, kind, name) for the default database");
    }

    @Test
    void commitsBeforeAKeyRepeatsSoWritesToOneKeyKeepTheirOrder() throws Exception {
        DatastoreWriter<DatastoreMutation> writer = writer(handler);
        DatastoreMutation first = upsert("a", 1);
        DatastoreMutation second = upsert("a", 2);
        writer.write(first, TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);
        writer.write(second, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests())
                .containsExactly(List.of(key("a"), key("b")), List.of(key("a")));
        assertThat(access.applied().get(key("a"))).isEqualTo(second);
    }

    @Test
    void commitsBeforeTheNextWriteWouldPassTheCountOrTheSize() throws Exception {
        DatastoreWriter<DatastoreMutation> byCount =
                writer(DatastoreWriterOptions.builder().maxBatchMutations(2).build(), handler);
        for (String name : List.of("a", "b", "c")) {
            byCount.write(upsert(name), TestContexts.NO_OP);
        }
        assertThat(access.requests()).containsExactly(List.of(key("a"), key("b")));

        FakeDatastoreDatabaseAccess sized = new FakeDatastoreDatabaseAccess();
        long one = MutationSizeEstimator.sizeOf(upsert("d"));
        DatastoreWriter<DatastoreMutation> bySize =
                new DatastoreWriter<>(
                        config(
                                identity(),
                                DatastoreWriterOptions.builder()
                                        .maxBatchBytes(
                                                MutationSizeEstimator.requestHeaderSize(DATABASE)
                                                        + 2 * one
                                                        + 1)
                                        .build(),
                                handler),
                        () -> sized,
                        (RampUpThrottle) null,
                        TestSinkWriterMetricGroup.create());
        for (String name : List.of("d", "e", "f")) {
            bySize.write(upsert(name), TestContexts.NO_OP);
        }
        assertThat(sized.requests()).containsExactly(List.of(key("d"), key("e")));
    }

    @Test
    void aWriteLargerThanTheByteCapIsStillSentAlone() throws Exception {
        DatastoreWriter<DatastoreMutation> writer =
                writer(DatastoreWriterOptions.builder().maxBatchBytes(1).build(), handler);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).containsExactly(List.of(key("a")), List.of(key("b")));
    }

    // --- retries ---

    @Test
    void aTransientFailureResendsTheWholeCommit() throws Exception {
        access.failNextCommits(StatusCode.Code.UNAVAILABLE, StatusCode.Code.DEADLINE_EXCEEDED);
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(10), handler);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).hasSize(3).allMatch(r -> r.size() == 2);
        assertThat(access.applied()).containsOnlyKeys(key("a"), key("b"));
        assertThat(handler.routed).isEmpty();
        assertThat(metrics.counterValue(DatastoreMetricNames.MUTATIONS_RETRIED)).isEqualTo(4);
        assertThat(metrics.counterValue(DatastoreMetricNames.BATCHES_SENT)).isEqualTo(3);
        assertThat(errorClass("UNAVAILABLE")).isEqualTo(1);
        assertThat(errorClass("DEADLINE_EXCEEDED")).isEqualTo(1);
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_RECORDS_SEND)).isEqualTo(2);
    }

    @Test
    void failsTheJobWhenTheRetryBudgetIsSpent() throws Exception {
        access.failNextCommits(
                StatusCode.Code.UNAVAILABLE,
                StatusCode.Code.UNAVAILABLE,
                StatusCode.Code.RESOURCE_EXHAUSTED);
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), handler);
        writer.write(upsert("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Giving up on 1 Datastore mutation(s) after 3 attempt(s)")
                .hasMessageContaining("recoveryMaxAttempts");
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aStatusNoWriteCanEarnFailsTheJobWithoutRouting() throws Exception {
        access.failNextCommits(StatusCode.Code.PERMISSION_DENIED);
        DatastoreWriter<DatastoreMutation> writer = writer(handler);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("commit of 2 mutation(s)")
                .hasMessageContaining("PERMISSION_DENIED");
        assertThat(access.requests()).hasSize(1);
        assertThat(handler.routed).isEmpty();
        assertThat(errorClass("PERMISSION_DENIED")).isEqualTo(1);
    }

    // --- solo confirmation ---

    @Test
    void anInvalidArgumentIsConfirmedAloneAndOnlyTheCulpritIsRouted() throws Exception {
        access.refuse(key("bad"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("bad"), TestContexts.NO_OP);
        writer.write(upsert("c"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests())
                .containsExactly(
                        List.of(key("a"), key("bad"), key("c")),
                        List.of(key("a")),
                        List.of(key("bad")),
                        List.of(key("c")));
        assertThat(access.applied()).containsOnlyKeys(key("a"), key("c"));
        assertThat(handler.routed)
                .extracting(f -> f.getMutation().getKey())
                .containsExactly(key("bad"));
        assertThat(handler.routed.get(0).getErrorMessage()).contains("INVALID_ARGUMENT");
        assertThat(metrics.counterValue(DatastoreMetricNames.MUTATIONS_CONFIRMED_ALONE))
                .isEqualTo(3);
        // Counted once, for the confirmed verdict, not for the commit that may have answered for
        // the culprit alone.
        assertThat(errorClass("INVALID_ARGUMENT")).isEqualTo(1);
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_RECORDS_SEND)).isEqualTo(3);
        assertThat(metrics.counterValue(TestSinkWriterMetricGroup.NUM_RECORDS_SEND_ERRORS))
                .isEqualTo(1);
    }

    @Test
    void aCommitOfOneWriteIsItsOwnConfirmation() throws Exception {
        access.refuse(key("bad"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("bad"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).containsExactly(List.of(key("bad")));
        assertThat(handler.routed).hasSize(1);
        assertThat(metrics.counterValue(DatastoreMetricNames.MUTATIONS_CONFIRMED_ALONE)).isZero();
    }

    @Test
    void anAlreadyExistsIsRoutedForAnInsertOnly() throws Exception {
        access.refuse(key("dup"), StatusCode.Code.ALREADY_EXISTS);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        DatastoreMutation insert = DatastoreMutation.insert(entity("dup", 1));
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(insert, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.routed).extracting(FailedMutation::getMutation).containsExactly(insert);
        assertThat(access.applied()).containsOnlyKeys(key("a"));

        // The same status for an upsert is not that write's to earn.
        FakeDatastoreDatabaseAccess other =
                new FakeDatastoreDatabaseAccess().refuse(key("x"), StatusCode.Code.ALREADY_EXISTS);
        DatastoreWriter<DatastoreMutation> upserting =
                new DatastoreWriter<>(
                        config(identity(), DatastoreWriterOptions.defaults(), dropping()),
                        () -> other,
                        (RampUpThrottle) null,
                        TestSinkWriterMetricGroup.create());
        upserting.write(upsert("x"), TestContexts.NO_OP);
        assertThatThrownBy(() -> upserting.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("UPSERT")
                .hasMessageContaining("ALREADY_EXISTS")
                .hasMessageContaining("does not route");
    }

    @Test
    void anAlreadyExistsOnACommitWithoutAnInsertFailsTheJobWithoutConfirming() throws Exception {
        access.refuse(key("a"), StatusCode.Code.ALREADY_EXISTS);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("commit of 2 mutation(s)");
        assertThat(access.requests()).hasSize(1);
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aNotFoundIsRoutedForAnUpdateOnceALookupOfItsKeyIsAnswered() throws Exception {
        access.refuse(key("gone"), StatusCode.Code.NOT_FOUND);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        DatastoreMutation update = DatastoreMutation.update(entity("gone", 1));
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(update, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.lookups()).containsExactly(key("gone"));
        assertThat(handler.routed).extracting(FailedMutation::getMutation).containsExactly(update);
        assertThat(handler.routed.get(0).getErrorMessage()).contains("NOT_FOUND");
        assertThat(access.applied()).containsOnlyKeys(key("a"));
    }

    @Test
    void aNotFoundWhoseLookupIsRefusedFailsTheJobAsAMissingDatabase() throws Exception {
        access.refuse(key("gone"), StatusCode.Code.NOT_FOUND)
                .failNextLookups(StatusCode.Code.NOT_FOUND);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(DatastoreMutation.update(entity("gone", 1)), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("a lookup of the same key failed with NOT_FOUND")
                .hasMessageContaining("missing or unreachable database");
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aTransientLookupFailureIsRetried() throws Exception {
        access.refuse(key("gone"), StatusCode.Code.NOT_FOUND)
                .failNextLookups(StatusCode.Code.UNAVAILABLE);
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        writer.write(DatastoreMutation.update(entity("gone", 1)), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.lookups()).containsExactly(key("gone"), key("gone"));
        assertThat(handler.routed).hasSize(1);
    }

    @Test
    void aNotFoundForAnythingButAnUpdateFailsTheJob() throws Exception {
        access.refuse(key("a"), StatusCode.Code.NOT_FOUND);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("NOT_FOUND")
                .hasMessageContaining("database is missing or unreachable");
        assertThat(access.lookups()).isEmpty();
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aTransientFailureOfASoloCommitIsRetriedWithinItsOwnBudget() throws Exception {
        FakeDatastoreDatabaseAccess flaky =
                new FakeDatastoreDatabaseAccess()
                        .refuse(key("bad"), StatusCode.Code.INVALID_ARGUMENT);
        TestSinkWriterMetricGroup flakyMetrics = TestSinkWriterMetricGroup.create();
        DatastoreWriter<DatastoreMutation> retrying =
                new DatastoreWriter<>(
                        config(identity(), fastRetries(2), dropping()),
                        () -> new SoloFlakyAccess(flaky),
                        (RampUpThrottle) null,
                        flakyMetrics);
        retrying.write(upsert("a"), TestContexts.NO_OP);
        retrying.write(upsert("bad"), TestContexts.NO_OP);
        retrying.flush(false);
        assertThat(flaky.applied()).containsOnlyKeys(key("a"));
        assertThat(handler.routed)
                .extracting(f -> f.getMutation().getKey())
                .containsExactly(key("bad"));
        assertThat(flakyMetrics.counterValue(DatastoreMetricNames.MUTATIONS_RETRIED)).isEqualTo(1);
    }

    // --- the rejection bound ---

    @Test
    void consecutiveInvalidArgumentsFailTheJobAfterRoutingAndAnAppliedWriteResetsTheRun()
            throws Exception {
        access.refuse(key("bad1"), StatusCode.Code.INVALID_ARGUMENT)
                .refuse(key("bad2"), StatusCode.Code.INVALID_ARGUMENT)
                .refuse(key("bad3"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        dropping());
        for (String name : List.of("bad1", "good", "bad2")) {
            writer.write(upsert(name), TestContexts.NO_OP);
            writer.flush(false);
        }
        writer.write(upsert("bad3"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)")
                .hasMessageContaining("maxConsecutiveRejections(-1)");
        assertThat(handler.routed).hasSize(3);
    }

    @Test
    void aWriteAppliedDuringConfirmationAlsoResetsTheRun() throws Exception {
        access.refuse(key("bad1"), StatusCode.Code.INVALID_ARGUMENT)
                .refuse(key("bad2"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        dropping());
        for (String name : List.of("bad1", "good", "bad2")) {
            writer.write(upsert(name), TestContexts.NO_OP);
        }
        writer.flush(false);

        assertThat(handler.routed).hasSize(2);
        assertThat(access.applied()).containsOnlyKeys(key("good"));
    }

    @Test
    void replayAnswersDoNotCountTowardTheBound() throws Exception {
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(1).build(),
                        dropping());
        for (int i = 0; i < 3; i++) {
            access.refuse(key("dup" + i), StatusCode.Code.ALREADY_EXISTS)
                    .refuse(key("gone" + i), StatusCode.Code.NOT_FOUND);
            writer.write(DatastoreMutation.insert(entity("dup" + i, 1)), TestContexts.NO_OP);
            writer.write(DatastoreMutation.update(entity("gone" + i, 1)), TestContexts.NO_OP);
        }
        writer.flush(false);

        assertThat(handler.routed).hasSize(6);
    }

    @Test
    void anUnboundedRunNeverFailsTheJob() throws Exception {
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder()
                                .maxConsecutiveRejections(DatastoreWriterOptions.UNBOUNDED)
                                .build(),
                        dropping());
        for (int i = 0; i < 5; i++) {
            access.refuse(key("bad" + i), StatusCode.Code.INVALID_ARGUMENT);
            writer.write(upsert("bad" + i), TestContexts.NO_OP);
            writer.flush(false);
        }

        assertThat(handler.routed).hasSize(5);
    }

    @Test
    void aReplayAnswerLeavesARunOfRejectionsAsItWas() throws Exception {
        access.refuse(key("bad1"), StatusCode.Code.INVALID_ARGUMENT)
                .refuse(key("dup"), StatusCode.Code.ALREADY_EXISTS)
                .refuse(key("bad2"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        dropping());
        writer.write(upsert("bad1"), TestContexts.NO_OP);
        writer.write(DatastoreMutation.insert(entity("dup", 1)), TestContexts.NO_OP);
        writer.write(upsert("bad2"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)");
    }

    @Test
    void theHandlerIsFlushedAfterTheRoutesTheBarriersCommitMade() throws Exception {
        access.refuse(key("bad"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("bad"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.events).containsExactly("route bad", "flush");
    }

    @Test
    void anInsertAppliedByACommitWhoseAnswerWasLostIsCountedAsAppliedOnItsRetry() throws Exception {
        // The retry of a commit that was in fact applied answers ALREADY_EXISTS; a lookup finds the
        // entity the insert wrote, so the record is not routed.
        DatastoreMutation insert = DatastoreMutation.insert(entity("i", 1));
        access.applyThenFailNextCommit(StatusCode.Code.DEADLINE_EXCEEDED)
                .refuse(key("i"), StatusCode.Code.ALREADY_EXISTS);
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        writer.write(insert, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.applied()).containsEntry(key("i"), insert);
        assertThat(handler.routed).isEmpty();
        assertThat(access.lookups()).containsExactly(key("i"));
        assertThat(errorClass("DEADLINE_EXCEEDED")).isEqualTo(1);
    }

    @Test
    void anInsertAPartlyAppliedCommitAlreadyWroteIsCountedAsAppliedWhenItIsConfirmed()
            throws Exception {
        // The emulator applied the writes ahead of an oversized entity before refusing the commit;
        // the confirmation pass re-sends them all, and an insert among them meets itself, which a
        // lookup tells from a key that held another entity.
        DatastoreMutation insert = DatastoreMutation.insert(entity("i", 1));
        access.applyWritesBeforeARefusal().refuse(key("big"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer =
                new DatastoreWriter<>(
                        config(identity(), DatastoreWriterOptions.defaults(), dropping()),
                        () -> new InsertOnceAccess(access),
                        (RampUpThrottle) null,
                        metrics);
        writer.write(insert, TestContexts.NO_OP);
        writer.write(upsert("big"), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.applied()).containsEntry(key("i"), insert);
        assertThat(handler.routed)
                .extracting(f -> f.getMutation().getKey().getName())
                .containsExactly("big");
    }

    @Test
    void theInsertsAServiceAppliedBesideARefusedOneAreNotRouted() throws Exception {
        // The service applies every write of a commit but the one it refuses with ALREADY_EXISTS
        // (measured 2026-10-11), so each new insert meets itself in the confirmation pass.
        access.servesLikeTheService().store(entity("dup", 0));
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        DatastoreMutation first = DatastoreMutation.insert(entity("new1", 1));
        DatastoreMutation duplicate = DatastoreMutation.insert(entity("dup", 2));
        DatastoreMutation second = DatastoreMutation.insert(entity("new2", 3));
        writer.write(first, TestContexts.NO_OP);
        writer.write(duplicate, TestContexts.NO_OP);
        writer.write(second, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.routed)
                .extracting(FailedMutation::getMutation)
                .containsExactly(duplicate);
        assertThat(access.applied())
                .containsEntry(key("new1"), first)
                .containsEntry(key("new2"), second);
        assertThat(access.applied().get(key("dup")).getEntity()).isEqualTo(entity("dup", 0));
    }

    @Test
    void anInsertWhoseKeyHoldsTheSameEntityIsCountedAsApplied() throws Exception {
        // A replay after a restart: the key already holds what the insert writes.
        access.servesLikeTheService().store(entity("i", 1));
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        writer.write(DatastoreMutation.insert(entity("i", 1)), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.routed).isEmpty();
    }

    @Test
    void anInsertIsRoutedWhenTheLookupAfterItsRefusalIsRefused() throws Exception {
        access.servesLikeTheService()
                .store(entity("i", 1))
                .failNextLookups(StatusCode.Code.PERMISSION_DENIED);
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        DatastoreMutation insert = DatastoreMutation.insert(entity("i", 1));
        writer.write(insert, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.lookups()).containsExactly(key("i"));
        assertThat(handler.routed).extracting(FailedMutation::getMutation).containsExactly(insert);
        assertThat(handler.routed.get(0).getCause().getSuppressed())
                .singleElement()
                .satisfies(
                        lookup ->
                                assertThat(DatastoreErrorClassifier.statusCode(lookup))
                                        .isEqualTo(StatusCode.Code.PERMISSION_DENIED));
        assertThat(errorClass("PERMISSION_DENIED")).isEqualTo(1);
    }

    @Test
    void aTransientLookupFailureAfterAnInsertsRefusalIsRetried() throws Exception {
        access.servesLikeTheService()
                .store(entity("i", 1))
                .failNextLookups(StatusCode.Code.UNAVAILABLE);
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        writer.write(DatastoreMutation.insert(entity("i", 1)), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.lookups()).containsExactly(key("i"), key("i"));
        assertThat(handler.routed).isEmpty();
        assertThat(errorClass("UNAVAILABLE")).isEqualTo(1);
    }

    @Test
    void theInsertsAServiceAppliedBesideARefusedUpdateAreNotRouted() throws Exception {
        // The NOT_FOUND half: a commit refused for an update of a missing key applies the insert
        // beside it, which then meets itself in the confirmation pass.
        access.servesLikeTheService();
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        DatastoreMutation insert = DatastoreMutation.insert(entity("new", 1));
        DatastoreMutation update = DatastoreMutation.update(entity("missing", 2));
        writer.write(insert, TestContexts.NO_OP);
        writer.write(update, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.routed).extracting(FailedMutation::getMutation).containsExactly(update);
        assertThat(access.applied()).containsEntry(key("new"), insert);
    }

    @Test
    void anInsertCountedAsAppliedLeavesARunOfRejectionsAsItWas() throws Exception {
        // The key holding the insert's entity is what a replay meets, so it neither resets the
        // run of rejections nor adds to it.
        access.servesLikeTheService()
                .store(entity("dup", 1))
                .refuse(key("bad1"), StatusCode.Code.INVALID_ARGUMENT)
                .refuse(key("bad2"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        dropping());
        writer.write(upsert("bad1"), TestContexts.NO_OP);
        writer.write(DatastoreMutation.insert(entity("dup", 1)), TestContexts.NO_OP);
        writer.write(upsert("bad2"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("maxConsecutiveRejections(2)");
    }

    @Test
    void theServiceFakeAppliesNoWriteThatIsRefusedItself() throws Exception {
        // Two duplicates in one commit: the fake must not write the second's entity while refusing
        // the first, or the second would look applied to the lookup.
        access.servesLikeTheService().store(entity("dupA", 0)).store(entity("dupB", 0));
        DatastoreWriter<DatastoreMutation> writer = writer(fastRetries(3), dropping());
        writer.write(DatastoreMutation.insert(entity("dupA", 1)), TestContexts.NO_OP);
        writer.write(DatastoreMutation.insert(entity("dupB", 2)), TestContexts.NO_OP);
        writer.flush(false);

        assertThat(handler.routed)
                .extracting(f -> f.getMutation().getKey().getName())
                .containsExactly("dupA", "dupB");
    }

    @Test
    void aNotFoundOnACommitWithoutAnUpdateFailsTheJobWithoutConfirming() throws Exception {
        access.refuse(key("a"), StatusCode.Code.NOT_FOUND);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(DatastoreMutation.insert(entity("b", 1)), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("commit of 2 mutation(s)")
                .hasMessageContaining("NOT_FOUND");
        assertThat(access.requests()).hasSize(1);
        assertThat(access.lookups()).isEmpty();
    }

    @Test
    void aFailureCarryingNoStatusFailsTheJobAndIsCountedUnclassified() throws Exception {
        access.failNextCommitWith(new IllegalStateException("channel broke"));
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("no status code")
                .hasRootCauseMessage("channel broke");
        assertThat(errorClass("UNCLASSIFIED")).isEqualTo(1);
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void anInterruptStopsAConfirmationPassBeforeItsNextCommit() throws Exception {
        access.refuse(key("bad"), StatusCode.Code.INVALID_ARGUMENT);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("bad"), TestContexts.NO_OP);

        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> writer.flush(false))
                    .isInstanceOf(InterruptedIOException.class);
            assertThat(access.requests()).hasSize(1);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void aKeyWithANullDatabaseIdIsRoutedRatherThanFailingTheJob() throws Exception {
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        DatastoreMutation nullDatabase =
                DatastoreMutation.delete(Key.newBuilder(PROJECT, KIND, "x", null).build());
        writer.write(nullDatabase, TestContexts.NO_OP);
        writer.flush(false);

        assertThat(access.requests()).isEmpty();
        assertThat(handler.routed)
                .extracting(FailedMutation::getMutation)
                .containsExactly(nullDatabase);
    }

    @Test
    void theRequestsOwnFieldsCountTowardTheByteCap() throws Exception {
        long one = MutationSizeEstimator.sizeOf(upsert("a"));
        long header = MutationSizeEstimator.requestHeaderSize(DATABASE);
        FakeDatastoreDatabaseAccess fits = new FakeDatastoreDatabaseAccess();
        DatastoreWriter<DatastoreMutation> exactly =
                new DatastoreWriter<>(
                        config(
                                identity(),
                                DatastoreWriterOptions.builder()
                                        .maxBatchBytes(header + 2 * one)
                                        .build(),
                                handler),
                        () -> fits,
                        (RampUpThrottle) null,
                        TestSinkWriterMetricGroup.create());
        exactly.write(upsert("a"), TestContexts.NO_OP);
        exactly.write(upsert("b"), TestContexts.NO_OP);
        exactly.flush(false);
        assertThat(fits.requests()).containsExactly(List.of(key("a"), key("b")));

        DatastoreWriter<DatastoreMutation> oneShort =
                writer(
                        DatastoreWriterOptions.builder()
                                .maxBatchBytes(header + 2 * one - 1)
                                .build(),
                        handler);
        oneShort.write(upsert("a"), TestContexts.NO_OP);
        oneShort.write(upsert("b"), TestContexts.NO_OP);
        assertThat(access.requests()).containsExactly(List.of(key("a")));
    }

    @Test
    void keysAddressingAnotherDatabaseCountTowardTheBound() throws Exception {
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        dropping());
        writer.write(
                DatastoreMutation.delete(Key.newBuilder(PROJECT, KIND, "a", "orders-db").build()),
                TestContexts.NO_OP);

        assertThatThrownBy(
                        () ->
                                writer.write(
                                        DatastoreMutation.delete(
                                                Key.newBuilder(PROJECT, KIND, "b", "orders-db")
                                                        .build()),
                                        TestContexts.NO_OP))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("a key addressing another database")
                .hasMessageContaining("maxConsecutiveRejections(2)");
        assertThat(handler.routed).hasSize(2);
        assertThat(access.requests()).isEmpty();
    }

    @Test
    void bufferedWritesAheadOfAStrayKeyAreCommittedAndResetTheRun() throws Exception {
        DatastoreWriter<DatastoreMutation> writer =
                writer(
                        DatastoreWriterOptions.builder().maxConsecutiveRejections(2).build(),
                        dropping());
        for (String name : List.of("a", "b", "c")) {
            writer.write(upsert(name), TestContexts.NO_OP);
            writer.write(
                    DatastoreMutation.delete(
                            Key.newBuilder(PROJECT, KIND, name, "orders-db").build()),
                    TestContexts.NO_OP);
        }

        assertThat(handler.routed).hasSize(3);
        assertThat(access.requests())
                .containsExactly(List.of(key("a")), List.of(key("b")), List.of(key("c")));
    }

    @Test
    void aKeyNamingTheDefaultDatabaseByNameIsToldTheApisSpelling() throws Exception {
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(
                DatastoreMutation.delete(Key.newBuilder(PROJECT, KIND, "a", "(default)").build()),
                TestContexts.NO_OP);

        assertThat(handler.routed.get(0).getErrorMessage())
                .contains("spells the default database as an empty id, never '(default)'");
    }

    @Test
    void aRequestLevelRefusalOfACommitOfOneReadsAsTheCommitsAndNamesTheRemedy() throws Exception {
        access.failNextCommits(StatusCode.Code.PERMISSION_DENIED);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("which is not a failure of one mutation")
                .hasMessageContaining("roles/datastore.user");
        assertThat(handler.routed).isEmpty();
    }

    @Test
    void aNotFoundForACommitWithoutAnUpdateNamesTheMissingDatabase() throws Exception {
        access.failNextCommits(StatusCode.Code.NOT_FOUND);
        DatastoreWriter<DatastoreMutation> writer = writer(dropping());
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);

        assertThatThrownBy(() -> writer.flush(false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("The database is missing, or its id is mistyped.");
    }

    // --- lifecycle and throttling ---

    @Test
    void closeSendsNothingAndClosesTheAccessAndTheHandler() throws Exception {
        DatastoreWriter<DatastoreMutation> writer = writer(handler);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.close();

        assertThat(access.requests()).isEmpty();
        assertThat(access.closeCalls()).isEqualTo(1);
        assertThat(handler.events).containsExactly("close");
        assertThat((Integer) metrics.gaugeValue(DatastoreMetricNames.BUFFERED_MUTATIONS)).isZero();
    }

    @Test
    void theThrottleIsAskedOncePerRecordItLetsThrough() throws Exception {
        AtomicLong clock = new AtomicLong(0);
        List<Long> sleeps = new ArrayList<>();
        RampUpThrottle throttle =
                new RampUpThrottle(
                        RampUpThrottle.BASE_OPS_PER_SECOND,
                        clock::get,
                        millis -> {
                            sleeps.add(millis);
                            clock.addAndGet(millis);
                        });
        DatastoreWriter<DatastoreMutation> writer =
                new DatastoreWriter<>(
                        config(identity(), DatastoreWriterOptions.defaults(), handler),
                        () -> access,
                        throttle,
                        metrics);
        writer.write(upsert("a"), TestContexts.NO_OP);
        writer.write(upsert("b"), TestContexts.NO_OP);
        writer.write((DatastoreMutation) null, TestContexts.NO_OP);

        // One operation per second for this subtask: the second record waits out the window.
        assertThat(sleeps).containsExactly(1000L);
        assertThat(metrics.counterValue(DatastoreMetricNames.THROTTLED_MILLIS)).isEqualTo(1000);
    }

    @Test
    void aDeserializedOptionsObjectIsCheckedAgainWhereTheWriterOpens() throws Exception {
        // Forged on fresh instances, never on defaults(): that one is a JVM-wide singleton
        // (ADR-0002).
        assertForgedRefused("maxBatchMutations", 0);
        assertForgedRefused("maxBatchBytes", 0L);
        assertForgedRefused("maxBatchBytes", 10L * 1024 * 1024 + 1);
        assertForgedRefused("maxConsecutiveRejections", 0);
        assertForgedRefused("requestTimeout", Duration.ZERO);
        assertForgedRefused("requestTimeout", null);
        assertForgedRefused("throttlingParallelism", 0);
        assertForgedRefused("recoveryMaxAttempts", 0);
        assertForgedRefused("recoveryInitialBackoff", Duration.ofSeconds(60));

        assertThat(DatastoreWriterOptions.defaults())
                .isEqualTo(DatastoreWriterOptions.builder().build());
        assertThat(access.closeCalls()).isZero();
    }

    private void assertForgedRefused(String fieldName, Object value) throws Exception {
        DatastoreWriterOptions forged = DatastoreWriterOptions.builder().build();
        Field field = DatastoreWriterOptions.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(forged, value);

        assertThatThrownBy(
                        () ->
                                new DatastoreWriter<>(
                                        config(identity(), forged, handler),
                                        () -> access,
                                        (RampUpThrottle) null,
                                        TestSinkWriterMetricGroup.create()))
                .as(fieldName + "=" + value)
                .isInstanceOfAny(
                        IllegalArgumentException.class,
                        IllegalStateException.class,
                        NullPointerException.class)
                .hasMessageContaining(fieldName);
    }

    @Test
    void theThrottleFollowsTheOptions() {
        assertThat(
                        DatastoreWriter.throttle(
                                DatastoreWriterOptions.builder().throttlingEnabled(false).build(),
                                4))
                .isNull();
        assertThat(DatastoreWriter.throttle(DatastoreWriterOptions.defaults(), 4).parallelism())
                .isEqualTo(4);
        assertThat(DatastoreWriter.throttle(DatastoreWriterOptions.defaults(), 0).parallelism())
                .isEqualTo(1);
        assertThat(
                        DatastoreWriter.throttle(
                                        DatastoreWriterOptions.builder()
                                                .throttlingParallelism(9)
                                                .build(),
                                        4)
                                .parallelism())
                .isEqualTo(9);
    }

    // --- fixtures ---

    private DatastoreWriter<DatastoreMutation> writer(RecordingHandler failureHandler)
            throws IOException {
        return writer(DatastoreWriterOptions.defaults(), failureHandler);
    }

    private DatastoreWriter<DatastoreMutation> writer(
            DatastoreWriterOptions options, RecordingHandler failureHandler) throws IOException {
        return writer(identity(), options, failureHandler);
    }

    private <T> DatastoreWriter<T> writer(
            DatastoreMutationSerializationSchema<T> serializer,
            DatastoreWriterOptions options,
            RecordingHandler failureHandler)
            throws IOException {
        return new DatastoreWriter<>(
                config(serializer, options, failureHandler),
                () -> access,
                (RampUpThrottle) null,
                metrics);
    }

    @SuppressWarnings("unchecked")
    private static <T> DatastoreSinkConfig<T> config(
            DatastoreMutationSerializationSchema<T> serializer,
            DatastoreWriterOptions options,
            RecordingHandler failureHandler) {
        return ((DatastoreCommitSink<T>)
                        DatastoreSink.<T>builder()
                                .database(DATABASE)
                                .serializer(serializer)
                                .writerOptions(options)
                                .failedMutationHandler(failureHandler)
                                .build())
                .getConfig();
    }

    private static DatastoreMutationSerializationSchema<DatastoreMutation> identity() {
        return (element, context) -> element;
    }

    private RecordingHandler dropping() {
        return handler;
    }

    private static DatastoreWriterOptions fastRetries(int attempts) {
        return DatastoreWriterOptions.builder()
                .recoveryInitialBackoff(Duration.ofMillis(1))
                .recoveryMaxBackoff(Duration.ofMillis(1))
                .recoveryMaxAttempts(attempts)
                .build();
    }

    private long errorClass(String code) {
        return metrics.counterValue("errorClass", code, "errors");
    }

    private static Key key(String name) {
        return Key.newBuilder(PROJECT, KIND, name).build();
    }

    private static Entity entity(String name, long value) {
        return Entity.newBuilder(key(name)).set("v", value).build();
    }

    private static DatastoreMutation upsert(String name) {
        return upsert(name, 0);
    }

    private static DatastoreMutation upsert(String name, long value) {
        return DatastoreMutation.upsert(entity(name, value));
    }

    /** Refuses an insert of a key the wrapped fake already holds, as the service does. */
    private static final class InsertOnceAccess implements DatastoreDatabaseAccess {

        private final FakeDatastoreDatabaseAccess delegate;

        private InsertOnceAccess(FakeDatastoreDatabaseAccess delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit(List<DatastoreMutation> writes) {
            for (DatastoreMutation write : writes) {
                if (write.getOperation() == DatastoreMutation.Operation.INSERT
                        && delegate.applied().containsKey(write.getKey())) {
                    delegate.requests().add(List.of(write.getKey()));
                    throw FakeDatastoreDatabaseAccess.failure(StatusCode.Code.ALREADY_EXISTS);
                }
            }
            delegate.commit(writes);
        }

        @Override
        @Nullable
        public Entity lookup(Key key) {
            return delegate.lookup(key);
        }

        @Override
        public List<Key> allocateIds(List<IncompleteKey> keys) {
            throw new UnsupportedOperationException("This test allocates no ids.");
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /** Fails the first commit of one write transiently, then defers to the wrapped fake. */
    private static final class SoloFlakyAccess implements DatastoreDatabaseAccess {

        private final FakeDatastoreDatabaseAccess delegate;
        private boolean failed;

        private SoloFlakyAccess(FakeDatastoreDatabaseAccess delegate) {
            this.delegate = delegate;
        }

        @Override
        public void commit(List<DatastoreMutation> writes) {
            if (writes.size() == 1 && !failed) {
                failed = true;
                throw FakeDatastoreDatabaseAccess.failure(StatusCode.Code.UNAVAILABLE);
            }
            delegate.commit(writes);
        }

        @Override
        @Nullable
        public Entity lookup(Key key) {
            return delegate.lookup(key);
        }

        @Override
        public List<Key> allocateIds(List<IncompleteKey> keys) {
            throw new UnsupportedOperationException("This test allocates no ids.");
        }

        @Override
        public void close() {
            delegate.close();
        }
    }

    /**
     * Records what reaches it and drops it: every test of routing runs under a policy that keeps
     * the job alive, so the writer's own decisions are what is observed.
     */
    private static final class RecordingHandler implements FailureHandler<FailedMutation> {

        private static final long serialVersionUID = 1L;

        private final List<FailedMutation> routed = new ArrayList<>();
        private final List<String> events = new ArrayList<>();

        @Override
        public void handle(FailedMutation failed) {
            routed.add(failed);
            events.add(
                    failed.getMutation() == null
                            ? "route unserialized"
                            : "route " + failed.getMutation().getKey().getName());
        }

        @Override
        public void flush() {
            events.add("flush");
        }

        @Override
        public void close() {
            events.add("close");
        }
    }
}
