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

package io.github.flink.gcp.connector.bigquery.table.source;

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.cloud.bigquery.storage.v1.CreateReadSessionRequest;
import com.google.cloud.bigquery.storage.v1.ReadSession;
import io.github.flink.gcp.connector.base.rpc.GrpcEventLoops;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySource;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceBuilder;
import io.github.flink.gcp.connector.bigquery.source.BigQuerySourceConfig;
import io.github.flink.gcp.connector.bigquery.source.BigQueryStorageReadSource;
import io.github.flink.gcp.connector.bigquery.source.TestSources;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ReadSessionCreatorFactory;
import io.github.flink.gcp.connector.bigquery.source.enumerator.ScriptedReadSessionCreator;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStream;
import io.github.flink.gcp.connector.bigquery.source.reader.RowStreamOpener;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializationSchema;
import io.grpc.Status;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.UnaryOperator;

import static io.github.flink.gcp.connector.bigquery.source.enumerator.ScriptedReadSessionCreator.streamName;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryFullCacheInputFormatTest {

    private static final String OPENER = "full-cache-format";

    private static final RowType ROW_TYPE =
            (RowType)
                    DataTypes.ROW(
                                    DataTypes.FIELD("id", DataTypes.BIGINT().notNull()),
                                    DataTypes.FIELD("name", DataTypes.STRING().notNull()))
                            .getLogicalType();

    @AfterEach
    void clearInterrupt() {
        // A test that fails before it clears its own interrupt must not leak it into the next.
        Thread.interrupted();
    }

    @Test
    void aLoadReadsEveryStreamOfOneSessionCreatedInTheOpenedCopy() throws Exception {
        ScriptedReadSessionCreator.Factory sessions =
                ScriptedReadSessionCreator.Factory.withStreams(3);
        BigQueryFullCacheInputFormat planned =
                format(sessions, opener(new int[] {0, 4}, new int[] {4, 0}, new int[] {4, 3}));

        GenericInputSplit[] splits = planned.createInputSplits(4);
        // Flink plans on the format it holds and opens a deserialized copy, closing only the copy.
        BigQueryFullCacheInputFormat copy = InstantiationUtil.clone(planned);
        List<Long> ids = readAll(copy, splits);

        assertThat(splits).hasSize(1);
        assertThat(sessions.minted()).as("planning opens no client").isEmpty();
        ScriptedReadSessionCreator creator = sessionsOf(copy).only();
        assertThat(creator.creations()).isEqualTo(1);
        assertThat(creator.isClosed()).as("closed once the session exists").isTrue();
        assertThat(ids).containsExactly(0L, 1L, 2L, 3L, 4L, 5L, 6L);
        assertThat(LoadRowStreamOpener.events(OPENER))
                .containsExactly(
                        "open " + streamName(0) + "@0",
                        "close " + streamName(0),
                        "open " + streamName(1) + "@0",
                        "close " + streamName(1),
                        "open " + streamName(2) + "@0",
                        "close " + streamName(2),
                        "close opener");
    }

    @Test
    void everyLoadReadsThroughItsOwnCopyAndSession() throws Exception {
        // The opener refuses to open a stream once closed, as the real one does, so a second load
        // through the same copy would fail; Flink clones the format for every load.
        BigQueryFullCacheInputFormat planned =
                format(ScriptedReadSessionCreator.Factory.withStreams(1), opener(new int[] {0, 2}));
        List<BigQueryFullCacheInputFormat> copies = new ArrayList<>();

        for (int load = 0; load < 2; load++) {
            BigQueryFullCacheInputFormat copy = InstantiationUtil.clone(planned);
            copies.add(copy);
            assertThat(readAll(copy, planned.createInputSplits(1))).containsExactly(0L, 1L);
        }

        assertThat(copies)
                .allSatisfy(copy -> assertThat(sessionsOf(copy).only().isClosed()).isTrue());
        assertThat(LoadRowStreamOpener.events(OPENER))
                .filteredOn("close opener"::equals)
                .hasSize(2);
    }

    @Test
    void everyCallThatCanReleaseAGrpcLeaseRunsWhereAnInterruptCanEndTheWaitForTheGroup()
            throws Exception {
        // Flink stops a load by interrupting it and gives it 10 s; a release of the last gRPC
        // lease outside the scope would wait for the event-loop group up to 11 s whatever the
        // interrupt says (#2269). The real clients release on the calling thread: the session
        // creator inside create(), the opener's client on a failed creation inside open() and
        // otherwise inside close(). The fakes record where the format makes those calls.
        ScopeRecording.SEEN.clear();
        BigQueryFullCacheInputFormat format =
                format(
                        new ScopeRecording.Sessions(),
                        new ScopeRecording.Opener(opener(new int[] {0, 2})));

        assertThat(readAll(format, format.createInputSplits(1))).containsExactly(0L, 1L);

        assertThat(ScopeRecording.SEEN)
                .containsExactly(
                        "create session in scope: true",
                        "release creator in scope: true",
                        "open stream in scope: true",
                        "release opener in scope: true");
        assertThat(GrpcEventLoops.inInterruptibleReleases())
                .as("the scope ended with each call of the load")
                .isFalse();
    }

    @Test
    void anEmptyTableLoadsNoRow() throws Exception {
        BigQueryFullCacheInputFormat format =
                format(ScriptedReadSessionCreator.Factory.withStreams(0), opener());

        assertThat(readAll(format, format.createInputSplits(1))).isEmpty();
        assertThat(LoadRowStreamOpener.events(OPENER)).containsExactly("close opener");
    }

    @Test
    void aStreamThatBreaksFailsTheLoadAndIsReleasedBeforeItsClient() throws Exception {
        // The session expired in the past, so the failure also carries a load's explanation of
        // the expiry in place of the scan's remedies, its fraction of a second kept.
        BigQueryFullCacheInputFormat format =
                format(
                        new SessionsExpiringAt(2, Instant.parse("2026-08-09T12:00:00.900Z")),
                        opener(new int[] {0, 3}, new int[] {3, 1}).breakingAfter(streamName(0), 1));
        format.open(format.createInputSplits(1)[0]);

        // The fetch that reads row 0 meets the break and fails whole, so no row is handed over.
        assertThatThrownBy(() -> format.nextRecord(null))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("expired at 2026-08-09T12:00:00.900Z")
                .hasMessageContaining("until the job restarts")
                .cause()
                .hasMessageStartingWith(
                        "Failed to read from the BigQuery read stream "
                                + streamName(0)
                                + " at offset 1")
                .hasMessageNotContaining("parallelism");
        format.close();

        // The broken stream is still the reader's open one, so close() releases it, and the client
        // after it.
        assertThat(LoadRowStreamOpener.events(OPENER))
                .containsExactly(
                        "open " + streamName(0) + "@0", "close " + streamName(0), "close opener");
    }

    @Test
    void aRowThatCannotBeConvertedFailsTheLoad() throws Exception {
        // The table's own converter refuses an id no TINYINT holds, as it refuses an overflowing
        // INT in the emulator ITCase, with an unchecked exception; the load must fail on it rather
        // than end empty.
        RowType tinyIds =
                (RowType)
                        DataTypes.ROW(
                                        DataTypes.FIELD("id", DataTypes.TINYINT().notNull()),
                                        DataTypes.FIELD("name", DataTypes.STRING().notNull()))
                                .getLogicalType();
        BigQueryFullCacheInputFormat format =
                new BigQueryFullCacheInputFormat(
                        config(
                                ScriptedReadSessionCreator.Factory.withStreams(1),
                                opener(new int[] {126, 3}),
                                new RowDataDeserializationSchema(
                                        tinyIds, null, InternalTypeInfo.of(tinyIds)),
                                builder -> builder.maxStreamCount(1)));
        format.open(format.createInputSplits(1)[0]);

        assertThatThrownBy(() -> format.nextRecord(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not fit TINYINT");
        format.close();
    }

    @Test
    void aFailedReleaseAfterACompleteReadIsReportedAsAnIoException() throws Exception {
        // Flink's loader keeps the rows of a load whose close() throws an IOException and fails
        // the load on anything else.
        BigQueryFullCacheInputFormat format =
                format(
                        ScriptedReadSessionCreator.Factory.withStreams(1),
                        opener(new int[] {0, 1}).failingRelease());
        format.open(format.createInputSplits(1)[0]);
        assertThat(format.nextRecord(null).getLong(0)).isZero();
        assertThat(format.reachedEnd()).isTrue();

        assertThatThrownBy(format::close)
                .isInstanceOf(IOException.class)
                .hasRootCauseMessage("release failed");
    }

    @Test
    void aSessionThatCannotBeCreatedFailsTheOpenAndReleasesItsCreator() throws Exception {
        RefusingSessions.MINTED.clear();
        BigQueryFullCacheInputFormat format = format(new RefusingSessions(), opener());

        assertThatThrownBy(() -> format.open(format.createInputSplits(1)[0]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("refused");
        format.close();

        assertThat(RefusingSessions.MINTED)
                .singleElement()
                .satisfies(c -> assertThat(c.isClosed()).isTrue());
        assertThat(LoadRowStreamOpener.events(OPENER)).isEmpty();
    }

    @Test
    void anInterruptedLoadEndsBeforeItCreatesASession() throws Exception {
        ScriptedReadSessionCreator.Factory sessions =
                ScriptedReadSessionCreator.Factory.withStreams(1);
        BigQueryFullCacheInputFormat format = format(sessions, opener(new int[] {0, 2}));

        Thread.currentThread().interrupt();
        format.open(format.createInputSplits(1)[0]);
        boolean ended = format.reachedEnd();
        format.close();

        assertThat(Thread.interrupted()).as("the interrupt is kept for Flink's loader").isTrue();
        assertThat(ended).isTrue();
        assertThat(sessions.minted()).isEmpty();
        assertThat(LoadRowStreamOpener.events(OPENER)).isEmpty();
    }

    @Test
    void aLoadInterruptedAfterItsSessionAnswersEndsBeforeItBuildsAReadClient() throws Exception {
        // An interrupt can arrive after the session's call has answered, before the load reads.
        InterruptingSessions.MINTED.clear();
        BigQueryFullCacheInputFormat format =
                format(new InterruptingSessions(), opener(new int[] {0, 2}));

        format.open(format.createInputSplits(1)[0]);
        boolean ended = format.reachedEnd();
        format.close();

        assertThat(Thread.interrupted()).as("the interrupt is kept for Flink's loader").isTrue();
        assertThat(ended).isTrue();
        assertThat(InterruptingSessions.MINTED)
                .singleElement()
                .satisfies(creator -> assertThat(creator.isClosed()).isTrue());
        assertThat(LoadRowStreamOpener.events(OPENER)).isEmpty();
    }

    @Test
    void aSessionCreationInterruptedByFlinkEndsTheLoadInsteadOfFailingIt() throws Exception {
        // ReadClientSessionCreator cancels the call when its wait is interrupted, keeps the
        // interrupt and throws InterruptedIOException.
        InterruptedCreation.MINTED.clear();
        BigQueryFullCacheInputFormat format =
                format(new InterruptedCreation(true), opener(new int[] {0, 2}));

        format.open(format.createInputSplits(1)[0]);
        boolean ended = format.reachedEnd();
        format.close();

        assertThat(Thread.interrupted()).as("the interrupt is kept for Flink's loader").isTrue();
        assertThat(ended).isTrue();
        assertThat(InterruptedCreation.MINTED)
                .singleElement()
                .satisfies(closed -> assertThat(closed.get()).isTrue());
        assertThat(LoadRowStreamOpener.events(OPENER)).isEmpty();
    }

    @Test
    void anInterruptedIoFailureWithoutAnInterruptStillFailsTheLoad() throws Exception {
        BigQueryFullCacheInputFormat format =
                format(new InterruptedCreation(false), opener(new int[] {0, 2}));

        assertThatThrownBy(() -> format.open(format.createInputSplits(1)[0]))
                .isInstanceOf(InterruptedIOException.class);
        format.close();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();
    }

    @Test
    void aViewRefusalIsExplainedForALookup() throws Exception {
        // The shape ReadClientSessionCreator#create throws: its hint around the service's refusal.
        IOException refusal =
                new IOException(
                        "BigQuery refused to read this as a table ... read it with query(...)",
                        apiException(
                                Status.Code.INVALID_ARGUMENT,
                                "request failed: non-table entities cannot be read with the"
                                        + " storage API"));
        BigQueryFullCacheInputFormat format = format(new FailingSessions(refusal, null), opener());

        assertThatThrownBy(() -> format.open(format.createInputSplits(1)[0]))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("because it is a view")
                .hasMessageContaining("does not apply here")
                .cause()
                .isSameAs(refusal);
        format.close();
    }

    @Test
    void anotherRefusedSessionFailsUnexplained() throws Exception {
        // INVALID_ARGUMENT is also an unparsable row restriction, which a load does meet.
        IOException refusal =
                new IOException(
                        "Failed to create the session",
                        apiException(Status.Code.INVALID_ARGUMENT, "Invalid row restriction"));
        BigQueryFullCacheInputFormat format = format(new FailingSessions(refusal, null), opener());

        assertThatThrownBy(() -> format.open(format.createInputSplits(1)[0])).isSameAs(refusal);
        format.close();
    }

    @Test
    void aFailureBeforeTheSessionExpiresIsNotExplainedAsExpiry() throws Exception {
        BigQueryFullCacheInputFormat format =
                format(
                        new SessionsExpiringAt(1, Instant.now().plusSeconds(86_400)),
                        opener(new int[] {0, 3}).breakingAfter(streamName(0), 1));
        format.open(format.createInputSplits(1)[0]);

        assertThatThrownBy(() -> format.nextRecord(null))
                .isInstanceOf(IOException.class)
                .hasMessageStartingWith(
                        "Failed to read from the BigQuery read stream "
                                + streamName(0)
                                + " at offset 1");
        format.close();
    }

    @Test
    void aLoadInterruptedWhileReadingStopsInsteadOfFailing() throws Exception {
        BigQueryFullCacheInputFormat format =
                format(
                        ScriptedReadSessionCreator.Factory.withStreams(1),
                        opener(new int[] {0, 3})
                                .breakingAfter(streamName(0), 1)
                                .interruptingOnBreak());
        format.open(format.createInputSplits(1)[0]);

        RowData row = format.nextRecord(null);
        boolean ended = format.reachedEnd();
        format.close();

        assertThat(Thread.interrupted()).as("the interrupt is kept for Flink's loader").isTrue();
        assertThat(row).isNull();
        assertThat(ended).isTrue();
        assertThat(LoadRowStreamOpener.events(OPENER))
                .containsExactly(
                        "open " + streamName(0) + "@0", "close " + streamName(0), "close opener");
    }

    @Test
    void aFatalReleaseOfAFailedCreatorReplacesTheFailure() throws Exception {
        OutOfMemoryError fatal = new OutOfMemoryError("release");
        BigQueryFullCacheInputFormat format =
                format(new FailingSessions(new IllegalStateException("refused"), fatal), opener());

        assertThatThrownBy(() -> format.open(format.createInputSplits(1)[0]))
                .isSameAs(fatal)
                .satisfies(
                        e ->
                                assertThat(e.getSuppressed())
                                        .singleElement()
                                        .satisfies(s -> assertThat(s).hasMessage("refused")));
        format.close();
    }

    @Test
    void closeBeforeOpenAndTwiceIsHarmless() throws Exception {
        BigQueryFullCacheInputFormat format =
                format(ScriptedReadSessionCreator.Factory.withStreams(1), opener(new int[] {0, 1}));

        format.close();
        readAll(format, format.createInputSplits(1));
        format.close();

        assertThat(LoadRowStreamOpener.events(OPENER))
                .filteredOn("close opener"::equals)
                .hasSize(1);
    }

    @Test
    void aConfigurationTheLoadCannotReadIsRefused() {
        ScriptedReadSessionCreator.Factory sessions =
                ScriptedReadSessionCreator.Factory.withStreams(1);

        assertThatThrownBy(
                        () ->
                                new BigQueryFullCacheInputFormat(
                                        config(sessions, opener(), builder -> builder)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("one stream");
        assertThatThrownBy(
                        () ->
                                new BigQueryFullCacheInputFormat(
                                        config(
                                                sessions,
                                                opener(),
                                                builder ->
                                                        builder.maxStreamCount(1)
                                                                .snapshotTime(
                                                                        Instant.parse(
                                                                                "2026-01-01T00:00:00Z")))))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshot time");
    }

    private static List<Long> readAll(
            BigQueryFullCacheInputFormat format, GenericInputSplit[] splits) throws IOException {
        List<Long> ids = new ArrayList<>();
        for (GenericInputSplit split : splits) {
            format.open(split);
            try {
                RowData row;
                while ((row = format.nextRecord(null)) != null) {
                    ids.add(row.getLong(0));
                    assertThat(row.getString(1).toString()).isEqualTo("row-" + row.getLong(0));
                }
                assertThat(format.reachedEnd()).isTrue();
            } finally {
                format.close();
            }
        }
        return ids;
    }

    private static ScriptedReadSessionCreator.Factory sessionsOf(
            BigQueryFullCacheInputFormat format) {
        return (ScriptedReadSessionCreator.Factory) format.getConfig().getSessionCreatorFactory();
    }

    /** An opener whose stream {@code i} holds the {@code {firstId, count}} range at {@code i}. */
    private static LoadRowStreamOpener opener(int[]... ranges) {
        Map<String, int[]> byStream = new HashMap<>();
        for (int i = 0; i < ranges.length; i++) {
            byStream.put(streamName(i), ranges[i]);
        }
        return LoadRowStreamOpener.of(OPENER, byStream);
    }

    private static BigQueryFullCacheInputFormat format(
            ReadSessionCreatorFactory sessions, RowStreamOpener opener) {
        return new BigQueryFullCacheInputFormat(
                config(sessions, opener, builder -> builder.maxStreamCount(1)));
    }

    private static BigQuerySourceConfig<RowData> config(
            ReadSessionCreatorFactory sessions,
            RowStreamOpener opener,
            UnaryOperator<BigQuerySourceBuilder<RowData>> customizer) {
        return config(
                sessions,
                opener,
                new RowDataDeserializationSchema(ROW_TYPE, null, InternalTypeInfo.of(ROW_TYPE)),
                customizer);
    }

    private static BigQuerySourceConfig<RowData> config(
            ReadSessionCreatorFactory sessions,
            RowStreamOpener opener,
            BigQueryRowDeserializationSchema<RowData> deserializer,
            UnaryOperator<BigQuerySourceBuilder<RowData>> customizer) {
        BigQuerySourceBuilder<RowData> builder =
                BigQuerySource.<RowData>builder()
                        .table(TestSources.TABLE)
                        .deserializer(deserializer)
                        .emulatorEndpoint("localhost:1");
        TestSources.withSeams(customizer.apply(builder), sessions, opener);
        return ((BigQueryStorageReadSource<RowData>) builder.build()).getConfig();
    }

    /**
     * A session creator factory and an opener that record whether the load called them inside
     * {@link GrpcEventLoops#interruptibleReleases()}.
     */
    private static final class ScopeRecording {

        static final List<String> SEEN = new CopyOnWriteArrayList<>();

        private static void record(String what) {
            SEEN.add(what + " in scope: " + GrpcEventLoops.inInterruptibleReleases());
        }

        /** Mints creators of a one-stream session. */
        private static final class Sessions implements ReadSessionCreatorFactory {

            private static final long serialVersionUID = 1L;

            @Override
            public ReadSessionCreator create() {
                ScriptedReadSessionCreator scripted = ScriptedReadSessionCreator.withStreams(1);
                return new ReadSessionCreator() {
                    @Override
                    public ReadSession create(CreateReadSessionRequest request) throws IOException {
                        record("create session");
                        return scripted.create(request);
                    }

                    @Override
                    public void close() {
                        record("release creator");
                        scripted.close();
                    }
                };
            }
        }

        /** Delegates to an opener, recording where it opens a stream and where it is released. */
        private static final class Opener implements RowStreamOpener {

            private static final long serialVersionUID = 1L;

            private final RowStreamOpener delegate;

            private Opener(RowStreamOpener delegate) {
                this.delegate = delegate;
            }

            @Override
            public RowStream open(String streamName, long offset) throws IOException {
                record("open stream");
                return delegate.open(streamName, offset);
            }

            @Override
            public void setRetryListener(Runnable onRetry) {
                delegate.setRetryListener(onRetry);
            }

            @Override
            public void close() throws IOException {
                record("release opener");
                delegate.close();
            }
        }
    }

    /** Mints creators that refuse to create a session, and records them. */
    private static final class RefusingSessions implements ReadSessionCreatorFactory {

        private static final long serialVersionUID = 1L;

        static final List<ScriptedReadSessionCreator> MINTED = new CopyOnWriteArrayList<>();

        @Override
        public ReadSessionCreator create() {
            ScriptedReadSessionCreator creator =
                    ScriptedReadSessionCreator.failing(new IllegalStateException("refused"));
            MINTED.add(creator);
            return creator;
        }
    }

    /** Mints creators that interrupt the calling thread while they create a session. */
    private static final class InterruptingSessions implements ReadSessionCreatorFactory {

        private static final long serialVersionUID = 1L;

        static final List<ScriptedReadSessionCreator> MINTED = new CopyOnWriteArrayList<>();

        @Override
        public ReadSessionCreator create() {
            ScriptedReadSessionCreator scripted = ScriptedReadSessionCreator.withStreams(1);
            MINTED.add(scripted);
            return new ReadSessionCreator() {
                @Override
                public ReadSession create(CreateReadSessionRequest request) throws IOException {
                    ReadSession session = scripted.create(request);
                    Thread.currentThread().interrupt();
                    return session;
                }

                @Override
                public void close() {
                    scripted.close();
                }
            };
        }
    }

    /** Mints creators whose session creation fails, and whose release may fail too. */
    private static final class FailingSessions implements ReadSessionCreatorFactory {

        private static final long serialVersionUID = 1L;

        private final Exception creation;
        @Nullable private final Error release;

        private FailingSessions(Exception creation, @Nullable Error release) {
            this.creation = creation;
            this.release = release;
        }

        @Override
        public ReadSessionCreator create() {
            return new ReadSessionCreator() {
                @Override
                public ReadSession create(CreateReadSessionRequest request) throws IOException {
                    if (creation instanceof IOException) {
                        throw (IOException) creation;
                    }
                    throw (RuntimeException) creation;
                }

                @Override
                public void close() {
                    if (release != null) {
                        throw release;
                    }
                }
            };
        }
    }

    private static ApiException apiException(Status.Code code, String message) {
        return ApiExceptionFactory.createException(message, null, GrpcStatusCode.of(code), false);
    }

    /** Mints creators answering with sessions of the given stream count and expiry. */
    private static final class SessionsExpiringAt implements ReadSessionCreatorFactory {

        private static final long serialVersionUID = 1L;

        private final int streamCount;
        private final Instant expireTime;

        private SessionsExpiringAt(int streamCount, Instant expireTime) {
            this.streamCount = streamCount;
            this.expireTime = expireTime;
        }

        @Override
        public ReadSessionCreator create() {
            ScriptedReadSessionCreator scripted =
                    ScriptedReadSessionCreator.withStreams(streamCount);
            return new ReadSessionCreator() {
                @Override
                public ReadSession create(CreateReadSessionRequest request) throws IOException {
                    return scripted.create(request).toBuilder()
                            .setExpireTime(
                                    com.google.protobuf.Timestamp.newBuilder()
                                            .setSeconds(expireTime.getEpochSecond())
                                            .setNanos(expireTime.getNano()))
                            .build();
                }

                @Override
                public void close() {
                    scripted.close();
                }
            };
        }
    }

    /**
     * Mints creators failing as an interrupted {@code ReadClientSessionCreator} does, with the
     * interrupt kept or, to show the load does not stop on the exception type alone, cleared.
     */
    private static final class InterruptedCreation implements ReadSessionCreatorFactory {

        private static final long serialVersionUID = 1L;

        static final List<AtomicBoolean> MINTED = new CopyOnWriteArrayList<>();

        private final boolean interrupt;

        private InterruptedCreation(boolean interrupt) {
            this.interrupt = interrupt;
        }

        @Override
        public ReadSessionCreator create() {
            AtomicBoolean closed = new AtomicBoolean();
            MINTED.add(closed);
            return new ReadSessionCreator() {
                @Override
                public ReadSession create(CreateReadSessionRequest request) throws IOException {
                    if (interrupt) {
                        Thread.currentThread().interrupt();
                    }
                    throw new InterruptedIOException(
                            "Interrupted while creating a BigQuery read session");
                }

                @Override
                public void close() {
                    closed.set(true);
                }
            };
        }
    }
}
