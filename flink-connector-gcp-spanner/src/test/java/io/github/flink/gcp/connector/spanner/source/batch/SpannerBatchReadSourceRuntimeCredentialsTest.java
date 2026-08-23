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

package io.github.flink.gcp.connector.spanner.source.batch;

import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.BatchTransactionId;
import com.google.cloud.spanner.Partition;
import com.google.cloud.spanner.PartitionOptions;
import com.google.cloud.spanner.TimestampBound;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.source.SpannerReadOperation;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.SpannerSourceBuilder;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlan;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlanner;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.PartitionPlannerFactory;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStream;
import io.github.flink.gcp.connector.spanner.source.batch.reader.StructStreamOpener;
import io.github.flink.gcp.connector.testutils.FakeSourceReaderContext;
import io.github.flink.gcp.connector.testutils.FakeSplitEnumeratorContext;
import io.github.flink.gcp.connector.testutils.ServiceAccountKeyFiles;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.annotation.Nullable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that each runtime component loads the configured key and hands the result to the seam it
 * owns — and to no other seam.
 *
 * <p>The seams here are neither of the two {@code BatchClient*} types, which is the point: before
 * the seam interfaces declared the injection, the source reached its implementation by {@code
 * instanceof} and any other implementation was skipped in silence — reading as the process's
 * application default credentials rather than the configured service account, with nothing to
 * report it.
 */
class SpannerBatchReadSourceRuntimeCredentialsTest {

    @TempDir Path tempDir;

    @Test
    void theReaderHandsItsOpenerTheConfiguredKey() throws Exception {
        CapturingOpener opener = new CapturingOpener();
        CapturingPlannerFactory planners = new CapturingPlannerFactory();

        source(opener, planners).createReader(readerContext()).close();

        assertConfiguredKey(opener.credentials);
        assertThat(opener.pushes).isOne();
        assertThat(planners.minted())
                .as("the reader must not reach the enumerator's seam at all")
                .isEmpty();
    }

    @Test
    void theFreshEnumeratorHandsItsPlannerTheConfiguredKey() throws Exception {
        CapturingOpener opener = new CapturingOpener();
        CapturingPlannerFactory planners = new CapturingPlannerFactory();

        source(opener, planners).createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();

        assertConfiguredKey(planners.only().credentials);
        assertThat(planners.only().pushes).isOne();
        assertThat(opener.pushes)
                .as("the enumerator must not push onto the reader's seam")
                .isZero();
    }

    @Test
    void theRestoredEnumeratorHandsItsPlannerTheConfiguredKey() throws Exception {
        CapturingOpener opener = new CapturingOpener();
        CapturingPlannerFactory planners = new CapturingPlannerFactory();

        source(opener, planners)
                .restoreEnumerator(new FakeSplitEnumeratorContext<>(1), null)
                .close();

        assertConfiguredKey(planners.only().credentials);
        assertThat(planners.only().pushes).isOne();
        assertThat(opener.pushes)
                .as("the enumerator must not push onto the reader's seam")
                .isZero();
    }

    /**
     * Pins the "once per runtime component" half of the contract.
     *
     * <p>The reader and the enumerator run in different processes, so each reads the mounted path
     * for itself. Were the load hoisted to a field on the source instead, both would receive the
     * same object — and that field would travel in the job graph, which is exactly the credential
     * material {@code docs/adr/0096} keeps off the wire.
     */
    @Test
    void theReaderAndTheEnumeratorEachLoadTheirOwn() throws Exception {
        CapturingOpener opener = new CapturingOpener();
        CapturingPlannerFactory planners = new CapturingPlannerFactory();
        SpannerBatchReadSource<Long> source = source(opener, planners);

        source.createReader(readerContext()).close();
        source.createEnumerator(new FakeSplitEnumeratorContext<>(1)).close();

        assertConfiguredKey(opener.credentials);
        assertConfiguredKey(planners.only().credentials);
        assertThat(opener.credentials).isNotSameAs(planners.only().credentials);
    }

    /**
     * Asserts the credentials came from the configured key file rather than from anywhere else.
     *
     * <p>Non-nullness alone would not: application default credentials are non-null too on a
     * machine that has them, so a source that ignored {@code serviceAccountKeyFile(...)} entirely
     * would pass. The client email is the fixture's own sentinel.
     */
    private static void assertConfiguredKey(@Nullable GoogleCredentials credentials) {
        assertThat(credentials)
                .isInstanceOf(ServiceAccountCredentials.class)
                .extracting(loaded -> ((ServiceAccountCredentials) loaded).getClientEmail())
                .isEqualTo(ServiceAccountKeyFiles.CLIENT_EMAIL);
    }

    /**
     * Builds a source over a real key file, with both seams injected.
     *
     * <p>No emulator endpoint, unlike {@link TestSources#source}: the builder rejects one combined
     * with a key file, and a key file is what this test is about. That also means nothing here may
     * reach {@code SpannerClients.settings(database, null, null)}, which would resolve application
     * default credentials and make these tests answer differently on a developer machine than in
     * CI. Nothing does today — no client is built, because no fetcher starts and no enumerator is
     * started — and a change that made {@code createReader} build one eagerly would have to keep
     * that true.
     */
    private SpannerBatchReadSource<Long> source(
            CapturingOpener opener, CapturingPlannerFactory planners) throws Exception {
        SpannerSourceBuilder<Long> builder =
                SpannerSource.<Long>builder()
                        .database(DatabaseDestination.of("p", "i", "d"))
                        .readOperation(TestSources.OPERATION)
                        .deserializer(new TestSources.IdDeserializer())
                        .serviceAccountKeyFile(ServiceAccountKeyFiles.create(tempDir).toString());
        TestSources.withOpener(builder, opener);
        return (SpannerBatchReadSource<Long>)
                TestSources.withPlannerFactory(builder, planners).build();
    }

    private static FakeSourceReaderContext readerContext() {
        return new FakeSourceReaderContext(
                InternalSourceReaderMetricGroup.mock(new MetricListener().getMetricGroup()));
    }

    /** Records what the reader handed it, and how often, while reaching no service. */
    private static final class CapturingOpener implements StructStreamOpener {

        private static final long serialVersionUID = 1L;

        @Nullable private GoogleCredentials credentials;
        private int pushes;

        @Override
        public StructStream open(BatchTransactionId batchTransactionId, Partition partition) {
            throw new UnsupportedOperationException("This opener never reads.");
        }

        @Override
        public void useCredentials(@Nullable GoogleCredentials credentials) {
            this.credentials = credentials;
            pushes++;
        }

        @Override
        public void close() {}
    }

    /** Mints capturing planners and records them, so a test can assert how many were minted. */
    private static final class CapturingPlannerFactory implements PartitionPlannerFactory {

        private static final long serialVersionUID = 1L;

        // Transient because the seams it records are no longer serializable. No test round-trips
        // this factory; one that did would need the lazy accessor the Scripted* doubles use,
        // because a transient field deserializes to null.
        private final transient List<CapturingPlanner> minted = new ArrayList<>();

        @Override
        public PartitionPlanner create() {
            CapturingPlanner planner = new CapturingPlanner();
            minted.add(planner);
            return planner;
        }

        private List<CapturingPlanner> minted() {
            return minted;
        }

        /**
         * Returns the one planner minted, failing when there was not exactly one.
         *
         * <p>The count is half of what each test asserts: one enumerator must mint one planner, and
         * a source that minted two would otherwise pass every credential assertion below.
         */
        private CapturingPlanner only() {
            assertThat(minted).as("one enumerator mints exactly one planner").hasSize(1);
            return minted.get(0);
        }
    }

    /** Records what the enumerator handed it, and how often, while reaching no service. */
    private static final class CapturingPlanner implements PartitionPlanner {

        @Nullable private GoogleCredentials credentials;
        private int pushes;

        @Override
        public PartitionPlan plan(
                SpannerReadOperation operation,
                TimestampBound bound,
                PartitionOptions partitionOptions,
                boolean dataBoostEnabled,
                @Nullable SpannerRpcPriority rpcPriority) {
            throw new UnsupportedOperationException("This planner never plans.");
        }

        @Override
        public void useCredentials(@Nullable GoogleCredentials credentials) {
            this.credentials = credentials;
            pushes++;
        }

        @Override
        public void close() {}
    }
}
