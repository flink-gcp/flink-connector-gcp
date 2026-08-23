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

import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.spanner.source.SpannerSource;
import io.github.flink.gcp.connector.spanner.source.TestSources;
import io.github.flink.gcp.connector.spanner.source.batch.enumerator.ScriptedPartitionPlanner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Verifies the source loads key files in every runtime process that opens a client. */
class SpannerBatchReadSourceCredentialsTest {

    private static final String MISSING_KEY = "/missing/spanner-service-account.json";

    @AfterEach
    void forgetRecordings() {
        ScriptedPartitionPlanner.reset();
    }

    @Test
    void freshEnumeratorLoadsCredentialsOnTheJobManager() {
        assertCredentialFailure(() -> source().createEnumerator(null));
    }

    @Test
    void restoredEnumeratorReloadsCredentialsOnTheJobManager() {
        assertCredentialFailure(() -> source().restoreEnumerator(null, null));
    }

    @Test
    void readerLoadsCredentialsOnTheTaskManager() {
        assertCredentialFailure(() -> source().createReader(null));
    }

    /**
     * Pins the ordering the source's own javadoc claims: the key is loaded <em>before</em> the
     * planner is minted, so a key file that cannot be read leaves nothing built to release.
     *
     * <p>Without this, moving the load below the mint would keep every assertion above green while
     * stranding one planner per failed attempt on the JobManager.
     */
    @Test
    void nothingIsMintedWhenTheKeyFileCannotBeRead() {
        ScriptedPartitionPlanner.Factory planner =
                ScriptedPartitionPlanner.planning("credentials", "a");
        SpannerBatchReadSource<Long> source =
                (SpannerBatchReadSource<Long>)
                        TestSources.withPlannerFactory(
                                        SpannerSource.<Long>builder()
                                                .database(DatabaseDestination.of("p", "i", "d"))
                                                .readOperation(TestSources.OPERATION)
                                                .deserializer(new TestSources.IdDeserializer())
                                                .serviceAccountKeyFile(MISSING_KEY),
                                        planner)
                                .build();

        assertCredentialFailure(() -> source.createEnumerator(null));

        assertThat(planner.minted()).as("the load failed before anything was minted").isEmpty();
    }

    private static SpannerBatchReadSource<Long> source() {
        return (SpannerBatchReadSource<Long>)
                SpannerSource.<Long>builder()
                        .database(DatabaseDestination.of("p", "i", "d"))
                        .readOperation(TestSources.OPERATION)
                        .deserializer(new TestSources.IdDeserializer())
                        .serviceAccountKeyFile(MISSING_KEY)
                        .build();
    }

    private static void assertCredentialFailure(ThrowingCall call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(IOException.class)
                .hasMessage("Failed to load the configured Spanner service-account key file.")
                .hasNoCause();
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
