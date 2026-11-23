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

package io.github.flink.gcp.connector.spanner.table.source;

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;

import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import io.github.flink.gcp.connector.testutils.GrpcEventLoopThreads;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The lookup's Spanner client runs on the connector's own event-loop group and stops it when the
 * lookup closes (docs/adr/0208, #2265); {@code SpannerEventLoopITCase} covers the other components.
 */
class SpannerLookupEventLoopITCase extends AbstractSpannerEmulatorITCase {

    @Test
    void theLookupsClientRunsOnTheGroupAndStopsItOnClose() throws Exception {
        assertThat(GrpcEventLoopThreads.awaitNone(Duration.ofSeconds(60)))
                .as("no %s* thread before the test", GrpcEventLoopThreads.PREFIX)
                .isTrue();
        DatabaseDestination database =
                createDatabase(
                        Dialect.GOOGLE_STANDARD_SQL,
                        "CREATE TABLE people (id INT64 NOT NULL, name STRING(MAX))"
                                + " PRIMARY KEY (id)");
        client(database)
                .write(
                        Collections.singletonList(
                                Mutation.newInsertBuilder("people")
                                        .set("id")
                                        .to(1)
                                        .set("name")
                                        .to("Ada")
                                        .build()));

        SpannerDatabaseRowLookup lookup =
                new SpannerDatabaseRowLookup(
                        database, "people", Arrays.asList("id", "name"), emulatorEndpoint());
        try {
            lookup.open(new UnregisteredMetricsGroup());
            assertThat(lookup.read(Key.of(1L))).isNotNull();
            assertThat(GrpcEventLoopThreads.alive())
                    .as("a %s* thread while the lookup is open", GrpcEventLoopThreads.PREFIX)
                    .isTrue();
        } finally {
            lookup.close();
        }

        assertThat(GrpcEventLoopThreads.awaitNone())
                .as("no %s* thread once the lookup closed", GrpcEventLoopThreads.PREFIX)
                .isTrue();
    }
}
