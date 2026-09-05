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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Dialect;
import io.github.flink.gcp.connector.spanner.AbstractSpannerRealGcpITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.EnumMap;
import java.util.Map;

/** Confirms float-key read exactness through the pinned SDK against both service dialects. */
@Tag("gated")
@EnabledIfEnvironmentVariable(named = "SPANNER_IT_PROJECT", matches = ".+")
class SpannerFloatKeyReadRealGcpITCase extends AbstractSpannerRealGcpITCase {
    private static final Map<Dialect, DatabaseDestination> DATABASES = new EnumMap<>(Dialect.class);
    private static final Map<Dialect, Timestamp> SNAPSHOTS = new EnumMap<>(Dialect.class);

    @BeforeAll
    static void createAndSeedDatabases() throws Exception {
        for (Dialect dialect : Dialect.values()) {
            DatabaseDestination database =
                    createDatabase(dialect, SpannerFloatKeyReadTestSupport.ddl(dialect));
            DATABASES.put(dialect, database);
            SNAPSHOTS.put(
                    dialect, SpannerFloatKeyReadTestSupport.seed(client(database), dialect, false));
        }
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void floatKeyRangesMatchSqlAndFlink(Dialect dialect) throws Exception {
        DatabaseDestination database = DATABASES.get(dialect);
        SpannerFloatKeyReadTestSupport.assertNanKeysAreRejected(client(database), dialect);
        SpannerFloatKeyReadTestSupport.assertNativeRanges(
                client(database), dialect, SNAPSHOTS.get(dialect));
        SpannerFloatKeyReadTestSupport.assertSignedZeroKeys(client(database));
        SpannerFloatKeyReadTestSupport.assertFlinkScans(database, dialect, null);
    }
}
