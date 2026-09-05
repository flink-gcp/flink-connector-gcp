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
import com.google.cloud.spanner.Mutation;
import io.github.flink.gcp.connector.spanner.AbstractSpannerEmulatorITCase;
import io.github.flink.gcp.connector.spanner.DatabaseDestination;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SpannerFloatKeyReadITCase extends AbstractSpannerEmulatorITCase {
    @ParameterizedTest
    @EnumSource(Dialect.class)
    void emulatorDoesNotEnforceTheNanKeyProhibition(Dialect dialect) throws Exception {
        DatabaseDestination database =
                createDatabase(dialect, SpannerFloatKeyReadTestSupport.ddl(dialect));
        for (String table : List.of("float_primary_asc", "float_indexed")) {
            client(database)
                    .write(
                            List.of(
                                    Mutation.newInsertBuilder(table)
                                            .set("bucket")
                                            .to(1L)
                                            .set("ratio")
                                            .to(Double.NaN)
                                            .set("id")
                                            .to(1L)
                                            .build()));
            assertThat(query(database, "SELECT ratio FROM " + table))
                    .singleElement()
                    .satisfies(row -> assertThat(Double.isNaN(row.getDouble(0))).isTrue());
        }
    }

    @ParameterizedTest
    @EnumSource(Dialect.class)
    void floatKeyRangesMatchSqlAndFlink(Dialect dialect) throws Exception {
        DatabaseDestination database =
                createDatabase(dialect, SpannerFloatKeyReadTestSupport.ddl(dialect));
        // Only GoogleSQL compares NaN as false for every ordered predicate. Keep that emulator
        // oracle while the real-service fixture checks NaN-key rejection in both dialects.
        Timestamp snapshot =
                SpannerFloatKeyReadTestSupport.seed(
                        client(database), dialect, dialect == Dialect.GOOGLE_STANDARD_SQL);
        SpannerFloatKeyReadTestSupport.assertNativeRanges(client(database), dialect, snapshot);
        SpannerFloatKeyReadTestSupport.assertSignedZeroKeys(client(database));
        SpannerFloatKeyReadTestSupport.assertFlinkScans(database, dialect, emulatorEndpoint());
    }
}
