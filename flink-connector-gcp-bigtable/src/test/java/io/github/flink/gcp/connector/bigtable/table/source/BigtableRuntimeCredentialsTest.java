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

import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.RowType;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.table.BigtableTableSchema;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests the boundary where every Table API source runtime turns its configuration into client
 * settings: the credential file it loads, and the emulator endpoint it parses.
 */
class BigtableRuntimeCredentialsTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "t");
    private static final BigtableTableSchema SCHEMA =
            BigtableTableSchema.of(
                    (RowType)
                            DataTypes.ROW(
                                            DataTypes.FIELD("rowkey", DataTypes.STRING()),
                                            DataTypes.FIELD(
                                                    "cf",
                                                    DataTypes.ROW(
                                                            DataTypes.FIELD(
                                                                    "q", DataTypes.STRING()))))
                                    .getLogicalType());
    private static final Filters.Filter FILTER = Filters.FILTERS.family().exactMatch("cf");
    private static final String MISSING_KEY = "/missing/mounted-bigtable-key.json";
    private static final String FAILURE =
            "Failed to load the configured Bigtable service-account key file.";

    @Test
    void synchronousLookupLoadsTheConfiguredCredential() {
        BigtableRowDataLookupFunction function =
                new BigtableRowDataLookupFunction(
                        TABLE,
                        SCHEMA,
                        null,
                        "null",
                        FILTER,
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        MISSING_KEY,
                        null,
                        0);

        assertSanitized(() -> function.open(null));
    }

    @Test
    void asynchronousLookupLoadsTheConfiguredCredential() {
        BigtableRowDataAsyncLookupFunction function =
                new BigtableRowDataAsyncLookupFunction(
                        TABLE,
                        SCHEMA,
                        null,
                        "null",
                        FILTER,
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        MISSING_KEY,
                        null,
                        0);

        assertSanitized(() -> function.open(null));
    }

    @Test
    void fullCacheLoadsTheConfiguredCredential() {
        BigtableFullCacheInputFormat input =
                new BigtableFullCacheInputFormat(
                        TABLE,
                        SCHEMA,
                        null,
                        "null",
                        FILTER,
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        MISSING_KEY,
                        null,
                        null);

        assertSanitized(() -> input.open(new GenericInputSplit(0, 1)));
    }

    @Test
    void theLookupNamesTheOptionKeyWhenTheEndpointIsMalformed() {
        // These runtimes hold the option's value and parse it when they open, naming the WITH key
        // rather than a builder setter (#895). Since #1009 a SQL caller no longer gets this far:
        // BigtableDynamicTableFactory parses the same value at planning. What is pinned here is the
        // check behind the @Internal constructor, which is reached by constructing it directly.
        BigtableRowDataLookupFunction function =
                new BigtableRowDataLookupFunction(
                        TABLE,
                        SCHEMA,
                        null,
                        "null",
                        FILTER,
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        null,
                        "localhost",
                        0);

        assertThatThrownBy(() -> function.open(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulator-endpoint must be host:port, was 'localhost'");
    }

    @Test
    void theFullCacheScanNamesTheOptionKeyWhenTheEndpointIsMalformed() {
        BigtableFullCacheInputFormat input =
                new BigtableFullCacheInputFormat(
                        TABLE,
                        SCHEMA,
                        null,
                        "null",
                        FILTER,
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        null,
                        "localhost",
                        null);

        assertThatThrownBy(() -> input.open(new GenericInputSplit(0, 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("emulator-endpoint must be host:port, was 'localhost'");
    }

    private static void assertSanitized(ThrowingCall call) {
        assertThatThrownBy(call::run)
                .isInstanceOf(java.io.IOException.class)
                .hasMessage(FAILURE)
                .hasNoCause();
    }

    @FunctionalInterface
    private interface ThrowingCall {
        void run() throws Exception;
    }
}
