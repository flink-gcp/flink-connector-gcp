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

package io.github.flink.gcp.connector.bigtable.table.catalog;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * What the {@code bigtable} catalog reports for an instance that does not exist, measured against
 * the service because the emulator accepts any instance. It creates nothing.
 */
@Tag("gated")
@Timeout(value = 600, threadMode = ThreadMode.SEPARATE_THREAD)
@EnabledIfEnvironmentVariable(named = "BIGTABLE_IT_PROJECT", matches = ".+")
class BigtableCatalogMissingInstanceRealGcpITCase {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigtableCatalogMissingInstanceRealGcpITCase.class);

    private static final String PROJECT = System.getenv("BIGTABLE_IT_PROJECT");

    /** A valid instance id that this project never creates. */
    private static final String MISSING = "catalog-absent";

    @Test
    void aListingAndALookupBothNameTheMissingInstance() {
        TableEnvironment table = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        table.executeSql(
                "CREATE CATALOG bt WITH ('type' = 'bigtable', 'project' = '"
                        + PROJECT
                        + "', 'instance' = '"
                        + MISSING
                        + "')");
        table.executeSql("USE CATALOG bt");
        try {
            Throwable listing = catchThrowable(() -> table.executeSql("SHOW TABLES"));
            Throwable lookup = catchThrowable(() -> table.executeSql("SELECT * FROM orders"));
            LOG.info("SHOW TABLES on a missing instance", listing);
            LOG.info("A lookup on a missing instance", lookup);

            // GetTable answers NOT_FOUND, which the catalog reports as a missing table; the planner
            // then lists the tables, and that failure names the instance.
            String named =
                    "Failed to list the tables of Bigtable instance '"
                            + PROJECT
                            + "/"
                            + MISSING
                            + "'";
            assertThat(listing).hasStackTraceContaining(named).hasStackTraceContaining("NOT_FOUND");
            assertThat(lookup).hasStackTraceContaining(named).hasStackTraceContaining("NOT_FOUND");
        } finally {
            table.getCatalog("bt").ifPresent(catalog -> catalog.close());
        }
    }
}
