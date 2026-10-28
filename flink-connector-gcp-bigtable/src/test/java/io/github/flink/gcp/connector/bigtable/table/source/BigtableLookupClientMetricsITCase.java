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

import org.apache.flink.metrics.testutils.MetricListener;

import com.google.cloud.bigtable.data.v2.models.Filters;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.metrics.ClientMetrics;
import io.github.flink.gcp.connector.bigtable.AbstractBigtableEmulatorITCase;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.testutils.LogCapture;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The method name a point lookup registers, held against what the real client records into it
 * against the emulator, which serves {@code ReadRow} on the classic path: the emulator builder
 * turns the session path off.
 */
class BigtableLookupClientMetricsITCase extends AbstractBigtableEmulatorITCase {

    @Test
    void aPointLookupRecordsReadRow() throws Exception {
        TableDestination table = createTable("client-metrics-lookup");
        seedRows(table, "present");
        MetricListener listener = new MetricListener();
        BigtableDataClientRowLookup lookup =
                new BigtableDataClientRowLookup(
                        table,
                        Filters.FILTERS.pass(),
                        Collections.singletonList(ByteStringRange.unbounded()),
                        null,
                        null,
                        emulatorEndpoint(),
                        true,
                        true);

        try (LogCapture capture = LogCapture.of(ClientMetrics.class)) {
            lookup.open(listener.getMetricGroup());
            try {
                assertThat(lookup.read(ByteString.copyFromUtf8("present"))).isNotNull();
                assertThat(lookup.read(ByteString.copyFromUtf8("absent"))).isNull();
                assertThat(lookup.readAsync(ByteString.copyFromUtf8("present")).get()).isNotNull();
            } finally {
                lookup.close();
            }

            assertThat(
                            listener.getCounter("rpcMethod", "ReadRow", "operations")
                                    .orElseThrow()
                                    .getCount())
                    .isEqualTo(3);
            assertThat(capture.getMessages()).isEmpty();
        }
    }
}
