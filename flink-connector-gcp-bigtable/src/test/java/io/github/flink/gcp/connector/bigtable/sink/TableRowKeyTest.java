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

package io.github.flink.gcp.connector.bigtable.sink;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.sink.PerKeyWriteSequencer;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableRowKeyTest {

    private static final TableDestination TABLE = TableDestination.of("p", "i", "orders");

    @Test
    void aRowIsItsKeyInOneTable() {
        TableRowKey row = new TableRowKey(TABLE, ByteString.copyFromUtf8("k"));

        assertThat(row)
                .isEqualTo(new TableRowKey(TABLE, ByteString.copyFromUtf8("k")))
                .hasSameHashCodeAs(new TableRowKey(TABLE, ByteString.copyFromUtf8("k")))
                .isNotEqualTo(new TableRowKey(TABLE, ByteString.copyFromUtf8("j")))
                .isNotEqualTo(
                        new TableRowKey(
                                TableDestination.of("p", "i", "events"),
                                ByteString.copyFromUtf8("k")));
    }

    @Test
    void itsRenderingNamesTheTableAndNotTheKey() {
        // The per-key sequencer puts a key's rendering in its invariant failures, and a row key is
        // the row's own data.
        String rendering =
                new TableRowKey(TABLE, ByteString.copyFromUtf8("customer-4711")).toString();

        assertThat(rendering)
                .isEqualTo("TableRowKey{table=" + TABLE + ", rowKeyBytes=13}")
                .doesNotContain("4711");
    }

    @Test
    void theSequencersFailureNamesTheRowWithoutItsKey() {
        TableRowKey row = new TableRowKey(TABLE, ByteString.copyFromUtf8("customer-4711"));

        assertThatThrownBy(() -> new PerKeyWriteSequencer<TableRowKey, String>().release(row))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage(
                        "Released key TableRowKey{table="
                                + TABLE
                                + ", rowKeyBytes=13}, which has no admitted write.");
    }
}
