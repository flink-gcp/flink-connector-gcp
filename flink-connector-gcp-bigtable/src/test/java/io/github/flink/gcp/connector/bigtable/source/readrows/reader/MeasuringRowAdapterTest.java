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

package io.github.flink.gcp.connector.bigtable.source.readrows.reader;

import com.google.cloud.bigtable.data.v2.models.RowAdapter.RowBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the stable byte measurement performed by {@link MeasuringRowAdapter}. */
class MeasuringRowAdapterTest {

    @Test
    void measuresDecodedContentOnceWhilePreservingTheSdkRow() {
        MeasuringRowAdapter adapter = new MeasuringRowAdapter();
        RowBuilder<MeasuredRow> builder = adapter.createRowBuilder();
        ByteString key = ByteString.copyFromUtf8("row");
        ByteString qualifier = ByteString.copyFromUtf8("q");
        builder.startRow(key);
        builder.startCell("cf", qualifier, 123L, Arrays.asList("one", "two"), 3L);
        builder.cellValue(ByteString.copyFromUtf8("ab"));
        builder.cellValue(ByteString.copyFromUtf8("c"));
        builder.finishCell();

        MeasuredRow row = builder.finishRow();

        long expected =
                CodedOutputStream.computeBytesSizeNoTag(key)
                        + CodedOutputStream.computeStringSizeNoTag("cf")
                        + CodedOutputStream.computeBytesSizeNoTag(qualifier)
                        + CodedOutputStream.computeInt64SizeNoTag(123L)
                        + CodedOutputStream.computeStringSizeNoTag("one")
                        + CodedOutputStream.computeStringSizeNoTag("two")
                        + CodedOutputStream.computeUInt64SizeNoTag(3L)
                        + 3L;
        assertThat(row.row().getKey()).isEqualTo(key);
        assertThat(row.estimatedBytes()).isEqualTo(expected);
        assertThat(adapter.getKey(row)).isEqualTo(key);
        assertThat(adapter.isScanMarkerRow(row)).isFalse();
    }

    @Test
    void givesEvenAnEmptyScanMarkerAPositiveSize() {
        MeasuringRowAdapter adapter = new MeasuringRowAdapter();

        MeasuredRow marker = adapter.createRowBuilder().createScanMarkerRow(ByteString.EMPTY);

        assertThat(marker.estimatedBytes()).isEqualTo(1);
        assertThat(adapter.isScanMarkerRow(marker)).isTrue();
    }
}
