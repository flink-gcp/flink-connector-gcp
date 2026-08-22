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

package io.github.flink.gcp.connector.bigtable.source.readrows.enumerator;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RowKeySampleTest {

    @Test
    void rendersItsKeyTheWayASplitRendersItsBounds() {
        // The same escaping RowRangeSplit.toString and BigtableSplitReader's INFO line use, which
        // is where these bytes already surface -- nothing renders a sample itself today.
        assertThat(RowKeySample.of(ByteString.copyFromUtf8("row-1"), 4096L))
                .hasToString("RowKeySample{key=row-1, offsetBytes=4096}");
        assertThat(RowKeySample.of(ByteString.copyFrom(new byte[] {0x00, (byte) 0xFF}), 0L))
                .hasToString("RowKeySample{key=\\x00\\xff, offsetBytes=0}");
    }

    @Test
    void marksTheEndOfTableSampleRatherThanRenderingItBlank() {
        // The empty key is admissible and means "end of table" rather than a boundary, so it has
        // to stay visible. RowRanges.format escapes and does not mark, so this rendering -- which
        // does not quote its value -- supplies the marker itself, or "key=" reads as truncated.
        assertThat(RowKeySample.of(ByteString.EMPTY, 8192L))
                .hasToString("RowKeySample{key=*, offsetBytes=8192}");
    }
}
