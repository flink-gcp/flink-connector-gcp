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

package io.github.flink.gcp.connector.bigtable.source.changestream.reader;

import com.google.cloud.bigtable.data.v2.models.Range.BoundType;
import com.google.cloud.bigtable.data.v2.models.Range.ByteStringRange;
import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ChangeStreamPartitionsTest {

    @Test
    void expressesTheWholeKeyspaceAsExplicitClosedOpenEmptyKeys() {
        ByteStringRange service = ChangeStreamPartitions.sdkRange(ByteStringRange.unbounded());

        assertThat(service.getStartBound()).isEqualTo(BoundType.CLOSED);
        assertThat(service.getStart()).isEqualTo(ByteString.EMPTY);
        assertThat(service.getEndBound()).isEqualTo(BoundType.OPEN);
        assertThat(service.getEnd()).isEqualTo(ByteString.EMPTY);
    }

    @Test
    void preservesFiniteClosedOpenPartitions() {
        ByteStringRange service =
                ChangeStreamPartitions.sdkRange(ByteStringRange.create("left", "right"));

        assertThat(service).isEqualTo(ByteStringRange.create("left", "right"));
    }

    @Test
    void rejectsBoundsTheServiceCannotReadAsAPartition() {
        assertThatThrownBy(
                        () ->
                                ChangeStreamPartitions.sdkRange(
                                        ByteStringRange.unbounded().startOpen("left")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("closed start");
        assertThatThrownBy(
                        () ->
                                ChangeStreamPartitions.sdkRange(
                                        ByteStringRange.unbounded().endClosed("right")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("open end");
    }
}
