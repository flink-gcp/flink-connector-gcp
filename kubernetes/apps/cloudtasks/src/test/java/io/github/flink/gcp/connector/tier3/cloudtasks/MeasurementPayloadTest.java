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

package io.github.flink.gcp.connector.tier3.cloudtasks;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MeasurementPayloadTest {
    @Test
    void retainsOriginInBothExactBodySizes() {
        UUID process = UUID.randomUUID();
        for (int size : new int[] {1024, 65536}) {
            ByteString body = MeasurementPayload.create(size, 17, process, 900, 1200);
            assertThat(body.size()).isEqualTo(size);
            var origin = MeasurementPayload.read(body);
            assertThat(origin).isEqualTo(new MeasurementPayload.Origin(17, process, 900, 1200));
            assertThat(origin.elapsedNanos(process, 1300)).isEqualTo(400);
            assertThat(origin.elapsedNanos(UUID.randomUUID(), 1300)).isEqualTo(-1);
            assertThat(body.substring(44))
                    .isNotEqualTo(
                            MeasurementPayload.create(size, 18, process, 900, 1200).substring(44));
        }
    }

    @Test
    void refusesForeignOrTruncatedBodies() {
        assertThatThrownBy(() -> MeasurementPayload.read(ByteString.copyFrom(new byte[1024])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("header");
        assertThatThrownBy(() -> MeasurementPayload.read(ByteString.copyFrom(new byte[43])))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("size");
        assertThatThrownBy(() -> MeasurementPayload.create(1000, 1))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
