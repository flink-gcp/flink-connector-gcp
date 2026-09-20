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

package io.github.flink.gcp.connector.tier3.bigquery;

import org.apache.flink.util.InstantiationUtil;

import com.google.protobuf.ByteString;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class RecoveryRowsTest {
    @ParameterizedTest
    @EnumSource(RecoveryOptions.Mode.class)
    void exactSizedRowsRemainDeterministicAcrossSerialization(RecoveryOptions.Mode mode)
            throws Exception {
        var options =
                RecoveryOptions.parse(
                        RecoveryOptionsTest.arguments(
                                "--mode", mode.name(), "--destinations", "50"));
        var rows = new RecoveryRows(options);
        var copy = InstantiationUtil.clone(rows);
        var descriptor = rows.getDescriptor(options.table(0));
        for (long sequence : new long[] {0, 1, 127, 128, 16383, 16384, options.records - 1}) {
            ByteString bytes = rows.serialize(sequence);
            assertThat(bytes.size()).isEqualTo(mode.rowBytes);
            assertThat(copy.serialize(sequence)).isEqualTo(bytes);
            var decoded = DynamicMessage.parseFrom(descriptor, bytes);
            assertThat(decoded.getField(descriptor.findFieldByName("run_id")))
                    .isEqualTo(options.runId);
            assertThat(decoded.getField(descriptor.findFieldByName("sequence")))
                    .isEqualTo(sequence);
            assertThat(decoded.getField(descriptor.findFieldByName("destination")))
                    .isEqualTo(sequence % 50);
            ByteString payload =
                    (ByteString) decoded.getField(descriptor.findFieldByName("payload"));
            assertThat(payload.size()).isGreaterThan(mode.rowBytes - 100);
            assertThat(
                            java.util.stream.IntStream.range(0, payload.size())
                                    .map(payload::byteAt)
                                    .distinct()
                                    .count())
                    .isGreaterThan(200);
        }
        assertThat(rows.serialize(0L)).isNotEqualTo(rows.serialize(1L));
        assertThat(rows.getTableSchema(options.table(0)).getFieldsList())
                .extracting(field -> field.getName())
                .containsExactly("run_id", "sequence", "destination", "payload");
    }
}
