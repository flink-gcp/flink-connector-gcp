/*
 * Copyright 2026 laughingman7743
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

package io.github.flink.gcp.connector.bigquery.source.enumerator;

import io.github.flink.gcp.connector.bigquery.source.TestRows;
import io.github.flink.gcp.connector.bigquery.source.split.BigQueryReadStreamSplit;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQueryReadEnumeratorStateSerializerTest {

    private final BigQueryReadEnumeratorStateSerializer serializer =
            new BigQueryReadEnumeratorStateSerializer();

    @Test
    void roundTripsAnInitializedStateWithPendingSplits() throws Exception {
        BigQueryReadEnumeratorState state =
                new BigQueryReadEnumeratorState(
                        true,
                        "projects/p/locations/l/sessions/s",
                        Instant.parse("2026-08-09T12:34:56.789Z"),
                        Arrays.asList(split(0, 0), split(1, 17)));

        BigQueryReadEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
        assertThat(restored.getSessionExpireTime())
                .isEqualTo(Instant.parse("2026-08-09T12:34:56.789Z"));
        assertThat(restored.getPendingSplits().get(1).getOffset()).isEqualTo(17);
    }

    @Test
    void roundTripsTheStateOfAnEnumeratorWithoutASessionYet() throws Exception {
        BigQueryReadEnumeratorState state =
                new BigQueryReadEnumeratorState(false, null, null, Collections.emptyList());

        BigQueryReadEnumeratorState restored =
                serializer.deserialize(serializer.getVersion(), serializer.serialize(state));

        assertThat(restored).isEqualTo(state);
        assertThat(restored.isInitialized()).isFalse();
        assertThat(restored.getSessionName()).isNull();
        assertThat(restored.getSessionExpireTime()).isNull();
    }

    @Test
    void rejectsAnUnknownVersionNamingBoth() throws Exception {
        byte[] bytes =
                serializer.serialize(
                        new BigQueryReadEnumeratorState(
                                false, null, null, Collections.emptyList()));

        assertThatThrownBy(() -> serializer.deserialize(2, bytes))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("version 2")
                .hasMessageContaining("version 1");
    }

    private static BigQueryReadStreamSplit split(int index, long offset) {
        return new BigQueryReadStreamSplit(
                "projects/p/locations/l/sessions/s/streams/" + index, offset, TestRows.SCHEMA_JSON);
    }
}
