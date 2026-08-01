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

package io.github.flink.gcp.connector.bigquery.sink.failure;

import com.google.protobuf.ByteString;
import io.github.flink.gcp.connector.base.failure.FailedElement;
import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FailedRow}'s view under the shared {@link FailedElement} contract. */
class FailedRowTest {

    @Test
    void exposesTheSharedFailedElementView() {
        RuntimeException cause = new RuntimeException("cause");
        FailedRow row =
                FailedRow.of(
                        TableDestination.of("p", "d", "t"),
                        ByteString.copyFromUtf8("row"),
                        "bad row",
                        cause);

        FailedElement element = row;
        assertThat(element.getConnector()).isEqualTo("bigquery");
        assertThat(element.describeDestination()).isEqualTo("p.d.t");
        assertThat(element.getPayloadBytes()).isEqualTo(ByteString.copyFromUtf8("row"));
        assertThat(element.getErrorMessage()).isEqualTo("bad row");
        assertThat(element.getCause()).isSameAs(cause);
    }

    @Test
    void aSerializationFailureCarriesNoPayload() {
        FailedRow row =
                FailedRow.of(TableDestination.of("p", "d", "t"), null, "cannot serialize", null);

        assertThat(row.getPayloadBytes()).isNull();
        assertThat(row.getRowBytes()).isNull();
        assertThat(row.getCause()).isNull();
    }
}
