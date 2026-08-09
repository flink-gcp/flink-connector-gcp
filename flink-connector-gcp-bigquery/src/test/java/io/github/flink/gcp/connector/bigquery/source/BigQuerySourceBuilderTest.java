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

package io.github.flink.gcp.connector.bigquery.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;

import io.github.flink.gcp.connector.bigquery.sink.TableDestination;
import io.github.flink.gcp.connector.bigquery.source.serializer.BigQueryRowDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BigQuerySourceBuilderTest {

    @Test
    void buildsABoundedSource() {
        Source<GenericRecord, ?, ?> source = builder().build();

        assertThat(source.getBoundedness()).isEqualTo(Boundedness.BOUNDED);
    }

    @Test
    void defaultsToTheWholeTableAndBigQuerysOwnStreamCount() {
        BigQuerySourceConfig<GenericRecord> config = TestSources.config();

        assertThat(config.getSelectedFields()).isEmpty();
        assertThat(config.getRowRestriction()).isNull();
        assertThat(config.getSnapshotTime()).isNull();
        assertThat(config.getMaxStreamCount()).isZero();
        assertThat(config.getPreferredMinStreamCount()).isZero();
        // The literal, not the constant: a constant is inlined into this class at compile time, so
        // comparing it against itself would pass for any value — and the reference page states
        // 10000, which nothing else pins.
        assertThat(config.getMaxRecordsPerFetch()).isEqualTo(10_000);
    }

    @Test
    void billsTheReadToTheTablesProjectByDefault() {
        assertThat(TestSources.config().getParentProject()).isEqualTo("p");
    }

    @Test
    void requiresATable() {
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .deserializer(deserializer())
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A table is required");
    }

    @Test
    void requiresADeserializer() {
        assertThatThrownBy(
                        () ->
                                BigQuerySource.<GenericRecord>builder()
                                        .table(TestSources.TABLE)
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("A deserializer is required");
    }

    @Test
    void rejectsAPreferredMinimumAboveTheMaximum() {
        // BigQuery answers INVALID_ARGUMENT for this ("preferred_min_stream_count must be less than
        // or equal to max_stream_count", measured 2026-08-09); saying so here costs no round trip.
        assertThatThrownBy(() -> builder().maxStreamCount(2).preferredMinStreamCount(8).build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("preferredMinStreamCount must be at most maxStreamCount");
    }

    @Test
    void acceptsAPreferredMinimumWhenTheMaximumIsLeftToBigQuery() {
        assertThat(TestSources.config(builder -> builder.preferredMinStreamCount(8)))
                .extracting(BigQuerySourceConfig::getPreferredMinStreamCount)
                .isEqualTo(8);
    }

    @Test
    void rejectsNegativeStreamCounts() {
        assertThatThrownBy(() -> builder().maxStreamCount(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxStreamCount must not be negative");
        assertThatThrownBy(() -> builder().preferredMinStreamCount(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("preferredMinStreamCount must not be negative");
    }

    @Test
    void rejectsANonPositiveFetchCap() {
        assertThatThrownBy(() -> builder().maxRecordsPerFetch(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxRecordsPerFetch must be positive");
    }

    @Test
    void takesTheSelectedFieldsAsACollectionToo() {
        assertThat(
                        TestSources.config(
                                        builder ->
                                                builder.selectedFields(Arrays.asList("id", "name")))
                                .getSelectedFields())
                .containsExactly("id", "name");
    }

    @Test
    void rejectsABlankOrRepeatedSelectedField() {
        assertThatThrownBy(() -> builder().selectedFields("id", " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
        assertThatThrownBy(() -> builder().selectedFields("id", "id"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("named twice");
    }

    @Test
    void rejectsABlankRowRestrictionOrParentProject() {
        assertThatThrownBy(() -> builder().rowRestriction(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("rowRestriction must not be blank");
        assertThatThrownBy(() -> builder().parentProject(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parentProject must not be blank");
    }

    @Test
    void rejectsAMalformedEmulatorEndpointWhereItIsTyped() {
        assertThatThrownBy(() -> builder().emulatorEndpoint("not-a-host-port"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static BigQuerySourceBuilder<GenericRecord> builder() {
        return BigQuerySource.<GenericRecord>builder()
                .table(TableDestination.of("p", "d", "t"))
                .deserializer(deserializer())
                // The builder creates this source's real clients; the endpoint is never connected
                // to, but without it a machine with application-default credentials passes where
                // CI fails.
                .emulatorEndpoint("localhost:1");
    }

    private static BigQueryRowDeserializer<GenericRecord> deserializer() {
        return BigQueryRowDeserializer.genericRecord(TestRows.SCHEMA_JSON);
    }
}
