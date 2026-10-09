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

package io.github.flink.gcp.connector.bigquery.sink;

import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

/** Tests for {@link TableCreateOptions}. */
class TableCreateOptionsTest {

    @Test
    void defaultsAreEmpty() {
        TableCreateOptions options = TableCreateOptions.defaults();

        assertThat(options.getTimePartitioningType()).isNull();
        assertThat(options.getTimePartitioningField()).isNull();
        assertThat(options.getTimePartitioningExpirationMs()).isNull();
        assertThat(options.getClusteredFields()).isEmpty();
        assertThat(options.getDescription()).isNull();
        assertThat(options.getLabels()).isEmpty();
    }

    @Test
    void descriptionAndLabelsRoundTripInTheirOrder() {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("team", "data");
        labels.put("env", "");
        TableCreateOptions options =
                TableCreateOptions.builder().description("Orders").labels(labels).build();
        labels.put("later", "x");

        assertThat(options.getDescription()).isEqualTo("Orders");
        assertThat(options.getLabels()).containsExactly(entry("team", "data"), entry("env", ""));
        assertThatThrownBy(() -> options.getLabels().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void rejectsABlankDescription() {
        assertThatThrownBy(() -> TableCreateOptions.builder().description(" "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("description must not be blank");
    }

    @Test
    void rejectsABlankOrNullLabelKey() {
        Map<String, String> nullKey = new HashMap<>();
        nullKey.put(null, "v");

        assertThatThrownBy(() -> TableCreateOptions.builder().labels(Map.of(" ", "v")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("label keys must not be blank");
        assertThatThrownBy(() -> TableCreateOptions.builder().labels(nullKey))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("label keys must not be blank");
    }

    @Test
    void rejectsANullLabelValue() {
        Map<String, String> labels = new HashMap<>();
        labels.put("team", null);

        assertThatThrownBy(() -> TableCreateOptions.builder().labels(labels))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("label 'team' must not have a null value");
    }

    /**
     * BigQuery's own rules for label keys and values are BigQuery's answer (ADR-0127): an
     * upper-case key reaches the service rather than being refused here.
     */
    @Test
    void leavesTheLabelGrammarToBigQuery() {
        assertThat(TableCreateOptions.builder().labels(Map.of("Team", "Data")).build().getLabels())
                .containsExactly(entry("Team", "Data"));
    }

    @Test
    void rejectsTheLabelCdcProvisioningOwns() {
        assertThatThrownBy(() -> TableCreateOptions.builder().labels(Map.of("flink_gcp_cdc", "x")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(
                        "label 'flink_gcp_cdc' is reserved for the sink's CDC table provisioning");
    }

    @Test
    void builderRoundTrip() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "event_ts")
                        .timePartitioningExpiration(Duration.ofDays(30))
                        .clusteredFields(Arrays.asList("customer", "region"))
                        .build();

        assertThat(options.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.DAY);
        assertThat(options.getTimePartitioningField()).isEqualTo("event_ts");
        assertThat(options.getTimePartitioningExpirationMs())
                .isEqualTo(Duration.ofDays(30).toMillis());
        assertThat(options.getClusteredFields()).containsExactly("customer", "region");
    }

    @Test
    void ingestionTimePartitioningHasNoField() {
        TableCreateOptions options =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.HOUR)
                        .build();

        assertThat(options.getTimePartitioningType())
                .isEqualTo(TableCreateOptions.TimePartitioningType.HOUR);
        assertThat(options.getTimePartitioningField()).isNull();
    }

    @Test
    void expirationRequiresPartitioning() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .timePartitioningExpiration(Duration.ofDays(1))
                                        .build())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("requires timePartitioning");
    }

    /**
     * The setter converts to milliseconds on the spot, so a sub-millisecond expiration used to
     * reach the create request as a zero — an expiration the user never asked for (ADR-0068).
     */
    @Test
    void rejectsASubMillisecondExpiration() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .timePartitioningExpiration(Duration.ofNanos(500_000)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("timePartitioningExpiration must be at least 1 millisecond");
    }

    @Test
    void rejectsTooManyClusteredFields() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .clusteredFields(Arrays.asList("a", "b", "c", "d", "e")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at most 4");
    }

    @Test
    void rejectsBlankClusteredField() {
        assertThatThrownBy(
                        () -> TableCreateOptions.builder().clusteredFields(Arrays.asList("a", " ")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void rejectsBlankPartitioningField() {
        assertThatThrownBy(
                        () ->
                                TableCreateOptions.builder()
                                        .timePartitioning(
                                                TableCreateOptions.TimePartitioningType.DAY, " "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void valueEquality() {
        TableCreateOptions a =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "ts")
                        .build();
        TableCreateOptions b =
                TableCreateOptions.builder()
                        .timePartitioning(TableCreateOptions.TimePartitioningType.DAY, "ts")
                        .build();

        assertThat(a).isEqualTo(b).hasSameHashCodeAs(b);
        assertThat(a).isNotEqualTo(TableCreateOptions.defaults());
    }

    /**
     * An instance serialized before the labels existed restores the field as {@code null}, which
     * creation and equality must read as "no labels" rather than fail on.
     */
    @Test
    void anInstanceSerializedBeforeLabelsExistedHasNone() throws Exception {
        TableCreateOptions restored =
                InstantiationUtil.clone(
                        TableCreateOptions.builder()
                                .timePartitioning(TableCreateOptions.TimePartitioningType.DAY)
                                .build());
        Field labels = TableCreateOptions.class.getDeclaredField("labels");
        labels.setAccessible(true);
        labels.set(restored, null);

        assertThat(restored.getLabels()).isEmpty();
        assertThat(restored)
                .isEqualTo(
                        TableCreateOptions.builder()
                                .timePartitioning(TableCreateOptions.TimePartitioningType.DAY)
                                .build())
                .hasSameHashCodeAs(
                        TableCreateOptions.builder()
                                .timePartitioning(TableCreateOptions.TimePartitioningType.DAY)
                                .build());
    }

    @Test
    void theDescriptionAndLabelsTakePartInEquality() {
        TableCreateOptions described = TableCreateOptions.builder().description("a").build();
        TableCreateOptions labelled =
                TableCreateOptions.builder().labels(Map.of("team", "data")).build();

        assertThat(described)
                .isEqualTo(TableCreateOptions.builder().description("a").build())
                .isNotEqualTo(TableCreateOptions.builder().description("b").build())
                .isNotEqualTo(TableCreateOptions.defaults());
        assertThat(labelled)
                .isEqualTo(TableCreateOptions.builder().labels(Map.of("team", "data")).build())
                .hasSameHashCodeAs(
                        TableCreateOptions.builder().labels(Map.of("team", "data")).build())
                .isNotEqualTo(TableCreateOptions.defaults());
        assertThat(labelled.toString()).contains("labels={team=data}");
    }
}
