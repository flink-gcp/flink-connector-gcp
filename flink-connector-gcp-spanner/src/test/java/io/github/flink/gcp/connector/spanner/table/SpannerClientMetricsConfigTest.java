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

package io.github.flink.gcp.connector.spanner.table;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SpannerClientMetricsConfig}. */
class SpannerClientMetricsConfigTest {

    @Test
    void aTableThatDeclaresNeitherLeavesEveryBuilderAtItsDefault() {
        SpannerClientMetricsConfig options = SpannerClientMetricsConfig.from(new Configuration());
        List<Boolean> applied = new ArrayList<>();

        options.applyTo(applied::add, applied::add);

        assertThat(applied).isEmpty();
        assertThat(options.exportToFlink()).isFalse();
        assertThat(options.exportToCloudMonitoring()).isTrue();
        assertThat(options).isEqualTo(SpannerClientMetricsConfig.unset());
    }

    @Test
    void handsEachDeclaredOptionToItsOwnSetter() {
        SpannerClientMetricsConfig options =
                SpannerClientMetricsConfig.from(
                        Configuration.fromMap(
                                Map.of(
                                        "client-metrics.export-to-flink", "true",
                                        "client-metrics.export-to-cloud-monitoring", "false")));
        List<Boolean> toFlink = new ArrayList<>();
        List<Boolean> toCloudMonitoring = new ArrayList<>();

        options.applyTo(toFlink::add, toCloudMonitoring::add);

        assertThat(toFlink).containsExactly(true);
        assertThat(toCloudMonitoring).containsExactly(false);
        assertThat(options.exportToFlink()).isTrue();
        assertThat(options.exportToCloudMonitoring()).isFalse();
    }

    @Test
    void namesTheKeyWhenASetterRefusesTheValue() {
        SpannerClientMetricsConfig options =
                SpannerClientMetricsConfig.from(
                        Configuration.fromMap(
                                Map.of("client-metrics.export-to-cloud-monitoring", "false")));

        assertThatThrownBy(
                        () ->
                                options.applyTo(
                                        value -> {},
                                        value -> {
                                            throw new IllegalArgumentException("refused");
                                        }))
                .hasMessageContaining("client-metrics.export-to-cloud-monitoring");
    }
}
