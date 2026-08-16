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

package io.github.flink.gcp.connector.bigquery.table.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CdcTableOptionsMapper}. */
class CdcTableOptionsMapperTest {

    @Test
    void mapsTheDeclaredPrimaryKeyAndMaximumStaleness() {
        Configuration config = new Configuration();
        config.set(BigQueryConnectorOptions.SINK_CDC_MAX_STALENESS, Duration.ofMinutes(10));

        CdcTableOptions mapped = CdcTableOptionsMapper.map(config, Arrays.asList("id", "tenant"));

        assertThat(mapped.getPrimaryKeyColumns()).containsExactly("id", "tenant");
        assertThat(mapped.getMaxStaleness()).isEqualTo(Duration.ofMinutes(10));
        assertThat(mapped.managesMaxStaleness()).isTrue();
        assertThat(mapped.clearsMaxStaleness()).isFalse();
    }

    @Test
    void mapsAnExplicitClear() {
        Configuration config = new Configuration();
        config.set(BigQueryConnectorOptions.SINK_CDC_CLEAR_MAX_STALENESS, true);

        CdcTableOptions mapped = CdcTableOptionsMapper.map(config, Arrays.asList("id"));

        assertThat(mapped.getMaxStaleness()).isNull();
        assertThat(mapped.managesMaxStaleness()).isTrue();
        assertThat(mapped.clearsMaxStaleness()).isTrue();
    }

    @Test
    void rejectsSettingAndClearingMaximumStalenessTogether() {
        Configuration config = new Configuration();
        config.set(BigQueryConnectorOptions.SINK_CDC_MAX_STALENESS, Duration.ofMinutes(10));
        config.set(BigQueryConnectorOptions.SINK_CDC_CLEAR_MAX_STALENESS, true);

        assertThatThrownBy(() -> CdcTableOptionsMapper.map(config, Arrays.asList("id")))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("sink.cdc.max-staleness")
                .hasMessageContaining("sink.cdc.clear-max-staleness");
    }

    @Test
    void reconciliationPolicyDefaultsToVerificationOnly() {
        Configuration config = new Configuration();

        assertThat(CdcTableOptionsMapper.policy(config))
                .isEqualTo(CdcTableReconciliationPolicy.VERIFY_ONLY);

        config.set(
                BigQueryConnectorOptions.SINK_CDC_TABLE_RECONCILIATION,
                CdcTableReconciliationPolicy.RECONCILE);
        assertThat(CdcTableOptionsMapper.policy(config))
                .isEqualTo(CdcTableReconciliationPolicy.RECONCILE);
    }
}
