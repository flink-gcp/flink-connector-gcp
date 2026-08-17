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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.bigquery.sink.CdcTableOptions;
import io.github.flink.gcp.connector.bigquery.sink.CdcTableReconciliationPolicy;
import io.github.flink.gcp.connector.bigquery.table.BigQueryConnectorOptions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

/** Maps Table API CDC table options onto their DataStream contract. */
@Internal
public final class CdcTableOptionsMapper {

    private CdcTableOptionsMapper() {}

    /** Builds the desired CDC table contract from the declared primary key and CDC options. */
    public static CdcTableOptions map(ReadableConfig config, List<String> primaryKeyColumns) {
        Optional<Duration> maxStaleness =
                config.getOptional(BigQueryConnectorOptions.SINK_CDC_MAX_STALENESS);
        boolean clearMaxStaleness =
                config.getOptional(BigQueryConnectorOptions.SINK_CDC_CLEAR_MAX_STALENESS)
                        .orElse(false);
        if (maxStaleness.isPresent() && clearMaxStaleness) {
            throw new ValidationException(
                    String.format(
                            "Options '%s' and '%s' are mutually exclusive.",
                            BigQueryConnectorOptions.SINK_CDC_MAX_STALENESS.key(),
                            BigQueryConnectorOptions.SINK_CDC_CLEAR_MAX_STALENESS.key()));
        }

        CdcTableOptions.Builder builder = CdcTableOptions.builder();
        if (!primaryKeyColumns.isEmpty()) {
            builder.primaryKeyColumns(primaryKeyColumns);
        }
        maxStaleness.ifPresent(builder::maxStaleness);
        if (clearMaxStaleness) {
            builder.clearMaxStaleness();
        }
        return builder.build();
    }

    /**
     * Returns the configured policy, or {@code null} when the option is unset.
     *
     * <p>Null rather than the builder's default: an unset option has to leave its setter uncalled,
     * so that the table layer carries no copy of a default {@code BigQuerySinkBuilder} owns.
     *
     * @param config the table options
     * @return the policy, or null
     */
    @Nullable
    public static CdcTableReconciliationPolicy policy(ReadableConfig config) {
        return config.getOptional(BigQueryConnectorOptions.SINK_CDC_TABLE_RECONCILIATION)
                .orElse(null);
    }
}
