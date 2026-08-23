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

package io.github.flink.gcp.connector.spanner.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;

import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;
import io.github.flink.gcp.connector.spanner.table.OptionSetters;
import io.github.flink.gcp.connector.spanner.table.SpannerConnectorOptions;

/** Maps table options onto {@link SpannerWriterOptions}. */
@Internal
public final class WriterOptionsMapper {

    private WriterOptionsMapper() {}

    public static SpannerWriterOptions map(ReadableConfig config) {
        SpannerWriterOptions.Builder builder = SpannerWriterOptions.builder();
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_CELLS,
                builder::maxBatchCells);
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_MUTATIONS,
                builder::maxBatchMutations);
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE,
                size -> builder.maxBatchBytes(size.getBytes()));
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_BUFFER_FLUSH_MAX_COMMIT_DELAY,
                builder::maxCommitDelay);
        OptionSetters.apply(
                config, SpannerConnectorOptions.SINK_RPC_PRIORITY, builder::rpcPriority);
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                builder::recoveryInitialBackoff);
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                builder::recoveryMaxBackoff);
        OptionSetters.apply(
                config,
                SpannerConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                builder::recoveryMaxAttempts);
        return builder.build();
    }
}
