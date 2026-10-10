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

package io.github.flink.gcp.connector.datastore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.base.table.OptionSetters;
import io.github.flink.gcp.connector.datastore.sink.DatastoreWriterOptions;
import io.github.flink.gcp.connector.datastore.table.DatastoreConnectorOptions;

import java.time.Duration;

/**
 * Builds {@link DatastoreWriterOptions} from the table options.
 *
 * <p>Every knob goes through {@link OptionSetters}, so an option absent from the DDL leaves the
 * builder at its own default, and a value a setter refuses is renamed to its option key (ADR-0133).
 * The builder's one cross-check, a backoff cap below the initial backoff, names builder setters,
 * which a {@code WITH} clause does not contain, so when {@code build()} refuses, the refusal is
 * restated here in option keys.
 *
 * <p>{@code maxConsecutiveRejections} has no option: the table sink fails the job on the first
 * refused write, before the bound could matter.
 */
@Internal
public final class WriterOptionsMapper {

    private WriterOptionsMapper() {}

    /**
     * Maps the table options onto writer options.
     *
     * @param config the table options
     * @return the writer options
     * @throws ValidationException if an option or a combination of them is refused
     */
    public static DatastoreWriterOptions map(ReadableConfig config) {
        DatastoreWriterOptions.Builder builder = DatastoreWriterOptions.builder();
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_BUFFER_FLUSH_MAX_MUTATIONS,
                builder::maxBatchMutations);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_BUFFER_FLUSH_MAX_SIZE,
                size -> builder.maxBatchBytes(size.getBytes()));
        OptionSetters.apply(
                config, DatastoreConnectorOptions.SINK_REQUEST_TIMEOUT, builder::requestTimeout);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF,
                builder::recoveryInitialBackoff);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_RECOVERY_MAX_BACKOFF,
                builder::recoveryMaxBackoff);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_RECOVERY_MAX_ATTEMPTS,
                builder::recoveryMaxAttempts);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_THROTTLING_ENABLED,
                builder::throttlingEnabled);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_THROTTLING_PARALLELISM,
                builder::throttlingParallelism);
        OptionSetters.apply(
                config,
                DatastoreConnectorOptions.SINK_ID_ALLOCATION_BATCH_SIZE,
                builder::idAllocationBatchSize);
        try {
            return builder.build();
        } catch (IllegalStateException e) {
            throw invertedBackoff(config, e);
        }
    }

    /**
     * The builder's one cross-check, which is all its {@code build()} refuses, in option keys: each
     * value is the option's own, or the builder's default without one.
     */
    private static ValidationException invertedBackoff(
            ReadableConfig config, IllegalStateException cause) {
        DatastoreWriterOptions unset = DatastoreWriterOptions.builder().build();
        Duration initial =
                config.getOptional(DatastoreConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF)
                        .orElse(unset.getRecoveryInitialBackoff());
        Duration max =
                config.getOptional(DatastoreConnectorOptions.SINK_RECOVERY_MAX_BACKOFF)
                        .orElse(unset.getRecoveryMaxBackoff());
        return new ValidationException(
                "'"
                        + DatastoreConnectorOptions.SINK_RECOVERY_MAX_BACKOFF.key()
                        + "' ("
                        + max
                        + ") must be at least '"
                        + DatastoreConnectorOptions.SINK_RECOVERY_INITIAL_BACKOFF.key()
                        + "' ("
                        + initial
                        + "); an option not set takes its default.",
                cause);
    }
}
