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

package io.github.flink.gcp.connector.firestore.table.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;
import io.github.flink.gcp.connector.firestore.sink.writer.DefaultFirestoreDatabaseAccessFactory;
import io.github.flink.gcp.connector.firestore.table.FirestoreConnectorOptions;
import io.github.flink.gcp.connector.firestore.table.OptionSetters;

import java.util.List;

/**
 * Builds {@link FirestoreWriterOptions} from the table options.
 *
 * <p>Every knob goes through {@link OptionSetters}, so an option absent from the DDL leaves the
 * builder at its own default, and a value a setter refuses is renamed to its option key (ADR-0133).
 * The cross-checks the builder and the sink run across two knobs name builder setters, which a
 * {@code WITH} clause does not contain, and {@code FactoryUtil} would carry that sentence only as
 * the cause of its own generic one. So the two throttling checks are restated here in option keys,
 * and the retry pair check is called with the option keys as its names, as the Pub/Sub table sink
 * does.
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
    public static FirestoreWriterOptions map(ReadableConfig config) {
        rejectRatesWithoutThrottling(config);
        rejectInvertedRates(config);
        FirestoreWriterOptions.Builder builder = FirestoreWriterOptions.builder();
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_THROTTLING_ENABLED,
                builder::throttlingEnabled);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_THROTTLING_INITIAL_OPS_PER_SECOND,
                builder::initialOpsPerSecond);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_THROTTLING_MAX_OPS_PER_SECOND,
                builder::maxOpsPerSecond);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_WRITE_MAX_ATTEMPTS,
                builder::writeMaxAttempts);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_TOTAL_TIMEOUT,
                builder::retryTotalTimeout);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_INITIAL_DELAY,
                builder::retryInitialDelay);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_DELAY_MULTIPLIER,
                builder::retryDelayMultiplier);
        OptionSetters.apply(
                config, FirestoreConnectorOptions.SINK_RETRY_MAX_DELAY, builder::retryMaxDelay);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT,
                builder::retryInitialRpcTimeout);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_RPC_TIMEOUT_MULTIPLIER,
                builder::retryRpcTimeoutMultiplier);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT,
                builder::retryMaxRpcTimeout);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_RETRY_MAX_ATTEMPTS,
                builder::retryMaxAttempts);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_IN_FLIGHT_MAX_WRITES,
                builder::maxInFlightWrites);
        OptionSetters.apply(
                config,
                FirestoreConnectorOptions.SINK_IN_FLIGHT_MAX_BYTES,
                size -> builder.maxInFlightBytes(size.getBytes()));
        FirestoreWriterOptions options = builder.build();
        rejectRetryOverridesTheLibraryRefuses(options);
        return options;
    }

    /** The builder's first check, in option keys. */
    private static void rejectRatesWithoutThrottling(ReadableConfig config) {
        if (config.getOptional(FirestoreConnectorOptions.SINK_THROTTLING_ENABLED).orElse(true)) {
            return;
        }
        for (ConfigOption<Integer> rate :
                List.of(
                        FirestoreConnectorOptions.SINK_THROTTLING_INITIAL_OPS_PER_SECOND,
                        FirestoreConnectorOptions.SINK_THROTTLING_MAX_OPS_PER_SECOND)) {
            if (config.getOptional(rate).isPresent()) {
                throw new ValidationException(
                        quoted(rate)
                                + " sets the throttle's rate, so it cannot be combined with "
                                + quoted(FirestoreConnectorOptions.SINK_THROTTLING_ENABLED)
                                + " = false.");
            }
        }
    }

    /** The builder's second check, in option keys. */
    private static void rejectInvertedRates(ReadableConfig config) {
        Integer initial =
                config.getOptional(FirestoreConnectorOptions.SINK_THROTTLING_INITIAL_OPS_PER_SECOND)
                        .orElse(null);
        Integer max =
                config.getOptional(FirestoreConnectorOptions.SINK_THROTTLING_MAX_OPS_PER_SECOND)
                        .orElse(null);
        if (initial != null && max != null && initial > max) {
            throw new ValidationException(
                    quoted(FirestoreConnectorOptions.SINK_THROTTLING_INITIAL_OPS_PER_SECOND)
                            + " ("
                            + initial
                            + ") must not exceed "
                            + quoted(FirestoreConnectorOptions.SINK_THROTTLING_MAX_OPS_PER_SECOND)
                            + " ("
                            + max
                            + ").");
        }
    }

    /**
     * The sink builder's retry check, called with the option keys as the names: a maximum below its
     * initial value, which gax refuses, and overrides equal to gax's defaults, which the library
     * drops.
     */
    private static void rejectRetryOverridesTheLibraryRefuses(FirestoreWriterOptions options) {
        try {
            DefaultFirestoreDatabaseAccessFactory.retrySettings(
                    options,
                    quoted(FirestoreConnectorOptions.SINK_RETRY_INITIAL_DELAY),
                    quoted(FirestoreConnectorOptions.SINK_RETRY_MAX_DELAY),
                    quoted(FirestoreConnectorOptions.SINK_RETRY_INITIAL_RPC_TIMEOUT),
                    quoted(FirestoreConnectorOptions.SINK_RETRY_MAX_RPC_TIMEOUT),
                    "The 'sink.retry.*' options");
        } catch (IllegalArgumentException e) {
            throw new ValidationException(e.getMessage(), e);
        }
    }

    private static String quoted(ConfigOption<?> option) {
        return "'" + option.key() + "'";
    }
}
