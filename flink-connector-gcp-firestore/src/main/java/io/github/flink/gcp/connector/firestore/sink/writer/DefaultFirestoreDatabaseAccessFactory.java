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

package io.github.flink.gcp.connector.firestore.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.Credentials;
import com.google.cloud.ServiceOptions;
import com.google.cloud.firestore.BulkWriter;
import com.google.cloud.firestore.BulkWriterOptions;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import com.google.cloud.firestore.v1.FirestoreSettings;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.FirestoreClients;
import io.github.flink.gcp.connector.firestore.sink.FirestoreWriterOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Opens the production {@link FirestoreDatabaseAccess}: a Firestore client for the configured
 * database and one single-thread executor every {@code BulkWriter} of the access runs on.
 *
 * <p>The executor is the access's own rather than the library's, for two reasons. A {@code
 * BulkWriter} given an executor owns no threads, so one that is replaced can simply be dropped —
 * replacing is how the writer recovers from the library's pending-operation leak — and one that is
 * closed does not have to be: {@code BulkWriter.close()} flushes and waits without a bound, which a
 * failing writer must not do. And shutting that one executor down in {@link
 * FirestoreDatabaseAccess#close()} cancels every batch send and backoff retry still scheduled on
 * it. One thread is enough because the library runs its listeners under a single internal lock
 * anyway.
 */
@Internal
public final class DefaultFirestoreDatabaseAccessFactory implements FirestoreDatabaseAccessFactory {

    /**
     * The attempts the client library gives every call when it is left at its default retry
     * settings: {@code GrpcFirestoreRpc} (google-cloud-firestore 3.46.0) sets five on top of the
     * generated call settings, which bound {@code BatchWrite} by time alone. Retry overrides start
     * from the generated {@code BatchWrite} settings with this count, so an unset knob keeps what
     * the library does without any.
     */
    @VisibleForTesting static final int LIBRARY_DEFAULT_MAX_ATTEMPTS = 5;

    private final DatabaseDestination database;
    private final FirestoreWriterOptions writerOptions;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final Credentials credentials;

    /**
     * Creates the factory.
     *
     * @param database the database to write to
     * @param writerOptions the writer options the {@code BulkWriter}s are configured from
     * @param emulatorEndpoint the emulator to reach, or {@code null} for the real service
     * @param credentials credentials loaded from a configured key file, or {@code null} for ADC
     */
    public DefaultFirestoreDatabaseAccessFactory(
            DatabaseDestination database,
            FirestoreWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable Credentials credentials) {
        this.database = database;
        this.writerOptions = writerOptions;
        this.emulatorEndpoint = emulatorEndpoint;
        this.credentials = credentials;
    }

    @Override
    public FirestoreDatabaseAccess create(BulkWriterRetryPolicy retryPolicy) throws IOException {
        Firestore firestore = FirestoreClients.open(database, clientSettings());
        ScheduledExecutorService executor = null;
        try {
            executor =
                    Executors.newSingleThreadScheduledExecutor(
                            runnable -> {
                                Thread thread =
                                        new Thread(runnable, "firestore-bulk-writer-" + database);
                                thread.setDaemon(true);
                                return thread;
                            });
            ScheduledExecutorService bulkWriterExecutor = executor;
            BulkWriterOptions options = bulkWriterOptions(writerOptions, bulkWriterExecutor);
            return new BulkWriterDatabaseAccess(
                    firestore, bulkWriterExecutor, options, retryPolicy);
        } catch (Throwable e) {
            ScheduledExecutorService created = executor;
            Closers.closeAllSuppressing(
                    e, created == null ? null : created::shutdownNow, firestore);
            throw e;
        }
    }

    /**
     * Maps the writer options onto the library's, leaving an unset rate to its default.
     *
     * <p>The rates are checked again here, because a Java-deserialized options object never passed
     * through its builder, and a rate below the library's batch size makes the library spin on its
     * first full batch until the ramp-up passes it, or forever.
     */
    @VisibleForTesting
    static BulkWriterOptions bulkWriterOptions(
            FirestoreWriterOptions writerOptions, ScheduledExecutorService executor) {
        checkRate(writerOptions.getInitialOpsPerSecond(), "initialOpsPerSecond");
        checkRate(writerOptions.getMaxOpsPerSecond(), "maxOpsPerSecond");
        BulkWriterOptions.Builder options =
                BulkWriterOptions.builder()
                        .setThrottlingEnabled(writerOptions.isThrottlingEnabled())
                        .setExecutor(executor);
        if (writerOptions.getInitialOpsPerSecond() != null) {
            options.setInitialOpsPerSecond(writerOptions.getInitialOpsPerSecond());
        }
        if (writerOptions.getMaxOpsPerSecond() != null) {
            options.setMaxOpsPerSecond(writerOptions.getMaxOpsPerSecond());
        }
        return options.build();
    }

    /** The client's settings: the database, endpoint and credentials, and any retry overrides. */
    @VisibleForTesting
    FirestoreOptions clientSettings() {
        return FirestoreClients.settings(
                database, emulatorEndpoint, credentials, retrySettings(writerOptions));
    }

    /**
     * Builds the transport retry settings: the library's {@code BatchWrite} settings overlaid with
     * the set {@code retry*} knobs, or {@code null} when none is set, which leaves the library's
     * own settings in place.
     *
     * <p>An override is checked against the value it is paired with, set or the library's: gax
     * refuses a maximum retry delay shorter than the initial one, and a maximum attempt timeout
     * shorter than the initial one, and this names the options instead. The library applies
     * settings equal to gax's {@code ServiceOptions} defaults as if none were given, so overrides
     * that land exactly on those are refused rather than silently dropped. The sink's builder calls
     * this too, so both refusals happen before the job is submitted.
     *
     * @param writerOptions the writer options
     * @return the settings, or {@code null} to keep the library's
     * @throws IllegalArgumentException if a maximum is shorter than its initial value, or the
     *     overrides equal gax's defaults
     */
    @Nullable
    public static RetrySettings retrySettings(FirestoreWriterOptions writerOptions) {
        if (writerOptions.getRetryTotalTimeout() == null
                && writerOptions.getRetryInitialDelay() == null
                && writerOptions.getRetryDelayMultiplier() == null
                && writerOptions.getRetryMaxDelay() == null
                && writerOptions.getRetryInitialRpcTimeout() == null
                && writerOptions.getRetryRpcTimeoutMultiplier() == null
                && writerOptions.getRetryMaxRpcTimeout() == null
                && writerOptions.getRetryMaxAttempts() == null) {
            return null;
        }
        RetrySettings.Builder retry =
                FirestoreSettings.newBuilder().batchWriteSettings().getRetrySettings().toBuilder()
                        .setMaxAttempts(LIBRARY_DEFAULT_MAX_ATTEMPTS);
        if (writerOptions.getRetryTotalTimeout() != null) {
            retry.setTotalTimeoutDuration(writerOptions.getRetryTotalTimeout());
        }
        if (writerOptions.getRetryInitialDelay() != null) {
            retry.setInitialRetryDelayDuration(writerOptions.getRetryInitialDelay());
        }
        if (writerOptions.getRetryDelayMultiplier() != null) {
            retry.setRetryDelayMultiplier(writerOptions.getRetryDelayMultiplier());
        }
        if (writerOptions.getRetryMaxDelay() != null) {
            retry.setMaxRetryDelayDuration(writerOptions.getRetryMaxDelay());
        }
        if (writerOptions.getRetryInitialRpcTimeout() != null) {
            retry.setInitialRpcTimeoutDuration(writerOptions.getRetryInitialRpcTimeout());
        }
        if (writerOptions.getRetryRpcTimeoutMultiplier() != null) {
            retry.setRpcTimeoutMultiplier(writerOptions.getRetryRpcTimeoutMultiplier());
        }
        if (writerOptions.getRetryMaxRpcTimeout() != null) {
            retry.setMaxRpcTimeoutDuration(writerOptions.getRetryMaxRpcTimeout());
        }
        if (writerOptions.getRetryMaxAttempts() != null) {
            retry.setMaxAttempts(writerOptions.getRetryMaxAttempts());
        }
        checkPair(
                retry.getMaxRetryDelayDuration(),
                writerOptions.getRetryMaxDelay() != null,
                "retryMaxDelay",
                retry.getInitialRetryDelayDuration(),
                writerOptions.getRetryInitialDelay() != null,
                "retryInitialDelay");
        checkPair(
                retry.getMaxRpcTimeoutDuration(),
                writerOptions.getRetryMaxRpcTimeout() != null,
                "retryMaxRpcTimeout",
                retry.getInitialRpcTimeoutDuration(),
                writerOptions.getRetryInitialRpcTimeout() != null,
                "retryInitialRpcTimeout");
        RetrySettings settings = retry.build();
        if (settings.equals(ServiceOptions.getDefaultRetrySettings())) {
            throw new IllegalArgumentException(
                    "The retry* options add up to gax's default retry settings, "
                            + settings
                            + ", which the Firestore client library treats as unset and replaces"
                            + " with its own BatchWrite settings. Change one of them, or unset them"
                            + " all to use the library's settings.");
        }
        return settings;
    }

    private static void checkPair(
            Duration max,
            boolean maxSet,
            String maxName,
            Duration initial,
            boolean initialSet,
            String initialName) {
        if (max.compareTo(initial) < 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s (%s%s) must not be shorter than %s (%s%s). Raise the first or lower the"
                                    + " second.",
                            maxName,
                            max,
                            maxSet ? "" : ", the library's value because it is unset",
                            initialName,
                            initial,
                            initialSet ? "" : ", the library's value because it is unset"));
        }
    }

    private static void checkRate(@Nullable Integer rate, String name) {
        if (rate != null && rate < BulkWriter.MAX_BATCH_SIZE) {
            throw new IllegalArgumentException(
                    name + " must be at least " + BulkWriter.MAX_BATCH_SIZE + ", was " + rate);
        }
    }
}
