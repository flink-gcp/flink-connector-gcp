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

package io.github.flink.gcp.connector.spanner.source.changestream.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Spanner;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerCredentials;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;

import javax.annotation.Nullable;

import java.time.Duration;

/** Opens the Spanner service handle used for change-stream metadata discovery. */
@Internal
public final class DefaultSpannerChangeStreamCoordinatorClientFactory
        implements SpannerChangeStreamCoordinatorClientFactory {

    private static final long serialVersionUID = 1L;
    private static final Duration DEFAULT_ABSENT_RETENTION_FALLBACK = Duration.ofDays(7);

    private final SpannerDatabase database;
    private final String changeStreamName;
    private final Duration absentRetentionFallback;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;
    @Nullable private final String serviceAccountKeyFile;

    public DefaultSpannerChangeStreamCoordinatorClientFactory(
            SpannerDatabase database,
            String changeStreamName,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(database, changeStreamName, DEFAULT_ABSENT_RETENTION_FALLBACK, emulatorEndpoint, null);
    }

    public DefaultSpannerChangeStreamCoordinatorClientFactory(
            SpannerDatabase database,
            String changeStreamName,
            Duration absentRetentionFallback,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this(database, changeStreamName, absentRetentionFallback, emulatorEndpoint, null);
    }

    public DefaultSpannerChangeStreamCoordinatorClientFactory(
            SpannerDatabase database,
            String changeStreamName,
            Duration absentRetentionFallback,
            @Nullable EmulatorEndpoint emulatorEndpoint,
            @Nullable String serviceAccountKeyFile) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        this.changeStreamName =
                Preconditions.checkNotNull(changeStreamName, "changeStreamName must not be null");
        Preconditions.checkArgument(
                !changeStreamName.isEmpty(), "changeStreamName must not be empty");
        this.absentRetentionFallback =
                Preconditions.checkNotNull(
                        absentRetentionFallback, "absentRetentionFallback must not be null");
        Preconditions.checkArgument(
                !absentRetentionFallback.isZero() && !absentRetentionFallback.isNegative(),
                "absentRetentionFallback must be positive, but was %s",
                absentRetentionFallback);
        this.emulatorEndpoint = emulatorEndpoint;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
    }

    @Override
    public SpannerChangeStreamCoordinatorClient create() throws Exception {
        return create(
                SpannerClients.open(
                        database,
                        emulatorEndpoint,
                        SpannerCredentials.load(serviceAccountKeyFile)));
    }

    @VisibleForTesting
    SpannerChangeStreamCoordinatorClient create(Spanner spanner) {
        try {
            DatabaseClient client =
                    spanner.getDatabaseClient(
                            DatabaseId.of(
                                    database.getProject(),
                                    database.getInstance(),
                                    database.getDatabase()));
            return new SpannerChangeStreamMetadataAdapter(
                    database.toString(),
                    changeStreamName,
                    absentRetentionFallback,
                    client::getDialect,
                    statement -> client.singleUse().executeQuery(statement),
                    spanner::close);
        } catch (Throwable e) {
            Closers.closeAllSuppressing(e, spanner::close);
            throw e;
        }
    }
}
