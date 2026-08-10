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

package io.github.flink.gcp.connector.spanner.sink.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Options;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Options.TransactionOption;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import io.github.flink.gcp.connector.base.lifecycle.Closers;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.spanner.SpannerClients;
import io.github.flink.gcp.connector.spanner.SpannerDatabase;
import io.github.flink.gcp.connector.spanner.sink.SpannerRpcPriority;
import io.github.flink.gcp.connector.spanner.sink.SpannerWriterOptions;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Opens a real Spanner service handle and wraps it in a {@link SpannerServiceAdapter}. */
@Internal
public final class DefaultSpannerDatabaseAccessFactory implements SpannerDatabaseAccessFactory {

    private static final long serialVersionUID = 1L;

    private final SpannerDatabase database;
    private final SpannerWriterOptions writerOptions;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the factory.
     *
     * @param database the database to write to
     * @param writerOptions the writer tuning options
     * @param emulatorEndpoint the emulator to write to, or {@code null} for the real service
     */
    public DefaultSpannerDatabaseAccessFactory(
            SpannerDatabase database,
            SpannerWriterOptions writerOptions,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.database = database;
        this.writerOptions = writerOptions;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    @Override
    public SpannerDatabaseAccess create() throws IOException {
        return create(SpannerClients.open(database, emulatorEndpoint));
    }

    @VisibleForTesting
    SpannerDatabaseAccess create(Spanner spanner) {
        try {
            DatabaseClient client =
                    spanner.getDatabaseClient(
                            DatabaseId.of(
                                    database.getProject(),
                                    database.getInstance(),
                                    database.getDatabase()));
            TransactionOption[] transactionOptions = transactionOptions(writerOptions);
            return new SpannerServiceAdapter(
                    database.toString(),
                    client::getDialect,
                    sql -> client.singleUse().executeQuery(Statement.of(sql)),
                    groups -> client.batchWriteAtLeastOnce(groups, transactionOptions),
                    spanner::close);
        } catch (Throwable e) {
            // The service handle is this factory's until the adapter takes it over; nothing else
            // would ever close it. Throwable rather than Exception for the same reason every
            // creation guard in this project uses it: a first classload can fail with an Error.
            Closers.closeAllSuppressing(e, spanner::close);
            throw e;
        }
    }

    /**
     * Maps the writer options onto the transaction options every batch write carries. Unset knobs
     * contribute nothing, leaving the service's own handling in place rather than restating it.
     */
    @VisibleForTesting
    static TransactionOption[] transactionOptions(SpannerWriterOptions options) {
        List<TransactionOption> transactionOptions = new ArrayList<>(2);
        if (options.getMaxCommitDelay() != null) {
            transactionOptions.add(Options.maxCommitDelay(options.getMaxCommitDelay()));
        }
        if (options.getRpcPriority() != null) {
            transactionOptions.add(Options.priority(toRpcPriority(options.getRpcPriority())));
        }
        return transactionOptions.toArray(new TransactionOption[0]);
    }

    /**
     * Maps this connector's priority onto the client library's.
     *
     * <p>Written out rather than matched by enum name so that a value added to either side fails to
     * compile here instead of silently changing what a job asked for.
     */
    private static RpcPriority toRpcPriority(SpannerRpcPriority priority) {
        switch (priority) {
            case LOW:
                return RpcPriority.LOW;
            case MEDIUM:
                return RpcPriority.MEDIUM;
            case HIGH:
                return RpcPriority.HIGH;
            default:
                throw new IllegalStateException("Unsupported RPC priority: " + priority);
        }
    }
}
