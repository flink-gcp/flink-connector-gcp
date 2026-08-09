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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.bigtable.TableDestination;
import io.github.flink.gcp.connector.bigtable.sink.serializer.BigtableSerializationSchema;

import javax.annotation.Nullable;

/**
 * Builder for Bigtable sinks, obtained from {@link BigtableSink#builder()}.
 *
 * <p>Required settings: a table and a serialization schema.
 *
 * <p>The table is fixed for the sink's lifetime, unlike the Pub/Sub and BigQuery sinks' per-record
 * destinations: the client's bulk mutation batcher is bound to one table, so per-record tables
 * would mean a pool of batchers and a share of the in-flight budget for each. That is deferred
 * until there is a use case for it. By default the sink never creates the table either — the table
 * and its column families must exist; {@link #createDisposition(CreateDisposition)} with {@link
 * CreateDisposition#CREATE_IF_NEEDED} and {@link #tableCreateOptions(TableCreateOptions)} opts into
 * creating them.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public class BigtableSinkBuilder<T> {

    private TableDestination destination;
    private BigtableSerializationSchema<? super T> serializer;
    @Nullable private String appProfileId;
    private BigtableWriterOptions writerOptions = BigtableWriterOptions.defaults();
    private FailureHandler<? super FailedMutation> failedMutationHandler = FailureHandler.failJob();
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    private CreateDisposition createDisposition = CreateDisposition.CREATE_NEVER;
    @Nullable private TableCreateOptions tableCreateOptions;

    BigtableSinkBuilder() {}

    /**
     * Writes every mutation to the given table.
     *
     * @param table the destination table
     * @return this builder
     */
    public BigtableSinkBuilder<T> table(TableDestination table) {
        this.destination = Preconditions.checkNotNull(table, "table must not be null");
        return this;
    }

    /**
     * Sets the record serialization schema.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public BigtableSinkBuilder<T> serializer(BigtableSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Routes the client's requests through the given application profile, which is what selects an
     * instance's routing policy and its priority. Optional; when unset the instance's default
     * profile applies.
     *
     * <p>It is a sink option rather than part of {@link TableDestination} because it chooses a path
     * to the data, not the data's address.
     *
     * @param appProfileId the application profile id
     * @return this builder
     */
    public BigtableSinkBuilder<T> appProfileId(String appProfileId) {
        Preconditions.checkNotNull(appProfileId, "appProfileId must not be null");
        Preconditions.checkArgument(
                !appProfileId.trim().isEmpty(), "appProfileId must not be blank");
        this.appProfileId = appProfileId;
        return this;
    }

    /**
     * Sets the writer tuning options (the batch thresholds and the in-flight bounds). Optional;
     * defaults to {@link BigtableWriterOptions#defaults()}.
     *
     * @param writerOptions the options
     * @return this builder
     */
    public BigtableSinkBuilder<T> writerOptions(BigtableWriterOptions writerOptions) {
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        return this;
    }

    /**
     * Sets the policy for mutations that terminally fail — a record the serializer rejects, and a
     * mutation Bigtable rejects as malformed or oversized. Defaults to {@link
     * FailureHandler#failJob()}; transient failures never reach it, since the client retries them
     * and an exhausted retry fails the job.
     *
     * @param failedMutationHandler the handler
     * @return this builder
     */
    public BigtableSinkBuilder<T> failedMutationHandler(
            FailureHandler<? super FailedMutation> failedMutationHandler) {
        this.failedMutationHandler =
                Preconditions.checkNotNull(
                        failedMutationHandler, "failedMutationHandler must not be null");
        return this;
    }

    /**
     * Points the sink at a Bigtable emulator instead of the production service. The connection to
     * the given {@code host:port} uses a plaintext channel with no credentials, so this must only
     * ever be used against an emulator. Optional; when unset the sink connects to Bigtable with
     * application-default credentials.
     *
     * <p>The value is parsed here, so a malformed {@code host:port} is rejected on the client
     * instead of surfacing as a connection failure once the job has been deployed.
     *
     * @param emulatorEndpoint the emulator endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public BigtableSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint);
        return this;
    }

    /**
     * Sets whether the sink may create the destination table when a mutation finds it — or one of
     * its column families — missing. Defaults to {@link CreateDisposition#CREATE_NEVER}, under
     * which the table and its families must exist.
     *
     * <p>{@link CreateDisposition#CREATE_IF_NEEDED} requires {@link
     * #tableCreateOptions(TableCreateOptions)}: a Bigtable table's schema is its column families
     * and their garbage-collection policies, which the sink cannot guess.
     *
     * @param createDisposition the disposition
     * @return this builder
     */
    public BigtableSinkBuilder<T> createDisposition(CreateDisposition createDisposition) {
        this.createDisposition =
                Preconditions.checkNotNull(createDisposition, "createDisposition must not be null");
        return this;
    }

    /**
     * Sets the column families — and, per family, an optional garbage-collection rule — for the
     * table the sink creates under {@link CreateDisposition#CREATE_IF_NEEDED}. Creation only: an
     * existing table is used as it is, except that families declared here which it lacks are added.
     *
     * @param tableCreateOptions the creation settings
     * @return this builder
     */
    public BigtableSinkBuilder<T> tableCreateOptions(TableCreateOptions tableCreateOptions) {
        this.tableCreateOptions =
                Preconditions.checkNotNull(
                        tableCreateOptions, "tableCreateOptions must not be null");
        return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     */
    public Sink<T> build() {
        Preconditions.checkState(destination != null, "A table is required: set table(...).");
        Preconditions.checkState(serializer != null, "A serializer is required.");
        Preconditions.checkState(
                tableCreateOptions == null || createDisposition != CreateDisposition.CREATE_NEVER,
                "tableCreateOptions(...) configures a table the sink creates, but"
                        + " createDisposition(CREATE_NEVER) never creates one. Remove the options"
                        + " or use CREATE_IF_NEEDED.");
        Preconditions.checkState(
                createDisposition != CreateDisposition.CREATE_IF_NEEDED
                        || tableCreateOptions != null,
                "createDisposition(CREATE_IF_NEEDED) creates the table from its creation"
                        + " settings, but none are set. A Bigtable table's schema is its column"
                        + " families, which the sink cannot guess: set tableCreateOptions(...)"
                        + " naming them.");
        return new BigtableMutateRowsSink<>(
                new BigtableSinkConfig<>(
                        destination,
                        serializer,
                        appProfileId,
                        writerOptions,
                        failedMutationHandler,
                        emulatorEndpoint,
                        createDisposition,
                        tableCreateOptions));
    }
}
