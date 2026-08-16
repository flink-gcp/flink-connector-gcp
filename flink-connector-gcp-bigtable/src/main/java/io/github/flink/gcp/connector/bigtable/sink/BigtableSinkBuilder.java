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

package io.github.flink.gcp.connector.bigtable.sink;

import org.apache.flink.annotation.Public;
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
 * <p>Required settings: a destination — {@link #table(TableDestination)} for one fixed table, or
 * {@link #destinationResolver(DestinationResolver)} to route per record — and a serialization
 * schema.
 *
 * <p>By default the sink creates no table: every table it writes to, and the column families the
 * mutations name, must exist. {@link #createDisposition(CreateDisposition)} with {@link
 * CreateDisposition#CREATE_IF_NEEDED} and {@link #tableCreateOptions(TableCreateOptions)} opts into
 * creating them, from one schema that serves every table the sink creates.
 *
 * @param <T> type of the records written by the sink
 */
@Public
public class BigtableSinkBuilder<T> {

    private DestinationResolver<? super T> destinationResolver;
    private BigtableSerializationSchema<? super T> serializer;
    @Nullable private String appProfileId;
    private BigtableWriterOptions writerOptions = BigtableWriterOptions.defaults();
    private FailureHandler<? super FailedMutation> failedMutationHandler = FailureHandler.failJob();
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;
    private CreateDisposition createDisposition = CreateDisposition.CREATE_NEVER;
    @Nullable private TableCreateOptions tableCreateOptions;

    BigtableSinkBuilder() {}

    /**
     * Writes every mutation to the given table. Sugar for a {@link DestinationResolver} returning
     * that table for every record; this and {@link #destinationResolver(DestinationResolver)} set
     * the same field, so the last call wins.
     *
     * @param table the destination table
     * @return this builder
     */
    public BigtableSinkBuilder<T> table(TableDestination table) {
        this.destinationResolver =
                new FixedDestinationResolver(
                        Preconditions.checkNotNull(table, "table must not be null"));
        return this;
    }

    /**
     * Resolves the destination table per record, so one sink writes to many tables. The resolver
     * runs before the serializer, and its result is what a failed mutation is reported against.
     *
     * <p>This and {@link #table(TableDestination)} set the same field, so the last call wins.
     *
     * <p>Each distinct table costs a bulk mutation batcher of its own, and beside {@link
     * CreateDisposition#CREATE_IF_NEEDED} each unseen table is created from the one {@link
     * #tableCreateOptions(TableCreateOptions)} schema — so a resolver's cardinality decides what
     * the sink holds, and what it may create.
     *
     * @param destinationResolver the resolver
     * @return this builder
     */
    public BigtableSinkBuilder<T> destinationResolver(
            DestinationResolver<? super T> destinationResolver) {
        this.destinationResolver =
                Preconditions.checkNotNull(
                        destinationResolver, "destinationResolver must not be null");
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
     * Authenticates the sink with the service-account JSON key at the given path instead of
     * application-default credentials. The file is read on each TaskManager when its writer is
     * created, so every TaskManager that can run the sink must see the same path. Optional; when
     * unset the sink uses application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel carries no credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public BigtableSinkBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
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
        Preconditions.checkState(
                destinationResolver != null,
                "A destination is required: set table(...) or destinationResolver(...).");
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
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        return new BigtableMutateRowsSink<>(
                new BigtableSinkConfig<>(
                        destinationResolver,
                        serializer,
                        appProfileId,
                        writerOptions,
                        failedMutationHandler,
                        serviceAccountKeyFile,
                        emulatorEndpoint,
                        createDisposition,
                        tableCreateOptions));
    }
}
