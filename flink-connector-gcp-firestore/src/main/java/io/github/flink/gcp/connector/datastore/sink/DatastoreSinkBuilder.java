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

package io.github.flink.gcp.connector.datastore.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.datastore.DatabaseDestination;
import io.github.flink.gcp.connector.datastore.sink.serializer.DatastoreMutationSerializationSchema;

import javax.annotation.Nullable;

/**
 * Builder for the Datastore sink; created through {@link DatastoreSink#builder()}.
 *
 * <p>{@link #database(DatabaseDestination)} and {@link
 * #serializer(DatastoreMutationSerializationSchema)} are required; everything else is defaulted.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public class DatastoreSinkBuilder<T> {

    @Nullable private DatabaseDestination database;
    @Nullable private DatastoreMutationSerializationSchema<? super T> serializer;
    private DatastoreWriterOptions writerOptions = DatastoreWriterOptions.defaults();
    private FailureHandler<? super FailedMutation> failedMutationHandler = FailureHandler.failJob();
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;

    DatastoreSinkBuilder() {}

    /**
     * Sets the database to write to. Required.
     *
     * <p>Every write's key must address this database: its project and database id must be this
     * database's, or the write is refused before it is sent and routed to {@link
     * #failedMutationHandler(FailureHandler)}.
     *
     * @param database the database
     * @return this builder
     */
    public DatastoreSinkBuilder<T> database(DatabaseDestination database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets the schema turning records into entity writes. Required.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public DatastoreSinkBuilder<T> serializer(
            DatastoreMutationSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Sets the writer's tuning options. Optional; defaults to {@link
     * DatastoreWriterOptions#defaults()}.
     *
     * @param writerOptions the writer options
     * @return this builder
     */
    public DatastoreSinkBuilder<T> writerOptions(DatastoreWriterOptions writerOptions) {
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        return this;
    }

    /**
     * Sets what happens to a mutation the service terminally refused, or a record the serializer
     * could not turn into one. Optional; defaults to {@code FailureHandler.failJob()}.
     *
     * @param failedMutationHandler the failure handler
     * @return this builder
     */
    public DatastoreSinkBuilder<T> failedMutationHandler(
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
     * unset the real-service path uses application-default credentials.
     *
     * <p>Service-account keys are long-lived secrets. Prefer an attached service account or
     * Workload Identity where the deployment supports one. This setting cannot be combined with
     * {@link #emulatorEndpoint(String)}, whose plaintext channel carries no credentials.
     *
     * @param serviceAccountKeyFile the service-account JSON key-file path
     * @return this builder
     */
    public DatastoreSinkBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the sink at a Firestore emulator running in Datastore mode instead of the real
     * service. Optional; for tests.
     *
     * <p>The emulator needs no credentials, so setting this also stops the client from looking for
     * any. This setting is the only way the sink reaches an emulator: the {@code
     * DATASTORE_EMULATOR_HOST} environment variable the client library would otherwise read is
     * never consulted.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public DatastoreSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     * @throws IllegalStateException if a required option is missing
     */
    public Sink<T> build() {
        Preconditions.checkState(
                database != null, "A database is required. Set it with database(...).");
        Preconditions.checkState(
                serializer != null, "A serializer is required. Set it with serializer(...).");
        Preconditions.checkState(
                serviceAccountKeyFile == null || emulatorEndpoint == null,
                "serviceAccountKeyFile(...) cannot be combined with emulatorEndpoint(...): an"
                        + " emulator uses a plaintext channel with no credentials. Remove one of"
                        + " the two settings.");
        return new DatastoreCommitSink<>(
                new DatastoreSinkConfig<>(
                        database,
                        serializer,
                        writerOptions,
                        failedMutationHandler,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }
}
