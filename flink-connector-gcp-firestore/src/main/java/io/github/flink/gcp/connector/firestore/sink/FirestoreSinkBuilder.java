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

package io.github.flink.gcp.connector.firestore.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.util.Preconditions;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;
import io.github.flink.gcp.connector.firestore.sink.writer.DefaultFirestoreDatabaseAccessFactory;

import javax.annotation.Nullable;

/**
 * Builder for the Firestore sink; created through {@link FirestoreSink#builder()}.
 *
 * <p>{@link #database(DatabaseDestination)} and {@link
 * #serializer(FirestoreWriteSerializationSchema)} are required; everything else is defaulted.
 *
 * @param <T> type of the records written by the sink
 */
@PublicEvolving
public class FirestoreSinkBuilder<T> {

    @Nullable private DatabaseDestination database;
    @Nullable private FirestoreWriteSerializationSchema<? super T> serializer;
    private FirestoreWriterOptions writerOptions = FirestoreWriterOptions.defaults();
    private FailureHandler<? super FailedWrite> failedWriteHandler = FailureHandler.failJob();
    private PreconditionFailurePolicy preconditionFailurePolicy =
            PreconditionFailurePolicy.FAIL_JOB;
    @Nullable private String serviceAccountKeyFile;
    @Nullable private EmulatorEndpoint emulatorEndpoint;

    FirestoreSinkBuilder() {}

    /**
     * Sets the database to write to. Required.
     *
     * @param database the database
     * @return this builder
     */
    public FirestoreSinkBuilder<T> database(DatabaseDestination database) {
        this.database = Preconditions.checkNotNull(database, "database must not be null");
        return this;
    }

    /**
     * Sets the schema turning records into document writes. Required.
     *
     * @param serializer the serialization schema
     * @return this builder
     */
    public FirestoreSinkBuilder<T> serializer(
            FirestoreWriteSerializationSchema<? super T> serializer) {
        this.serializer = Preconditions.checkNotNull(serializer, "serializer must not be null");
        return this;
    }

    /**
     * Sets the writer's tuning options. Optional; defaults to {@link
     * FirestoreWriterOptions#defaults()}.
     *
     * @param writerOptions the writer options
     * @return this builder
     */
    public FirestoreSinkBuilder<T> writerOptions(FirestoreWriterOptions writerOptions) {
        this.writerOptions =
                Preconditions.checkNotNull(writerOptions, "writerOptions must not be null");
        return this;
    }

    /**
     * Sets what happens to a write the service terminally refused, or a record the serializer could
     * not turn into one. Optional; defaults to {@code FailureHandler.failJob()}.
     *
     * @param failedWriteHandler the failure handler
     * @return this builder
     */
    public FirestoreSinkBuilder<T> failedWriteHandler(
            FailureHandler<? super FailedWrite> failedWriteHandler) {
        this.failedWriteHandler =
                Preconditions.checkNotNull(
                        failedWriteHandler, "failedWriteHandler must not be null");
        return this;
    }

    /**
     * Sets what happens to a write whose {@code lastUpdateTime} precondition no longer holds.
     * Optional; defaults to {@link PreconditionFailurePolicy#FAIL_JOB}.
     *
     * <p>Under {@link PreconditionFailurePolicy#ROUTE_TO_FAILURE_HANDLER} such a write reaches
     * {@link #failedWriteHandler(FailureHandler)}, so that handler decides whether it fails the
     * job, is dropped or is dead-lettered.
     *
     * @param preconditionFailurePolicy the policy
     * @return this builder
     */
    public FirestoreSinkBuilder<T> preconditionFailurePolicy(
            PreconditionFailurePolicy preconditionFailurePolicy) {
        this.preconditionFailurePolicy =
                Preconditions.checkNotNull(
                        preconditionFailurePolicy, "preconditionFailurePolicy must not be null");
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
    public FirestoreSinkBuilder<T> serviceAccountKeyFile(String serviceAccountKeyFile) {
        String checked =
                Preconditions.checkNotNull(
                        serviceAccountKeyFile, "serviceAccountKeyFile must not be null");
        Preconditions.checkArgument(!checked.isBlank(), "serviceAccountKeyFile must not be blank");
        this.serviceAccountKeyFile = checked;
        return this;
    }

    /**
     * Points the sink at a Firestore emulator instead of the real service. Optional; for tests.
     *
     * <p>The emulator needs no credentials, so setting this also stops the client from looking for
     * any. This setting is the only way the sink reaches an emulator: the client library would also
     * read the {@code FIRESTORE_EMULATOR_HOST} environment variable on its own, and the writer logs
     * a warning when that variable is set and this setting is not.
     *
     * @param emulatorEndpoint the emulator's gRPC endpoint as {@code host:port}
     * @return this builder
     * @throws IllegalArgumentException if the endpoint is not {@code host:port} with a port in
     *     1..65535
     */
    public FirestoreSinkBuilder<T> emulatorEndpoint(String emulatorEndpoint) {
        this.emulatorEndpoint = EmulatorEndpoint.parse(emulatorEndpoint, "emulatorEndpoint");
        return this;
    }

    /**
     * Builds the sink.
     *
     * @return the sink
     * @throws IllegalStateException if a required option is missing
     * @throws IllegalArgumentException if a writer options' {@code retry*} maximum is shorter than
     *     its initial value, set or the library's, or the overrides add up to gax's default retry
     *     settings, which the client library would treat as unset
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
        // Refuses retry overrides the client library would drop, before the job is submitted.
        DefaultFirestoreDatabaseAccessFactory.retrySettings(writerOptions);
        return new FirestoreBulkWriterSink<>(
                new FirestoreSinkConfig<>(
                        database,
                        serializer,
                        writerOptions,
                        failedWriteHandler,
                        preconditionFailurePolicy,
                        serviceAccountKeyFile,
                        emulatorEndpoint));
    }
}
