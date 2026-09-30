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

import org.apache.flink.annotation.Internal;

import io.github.flink.gcp.connector.base.failure.FailureHandler;
import io.github.flink.gcp.connector.base.rpc.EmulatorEndpoint;
import io.github.flink.gcp.connector.firestore.DatabaseDestination;
import io.github.flink.gcp.connector.firestore.sink.serializer.FirestoreWriteSerializationSchema;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The immutable configuration {@link FirestoreSinkBuilder} builds and the sink carries to the task
 * managers.
 *
 * @param <T> type of the records written by the sink
 */
@Internal
public final class FirestoreSinkConfig<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final DatabaseDestination database;
    private final FirestoreWriteSerializationSchema<? super T> serializer;
    private final FirestoreWriterOptions writerOptions;
    private final FailureHandler<? super FailedWrite> failedWriteHandler;
    private final PreconditionFailurePolicy preconditionFailurePolicy;
    @Nullable private final String serviceAccountKeyFile;
    @Nullable private final EmulatorEndpoint emulatorEndpoint;

    /**
     * Creates the configuration. The only caller is {@link FirestoreSinkBuilder#build()}, which has
     * already rejected a null for every one of these.
     */
    FirestoreSinkConfig(
            DatabaseDestination database,
            FirestoreWriteSerializationSchema<? super T> serializer,
            FirestoreWriterOptions writerOptions,
            FailureHandler<? super FailedWrite> failedWriteHandler,
            PreconditionFailurePolicy preconditionFailurePolicy,
            @Nullable String serviceAccountKeyFile,
            @Nullable EmulatorEndpoint emulatorEndpoint) {
        this.database = database;
        this.serializer = serializer;
        this.writerOptions = writerOptions;
        this.failedWriteHandler = failedWriteHandler;
        this.preconditionFailurePolicy = preconditionFailurePolicy;
        this.serviceAccountKeyFile = serviceAccountKeyFile;
        this.emulatorEndpoint = emulatorEndpoint;
    }

    /** Returns the database to write to. */
    public DatabaseDestination getDatabase() {
        return database;
    }

    /** Returns the serialization schema. */
    public FirestoreWriteSerializationSchema<? super T> getSerializer() {
        return serializer;
    }

    /** Returns the writer tuning options. */
    public FirestoreWriterOptions getWriterOptions() {
        return writerOptions;
    }

    /** Returns the policy applied to terminally failed writes. */
    public FailureHandler<? super FailedWrite> getFailedWriteHandler() {
        return failedWriteHandler;
    }

    /** Returns what happens to a conditional write whose precondition no longer holds. */
    public PreconditionFailurePolicy getPreconditionFailurePolicy() {
        return preconditionFailurePolicy;
    }

    /**
     * Returns the service-account key-file path, or {@code null} when no override is configured.
     */
    @Nullable
    public String getServiceAccountKeyFile() {
        return serviceAccountKeyFile;
    }

    /** Returns the emulator endpoint, or {@code null} when writing to the real service. */
    @Nullable
    public EmulatorEndpoint getEmulatorEndpoint() {
        return emulatorEndpoint;
    }
}
